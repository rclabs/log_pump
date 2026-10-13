import copy
import io
import json
import os
from pathlib import Path
import struct
import subprocess
import sys
import tempfile
import unittest
import zlib

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tools"))
from schema import TYPES, digest, load, validate
from generate import generate
from decode import DecodeError, decode

SCHEMA = {"schema_version": 1, "namespace": "test_records", "records": [
    {"id": 1, "name": "TestObservation", "fields": [{"name": "value", "type": "float32"}]}]}

def stream(schema, body):
    return (struct.pack("<8sHH32s", b"LOGPUMP\0", 1, 44, bytes.fromhex(digest(schema))) +
            struct.pack("<4sII", b"LPB1", len(body), zlib.crc32(body)) + body)

class ToolsTests(unittest.TestCase):
    def setUp(self):
        self.schema = validate(copy.deepcopy(SCHEMA))
        self.body = struct.pack("<HHQIf", 1, 4, 123, 7, 1.5)

    def test_roundtrip(self):
        records = list(decode(io.BytesIO(stream(self.schema, self.body)), self.schema))
        self.assertEqual(records[0]["values"], {"value": 1.5})
        self.assertEqual(records[0]["timestamp_us"], 123)
        self.assertEqual(records[0]["sequence"], 7)

    def test_corruption_wrong_schema_and_truncation(self):
        data = stream(self.schema, self.body)
        for invalid in (data[:-1], data[:10], data[:-1] + b"x"):
            with self.assertRaises(DecodeError):
                list(decode(io.BytesIO(invalid), self.schema))
        changed = copy.deepcopy(self.schema)
        changed["records"][0]["fields"][0]["units"] = "different"
        with self.assertRaises(DecodeError):
            list(decode(io.BytesIO(data), changed))
        self.assertEqual(list(decode(io.BytesIO(data[:-1]), self.schema, True)), [])
        complete_then_partial = data + b"LP"
        self.assertEqual(len(list(decode(io.BytesIO(complete_then_partial), self.schema, True))), 1)

    def test_invalid_length_unknown_record_and_multiple_records(self):
        with self.assertRaises(DecodeError):
            list(decode(io.BytesIO(stream(self.schema, struct.pack("<HHQI", 1, 3, 0, 0) + b"abc")), self.schema))
        unknown = struct.pack("<HHQI", 99, 2, 0, 0) + b"ab"
        records = list(decode(io.BytesIO(stream(self.schema, self.body + unknown)), self.schema))
        self.assertEqual(records[1]["payload_hex"], "6162")
        with self.assertRaises(DecodeError):
            list(decode(io.BytesIO(stream(self.schema, self.body + b"x")), self.schema))

    def test_nonfinite_values_and_block_bounds(self):
        body = struct.pack("<HHQIf", 1, 4, 0, 0, float("nan"))
        records = list(decode(io.BytesIO(stream(self.schema, body)), self.schema))
        self.assertEqual(records[0]["values"]["value"], "nan")
        header = stream(self.schema, self.body)[:44]
        for length in (0, 1024 * 1024 + 1):
            with self.assertRaises(DecodeError):
                list(decode(io.BytesIO(header + struct.pack("<4sII", b"LPB1", length, 0)), self.schema))

    def test_schema_validation(self):
        for change in (
            lambda s: s["records"].append(copy.deepcopy(s["records"][0])),
            lambda s: s["records"][0].update(name="class"),
            lambda s: s["records"][0]["fields"][0].update(name="encode"),
            lambda s: s["records"][0]["fields"][0].update(count=0),
            lambda s: s["records"][0]["fields"][0].update(type="string"),
            lambda s: s["records"][0]["fields"][0].update(count=65535),
        ):
            schema = copy.deepcopy(SCHEMA)
            change(schema)
            with self.assertRaises(ValueError): validate(schema)

    def test_yaml_duplicate_keys_and_identity(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "schema.yaml"
            path.write_text("schema_version: 1\nschema_version: 1\n")
            with self.assertRaises(ValueError): load(path)
            path.write_text(json.dumps(SCHEMA)) # JSON is valid YAML.
            self.assertEqual(digest(load(path)), digest(self.schema))
        self.assertEqual(generate(self.schema), generate(copy.deepcopy(self.schema)))

    def test_generated_cpp_all_types(self):
        schema = {"schema_version": 1, "namespace": "generated_test", "records": [
            {"id": 1, "name": "AllTypes", "fields": [
                {"name": "field" + str(i), "type": typ, "count": 2} for i, typ in enumerate(TYPES)]}]}
        schema = validate(schema)
        assignments, expected = [], {}
        for i, typ in enumerate(TYPES):
            values = [1.5, -2.5] if typ.startswith("float") else ([-1, -2] if typ.startswith("int") else [1, 2])
            assignments.append(f"v.field{i} = {{{values[0]}, {values[1]}}};")
            expected["field" + str(i)] = values
        with tempfile.TemporaryDirectory() as tmp:
            tmp = Path(tmp)
            (tmp / "observations.hpp").write_text(generate(schema))
            (tmp / "main.cpp").write_text('''#include "observations.hpp"
#include <cstdio>
int main() {
    generated_test::AllTypes v{};
    ''' + "\n".join(assignments) + '''
    std::array<std::uint8_t, generated_test::AllTypes::encoded_size> bytes{};
    v.encode(bytes.data());
    return std::fwrite(bytes.data(), 1, bytes.size(), stdout) == bytes.size() ? 0 : 1;
}
''')
            subprocess.run([os.environ.get("CXX", "c++"), "-std=c++17", "-Wall", "-Wextra", "-Werror",
                            "-I" + str(ROOT / "include"), str(tmp / "main.cpp"), "-o", str(tmp / "test")], check=True)
            payload = subprocess.check_output([str(tmp / "test")])
        body = struct.pack("<HHQI", 1, len(payload), 0, 0) + payload
        records = list(decode(io.BytesIO(stream(schema, body)), schema))
        self.assertEqual(records[0]["values"], expected)

if __name__ == "__main__":
    unittest.main()
