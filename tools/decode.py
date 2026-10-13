#!/usr/bin/env python3
"""Decode a log_pump v1 stream to JSON Lines using the matching dictionary."""
import argparse
import json
import math
from pathlib import Path
import struct
import sys
import zlib

from schema import TYPES, digest, load

class DecodeError(ValueError):
    pass

def json_value(value):
    if isinstance(value, float) and not math.isfinite(value):
        return "nan" if math.isnan(value) else ("+inf" if value > 0 else "-inf")
    return value

def decode(stream, schema, allow_truncated_tail=False):
    header = stream.read(44)
    if len(header) != 44:
        raise DecodeError("missing or truncated stream header")
    magic, version, size, schema_hash = struct.unpack("<8sHH32s", header)
    if magic != b"LOGPUMP\0" or version != 1 or size != 44:
        raise DecodeError("unsupported stream header")
    if schema_hash.hex() != digest(schema):
        raise DecodeError("schema hash mismatch; supply the exact dictionary")
    records = {record["id"]: record for record in schema["records"]}
    while True:
        block = stream.read(12)
        if not block:
            return
        if len(block) != 12:
            if allow_truncated_tail: return
            raise DecodeError("truncated block header")
        magic, length, checksum = struct.unpack("<4sII", block)
        if magic != b"LPB1" or not 0 < length <= 1024 * 1024:
            raise DecodeError("invalid block header")
        data = stream.read(length)
        if len(data) != length:
            if allow_truncated_tail: return
            raise DecodeError("truncated block body")
        if zlib.crc32(data) != checksum:
            raise DecodeError("block checksum mismatch")
        offset = 0
        while offset < len(data):
            if len(data) - offset < 16:
                raise DecodeError("incomplete record header inside block")
            rid, payload_size, timestamp_us, sequence = struct.unpack_from("<HHQI", data, offset)
            offset += 16
            if payload_size > len(data) - offset:
                raise DecodeError("record payload exceeds block")
            payload = data[offset:offset + payload_size]
            offset += payload_size
            result = {"type_id": rid, "timestamp_us": timestamp_us, "sequence": sequence}
            record = records.get(rid)
            if record is None:
                result.update(name="unknown", payload_hex=payload.hex())
                yield result
                continue
            expected_size = sum(TYPES[f["type"]][2] * f["count"] for f in record["fields"])
            if len(payload) != expected_size:
                raise DecodeError(f"payload size mismatch for {record['name']}")
            values, position = {}, 0
            for field in record["fields"]:
                _, code, width = TYPES[field["type"]]
                decoded = struct.unpack_from("<" + code * field["count"], payload, position)
                position += width * field["count"]
                values[field["name"]] = json_value(decoded[0]) if field["count"] == 1 else [json_value(v) for v in decoded]
            result.update(name=record["name"], values=values)
            yield result

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("dictionary", help="source YAML or generated schema.json")
    parser.add_argument("log")
    parser.add_argument("--allow-truncated-tail", action="store_true", help="ignore an incomplete final block only")
    args = parser.parse_args()
    try:
        schema = load(args.dictionary)
        with Path(args.log).open("rb") as stream:
            for record in decode(stream, schema, args.allow_truncated_tail):
                print(json.dumps(record, allow_nan=False))
    except (ValueError, TypeError, OSError, struct.error) as exc:
        parser.exit(2, f"decode failed: {exc}\n")

if __name__ == "__main__":
    main()
