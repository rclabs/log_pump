"""Shared, domain-independent schema validation and identity."""
import hashlib
import json
import re
from pathlib import Path

import yaml

TYPES = {
    "uint8": ("std::uint8_t", "B", 1), "int8": ("std::int8_t", "b", 1),
    "uint16": ("std::uint16_t", "H", 2), "int16": ("std::int16_t", "h", 2),
    "uint32": ("std::uint32_t", "I", 4), "int32": ("std::int32_t", "i", 4),
    "uint64": ("std::uint64_t", "Q", 8), "int64": ("std::int64_t", "q", 8),
    "float32": ("float", "f", 4), "float64": ("double", "d", 8),
}
CPP_RESERVED = set("alignas alignof and and_eq asm auto bitand bitor bool break case catch char char16_t char32_t class compl const constexpr const_cast continue decltype default delete do double dynamic_cast else enum explicit export extern false float for friend goto if inline int long mutable namespace new noexcept not not_eq nullptr operator or or_eq private protected public register reinterpret_cast return short signed sizeof static static_assert static_cast struct switch template this thread_local throw true try typedef typeid typename union unsigned using virtual void volatile wchar_t while xor xor_eq".split())

def identifier(value):
    if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z][A-Za-z0-9_]*", value) or "__" in value or value in CPP_RESERVED:
        raise ValueError(f"invalid C++ identifier: {value!r}")
    return value

class UniqueLoader(yaml.SafeLoader):
    pass

def unique_mapping(loader, node, deep=False):
    result = {}
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node, deep=deep)
        if key in result:
            raise ValueError(f"duplicate YAML key: {key}")
        result[key] = loader.construct_object(value_node, deep=deep)
    return result

UniqueLoader.add_constructor(yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, unique_mapping)

def validate(schema):
    if not isinstance(schema, dict) or type(schema.get("schema_version")) is not int or schema.get("schema_version") != 1:
        raise ValueError("schema_version must be 1")
    if set(schema) - {"schema_version", "namespace", "records"}:
        raise ValueError("unknown schema keys")
    identifier(schema.get("namespace"))
    records = schema.get("records")
    if not isinstance(records, list) or not records:
        raise ValueError("records must be a nonempty list")
    ids, names = set(), set()
    for record in records:
        if not isinstance(record, dict) or set(record) - {"id", "name", "category", "description", "fields"}:
            raise ValueError("invalid record keys")
        rid = record.get("id")
        name = identifier(record.get("name"))
        if type(rid) is not int or not 1 <= rid <= 65535 or rid in ids or name in names:
            raise ValueError("record IDs/names must be unique; IDs range from 1 to 65535")
        if name in {"schema_hash", "max_payload_size"}:
            raise ValueError("record name conflicts with generated API")
        ids.add(rid); names.add(name)
        if record.get("category", "observation") != "observation":
            raise ValueError("v1 supports observations only")
        record["category"] = "observation"
        fields = record.get("fields")
        if not isinstance(fields, list) or not fields:
            raise ValueError("record fields must be a nonempty list")
        field_names, size = set(), 0
        for field in fields:
            if not isinstance(field, dict) or set(field) - {"name", "type", "count", "units", "frame", "components", "description"}:
                raise ValueError("invalid field keys")
            fname = identifier(field.get("name"))
            if fname in field_names or fname in {"type_id", "encoded_size", "encode", name}:
                raise ValueError("field name conflicts with generated API or another field")
            field_names.add(fname)
            typ = field.get("type")
            if typ not in TYPES:
                raise ValueError(f"unsupported field type: {typ}")
            count = field.get("count", 1)
            if type(count) is not int or not 1 <= count <= 65535:
                raise ValueError("count must be a positive integer <= 65535")
            field["count"] = count
            components = field.get("components")
            if components is not None and (not isinstance(components, list) or len(components) != count or any(not isinstance(c, str) for c in components) or len(set(components)) != count):
                raise ValueError("components must contain one unique string per element")
            size += TYPES[typ][2] * count
            for key in ("units", "frame", "description"):
                if key in field and not isinstance(field[key], str):
                    raise ValueError(f"{key} must be a string")
        if size > 65535:
            raise ValueError("record payload exceeds uint16 length")
        if "description" in record and not isinstance(record["description"], str):
            raise ValueError("description must be a string")
    schema["records"] = sorted(records, key=lambda r: r["id"])
    return schema

def digest(schema):
    canonical = json.dumps(schema, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()
    return hashlib.sha256(canonical).hexdigest()

def load(path):
    with Path(path).open() as stream:
        if Path(path).suffix == ".json":
            schema = json.load(stream)
        else:
            try:
                schema = yaml.load(stream, Loader=UniqueLoader)
            except yaml.YAMLError as exc:
                raise ValueError(f"invalid YAML: {exc}") from exc
    return validate(schema)
