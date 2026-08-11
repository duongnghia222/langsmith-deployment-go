#!/usr/bin/env python3
"""Dump .proto source from a Python _pb2.py module's embedded FileDescriptorProto.

Usage:
    LANGGRAPH_API_ROOT=/path/to/python/api python scripts/dump_proto.py <module_name>

Reads $LANGGRAPH_API_ROOT/langgraph_grpc_common/proto/<module_name>_pb2.py and
writes proto/<snake_module>/<fd.name> where <snake_module> is the bare Python
module name (e.g. core_api, engine_common) and <fd.name> is the original
proto filename (e.g. core-api.proto).

Rules:
- fd.package is used verbatim (no override)
- fd.name is used verbatim as output filename
- All fd.dependency entries are emitted, with cross-file ones rewritten to
  <snake_module>/<fd.name> using a pre-built lookup table
- option go_package is added for each file
- Google well-known types (google/protobuf/...) are kept as-is
"""
import importlib
import os
import sys
import types
from pathlib import Path

from google.protobuf.descriptor_pb2 import (
    DescriptorProto,
    EnumDescriptorProto,
    FieldDescriptorProto,
    FileDescriptorProto,
)

REPO = Path(__file__).resolve().parents[1]
DST = REPO / "proto"
MODULES_TXT = REPO / "scripts" / "proto_modules.txt"


def api_root() -> Path:
    """Root of the Python langgraph_api source tree (holds langgraph_grpc_common/)."""
    env = os.environ.get("LANGGRAPH_API_ROOT")
    if not env:
        sys.exit(
            "LANGGRAPH_API_ROOT is not set. Point it at the Python langgraph_api\n"
            "source tree that contains langgraph_grpc_common/proto/*_pb2.py, e.g.\n"
            "    LANGGRAPH_API_ROOT=../on-behalf/api make proto-bootstrap"
        )
    root = Path(env).resolve()
    if not (root / "langgraph_grpc_common" / "proto").is_dir():
        sys.exit(f"{root}/langgraph_grpc_common/proto not found")
    return root

GO_MODULE = "github.com/duongnghia222/langsmith-deployment-go/gen"


def _bootstrap_package_stubs() -> None:
    """Register stub parent packages so _pb2 relative imports work without
    triggering the langgraph_grpc_common.proto.__init__.py, which imports
    grpc gRPC stubs that may not be compatible with the installed grpcio version."""
    root = api_root()
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))

    if "langgraph_grpc_common" not in sys.modules:
        pkg = types.ModuleType("langgraph_grpc_common")
        pkg.__path__ = [str(root / "langgraph_grpc_common")]  # type: ignore[attr-defined]
        pkg.__package__ = "langgraph_grpc_common"
        sys.modules["langgraph_grpc_common"] = pkg

    if "langgraph_grpc_common.proto" not in sys.modules:
        proto_pkg = types.ModuleType("langgraph_grpc_common.proto")
        proto_pkg.__path__ = [str(root / "langgraph_grpc_common" / "proto")]  # type: ignore[attr-defined]
        proto_pkg.__package__ = "langgraph_grpc_common.proto"
        sys.modules["langgraph_grpc_common.proto"] = proto_pkg


def get_file_descriptor(module_name: str) -> FileDescriptorProto:
    _bootstrap_package_stubs()
    mod = importlib.import_module(f"langgraph_grpc_common.proto.{module_name}_pb2")
    fd = FileDescriptorProto()
    fd.ParseFromString(mod.DESCRIPTOR.serialized_pb)
    return fd


def build_import_lookup() -> dict[str, str]:
    """Build a mapping from original fd.name → new proto path (<snake_module>/<fd.name>).

    This must be built BEFORE rewriting any imports so all cross-file
    references can be resolved regardless of processing order.
    """
    _bootstrap_package_stubs()
    modules = [line.strip() for line in MODULES_TXT.read_text().splitlines() if line.strip()]
    lookup: dict[str, str] = {}
    for snake_module in modules:
        mod = importlib.import_module(f"langgraph_grpc_common.proto.{snake_module}_pb2")
        fd = FileDescriptorProto()
        fd.ParseFromString(mod.DESCRIPTOR.serialized_pb)
        # fd.name is the original filename, e.g. "core-api.proto"
        new_path = f"{snake_module}/{fd.name}"
        if fd.name in lookup:
            raise ValueError(
                f"fd.name collision: {fd.name!r} used by both "
                f"{lookup[fd.name]!r} and {new_path!r}"
            )
        lookup[fd.name] = new_path
    return lookup


TYPE_NAMES = {
    FieldDescriptorProto.TYPE_DOUBLE: "double",
    FieldDescriptorProto.TYPE_FLOAT: "float",
    FieldDescriptorProto.TYPE_INT64: "int64",
    FieldDescriptorProto.TYPE_UINT64: "uint64",
    FieldDescriptorProto.TYPE_INT32: "int32",
    FieldDescriptorProto.TYPE_FIXED64: "fixed64",
    FieldDescriptorProto.TYPE_FIXED32: "fixed32",
    FieldDescriptorProto.TYPE_BOOL: "bool",
    FieldDescriptorProto.TYPE_STRING: "string",
    FieldDescriptorProto.TYPE_BYTES: "bytes",
    FieldDescriptorProto.TYPE_UINT32: "uint32",
    FieldDescriptorProto.TYPE_SFIXED32: "sfixed32",
    FieldDescriptorProto.TYPE_SFIXED64: "sfixed64",
    FieldDescriptorProto.TYPE_SINT32: "sint32",
    FieldDescriptorProto.TYPE_SINT64: "sint64",
}


def field_type(field: FieldDescriptorProto) -> str:
    if field.type in (FieldDescriptorProto.TYPE_MESSAGE, FieldDescriptorProto.TYPE_ENUM):
        return field.type_name.lstrip(".")
    return TYPE_NAMES[field.type]


def emit_enum(e: EnumDescriptorProto, indent: int = 0) -> str:
    pad = "  " * indent
    out = [f"{pad}enum {e.name} {{"]
    for v in e.value:
        out.append(f"{pad}  {v.name} = {v.number};")
    out.append(f"{pad}}}")
    return "\n".join(out)


def emit_field(
    f: FieldDescriptorProto,
    indent: int,
    in_oneof: bool = False,
    map_entries: dict | None = None,
) -> str:
    pad = "  " * indent
    # Check if this is a map field: LABEL_REPEATED + type_name resolves to a map-entry message.
    if (
        f.label == FieldDescriptorProto.LABEL_REPEATED
        and map_entries is not None
        and f.type_name
    ):
        entry_name = f.type_name.split(".")[-1]
        if entry_name in map_entries:
            key_t, val_t = map_entries[entry_name]
            return f"{pad}map<{key_t}, {val_t}> {f.name} = {f.number};"

    label = ""
    if f.label == FieldDescriptorProto.LABEL_REPEATED:
        label = "repeated "
    elif f.proto3_optional and not in_oneof:
        # proto3_optional fields emitted at message scope get the 'optional' keyword.
        # Fields inside a real oneof block never get a label (oneof implies optional).
        label = "optional "
    return f"{pad}{label}{field_type(f)} {f.name} = {f.number};"


def _is_synthetic_oneof(msg: DescriptorProto, oneof_index: int) -> bool:
    """Return True if the oneof at oneof_index is a synthetic proto3-optional oneof.

    Synthetic oneofs are generated by protoc for proto3 optional fields. They have
    a name starting with '_', contain exactly one field, and that field has
    proto3_optional=True. They should NOT be emitted as oneof blocks; the field
    should instead be emitted at message scope with the 'optional' label.
    """
    oneof = msg.oneof_decl[oneof_index]
    fields_in = [f for f in msg.field if f.HasField("oneof_index") and f.oneof_index == oneof_index]
    return (
        oneof.name.startswith("_")
        and len(fields_in) == 1
        and fields_in[0].proto3_optional
    )


def emit_message(msg: DescriptorProto, indent: int = 0) -> str:
    pad = "  " * indent
    out = [f"{pad}message {msg.name} {{"]

    # Build map-entry index: entry message name -> (key_type, value_type).
    # A nested message is a map entry when options.map_entry == True and it
    # has exactly a "key" field (number 1) and a "value" field (number 2).
    map_entries: dict[str, tuple[str, str]] = {}
    for nested in msg.nested_type:
        if nested.options.map_entry:
            key_field = next(f for f in nested.field if f.name == "key")
            value_field = next(f for f in nested.field if f.name == "value")
            map_entries[nested.name] = (field_type(key_field), field_type(value_field))

    # Emit non-map nested types (map-entry helpers are suppressed).
    for nested in msg.nested_type:
        if not nested.options.map_entry:
            out.append(emit_message(nested, indent + 1))
    for nested in msg.enum_type:
        out.append(emit_enum(nested, indent + 1))

    # Determine which oneof indices are synthetic (proto3-optional wrappers)
    synthetic_oneof_indices = {
        i for i in range(len(msg.oneof_decl)) if _is_synthetic_oneof(msg, i)
    }

    # Group fields: those in real oneofs go to oneof_fields; everything else
    # (plain fields and synthetic-oneof fields) goes to inline_fields.
    oneof_fields: dict[int, list[FieldDescriptorProto]] = {}
    # Track the first field number for each real oneof so we can interleave
    # oneof blocks at the right position when iterating by field number.
    oneof_first_field_num: dict[int, int] = {}
    inline_fields: list[FieldDescriptorProto] = []
    for f in msg.field:
        if f.HasField("oneof_index") and f.oneof_index not in synthetic_oneof_indices:
            idx = f.oneof_index
            if idx not in oneof_first_field_num:
                oneof_first_field_num[idx] = f.number
            oneof_fields.setdefault(idx, []).append(f)
        else:
            inline_fields.append(f)

    # Build an ordered list of items to emit, interleaving real-oneof blocks
    # at the position of their first field number to preserve semantic order.
    # Items are either FieldDescriptorProto (inline) or int (oneof index to emit as block).
    items: list = []
    emitted_oneof_indices: set[int] = set()
    for f in sorted(msg.field, key=lambda x: x.number):
        if f.HasField("oneof_index") and f.oneof_index not in synthetic_oneof_indices:
            idx = f.oneof_index
            if idx not in emitted_oneof_indices:
                items.append(("oneof", idx))
                emitted_oneof_indices.add(idx)
        else:
            items.append(("field", f))

    for kind, value in items:
        if kind == "field":
            out.append(emit_field(value, indent + 1, map_entries=map_entries))
        else:  # oneof block
            idx = value
            oneof = msg.oneof_decl[idx]
            out.append(f"{'  ' * (indent + 1)}oneof {oneof.name} {{")
            for f in oneof_fields[idx]:
                out.append(emit_field(f, indent + 2, in_oneof=True, map_entries=map_entries))
            out.append(f"{'  ' * (indent + 1)}}}")
    out.append(f"{pad}}}")
    return "\n".join(out)


def emit_service(svc, indent: int = 0) -> str:
    pad = "  " * indent
    out = [f"{pad}service {svc.name} {{"]
    for m in svc.method:
        client_stream = "stream " if m.client_streaming else ""
        server_stream = "stream " if m.server_streaming else ""
        out.append(
            f"{pad}  rpc {m.name} ({client_stream}{m.input_type.lstrip('.')}) "
            f"returns ({server_stream}{m.output_type.lstrip('.')});"
        )
    out.append(f"{pad}}}")
    return "\n".join(out)


def emit_file(
    fd: FileDescriptorProto,
    snake_module: str,
    import_lookup: dict[str, str],
) -> str:
    """Emit a complete .proto file from a FileDescriptorProto.

    - fd.package is used verbatim
    - All fd.dependency entries are emitted; cross-file ones are rewritten
      using import_lookup; google/ ones are kept as-is
    - go_package option is added
    """
    out = [
        'syntax = "proto3";',
        "",
        f"package {fd.package};",
        "",
        f'option go_package = "{GO_MODULE}/{snake_module};{snake_module}";',
    ]

    # Emit imports
    has_imports = bool(fd.dependency)
    if has_imports:
        out.append("")
    for dep in fd.dependency:
        if dep.startswith("google/"):
            out.append(f'import "{dep}";')
        elif dep in import_lookup:
            out.append(f'import "{import_lookup[dep]}";')
        else:
            # Unknown dep — emit as-is with a warning
            print(f"WARNING: unknown dependency {dep!r} in {fd.name!r}", file=sys.stderr)
            out.append(f'import "{dep}";')

    # Enums
    for e in fd.enum_type:
        out.append("")
        out.append(emit_enum(e))

    # Messages
    for m in fd.message_type:
        out.append("")
        out.append(emit_message(m))

    # Services
    for s in fd.service:
        out.append("")
        out.append(emit_service(s))

    return "\n".join(out).rstrip() + "\n"


def main(module_name: str) -> None:
    # Build the lookup table first (before processing any single file)
    import_lookup = build_import_lookup()

    fd = get_file_descriptor(module_name)
    proto_text = emit_file(fd, module_name, import_lookup)

    out_dir = DST / module_name
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / fd.name  # use fd.name verbatim (may contain hyphens)
    out_file.write_text(proto_text)
    print(f"wrote {out_file.relative_to(REPO)}")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print(__doc__, file=sys.stderr)
        sys.exit(2)
    main(sys.argv[1])
