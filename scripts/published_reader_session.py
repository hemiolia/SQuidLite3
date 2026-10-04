#!/usr/bin/env python3
"""Serve a pinned published SQLite reader over a bounded JSONL stdio session.

The process opens one local PublishedSQLiteReader for its lifetime. It performs
no network access, opens no source database, and accepts no SQL or filesystem
paths from requests. Every successful operation is framed as a complete,
hash-bound response; clients must ignore chunks without a matching end frame.
"""

from __future__ import annotations

import argparse
import base64
import binascii
from contextlib import contextmanager
import hashlib
import json
import math
import os
from pathlib import Path
import re
import sys
import tempfile
from typing import Any, BinaryIO, Iterator, Optional

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT / "scripts") not in sys.path:
    sys.path.insert(0, str(ROOT / "scripts"))

from published_sqlite_reader import (  # noqa: E402
    PublishedSQLiteReader,
    PublishedSQLiteReaderError,
)

PROTOCOL_VERSION = 1
MAX_REQUEST_LINE_BYTES = 64 * 1024
RESPONSE_CHUNK_BYTES = 64 * 1024
_REQUEST_ID = re.compile(r"[A-Za-z0-9._:-]{1,64}\Z", re.ASCII)
_INTEGER_DECIMAL = re.compile(r"(?:0|[1-9][0-9]*|-[1-9][0-9]*)\Z", re.ASCII)
_INT64_MIN = -(1 << 63)
_INT64_MAX = (1 << 63) - 1
_VALIDATION_READER_CATEGORIES = {
    "DELTA_LOOKUP_TABLE_UNKNOWN",
    "DELTA_LOOKUP_COLUMN_UNKNOWN",
    "DELTA_LOOKUP_CRITERIA_INVALID",
    "DELTA_LOOKUP_NOT_UNIQUE",
}


class SessionRequestError(ValueError):
    """A stable, value-free request validation category."""

    def __init__(self, category: str, request_id: Optional[str] = None):
        super().__init__(category)
        self.category = category
        self.request_id = request_id


class SessionFatalRequestError(SessionRequestError):
    """A framing violation that closes the bounded input session."""


class _Base64Value:
    __slots__ = ("raw",)

    def __init__(self, raw: bytes):
        self.raw = raw


class _SpoolWriter:
    """Incrementally serialize JSON bytes into a private spool and digest."""

    def __init__(self, stream: BinaryIO):
        self.stream = stream
        self.digest = hashlib.sha256()
        self.byte_count = 0

    def write(self, raw: bytes) -> None:
        if not raw:
            return
        self.stream.write(raw)
        self.digest.update(raw)
        self.byte_count += len(raw)

    def json_string(self, value: str) -> None:
        self.write(b'"')
        fragments: list[str] = []
        buffered = 0

        def flush() -> None:
            nonlocal buffered
            if fragments:
                self.write("".join(fragments).encode("ascii"))
                fragments.clear()
                buffered = 0

        for char in value:
            point = ord(char)
            if char == '"':
                escaped = r'\"'
            elif char == "\\":
                escaped = r"\\"
            elif char == "\b":
                escaped = r"\b"
            elif char == "\f":
                escaped = r"\f"
            elif char == "\n":
                escaped = r"\n"
            elif char == "\r":
                escaped = r"\r"
            elif char == "\t":
                escaped = r"\t"
            elif point < 0x20 or 0x7F <= point <= 0x9F or 0xD800 <= point <= 0xDFFF:
                escaped = f"\\u{point:04x}"
            elif point <= 0x7E:
                escaped = char
            elif point <= 0xFFFF:
                escaped = f"\\u{point:04x}"
            else:
                scalar = point - 0x10000
                high = 0xD800 + (scalar >> 10)
                low = 0xDC00 + (scalar & 0x3FF)
                escaped = f"\\u{high:04x}\\u{low:04x}"
            fragments.append(escaped)
            buffered += len(escaped)
            if buffered >= 4096:
                flush()
        flush()
        self.write(b'"')

    def value(self, item: Any) -> None:
        if item is None:
            self.write(b"null")
        elif type(item) is bool:
            self.write(b"true" if item else b"false")
        elif type(item) is int:
            self.write(str(item).encode("ascii"))
        elif type(item) is float:
            if not math.isfinite(item):
                raise ValueError("SESSION_JSON_NONFINITE")
            self.write(repr(item).encode("ascii"))
        elif type(item) is str:
            self.json_string(item)
        elif isinstance(item, _Base64Value):
            self.write(b'"')
            raw = item.raw
            block_size = 48 * 1024  # divisible by three; only the last block pads.
            for offset in range(0, len(raw), block_size):
                self.write(base64.b64encode(raw[offset:offset + block_size]))
            self.write(b'"')
        elif type(item) in (list, tuple):
            self.write(b"[")
            for index, child in enumerate(item):
                if index:
                    self.write(b",")
                self.value(child)
            self.write(b"]")
        elif type(item) is dict:
            self.write(b"{")
            for index, (key, child) in enumerate(item.items()):
                if type(key) is not str:
                    raise ValueError("SESSION_JSON_KEY_INVALID")
                if index:
                    self.write(b",")
                self.json_string(key)
                self.write(b":")
                self.value(child)
            self.write(b"}")
        else:
            raise ValueError("SESSION_JSON_VALUE_INVALID")


def _cell(value: Any) -> dict[str, Any]:
    if value is None:
        return {"sqlite_type": "null", "encoding": "none", "value": None}
    if type(value) is int:
        if value < _INT64_MIN or value > _INT64_MAX:
            raise ValueError("SESSION_CELL_INTEGER_OUT_OF_RANGE")
        return {"sqlite_type": "integer", "encoding": "decimal", "value": str(value)}
    if type(value) is float:
        if math.isnan(value):
            raise ValueError("SESSION_CELL_REAL_INVALID")
        return {"sqlite_type": "real", "encoding": "float.hex", "value": value.hex()}
    if type(value) is str:
        return {"sqlite_type": "text", "encoding": "unicode", "value": value}
    if type(value) is bytes:
        return {"sqlite_type": "blob", "encoding": "base64", "value": _Base64Value(value)}
    raise ValueError("SESSION_CELL_TYPE_UNSUPPORTED")


def _decode_cell(value: Any) -> Any:
    if type(value) is not dict or set(value) != {"sqlite_type", "encoding", "value"}:
        raise SessionRequestError("SESSION_CRITERIA_CELL_INVALID")
    sqlite_type = value["sqlite_type"]
    encoding = value["encoding"]
    raw = value["value"]
    if sqlite_type == "null" and encoding == "none" and raw is None:
        return None
    if sqlite_type == "integer" and encoding == "decimal" and type(raw) is str:
        if not _INTEGER_DECIMAL.fullmatch(raw):
            raise SessionRequestError("SESSION_CRITERIA_CELL_INVALID")
        try:
            parsed = int(raw, 10)
        except ValueError:
            raise SessionRequestError("SESSION_CRITERIA_CELL_INVALID") from None
        if parsed < _INT64_MIN or parsed > _INT64_MAX or str(parsed) != raw:
            raise SessionRequestError("SESSION_CRITERIA_CELL_INVALID")
        return parsed
    if sqlite_type == "real" and encoding == "float.hex" and type(raw) is str:
        try:
            parsed = float.fromhex(raw)
        except (ValueError, OverflowError):
            raise SessionRequestError("SESSION_CRITERIA_CELL_INVALID") from None
        if math.isnan(parsed) or parsed.hex() != raw:
            raise SessionRequestError("SESSION_CRITERIA_CELL_INVALID")
        return parsed
    if sqlite_type == "text" and encoding == "unicode" and type(raw) is str:
        return raw
    if sqlite_type == "blob" and encoding == "base64" and type(raw) is str:
        try:
            decoded = base64.b64decode(raw, validate=True)
        except (binascii.Error, ValueError):
            raise SessionRequestError("SESSION_CRITERIA_CELL_INVALID") from None
        if base64.b64encode(decoded).decode("ascii") != raw:
            raise SessionRequestError("SESSION_CRITERIA_CELL_INVALID")
        return decoded
    raise SessionRequestError("SESSION_CRITERIA_CELL_INVALID")


def _pairs_without_duplicates(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate key")
        result[key] = value
    return result


def _reject_constant(_value):
    raise ValueError("non-standard JSON constant")


def _parse_finite_json_float(raw: str) -> float:
    value = float(raw)
    if not math.isfinite(value):
        raise ValueError("non-finite JSON number")
    return value


def _read_request_line(stream: BinaryIO) -> Optional[bytes]:
    raw = stream.readline(MAX_REQUEST_LINE_BYTES + 1)
    if raw == b"":
        return None
    if type(raw) is not bytes:
        raise SessionRequestError("SESSION_REQUEST_INVALID")
    if len(raw) > MAX_REQUEST_LINE_BYTES:
        raise SessionFatalRequestError("SESSION_REQUEST_TOO_LARGE")
    return raw


def _parse_request(raw: bytes) -> dict[str, Any]:
    try:
        request = json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=_pairs_without_duplicates,
            parse_constant=_reject_constant,
            parse_float=_parse_finite_json_float,
        )
    except (UnicodeError, json.JSONDecodeError, ValueError, RecursionError):
        raise SessionRequestError("SESSION_REQUEST_JSON_INVALID") from None
    if type(request) is not dict:
        raise SessionRequestError("SESSION_REQUEST_INVALID")
    request_id = request.get("request_id")
    if (type(request_id) is not str or not request_id.isascii()
            or not _REQUEST_ID.fullmatch(request_id)):
        raise SessionRequestError("SESSION_REQUEST_ID_INVALID")
    if type(request.get("version")) is not int or request["version"] != PROTOCOL_VERSION:
        raise SessionRequestError("SESSION_PROTOCOL_VERSION_INVALID", request_id)
    operation = request.get("op")
    if operation == "metadata":
        required = {"version", "request_id", "op"}
    elif operation == "close":
        required = {"version", "request_id", "op"}
    elif operation == "get_unique_row":
        required = {"version", "request_id", "op", "table", "criteria"}
    else:
        raise SessionRequestError("SESSION_OPERATION_INVALID", request_id)
    if set(request) != required:
        raise SessionRequestError("SESSION_REQUEST_FIELDS_INVALID", request_id)
    if operation == "get_unique_row":
        if type(request["table"]) is not str:
            raise SessionRequestError("SESSION_TABLE_INVALID", request_id)
        if type(request["criteria"]) is not dict:
            raise SessionRequestError("SESSION_CRITERIA_INVALID", request_id)
        decoded = {}
        for name, encoded in request["criteria"].items():
            if type(name) is not str:
                raise SessionRequestError("SESSION_CRITERIA_INVALID", request_id)
            try:
                decoded[name] = _decode_cell(encoded)
            except SessionRequestError as exc:
                exc.request_id = request_id
                raise
        request["_decoded_criteria"] = decoded
    return request


def _frame_bytes(value: dict[str, Any]) -> bytes:
    return (json.dumps(value, ensure_ascii=True, separators=(",", ":"), allow_nan=False) + "\n").encode("ascii")


def _write_frame(stream: BinaryIO, value: dict[str, Any]) -> None:
    stream.write(_frame_bytes(value))
    stream.flush()


def _error_frame(stream: BinaryIO, category: str, request_id: Optional[str]) -> None:
    _write_frame(stream, {
        "type": "error",
        "version": PROTOCOL_VERSION,
        "request_id": request_id,
        "category": category,
    })


def _assert_public_guard(reader: Any, expected_generation: str, expected_latest_sha256: str) -> None:
    # These are public reader properties. The control-binding property also
    # invokes its normal published-input guards; no private guard is bypassed.
    if reader.published_control_binding_verified is not True:
        raise PublishedSQLiteReaderError("PUBLISHED_CONTROL_BINDING_UNVERIFIED")
    if reader.generation_id != expected_generation:
        raise PublishedSQLiteReaderError("PUBLISHED_SESSION_PIN_CHANGED")
    if reader.pinned_latest_sha256 != expected_latest_sha256:
        raise PublishedSQLiteReaderError("PUBLISHED_SESSION_PIN_CHANGED")


def _reader_pin(reader: Any, expected_generation: str) -> dict[str, Any]:
    generation = reader.generation_id
    latest_sha256 = reader.pinned_latest_sha256
    if generation != expected_generation:
        raise PublishedSQLiteReaderError("PUBLISHED_EXPECTED_GENERATION_MISMATCH")
    if (type(latest_sha256) is not str
            or not re.fullmatch(r"[0-9a-f]{64}", latest_sha256, re.ASCII)):
        raise PublishedSQLiteReaderError("PUBLISHED_PIN_INVALID")
    binding = reader.published_control_binding_verified
    deltas = reader.published_deltas_verified
    if type(binding) is not bool or type(deltas) is not bool or binding is not True:
        raise PublishedSQLiteReaderError("PUBLISHED_CONTROL_BINDING_UNVERIFIED")
    return {
        "generation_id": generation,
        "baseline_generation_id": reader.baseline_generation_id,
        "pinned_latest_sha256": latest_sha256,
        "captured_at": reader.captured_at,
        "published_control_binding_verified": binding,
        "published_deltas_verified": deltas,
    }


def _metadata_response(reader: Any, pin: dict[str, Any]) -> dict[str, Any]:
    table_rows = reader.tables()
    tables = []
    for table in table_rows:
        if type(table) is not dict:
            raise PublishedSQLiteReaderError("PUBLISHED_METADATA_INVALID")
        name = table.get("name")
        if type(name) is not str:
            raise PublishedSQLiteReaderError("PUBLISHED_METADATA_INVALID")
        full_table = dict(table)
        full_table["column_schema"] = reader.columns(name)
        full_table["foreign_keys"] = reader.foreign_keys(name)
        tables.append(full_table)
    return {
        "kind": "metadata",
        "protocol_version": PROTOCOL_VERSION,
        **pin,
        "tables": tables,
        "schema_objects": reader.schema_objects(),
        "all_remote_artifacts_verified": False,
        "realtime_synchronized": False,
        "scope": "local control + local SQLite package",
    }


def _unique_row_response(reader: Any, table: str, criteria: dict[str, Any]) -> dict[str, Any]:
    result = reader.get_unique_row(table, criteria)
    xinfo = reader.columns(table)
    if type(xinfo) is not list:
        raise PublishedSQLiteReaderError("PUBLISHED_METADATA_INVALID")
    columns = []
    for item in xinfo:
        if type(item) is not dict or type(item.get("name")) is not str:
            raise PublishedSQLiteReaderError("PUBLISHED_METADATA_INVALID")
        if item.get("hidden") != 1:
            columns.append(item["name"])
    if result is None:
        return {
            "kind": "get_unique_row",
            "table": table,
            "found": False,
            "columns": columns,
            "source_rowid": None,
            "values": None,
        }
    values = result["values"]
    if len(values) != len(columns):
        raise PublishedSQLiteReaderError("PUBLISHED_LOOKUP_RESULT_INVALID")
    return {
        "kind": "get_unique_row",
        "table": table,
        "found": True,
        "columns": columns,
        "source_rowid": (None if result["source_rowid"] is None
                          else _cell(result["source_rowid"])),
        "values": [_cell(value) for value in values],
    }


@contextmanager
def _preserving_reader_context(reader: Any) -> Iterator[Any]:
    """Run the reader's normal context and preserve non-Exception interrupts."""
    reader.__enter__()
    try:
        yield reader
    except BaseException as body_error:
        traceback = body_error.__traceback__
        try:
            suppressed = reader.__exit__(type(body_error), body_error, traceback)
        except BaseException:
            if not isinstance(body_error, Exception):
                raise body_error.with_traceback(traceback)
            raise
        if not isinstance(body_error, Exception):
            raise body_error.with_traceback(traceback)
        if suppressed:
            return
        raise
    else:
        reader.__exit__(None, None, None)


@contextmanager
def _serialized_payload(payload: dict[str, Any]) -> Iterator[tuple[BinaryIO, int, str]]:
    with tempfile.TemporaryFile(mode="w+b", prefix="ikaring-published-session-") as spool:
        os.fchmod(spool.fileno(), 0o600)
        writer = _SpoolWriter(spool)
        writer.value(payload)
        spool.flush()
        byte_count = writer.byte_count
        digest = writer.digest.hexdigest()
        spool.seek(0)
        yield spool, byte_count, digest


def _emit_success(
    stream: BinaryIO,
    reader: Any,
    pin: dict[str, Any],
    request_id: str,
    payload: dict[str, Any],
) -> None:
    with _serialized_payload(payload) as (spool, byte_count, digest):
        chunk_count = (byte_count + RESPONSE_CHUNK_BYTES - 1) // RESPONSE_CHUNK_BYTES
        common = {
            "version": PROTOCOL_VERSION,
            "request_id": request_id,
            "generation_id": pin["generation_id"],
            "pinned_latest_sha256": pin["pinned_latest_sha256"],
            "response_sha256": digest,
            "response_bytes": byte_count,
        }
        begin = {
            "type": "response_begin",
            **common,
            "chunk_bytes": RESPONSE_CHUNK_BYTES,
            "chunk_count": chunk_count,
        }
        begin_raw = _frame_bytes(begin)
        _assert_public_guard(reader, pin["generation_id"], pin["pinned_latest_sha256"])
        stream.write(begin_raw)
        stream.flush()

        streamed_digest = hashlib.sha256()
        streamed_bytes = 0
        for index in range(chunk_count):
            raw = spool.read(RESPONSE_CHUNK_BYTES)
            if not raw:
                raise PublishedSQLiteReaderError("PUBLISHED_RESPONSE_SPOOL_INVALID")
            streamed_digest.update(raw)
            streamed_bytes += len(raw)
            frame = {
                "type": "response_chunk",
                **common,
                "chunk_index": index,
                "data_base64": base64.b64encode(raw).decode("ascii"),
            }
            stream.write(_frame_bytes(frame))
            stream.flush()
        if (spool.read(1) or streamed_bytes != byte_count
                or streamed_digest.hexdigest() != digest):
            raise PublishedSQLiteReaderError("PUBLISHED_RESPONSE_SPOOL_INVALID")

        end_raw = _frame_bytes({"type": "response_end", **common, "chunk_count": chunk_count})
        _assert_public_guard(reader, pin["generation_id"], pin["pinned_latest_sha256"])
        stream.write(end_raw)
        stream.flush()


def _validation_reader_error(exc: PublishedSQLiteReaderError) -> bool:
    return exc.category in _VALIDATION_READER_CATEGORIES


def serve_session(
    reader_factory,
    *,
    controls: Path,
    root: Path,
    delta_root: Path,
    expected_generation_id: str,
    input_stream: BinaryIO,
    output_stream: BinaryIO,
) -> int:
    """Serve one session; exposed for deterministic artificial tests."""
    reader = None
    pin = None
    close_request_id: Optional[str] = None
    fatal_category: Optional[str] = None
    fatal_request_id: Optional[str] = None
    started = False
    try:
        reader = reader_factory(
            controls,
            root,
            delta_root,
            expected_generation_id=expected_generation_id,
        )
        with _preserving_reader_context(reader):
            pin = _reader_pin(reader, expected_generation_id)
            _assert_public_guard(reader, pin["generation_id"], pin["pinned_latest_sha256"])
            _write_frame(output_stream, {
                "type": "ready",
                "version": PROTOCOL_VERSION,
                "generation_id": pin["generation_id"],
                "baseline_generation_id": pin["baseline_generation_id"],
                "pinned_latest_sha256": pin["pinned_latest_sha256"],
                "published_control_binding_verified": True,
                "published_deltas_verified": pin["published_deltas_verified"],
                "all_remote_artifacts_verified": False,
                "realtime_synchronized": False,
                "scope": "local control + local SQLite package",
            })
            started = True

            while True:
                request_id = None
                try:
                    raw = _read_request_line(input_stream)
                    if raw is None:
                        break
                    request = _parse_request(raw)
                    request_id = request["request_id"]
                except SessionFatalRequestError as exc:
                    fatal_category = exc.category
                    fatal_request_id = exc.request_id
                    break
                except SessionRequestError as exc:
                    _error_frame(output_stream, exc.category, exc.request_id or request_id)
                    continue

                if request["op"] == "close":
                    close_request_id = request_id
                    break

                try:
                    if request["op"] == "metadata":
                        payload = _metadata_response(reader, pin)
                    else:
                        payload = _unique_row_response(
                            reader, request["table"], request["_decoded_criteria"],
                        )
                    _emit_success(output_stream, reader, pin, request_id, payload)
                except SessionRequestError as exc:
                    _error_frame(output_stream, exc.category, request_id)
                except PublishedSQLiteReaderError as exc:
                    if _validation_reader_error(exc):
                        _error_frame(output_stream, exc.category, request_id)
                    else:
                        fatal_category = exc.category
                        fatal_request_id = request_id
                        break
                except Exception:
                    fatal_category = "SESSION_PROVIDER_FAILED"
                    fatal_request_id = request_id
                    break
    except PublishedSQLiteReaderError as exc:
        fatal_category = exc.category
        fatal_request_id = close_request_id if close_request_id is not None else fatal_request_id
        if not started and fatal_category is None:
            fatal_category = "SESSION_OPEN_FAILED"
    except Exception:
        if fatal_category is None:
            fatal_category = "SESSION_OPEN_FAILED" if not started else "SESSION_PROVIDER_FAILED"
        if close_request_id is not None:
            fatal_request_id = close_request_id

    if fatal_category is not None:
        _error_frame(output_stream, fatal_category, fatal_request_id)
        return 2
    if close_request_id is not None and pin is not None:
        _write_frame(output_stream, {
            "type": "closed",
            "version": PROTOCOL_VERSION,
            "request_id": close_request_id,
            "generation_id": pin["generation_id"],
            "pinned_latest_sha256": pin["pinned_latest_sha256"],
        })
    return 0


class _SessionArgumentParser(argparse.ArgumentParser):
    def error(self, _message: str) -> None:
        try:
            _error_frame(sys.stdout.buffer, "SESSION_ARGUMENTS_INVALID", None)
        finally:
            raise SystemExit(2)


def main(argv: Optional[list[str]] = None) -> int:
    os.umask(0o077)
    parser = _SessionArgumentParser(description=__doc__)
    parser.add_argument("--controls", type=Path, required=True)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--delta-root", type=Path, required=True)
    parser.add_argument("--expected-generation-id", required=True)
    args = parser.parse_args(argv)
    try:
        return serve_session(
            PublishedSQLiteReader,
            controls=args.controls,
            root=args.root,
            delta_root=args.delta_root,
            expected_generation_id=args.expected_generation_id,
            input_stream=sys.stdin.buffer,
            output_stream=sys.stdout.buffer,
        )
    except BrokenPipeError:
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
