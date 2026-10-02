#!/usr/bin/env python3
"""静止SQLiteの全通常テーブルをXLSXへ出力し、独立に全件照合するCLI。"""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import stat
import sys
import tempfile
import threading
from datetime import datetime, timezone
from typing import Any, Iterator
import xml.etree.ElementTree as ET
import zipfile


_REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_REPO_ROOT / "src" / "python"))

from ikarchive.lossless_xlsx import export_sqlite_tables  # noqa: E402


HEADERS = (
    "table",
    "row_number",
    "column_name",
    "sqlite_type",
    "chunk_number",
    "total_chunks",
    "value_chunk",
    "payload_encoding",
    "source_rowid",
)
MAIN_NS = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
_Q = lambda name: f"{{{MAIN_NS}}}{name}"
_EXCEL_ESCAPE_PREFIX = "_x"
_ROW_IDENTITY_FORMAT = "source_rowid_column_v1"
_ROWID_ALIASES = ("_rowid_", "rowid", "oid")
_MIN_ROWID = -(1 << 63)
_MAX_ROWID = (1 << 63) - 1


class ExportVerificationError(ValueError):
    """エクスポート成果物が元SQLiteと完全一致しない。"""


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _absolute(path: str | os.PathLike[str]) -> Path:
    return Path(os.path.abspath(os.fspath(path)))


def _assert_no_symlink_components(path: Path, *, allow_missing: bool) -> None:
    for component in reversed(path.parents):
        try:
            mode = component.lstat().st_mode
        except FileNotFoundError:
            if allow_missing:
                continue
            raise ValueError("path component does not exist") from None
        if stat.S_ISLNK(mode):
            raise ValueError("symbolic links are not allowed in paths")
    try:
        mode = path.lstat().st_mode
    except FileNotFoundError:
        if allow_missing:
            return
        raise ValueError("path does not exist") from None
    if stat.S_ISLNK(mode):
        raise ValueError("symbolic links are not allowed in paths")


def _hash_file(path: Path) -> tuple[int, str]:
    digest = hashlib.sha256()
    size = 0
    with path.open("rb") as stream:
        while block := stream.read(1024 * 1024):
            size += len(block)
            digest.update(block)
    return size, digest.hexdigest()


def _check_static_source(db_path: Path) -> None:
    _assert_no_symlink_components(db_path, allow_missing=False)
    if not stat.S_ISREG(db_path.lstat().st_mode):
        raise ValueError("source must be a regular file")
    with db_path.open("rb") as source_file:
        header = source_file.read(16)
    if header != b"SQLite format 3\x00":
        raise ValueError("source is not a SQLite database")
    for suffix in ("-wal", "-journal"):
        sidecar = Path(f"{db_path}{suffix}")
        try:
            sidecar_stat = sidecar.lstat()
        except FileNotFoundError:
            continue
        if stat.S_ISLNK(sidecar_stat.st_mode):
            raise ValueError("SQLite sidecar must not be a symbolic link")
        if sidecar_stat.st_size:
            raise ValueError("non-empty SQLite sidecar found")


def _source_uri(db_path: Path) -> str:
    return f"{db_path.as_uri()}?mode=ro&immutable=1"


def _quote_identifier(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _sqlite_schema_objects(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    rows = conn.execute(
        "SELECT name, type, tbl_name, sql FROM sqlite_master ORDER BY type, name"
    )
    return [
        {"name": name, "type": obj_type, "tbl_name": table_name, "sql": sql}
        for name, obj_type, table_name, sql in rows
    ]


def _target_tables(conn: sqlite3.Connection) -> list[str]:
    # Completeness is measured against the source, including empty/internal
    # tables. Unsupported kinds must fail in preflight, never disappear here.
    return [row[0] for row in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    )]


def _source_row_identity(
    conn: sqlite3.Connection, table: str, columns: list[str]
) -> tuple[str, str | None]:
    """Independently determine whether a source pseudo-rowid is addressable."""
    table_flags = {
        row[1]: row for row in conn.execute("PRAGMA table_list") if len(row) >= 6
    }
    flags = table_flags.get(table)
    if flags is None:
        raise ExportVerificationError("source table is absent from PRAGMA table_list")
    if flags[2] in {"virtual", "shadow"}:
        raise ExportVerificationError("virtual/shadow table row identity is unsupported")
    if bool(flags[4]):
        return "without_rowid", None
    names = {name.casefold() for name in columns}
    for alias in _ROWID_ALIASES:
        if alias.casefold() not in names:
            return "rowid", alias
    return "shadowed", None


def _ordered_source_cursor(
    conn: sqlite3.Connection,
    table: str,
    columns: list[str],
    identity_kind: str,
    identity_alias: str | None,
) -> sqlite3.Cursor:
    quoted = _quote_identifier(table)
    select = f"SELECT * FROM {quoted}"
    if identity_kind == "rowid":
        if identity_alias not in _ROWID_ALIASES:
            raise ExportVerificationError("source rowid alias is invalid")
        quoted_alias = _quote_identifier(identity_alias)
        select = f"SELECT *, {quoted_alias} FROM {quoted} ORDER BY {quoted_alias}"
    else:
        xinfo = conn.execute(f"PRAGMA table_xinfo({_quote_identifier(table)})").fetchall()
        primary_key = [
            row[1] for row in sorted((row for row in xinfo if row[5]), key=lambda row: row[5])
        ]
        if primary_key:
            select += " ORDER BY " + ", ".join(_quote_identifier(name) for name in primary_key)
    cursor = conn.execute(select)
    actual = [description[0] for description in (cursor.description or ())]
    expected_width = len(columns) + (1 if identity_kind == "rowid" else 0)
    if actual[: len(columns)] != columns or len(actual) != expected_width:
        raise ExportVerificationError("source SELECT columns disagree with row identity plan")
    return cursor


def _preflight_table_names(conn: sqlite3.Connection) -> None:
    # Validate UTF-8 before the low-level writer creates files. It uses a digest
    # in the piece filename, so SQLite names are never path segments.
    for name in _target_tables(conn):
        name.encode("utf-8")


def _export_progress_reporter(output_dir: Path, stop: threading.Event) -> None:
    last_reported = 0
    while not stop.wait(1.0):
        try:
            piece_count = sum(
                1 for path in output_dir.glob("*.xlsx") if path.is_file() and not path.is_symlink()
            )
        except OSError:
            continue
        milestone = piece_count // 100 * 100
        if milestone > last_reported:
            print(f"export progress: pieces={milestone}", file=sys.stderr, flush=True)
            last_reported = milestone


def _atomic_json(path: Path, value: Any) -> None:
    fd, temporary_name = tempfile.mkstemp(prefix=f"{path.name}.", suffix=".tmp", dir=path.parent)
    temporary = Path(temporary_name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as stream:
            json.dump(value, stream, ensure_ascii=False, indent=2, sort_keys=True)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.chmod(temporary, 0o600)
        os.replace(temporary, path)
        directory_fd = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        temporary.unlink(missing_ok=True)


def _mark_failed(
    output_dir: Path,
    *,
    phase: str,
    error: BaseException,
    snapshot_identifier: str,
    source_info: dict[str, Any] | None,
    counts: dict[str, Any] | None,
) -> None:
    for filename in ("index.json", "manifest.json"):
        path = output_dir / filename
        if not path.is_file():
            continue
        try:
            document = json.loads(path.read_text(encoding="utf-8"))
            document["status"] = "failed"
            document["verification_status"] = "failed"
            document.pop("verified_at", None)
            _atomic_json(path, document)
        except (OSError, ValueError, TypeError):
            # The explicit failure receipt below remains the authoritative record.
            pass
    (output_dir / "verification.json").unlink(missing_ok=True)
    failure: dict[str, Any] = {
        "version": 1,
        "status": "failed",
        "phase": phase,
        "error_type": type(error).__name__,
        "snapshot_identifier": snapshot_identifier,
        "failed_at": _utc_now(),
    }
    if source_info is not None:
        failure["source"] = source_info
    if counts is not None:
        failure["counts"] = counts
    _atomic_json(output_dir / "verification.failed.json", failure)


def _cell_spec(value: Any) -> tuple[str, str, Any]:
    if value is None:
        return "null", "plain", None
    if isinstance(value, bool):
        return "integer", "plain", "1" if value else "0"
    if isinstance(value, int):
        return "integer", "plain", str(value)
    if isinstance(value, float):
        return "real", "plain", value.hex()
    if isinstance(value, str):
        if _is_xml_10_safe(value):
            return "text", "plain", value
        return "text", "utf8-base64", value.encode("utf-8")
    if isinstance(value, (bytes, bytearray, memoryview)):
        return "blob", "plain", bytes(value)
    raise ExportVerificationError("unsupported SQLite value type encountered")


def _is_xml_10_safe(value: str) -> bool:
    for char in value:
        codepoint = ord(char)
        if not (
            codepoint in (0x09, 0x0A, 0x0D)
            or 0x20 <= codepoint <= 0xD7FF
            or 0xE000 <= codepoint <= 0xFFFD
            or 0x10000 <= codepoint <= 0x10FFFF
        ):
            return False
    return True


def _cell_from_xml(cell: ET.Element) -> str | None:
    cell_type = cell.get("t")
    if cell_type == "inlineStr":
        inline = cell.find(_Q("is"))
        if inline is None:
            raise ExportVerificationError("malformed inline string cell")
        return _excel_xstring_unescape(
            "".join(text.text or "" for text in inline.iter(_Q("t")))
        )
    value = cell.find(_Q("v"))
    return None if value is None else (value.text or "")


def _excel_xstring_unescape(text: str) -> str:
    """ST_Xstringのエスケープを1回だけ復号する（export helperに依存しない）。"""
    output: list[str] = []
    index = 0
    while index < len(text):
        token = text[index : index + 7]
        if (
            text.startswith(_EXCEL_ESCAPE_PREFIX, index)
            and len(token) == 7
            and token[6] == "_"
            and all(char in "0123456789abcdefABCDEF" for char in token[2:6])
        ):
            codepoint = int(token[2:6], 16)
            output.append("_" if codepoint == 0x005F else chr(codepoint))
            index += 7
        else:
            output.append(text[index])
            index += 1
    return "".join(output)


def _parse_row(element: ET.Element) -> dict[str, str | None]:
    cells: dict[str, str | None] = {}
    for cell in element.findall(_Q("c")):
        reference = cell.get("r", "")
        column = "".join(char for char in reference if char.isalpha()).upper()
        if not column or column in cells:
            raise ExportVerificationError("invalid or duplicate worksheet cell")
        cells[column] = _cell_from_xml(cell)
    return cells


def _iter_piece_rows(path: Path, expected_piece: dict[str, Any]) -> Iterator[dict[str, Any]]:
    try:
        with zipfile.ZipFile(path, "r") as workbook:
            names = workbook.namelist()
            if len(names) != len(set(names)) or "xl/worksheets/sheet1.xml" not in names:
                raise ExportVerificationError("invalid workbook members")
            required_members = {
                "[Content_Types].xml",
                "_rels/.rels",
                "xl/workbook.xml",
                "xl/_rels/workbook.xml.rels",
                "xl/worksheets/sheet1.xml",
            }
            if set(names) != required_members:
                raise ExportVerificationError("workbook members are incomplete or unexpected")
            # Parsing sheet1 reaches EOF and checks its CRC. Read only the small
            # package parts here, so a large worksheet is not inflated twice.
            for member in required_members - {"xl/worksheets/sheet1.xml"}:
                with workbook.open(member) as member_stream:
                    while member_stream.read(65536):
                        pass
            with workbook.open("xl/worksheets/sheet1.xml") as sheet:
                headers: list[str] | None = None
                chunk_count = 0
                worksheet_row = 0
                for event, element in ET.iterparse(sheet, events=("end",)):
                    if element.tag != _Q("row"):
                        continue
                    cells = _parse_row(element)
                    cell_types = {
                        "".join(char for char in (cell.get("r") or "") if char.isalpha()).upper():
                        cell.get("t")
                        for cell in element.findall(_Q("c"))
                    }
                    row_number = element.get("r")
                    if row_number == "1":
                        if headers is not None or worksheet_row != 0:
                            raise ExportVerificationError("worksheet header is duplicated or misplaced")
                        if set(cells) != set("ABCDEFGHI"):
                            raise ExportVerificationError("worksheet header row is incomplete")
                        headers = [cells.get(chr(ord("A") + index)) or "" for index in range(9)]
                        if tuple(headers) != HEADERS:
                            raise ExportVerificationError("unexpected worksheet headers")
                        worksheet_row = 1
                        element.clear()
                        continue
                    if headers is None:
                        raise ExportVerificationError("worksheet header is missing")
                    try:
                        worksheet_row += 1
                        if int(row_number or "") != worksheet_row:
                            raise ExportVerificationError("worksheet row numbering is discontinuous")
                    except ValueError:
                        raise ExportVerificationError("worksheet row numbering is invalid") from None
                    required = set("ABCDEFGHI")
                    if set(cells) != required:
                        raise ExportVerificationError("worksheet row is incomplete")
                    try:
                        row_index = int(cells["B"] or "")
                        chunk_number = int(cells["E"] or "")
                        total_chunks = int(cells["F"] or "")
                    except (TypeError, ValueError):
                        raise ExportVerificationError("invalid numeric chunk metadata") from None
                    payload = cells["G"]
                    sqlite_type = cells["D"] or ""
                    if cells.get("I") is None or cell_types.get("I") != "inlineStr":
                        raise ExportVerificationError("source_rowid must be stored as an inline string")
                    rowid_text = cells["I"]
                    if rowid_text == "":
                        source_rowid = None
                    else:
                        if not re.fullmatch(r"(?:0|-?[1-9][0-9]*)", rowid_text):
                            raise ExportVerificationError("source_rowid is not canonical decimal text")
                        source_rowid = int(rowid_text)
                        if not _MIN_ROWID <= source_rowid <= _MAX_ROWID:
                            raise ExportVerificationError("source_rowid is outside SQLite int64 range")
                    if sqlite_type == "null" and payload is None:
                        payload_value: str | None = None
                    elif payload is None:
                        raise ExportVerificationError("payload cell is missing")
                    else:
                        payload_value = payload
                    result = {
                        "table": cells["A"],
                        "row_number": row_index,
                        "column_name": cells["C"],
                        "sqlite_type": sqlite_type,
                        "chunk_number": chunk_number,
                        "total_chunks": total_chunks,
                        "value_chunk": payload_value,
                        "payload_encoding": cells["H"],
                        "source_rowid": source_rowid,
                    }
                    if (
                        result["table"] != expected_piece["table"]
                        or chunk_number < 1
                        or total_chunks < 1
                        or row_index < 1
                        or result["payload_encoding"] not in ("plain", "utf8-base64")
                    ):
                        raise ExportVerificationError("invalid chunk metadata")
                    chunk_count += 1
                    element.clear()
                    yield result
                if headers is None:
                    raise ExportVerificationError("worksheet header is missing")
                if chunk_count != expected_piece.get("chunk_count"):
                    raise ExportVerificationError("piece chunk count does not match index")
    except ExportVerificationError:
        raise
    except (OSError, zipfile.BadZipFile, ET.ParseError, KeyError, RuntimeError):
        raise ExportVerificationError("workbook or worksheet is unreadable") from None


def _connection_path(conn: sqlite3.Connection) -> Path | None:
    row = conn.execute("PRAGMA database_list").fetchone()
    if not row or not row[2]:
        return None
    return Path(row[2])


def _piece_inventory(index_doc: dict[str, Any], output_dir: Path) -> tuple[list[dict[str, Any]], dict[str, list[dict[str, Any]]]]:
    top_pieces = index_doc.get("pieces")
    table_map = index_doc.get("tables")
    if not isinstance(top_pieces, list) or not isinstance(table_map, dict):
        raise ExportVerificationError("index is missing table or piece metadata")
    by_table: dict[str, list[dict[str, Any]]] = {}
    names: set[str] = set()
    for table_name in sorted(table_map):
        table_info = table_map[table_name]
        pieces = table_info.get("pieces") if isinstance(table_info, dict) else None
        if not isinstance(pieces, list):
            raise ExportVerificationError("table piece list is invalid")
        normalized: list[dict[str, Any]] = []
        for piece in pieces:
            if not isinstance(piece, dict) or piece.get("table") != table_name:
                raise ExportVerificationError("piece table metadata is invalid")
            name = piece.get("name")
            if (
                not isinstance(name, str)
                or Path(name).name != name
                or "/" in name
                or "\\" in name
                or name in ("", ".", "..")
            ):
                raise ExportVerificationError("piece filename is unsafe")
            if name in names:
                raise ExportVerificationError("piece filename is duplicated")
            names.add(name)
            if not isinstance(piece.get("chunk_count"), int) or piece["chunk_count"] < 0:
                raise ExportVerificationError("piece chunk count is invalid")
            normalized.append(piece)
        by_table[table_name] = normalized
    top_groups: dict[str, list[dict[str, Any]]] = {}
    for piece in top_pieces:
        if (
            not isinstance(piece, dict)
            or not isinstance(piece.get("name"), str)
            or not isinstance(piece.get("table"), str)
        ):
            raise ExportVerificationError("top-level piece metadata is invalid")
        top_groups.setdefault(piece["table"], []).append(piece)
    if set(top_groups) != set(by_table):
        raise ExportVerificationError("top-level and per-table piece lists disagree")
    for table_name, group in by_table.items():
        if top_groups[table_name] != group:
            raise ExportVerificationError("top-level and per-table piece lists disagree")
        expected_names = table_map[table_name].get("piece_names")
        if expected_names is not None and expected_names != [piece["name"] for piece in group]:
            raise ExportVerificationError("piece_names do not match table piece metadata")
    expected_top_names = index_doc.get("piece_names")
    actual_top_names = [piece["name"] for piece in top_pieces]
    if expected_top_names is not None and expected_top_names != actual_top_names:
        raise ExportVerificationError("piece_names do not match top-level piece metadata")
    actual_xlsx = {path.name for path in output_dir.glob("*.xlsx") if path.is_file()}
    if actual_xlsx != names:
        raise ExportVerificationError("XLSX files do not match the index")
    all_files = list(output_dir.iterdir())
    allowed = names | {"index.json", "manifest.json", "verification.json"}
    if any(
        not path.is_file() or path.is_symlink() or path.name not in allowed
        for path in all_files
    ):
        raise ExportVerificationError("output contains an unexpected file")
    return top_pieces, by_table


def _next_record(iterator: Iterator[dict[str, Any]]) -> dict[str, Any]:
    try:
        return next(iterator)
    except StopIteration:
        raise ExportVerificationError("source has a cell missing from XLSX") from None


def _verify_cell(
    records: Iterator[dict[str, Any]],
    *,
    table: str,
    row_number: int,
    column: str,
    source_value: Any,
    source_rowid: int | None,
) -> int:
    expected_type, expected_encoding, expected = _cell_spec(source_value)
    first = _next_record(records)
    total = first["total_chunks"]
    if first["chunk_number"] != 1 or total < 1:
        raise ExportVerificationError("cell chunk sequence does not start at one")
    if expected_type in ("null", "integer", "real") and total != 1:
        raise ExportVerificationError("scalar cell has multiple chunks")
    if expected_type == "null":
        expected_data: str | bytes | None = None
    else:
        expected_data = expected
    is_base64 = expected_type == "blob" or expected_encoding == "utf8-base64"

    text_offset = 0
    byte_offset = 0
    base64_carry = ""
    for number in range(1, total + 1):
        record = first if number == 1 else _next_record(records)
        if (
            record["table"] != table
            or record["row_number"] != row_number
            or record["column_name"] != column
            or record["sqlite_type"] != expected_type
            or record["chunk_number"] != number
            or record["total_chunks"] != total
            or record["payload_encoding"] != expected_encoding
            or record["source_rowid"] != source_rowid
        ):
            raise ExportVerificationError("cell or source_rowid chunk metadata disagrees with source")
        payload = record["value_chunk"]
        if expected_type == "null":
            if payload not in (None, ""):
                raise ExportVerificationError("NULL cell contains a payload")
            continue
        if not isinstance(payload, str):
            raise ExportVerificationError("non-NULL payload is not text")
        if not is_base64:
            if not isinstance(expected_data, str):
                raise ExportVerificationError("plain payload has an invalid source value")
            next_offset = text_offset + len(payload)
            if expected_data[text_offset:next_offset] != payload:
                raise ExportVerificationError("plain payload differs from source")
            text_offset = next_offset
        else:
            if not isinstance(expected_data, bytes):
                raise ExportVerificationError("base64 payload has an invalid source value")
            combined = base64_carry + payload
            if number < total:
                if "=" in combined:
                    raise ExportVerificationError("base64 padding appears before final chunk")
                usable = (len(combined) // 4) * 4
                encoded, base64_carry = combined[:usable], combined[usable:]
            else:
                encoded, base64_carry = combined, ""
            try:
                decoded = base64.b64decode(encoded, validate=True) if encoded else b""
            except (ValueError, base64.binascii.Error):
                raise ExportVerificationError("base64 payload is invalid") from None
            next_offset = byte_offset + len(decoded)
            if expected_data[byte_offset:next_offset] != decoded:
                raise ExportVerificationError("decoded payload differs from source")
            byte_offset = next_offset
    if expected_type == "null":
        return total
    if not is_base64 and text_offset != len(expected_data):
        raise ExportVerificationError("plain payload is incomplete")
    if is_base64:
        if base64_carry or byte_offset != len(expected_data):
            raise ExportVerificationError("base64 payload is incomplete")
    return total


def verify_export(conn: sqlite3.Connection, output_dir: str | os.PathLike[str]) -> dict[str, Any]:
    """XLSX全チャンクをsourceのSELECT *と直接照合する。再構成APIには依存しない。"""
    output = Path(output_dir)
    try:
        index_doc = json.loads((output / "index.json").read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError):
        raise ExportVerificationError("index.json is missing or invalid") from None
    if not isinstance(index_doc, dict):
        raise ExportVerificationError("index.json root is invalid")
    if index_doc.get("status") == "failed" or index_doc.get("verification_status") == "failed":
        raise ExportVerificationError("index records a failed verification")
    if index_doc.get("row_identity_format") != _ROW_IDENTITY_FORMAT:
        raise ExportVerificationError("index is missing required source row identity format")

    source_objects = _sqlite_schema_objects(conn)
    if index_doc.get("schema_objects") != source_objects:
        raise ExportVerificationError("schema_objects do not match sqlite_master")
    top_pieces, pieces_by_table = _piece_inventory(index_doc, output)
    source_tables = _target_tables(conn)
    index_tables = index_doc.get("tables", {})
    if set(index_tables) != set(source_tables):
        raise ExportVerificationError("exported table set does not match source")

    exported_rows = 0
    exported_cells = 0
    exported_chunks = 0
    table_count = 0
    verified_piece_count = 0
    rowid_rows_checked = 0
    for table_name in source_tables:
        table_info = index_tables[table_name]
        if not isinstance(table_info, dict):
            raise ExportVerificationError("table metadata is invalid")
        table_identity_columns = [
            description[0]
            for description in (
                conn.execute(f"SELECT * FROM {_quote_identifier(table_name)} LIMIT 0").description
                or ()
            )
        ]
        identity_kind, identity_alias = _source_row_identity(
            conn, table_name, table_identity_columns
        )
        if (
            table_info.get("source_rowid_kind") != identity_kind
            or table_info.get("source_rowid_alias") != identity_alias
        ):
            raise ExportVerificationError("source row identity metadata does not match SQLite schema")
        cursor = _ordered_source_cursor(
            conn, table_name, table_identity_columns, identity_kind, identity_alias
        )
        columns = table_identity_columns
        declared_columns = table_info.get("columns", table_info.get("column_names"))
        if declared_columns != columns or not columns:
            raise ExportVerificationError("exported columns do not match SELECT *")
        if "column_names" in table_info and table_info["column_names"] != columns:
            raise ExportVerificationError("column_names do not match SELECT *")
        if "source_row_count" not in table_info:
            raise ExportVerificationError("source row count is missing")

        pieces = pieces_by_table[table_name]
        chunks_before_table = exported_chunks

        def table_records() -> Iterator[dict[str, Any]]:
            nonlocal verified_piece_count
            for piece in pieces:
                yield from _iter_piece_rows(output / piece["name"], piece)
                verified_piece_count += 1
                if verified_piece_count % 100 == 0:
                    print(
                        f"verification progress: pieces={verified_piece_count} chunks={exported_chunks}",
                        file=sys.stderr,
                    )

        piece_rows: Iterator[dict[str, Any]] = table_records()
        table_rows = 0
        for table_rows, selected_row in enumerate(cursor, 1):
            if len(selected_row) != len(table_identity_columns) + (identity_kind == "rowid"):
                raise ExportVerificationError("SELECT * returned an inconsistent row width")
            row = selected_row[: len(table_identity_columns)]
            source_rowid = selected_row[-1] if identity_kind == "rowid" else None
            if identity_kind == "rowid":
                if type(source_rowid) is not int or not _MIN_ROWID <= source_rowid <= _MAX_ROWID:
                    raise ExportVerificationError("SQLite returned an invalid source rowid")
                rowid_rows_checked += 1
            for column, source_value in zip(columns, row):
                exported_chunks += _verify_cell(
                    piece_rows,
                    table=table_name,
                    row_number=table_rows,
                    column=column,
                    source_value=source_value,
                    source_rowid=source_rowid,
                )
                exported_cells += 1
        if table_rows != table_info["source_row_count"]:
            raise ExportVerificationError("source row count does not match export index")
        if table_info.get("read_row_count", table_rows) != table_rows:
            raise ExportVerificationError("read row count does not match SELECT *")
        try:
            extra_record = next(piece_rows)
        except StopIteration:
            extra_record = None
        if extra_record is not None:
            raise ExportVerificationError("XLSX contains extra or duplicate cell chunks")
        table_chunk_count = exported_chunks - chunks_before_table
        if table_chunk_count != sum(piece["chunk_count"] for piece in pieces):
            raise ExportVerificationError("piece chunk totals do not match source cells")
        if table_info.get("total_chunks") != table_chunk_count:
            raise ExportVerificationError("table chunk count does not match index")
        expected_cells = table_rows * len(columns)
        if table_info.get("source_cell_count", expected_cells) != expected_cells:
            raise ExportVerificationError("source cell count does not match SELECT *")
        if table_info.get("read_cell_count", expected_cells) != expected_cells:
            raise ExportVerificationError("read cell count does not match SELECT *")
        exported_rows += table_rows
        table_count += 1

    files: list[dict[str, Any]] = []
    for piece in top_pieces:
        path = output / piece["name"]
        size, digest = _hash_file(path)
        if size != piece.get("bytes") or digest != piece.get("sha256"):
            raise ExportVerificationError("piece byte count or SHA-256 does not match index")
        files.append({"name": piece["name"], "bytes": size, "sha256": digest})

    source_path = _connection_path(conn)
    source_bytes: int | None = None
    if source_path is not None:
        source_bytes = source_path.stat().st_size
    return {
        "exported_tables": table_count,
        "exported_rows": exported_rows,
        "exported_cells": exported_cells,
        "chunks": exported_chunks,
        "pieces": len(top_pieces),
        "source_rowids_verified": True,
        "source_rowid_rows_checked": rowid_rows_checked,
        "source_bytes": source_bytes,
        "files": files,
    }


def _validate_output_path(db_path: Path, output_arg: str | os.PathLike[str]) -> tuple[Path, bool]:
    output = _absolute(output_arg)
    effective_output = output.resolve(strict=False)
    if effective_output == db_path or db_path in effective_output.parents or effective_output in db_path.parents:
        raise ValueError("source and output paths overlap")
    if output.is_symlink():
        raise ValueError("output directory must not be a symbolic link")
    if output.exists():
        if not stat.S_ISDIR(output.lstat().st_mode):
            raise ValueError("output must be a directory")
        if any(output.iterdir()):
            raise ValueError("output directory must be empty")
    else:
        if not output.parent.is_dir():
            raise ValueError("output parent directory must already exist")
        output.mkdir(mode=0o700)
    os.chmod(output, 0o700)
    return output, True


def export_full_xlsx(
    db_path: str | os.PathLike[str],
    output_arg: str | os.PathLike[str],
    *,
    snapshot_identifier: str,
    max_rows_per_sheet: int = 200_000,
    max_zip_bytes: int = 20_000_000,
) -> dict[str, Any]:
    """静止DBからXLSXを生成し、完了証拠を最後に記録する。"""
    db = _absolute(db_path)
    output: Path | None = None
    output_ready = False
    phase = "input_validation"
    source_before: dict[str, Any] | None = None
    counts: dict[str, Any] | None = None
    connection: sqlite3.Connection | None = None
    try:
        if not Path(db_path).is_absolute():
            raise ValueError("--db must be an absolute path")
        if not isinstance(snapshot_identifier, str):
            raise ValueError("snapshot identifier must be a string")
        if max_rows_per_sheet <= 0 or max_zip_bytes <= 0:
            raise ValueError("sheet and ZIP limits must be positive")
        output, output_ready = _validate_output_path(db, output_arg)

        phase = "source_validation"
        _check_static_source(db)
        source_size, source_sha = _hash_file(db)
        source_before = {"path": str(db), "bytes": source_size, "sha256": source_sha}

        phase = "source_open"
        connection = sqlite3.connect(_source_uri(db), uri=True)
        connection.execute("PRAGMA query_only = ON")
        connection.execute("BEGIN")
        opened_path = _connection_path(connection)
        if opened_path is None or _absolute(opened_path) != db:
            raise ValueError("SQLite opened a different source path")
        connection.execute("SELECT COUNT(*) FROM sqlite_master").fetchone()
        _preflight_table_names(connection)

        phase = "xlsx_export"
        progress_stop = threading.Event()
        progress_thread = threading.Thread(
            target=_export_progress_reporter,
            args=(output, progress_stop),
            name="xlsx-export-progress",
            daemon=True,
        )
        progress_thread.start()
        try:
            export_sqlite_tables(
                connection,
                output,
                snapshot_identifier=snapshot_identifier,
                max_rows_per_sheet=max_rows_per_sheet,
                max_zip_bytes=max_zip_bytes,
            )
        finally:
            progress_stop.set()
            progress_thread.join()

        phase = "independent_verification"
        counts = verify_export(connection, output)
        _check_static_source(db)
        source_size_after, source_sha_after = _hash_file(db)
        if (source_size_after, source_sha_after) != (source_size, source_sha):
            raise ExportVerificationError("source bytes changed during export")
        source_after = {"path": str(db), "bytes": source_size_after, "sha256": source_sha_after}
        if counts["source_bytes"] != source_size:
            raise ExportVerificationError("SQLite source identity changed during verification")

        verified_at = _utc_now()
        index_path = output / "index.json"
        index_doc = json.loads(index_path.read_text(encoding="utf-8"))
        expected_id = index_doc.get("snapshot_identifier", index_doc.get("snapshot_id"))
        if expected_id != snapshot_identifier:
            raise ExportVerificationError("snapshot identifier in index does not match request")
        index_doc["snapshot_identifier"] = snapshot_identifier
        index_doc["snapshot_id"] = snapshot_identifier
        index_doc["snapshot_sha256"] = source_sha
        index_doc["source"] = source_after
        index_doc["counts"] = {
            key: value for key, value in counts.items() if key != "files"
        }
        index_doc["status"] = "verified"
        index_doc["verification_status"] = "verified"
        index_doc["verified_at"] = verified_at
        index_doc["verification_receipt"] = "verification.json"
        _atomic_json(index_path, index_doc)

        output_files: list[dict[str, Any]] = []
        for item in counts["files"]:
            output_files.append(item)
        index_size, index_sha = _hash_file(index_path)
        output_files.append({"name": "index.json", "bytes": index_size, "sha256": index_sha})
        manifest = {
            "version": 1,
            "status": "verified",
            "snapshot_identifier": snapshot_identifier,
            "snapshot_sha256": source_sha,
            "source": source_after,
            "verified_at": verified_at,
            "counts": index_doc["counts"],
            "files": output_files,
            "verification_receipt": "verification.json",
        }
        _atomic_json(output / "manifest.json", manifest)

        receipt = {
            "version": 1,
            "status": "verified",
            "source_rowids_verified": counts["source_rowids_verified"],
            "snapshot_identifier": snapshot_identifier,
            "snapshot_sha256": source_sha,
            "source": source_after,
            "verified_at": verified_at,
            "counts": index_doc["counts"],
            "files": output_files,
            "manifest": "manifest.json",
            "index": "index.json",
        }
        _atomic_json(output / "verification.json", receipt)
        return {"output": str(output), "source": source_after, "counts": index_doc["counts"], "files": output_files}
    except Exception as exc:
        if output_ready and output is not None:
            try:
                _mark_failed(
                    output,
                    phase=phase,
                    error=exc,
                    snapshot_identifier=snapshot_identifier,
                    source_info=source_before,
                    counts=counts,
                )
            except OSError:
                pass
        raise
    finally:
        if connection is not None:
            connection.close()


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Verify and export every SQLite table to lossless XLSX pieces.")
    parser.add_argument("--db", required=True, help="existing absolute SQLite snapshot path")
    parser.add_argument("--output", required=True, help="new empty output directory")
    parser.add_argument("--snapshot-id", required=True, help="opaque snapshot identifier stored in JSON metadata")
    parser.add_argument("--max-rows-per-sheet", type=int, default=200_000)
    parser.add_argument("--max-zip-bytes", type=int, default=20_000_000)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    try:
        result = export_full_xlsx(
            args.db,
            args.output,
            snapshot_identifier=args.snapshot_id,
            max_rows_per_sheet=args.max_rows_per_sheet,
            max_zip_bytes=args.max_zip_bytes,
        )
    except Exception as exc:
        print(f"XLSX export failed: {type(exc).__name__}", file=sys.stderr)
        return 2
    summary = {
        "status": "verified",
        "output": result["output"],
        "source_bytes": result["source"]["bytes"],
        "source_sha256": result["source"]["sha256"],
        "counts": result["counts"],
        "files": len(result["files"]),
    }
    print(json.dumps(summary, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
