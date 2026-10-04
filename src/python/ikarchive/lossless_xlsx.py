"""SQLiteテーブルを.xlsxへ完全可逆（ロスレス）にエクスポートする。

正本データベースの全通常テーブル（値が存在するsqlite_sequenceを含む）を、
セル文字数制限（32,767文字）を超えないよう<=16,000文字のチャンクに分割し、
縦持ち形式（1行につき1チャンク）で.xlsx（OpenXML）ファイル群およびindex.jsonへ保存する。
"""

import base64
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import tempfile
from typing import Any, Generator, Optional, Union
import xml.etree.ElementTree as ET
import zipfile
import zlib

CHUNK_SIZE = 16000
CELL_LIMIT = 32767
MAX_BUFFERED_XML_BYTES = 64 * 1024 * 1024

NS_MAIN = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
NS_REL = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"
NS_PKG = "http://schemas.openxmlformats.org/package/2006/relationships"
NS_CT = "http://schemas.openxmlformats.org/package/2006/content-types"

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

ROW_IDENTITY_FORMAT = "source_rowid_column_v1"
_ROWID_ALIASES = ("_rowid_", "rowid", "oid")
_MIN_ROWID = -(1 << 63)
_MAX_ROWID = (1 << 63) - 1
_SQLITE_STAT_TABLES = frozenset({"sqlite_stat1", "sqlite_stat4"})
_SUPPORTED_INTERNAL_TABLES = frozenset({"sqlite_sequence"}) | _SQLITE_STAT_TABLES

CONTENT_TYPES_XML = f"""<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Types xmlns="{NS_CT}">
  <Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>
  <Default Extension="xml" ContentType="application/xml"/>
  <Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>
  <Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>
</Types>"""

ROOT_RELS_XML = f"""<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="{NS_PKG}">
  <Relationship Id="rId1" Type="{NS_REL}/officeDocument" Target="xl/workbook.xml"/>
</Relationships>"""

WORKBOOK_RELS_XML = f"""<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="{NS_PKG}">
  <Relationship Id="rId1" Type="{NS_REL}/worksheet" Target="worksheets/sheet1.xml"/>
</Relationships>"""

HEADER_ROW_XML = (
    '<row r="1">'
    '<c r="A1" t="inlineStr"><is><t xml:space="preserve">table</t></is></c>'
    '<c r="B1" t="inlineStr"><is><t xml:space="preserve">row_number</t></is></c>'
    '<c r="C1" t="inlineStr"><is><t xml:space="preserve">column_name</t></is></c>'
    '<c r="D1" t="inlineStr"><is><t xml:space="preserve">sqlite_type</t></is></c>'
    '<c r="E1" t="inlineStr"><is><t xml:space="preserve">chunk_number</t></is></c>'
    '<c r="F1" t="inlineStr"><is><t xml:space="preserve">total_chunks</t></is></c>'
    '<c r="G1" t="inlineStr"><is><t xml:space="preserve">value_chunk</t></is></c>'
    '<c r="H1" t="inlineStr"><is><t xml:space="preserve">payload_encoding</t></is></c>'
    '<c r="I1" t="inlineStr"><is><t xml:space="preserve">source_rowid</t></is></c>'
    "</row>\n"
)

SHEET_PREFIX = (
    f'<?xml version="1.0" encoding="UTF-8" standalone="yes"?>\n'
    f'<worksheet xmlns="{NS_MAIN}"><sheetData>\n'
    f"{HEADER_ROW_XML}"
).encode("utf-8")
SHEET_FOOTER = b"</sheetData></worksheet>\n"


def check_xml_safe(text: str) -> None:
    """XML 1.0で表現不可能なコードポイントを検査し、存在する場合は例外を送出する。"""
    for ch in text:
        cp = ord(ch)
        if 0xD800 <= cp <= 0xDFFF:
            raise ValueError(
                f"Surrogate U+{cp:04X} in SQLite text cannot be encoded as UTF-8"
            )
        # XML 1.0 Fifth Edition:
        # #x9 | #xA | #xD | [#x20-#xD7FF] | [#xE000-#xFFFD] | [#x10000-#x10FFFF]
        if (
            cp == 0x09
            or cp == 0x0A
            or cp == 0x0D
            or (0x20 <= cp <= 0xD7FF)
            or (0xE000 <= cp <= 0xFFFD)
            or (0x10000 <= cp <= 0x10FFFF)
        ):
            continue
        raise ValueError(
            f"Unrepresentable XML codepoint U+{cp:04X} in text: cannot be represented losslessly in XML 1.0"
        )


def escape_xml(text: str) -> str:
    """Excelのxstring表記とXMLエスケープを適用する。"""
    text = _excel_xstring_escape(text)
    return (
        text.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace("\r", "&#xD;")
    )


_EXCEL_ESCAPE_RE = re.compile(r"_x[0-9A-Fa-f]{4}_")
_UNSAFE_ASCII_XML_RE = re.compile(r"[\x00-\x08\x0B\x0C\x0E-\x1F]")


def _xml_char_allowed(cp: int) -> bool:
    return (
        cp in (0x09, 0x0A, 0x0D)
        or 0x20 <= cp <= 0xD7FF
        or 0xE000 <= cp <= 0xFFFD
        or 0x10000 <= cp <= 0x10FFFF
    )


def _excel_xstring_escape(text: str) -> str:
    """ST_Xstringのescape tokenを1段だけ安全に出力する。"""
    if (
        type(text) is str
        and text.isascii()
        and _EXCEL_ESCAPE_RE.search(text) is None
        and _UNSAFE_ASCII_XML_RE.search(text) is None
    ):
        return text
    out: list[str] = []
    i = 0
    while i < len(text):
        if text[i] == "_":
            match = _EXCEL_ESCAPE_RE.match(text, i)
            if match is not None:
                # Excel would otherwise interpret a literal token as an escape.
                out.append("_x005F_")
                out.append(text[i + 1 : match.end()])
                i = match.end()
                continue
        ch = text[i]
        cp = ord(ch)
        if 0xD800 <= cp <= 0xDFFF:
            raise ValueError(
                f"Surrogate U+{cp:04X} in SQLite text cannot be encoded as UTF-8"
            )
        if not _xml_char_allowed(cp):
            out.append(f"_x{cp:04X}_")
        else:
            out.append(ch)
        i += 1
    return "".join(out)


def _excel_xstring_unescape(text: str) -> str:
    """Excel escape tokenを再帰せず一度だけ復号する。"""
    if type(text) is str and _EXCEL_ESCAPE_RE.search(text) is None:
        return text
    out: list[str] = []
    i = 0
    while i < len(text):
        match = _EXCEL_ESCAPE_RE.match(text, i) if text[i] == "_" else None
        if match is None:
            out.append(text[i])
            i += 1
            continue
        code = int(text[i + 2 : i + 6], 16)
        if code == 0x005F:
            out.append("_")
        else:
            out.append(chr(code))
        i = match.end()
    return "".join(out)


def _zip_entry(name: str) -> zipfile.ZipInfo:
    """決定論的ZIPエントリを生成する。"""
    info = zipfile.ZipInfo(filename=name, date_time=(1980, 1, 1, 0, 0, 0))
    info.compress_type = zipfile.ZIP_DEFLATED
    info.external_attr = 0o600 << 16
    return info


def _sanitize_sheet_name(name: str) -> str:
    """Excelのシート名制約（最大31文字、特殊文字禁止）に合わせる。"""
    cleaned = "".join(c if c not in r":\/?*[]" else "_" for c in name)
    cleaned = cleaned[:31].strip()
    return cleaned if cleaned else "data"


def _format_cell_xml(
    ref: str,
    value: Any,
    sqlite_type: str,
    payload_encoding: str = "plain",
) -> str:
    if sqlite_type == "null" or value is None:
        return f'<c r="{ref}"/>'
    if sqlite_type == "integer":
        return f'<c r="{ref}" t="inlineStr"><is><t xml:space="preserve">{value}</t></is></c>'
    if sqlite_type in ("real", "float"):
        return f'<c r="{ref}" t="inlineStr"><is><t xml:space="preserve">{value}</t></is></c>'
    # text or blob
    escaped = escape_xml(str(value))
    return f'<c r="{ref}" t="inlineStr"><is><t xml:space="preserve">{escaped}</t></is></c>'


def _format_row_xml(row_idx: int, chunk_row: tuple) -> str:
    if len(chunk_row) == 7:
        table, row_num, col_name, sq_type, chunk_num, total_chunks, val_chunk = chunk_row
        payload_encoding = "plain"
        source_rowid = None
    elif len(chunk_row) == 8:
        (
            table,
            row_num,
            col_name,
            sq_type,
            chunk_num,
            total_chunks,
            val_chunk,
            payload_encoding,
        ) = chunk_row
        source_rowid = None
    elif len(chunk_row) == 9:
        (
            table,
            row_num,
            col_name,
            sq_type,
            chunk_num,
            total_chunks,
            val_chunk,
            payload_encoding,
            source_rowid,
        ) = chunk_row
    else:
        raise ValueError(f"chunk row must have 7, 8, or 9 fields, got {len(chunk_row)}")
    if payload_encoding not in ("plain", "utf8-base64"):
        raise ValueError(f"Unknown payload_encoding {payload_encoding!r}")
    if payload_encoding == "utf8-base64" and sq_type != "text":
        raise ValueError("utf8-base64 is only valid for text payloads")
    esc_table = escape_xml(table)
    esc_col = escape_xml(col_name)
    esc_type = escape_xml(sq_type)
    cell_g = _format_cell_xml(f"G{row_idx}", val_chunk, sq_type, payload_encoding)
    cell_h = _format_cell_xml(f"H{row_idx}", payload_encoding, "text")
    if source_rowid is not None:
        if type(source_rowid) is not int or not _MIN_ROWID <= source_rowid <= _MAX_ROWID:
            raise ValueError("source_rowid must be a signed 64-bit integer or None")
        rowid_text = str(source_rowid)
    else:
        rowid_text = ""
    cell_i = _format_cell_xml(f"I{row_idx}", rowid_text, "text")
    return (
        f'<row r="{row_idx}">'
        f'<c r="A{row_idx}" t="inlineStr"><is><t xml:space="preserve">{esc_table}</t></is></c>'
        f'<c r="B{row_idx}"><v>{row_num}</v></c>'
        f'<c r="C{row_idx}" t="inlineStr"><is><t xml:space="preserve">{esc_col}</t></is></c>'
        f'<c r="D{row_idx}" t="inlineStr"><is><t xml:space="preserve">{esc_type}</t></is></c>'
        f'<c r="E{row_idx}"><v>{chunk_num}</v></c>'
        f'<c r="F{row_idx}"><v>{total_chunks}</v></c>'
        f"{cell_g}"
        f"{cell_h}"
        f"{cell_i}"
        f"</row>\n"
    )


def _calc_zip_overhead(table_name: str) -> int:
    """テーブル固有の固定エントリおよびZIPヘッダ/ディレクトリのバイト総数を計算する。"""
    sheet_name = "data"
    wb_xml = f"""<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<workbook xmlns="{NS_MAIN}" xmlns:r="{NS_REL}">
  <sheets>
    <sheet name="{sheet_name}" sheetId="1" r:id="rId1"/>
  </sheets>
</workbook>"""

    entries = [
        ("[Content_Types].xml", CONTENT_TYPES_XML.encode("utf-8")),
        ("_rels/.rels", ROOT_RELS_XML.encode("utf-8")),
        ("xl/workbook.xml", wb_xml.encode("utf-8")),
        ("xl/_rels/workbook.xml.rels", WORKBOOK_RELS_XML.encode("utf-8")),
    ]
    total = 0
    for fname, data in entries:
        c = zlib.compressobj(level=6, wbits=-15)
        c_len = len(c.compress(data)) + len(c.flush(zlib.Z_FINISH))
        total += 30 + len(fname) + c_len + 46 + len(fname)

    s_name = "xl/worksheets/sheet1.xml"
    total += 30 + len(s_name) + 46 + len(s_name) + 22
    return total


def _write_part_workbook(
    part_path: Path,
    table_name: str,
    rows: list[tuple],
    max_zip_bytes: int,
    *,
    raise_on_overflow: bool = True,
) -> tuple[Optional[str], int]:
    sheet_name = "data"
    workbook_xml = f"""<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<workbook xmlns="{NS_MAIN}" xmlns:r="{NS_REL}">
  <sheets>
    <sheet name="{sheet_name}" sheetId="1" r:id="rId1"/>
  </sheets>
</workbook>"""

    fd, tmp_name = tempfile.mkstemp(
        prefix=f"{part_path.name}.",
        suffix=".tmp",
        dir=part_path.parent,
    )
    os.close(fd)
    tmp_path = Path(tmp_name)
    try:
        with zipfile.ZipFile(tmp_path, "w", compression=zipfile.ZIP_DEFLATED) as z:
            z.writestr(_zip_entry("[Content_Types].xml"), CONTENT_TYPES_XML.encode("utf-8"))
            z.writestr(_zip_entry("_rels/.rels"), ROOT_RELS_XML.encode("utf-8"))
            z.writestr(_zip_entry("xl/workbook.xml"), workbook_xml.encode("utf-8"))
            z.writestr(_zip_entry("xl/_rels/workbook.xml.rels"), WORKBOOK_RELS_XML.encode("utf-8"))

            sheet_info = _zip_entry("xl/worksheets/sheet1.xml")
            with z.open(sheet_info, "w") as sf:
                sf.write(b'<?xml version="1.0" encoding="UTF-8" standalone="yes"?>\n')
                sf.write(f'<worksheet xmlns="{NS_MAIN}"><sheetData>\n'.encode("utf-8"))
                sf.write(HEADER_ROW_XML.encode("utf-8"))

                buf: list[str] = []
                buf_bytes = 0
                for r_idx, chunk_row in enumerate(rows, 2):
                    row_xml = _format_row_xml(r_idx, chunk_row)
                    buf.append(row_xml)
                    buf_bytes += len(row_xml.encode("utf-8"))
                    if len(buf) >= 1000 or buf_bytes >= 1024 * 1024:
                        sf.write("".join(buf).encode("utf-8"))
                        buf.clear()
                        buf_bytes = 0
                if buf:
                    sf.write("".join(buf).encode("utf-8"))
                    buf.clear()

                sf.write(b"</sheetData></worksheet>\n")

        file_size = tmp_path.stat().st_size
        if file_size > max_zip_bytes:
            if raise_on_overflow:
                raise ValueError(
                    f"Generated workbook {part_path.name} ({file_size} bytes) exceeds limit max_zip_bytes={max_zip_bytes}"
                )
            return None, file_size

        h = hashlib.sha256()
        with open(tmp_path, "rb") as f:
            while block := f.read(65536):
                h.update(block)
        sha256_hex = h.hexdigest()

        tmp_path.chmod(0o600)
        tmp_path.replace(part_path)
        return sha256_hex, file_size
    finally:
        tmp_path.unlink(missing_ok=True)


def _write_part_with_retry(
    output_dir: Path,
    table_name: str,
    part_index: int,
    rows: list[tuple],
    max_zip_bytes: int,
    *,
    table_index: int = 1,
) -> tuple[dict, list[tuple]]:
    """決定論的2分割（halves）再試行によりmax_zip_bytes以内のピースを出力する。
    1チャンクすら収まらない場合のみValueErrorを送出する。
    """
    candidate = rows
    _utf8_length_and_validate(table_name)
    table_digest = hashlib.sha256(table_name.encode("utf-8")).hexdigest()[:12]
    piece_name = f"t{table_index:04d}_{table_digest}_{part_index:04d}.xlsx"
    part_path = output_dir / piece_name

    while True:
        can_halve = len(candidate) > 1
        sha, size = _write_part_workbook(
            part_path,
            table_name,
            candidate,
            max_zip_bytes,
            raise_on_overflow=not can_halve,
        )
        if sha is not None:
            piece_info = {
                "table": table_name,
                "name": piece_name,
                "chunk_count": len(candidate),
                "sha256": sha,
                "bytes": size,
            }
            remaining = rows[len(candidate):]
            return piece_info, remaining

        candidate = candidate[: len(candidate) // 2]


def _encode_cell_chunks(
    table_name: str,
    row_number: int,
    column_name: str,
    val: Any,
    chunk_size: int = CHUNK_SIZE,
    *,
    source_rowid: Optional[int] = None,
) -> Generator[tuple, None, None]:
    def emit(sqlite_type: str, chunk_num: int, total: int, value: Any, encoding: str = "plain"):
        return (
            table_name,
            row_number,
            column_name,
            sqlite_type,
            chunk_num,
            total,
            value,
            encoding,
            source_rowid,
        )

    if (
        type(val) is str
        and type(chunk_size) is int
        and chunk_size > 0
        and val.isascii()
        and _UNSAFE_ASCII_XML_RE.search(val) is None
    ):
        effective_chunk_size = min(chunk_size, CELL_LIMIT)
        total = max(1, (len(val) + effective_chunk_size - 1) // effective_chunk_size)
        if not val:
            yield emit("text", 1, 1, "")
        else:
            for chunk_number, offset in enumerate(
                range(0, len(val), effective_chunk_size), 1
            ):
                yield emit(
                    "text",
                    chunk_number,
                    total,
                    val[offset : offset + effective_chunk_size],
                )
        return

    if val is None:
        yield emit("null", 1, 1, None)
    elif isinstance(val, bool):
        int_val = 1 if val else 0
        yield emit("integer", 1, 1, int_val)
    elif isinstance(val, int):
        yield emit("integer", 1, 1, val)
    elif isinstance(val, float):
        yield emit("real", 1, 1, float.hex(val))
    elif isinstance(val, str):
        utf8_bytes = _utf8_length_and_validate(val)
        if _requires_xml_base64(val):
            encoded_length = 4 * ((utf8_bytes + 2) // 3)
            total = max(1, (encoded_length + chunk_size - 1) // chunk_size)
            encoded_chunks = _iter_base64_text_chunks(val, chunk_size)
            emitted = 0
            for emitted, c in enumerate(encoded_chunks, 1):
                yield emit("text", emitted, total, c, "utf8-base64")
            if emitted == 0:
                yield emit("text", 1, 1, "", "utf8-base64")
            elif emitted != total:
                raise AssertionError(f"UTF-8 base64 chunk count mismatch: {emitted} != {total}")
        else:
            chunks = _iter_text_chunks(val, chunk_size)
            if len(val) == 0:
                yield emit("text", 1, 1, "")
            else:
                total = _text_chunk_count(val, chunk_size)
                emitted = 0
                for emitted, c in enumerate(chunks, 1):
                    yield emit("text", emitted, total, c)
                if emitted != total:
                    raise AssertionError(f"Text chunk count mismatch: {emitted} != {total}")
    elif isinstance(val, (bytes, bytearray, memoryview)):
        raw_bytes = bytes(val)
        encoded_length = 4 * ((len(raw_bytes) + 2) // 3)
        total = max(1, (encoded_length + chunk_size - 1) // chunk_size)
        encoded_chunks = _iter_base64_bytes_chunks(raw_bytes, chunk_size)
        emitted = 0
        for emitted, c in enumerate(encoded_chunks, 1):
            yield emit("blob", emitted, total, c)
        if emitted == 0:
            yield emit("blob", 1, 1, "")
        elif emitted != total:
            raise AssertionError(f"BLOB base64 chunk count mismatch: {emitted} != {total}")
    else:
        raise TypeError(f"Unsupported SQLite value type for {column_name}: {type(val)}")


def _utf8_length_and_validate(text: str) -> int:
    total = 0
    for ch in text:
        cp = ord(ch)
        if 0xD800 <= cp <= 0xDFFF:
            raise ValueError(
                f"Surrogate U+{cp:04X} in SQLite text cannot be encoded as UTF-8"
            )
        if cp <= 0x7F:
            total += 1
        elif cp <= 0x7FF:
            total += 2
        elif cp <= 0xFFFF:
            total += 3
        else:
            total += 4
    return total


def _requires_xml_base64(text: str) -> bool:
    return any(not _xml_char_allowed(ord(ch)) for ch in text)


def _iter_text_chunks(text: str, chunk_size: int) -> Generator[str, None, None]:
    """文字数とUTF-16単位数の両方を制限し、サロゲートペアを分断しない。"""
    if not text:
        return
    current: list[str] = []
    chars = 0
    utf16_units = 0
    for ch in text:
        cp = ord(ch)
        units = 2 if cp > 0xFFFF else 1
        if chars and (chars >= chunk_size or utf16_units + units > CELL_LIMIT):
            yield "".join(current)
            current = []
            chars = 0
            utf16_units = 0
        current.append(ch)
        chars += 1
        utf16_units += units
    if current:
        yield "".join(current)


def _text_chunk_count(text: str, chunk_size: int) -> int:
    return sum(1 for _ in _iter_text_chunks(text, chunk_size))


def _iter_base64_bytes_chunks(raw: bytes, chunk_size: int) -> Generator[str, None, None]:
    """BLOBのbase64を全体複製せず、最大chunk_size文字で分割する。"""
    pending = ""
    carry = b""
    source_step = max(3, (chunk_size // 4) * 3)
    for start in range(0, len(raw), source_step):
        data = carry + raw[start : start + source_step]
        complete = (len(data) // 3) * 3
        if complete:
            pending += base64.b64encode(data[:complete]).decode("ascii")
            emit_length = (len(pending) // chunk_size) * chunk_size
            for offset in range(0, emit_length, chunk_size):
                yield pending[offset : offset + chunk_size]
            pending = pending[emit_length:]
        carry = data[complete:]
    if carry:
        pending += base64.b64encode(carry).decode("ascii")
    while pending:
        yield pending[:chunk_size]
        pending = pending[chunk_size:]


def _iter_base64_text_chunks(text: str, chunk_size: int) -> Generator[str, None, None]:
    """UTF-8テキストをbase64化し、文字列全体の符号化コピーを保持しない。"""
    pending = ""
    carry = b""
    for text_piece in _iter_text_chunks(text, max(1, min(chunk_size, 16000))):
        data = carry + text_piece.encode("utf-8")
        complete = (len(data) // 3) * 3
        if complete:
            pending += base64.b64encode(data[:complete]).decode("ascii")
            emit_length = (len(pending) // chunk_size) * chunk_size
            for offset in range(0, emit_length, chunk_size):
                yield pending[offset : offset + chunk_size]
            pending = pending[emit_length:]
        carry = data[complete:]
    if carry:
        pending += base64.b64encode(carry).decode("ascii")
    while pending:
        yield pending[:chunk_size]
        pending = pending[chunk_size:]


def _get_target_tables(source: sqlite3.Connection) -> list[tuple[str, str]]:
    cur = source.cursor()
    cur.execute(
        "SELECT name, sql FROM sqlite_master WHERE type='table' ORDER BY name"
    )
    tables = cur.fetchall()
    virtual_tables = [
        name
        for name, sql in tables
        if isinstance(sql, str)
        and re.match(r"\s*CREATE\s+VIRTUAL\s+TABLE\b", sql, re.IGNORECASE)
    ]
    if virtual_tables:
        raise ValueError(
            "Virtual tables are not supported by lossless XLSX reconstruction; "
            f"refusing to report a complete export: {virtual_tables!r}"
        )
    results = []
    for name, sql in tables:
        if name.startswith("sqlite_") and name not in _SUPPORTED_INTERNAL_TABLES:
            raise ValueError(
                "Internal SQLite table is not supported by lossless XLSX "
                f"reconstruction; refusing to report a complete export: {name!r}"
            )
        if not sql:
            if name == "sqlite_sequence":
                sql = "CREATE TABLE sqlite_sequence(name,seq)"
            else:
                raise ValueError(
                    f"SQLite table {name!r} has no CREATE TABLE SQL; "
                    "refusing to report a complete export"
                )
        results.append((name, sql))
    return results


def _get_table_xinfo(source: sqlite3.Connection, table_name: str) -> list[dict[str, Any]]:
    cur = source.cursor()
    quoted = '"' + table_name.replace('"', '""') + '"'
    info = cur.execute(f"PRAGMA table_xinfo({quoted})").fetchall()
    return [
        {
            "cid": row[0],
            "name": row[1],
            "type": row[2],
            "notnull": row[3],
            "dflt_value": row[4],
            "pk": row[5],
            "hidden": row[6],
        }
        for row in info
    ]


def _get_table_columns(source: sqlite3.Connection, table_name: str) -> list[str]:
    info = _get_table_xinfo(source, table_name)
    columns = [row["name"] for row in info if row["hidden"] != 1]
    if columns:
        return columns
    quoted = '"' + table_name.replace('"', '""') + '"'
    cur = source.cursor()
    cur.execute(f"SELECT * FROM {quoted} LIMIT 0")
    return [col[0] for col in cur.description] if cur.description else []


def _get_source_row_identity(
    source: sqlite3.Connection, table_name: str, columns: list[str]
) -> tuple[str, Optional[str]]:
    """Identify a pseudo-rowid only when SQLite exposes an unshadowed alias."""
    table_flags = {
        row[1]: row
        for row in source.execute("PRAGMA table_list")
        if len(row) >= 6
    }
    table_info = table_flags.get(table_name)
    if table_info is not None and bool(table_info[4]):
        return "without_rowid", None
    names = {name.casefold() for name in columns}
    for alias in _ROWID_ALIASES:
        if alias.casefold() not in names:
            return "rowid", alias
    return "shadowed", None


def _table_primary_key_columns(
    source: sqlite3.Connection, table_name: str
) -> list[str]:
    info = _get_table_xinfo(source, table_name)
    return [
        item["name"]
        for item in sorted((column for column in info if column["pk"]), key=lambda item: item["pk"])
    ]


def _get_schema_objects(source: sqlite3.Connection) -> list[dict[str, Any]]:
    rows = source.execute(
        "SELECT type, name, tbl_name, sql FROM sqlite_master ORDER BY type, name"
    ).fetchall()
    return [
        {"name": name, "type": obj_type, "tbl_name": tbl_name, "sql": sql}
        for obj_type, name, tbl_name, sql in rows
    ]


def _iter_table_chunks(
    source: sqlite3.Connection,
    table_name: str,
    columns: list[str],
    chunk_size: int = CHUNK_SIZE,
    *,
    stats: Optional[dict[str, int]] = None,
    source_rowid_kind: str,
    source_rowid_alias: Optional[str],
) -> Generator[tuple, None, None]:
    quoted_table = '"' + table_name.replace('"', '""') + '"'
    select = f"SELECT * FROM {quoted_table}"
    if source_rowid_kind == "rowid":
        if source_rowid_alias not in _ROWID_ALIASES:
            raise ValueError(f"Invalid rowid alias for {table_name!r}")
        quoted_alias = '"' + source_rowid_alias.replace('"', '""') + '"'
        select = f"SELECT *, {quoted_alias} FROM {quoted_table} ORDER BY {quoted_alias}"
    elif source_rowid_kind not in ("without_rowid", "shadowed") or source_rowid_alias is not None:
        raise ValueError(f"Invalid row identity metadata for {table_name!r}")
    else:
        primary_key = _table_primary_key_columns(source, table_name)
        if primary_key:
            order = ", ".join(
                '"' + column.replace('"', '""') + '"' for column in primary_key
            )
            select += f" ORDER BY {order}"
    cursor = source.execute(select)
    selected_columns = [description[0] for description in cursor.description or ()]
    if (
        selected_columns[: len(columns)] != columns
        or len(selected_columns)
        != len(columns) + (1 if source_rowid_kind == "rowid" else 0)
    ):
        raise ValueError(
            f"SELECT * column names mismatch for {table_name!r}: "
            f"expected {expected_selected_columns!r}, SELECT returned {selected_columns!r}"
        )
    for row_idx, row in enumerate(cursor, 1):
        if stats is not None:
            stats["read_row_count"] += 1
        if len(row) != len(columns) + (1 if source_rowid_kind == "rowid" else 0):
            raise ValueError(
                f"SELECT * column count mismatch for {table_name!r}: "
                f"expected {len(expected_selected_columns)}, SELECT returned {len(row)}"
            )
        source_rowid = row[-1] if source_rowid_kind == "rowid" else None
        if source_rowid is not None and (
            type(source_rowid) is not int or not _MIN_ROWID <= source_rowid <= _MAX_ROWID
        ):
            raise ValueError(f"SQLite returned an invalid source rowid for {table_name!r}")
        values = row[: len(columns)]
        if stats is not None:
            stats["read_cell_count"] += len(values)
        for col_name, val in zip(columns, values):
            yield from _encode_cell_chunks(
                table_name,
                row_idx,
                col_name,
                val,
                chunk_size,
                source_rowid=source_rowid,
            )


def export_sqlite_tables(
    source: sqlite3.Connection,
    output_dir: Union[str, Path],
    *,
    max_rows_per_sheet: int = 200_000,
    max_zip_bytes: int = 20_000_000,
    snapshot_identifier: Optional[str] = None,
    snapshot_id: Optional[str] = None,
    chunk_size: int = CHUNK_SIZE,
    **extra: Any,
) -> dict:
    """SQLiteの全テーブルを完全に可逆な.xlsxファイル群へエクスポートする。"""
    if not isinstance(source, sqlite3.Connection):
        raise TypeError(f"source must be a sqlite3.Connection, got {type(source)}")
    if max_rows_per_sheet <= 0 or max_rows_per_sheet > 1_048_575:
        raise ValueError(
            f"max_rows_per_sheet must be between 1 and 1048575, got {max_rows_per_sheet}"
        )
    if max_zip_bytes <= 0:
        raise ValueError(f"max_zip_bytes must be > 0, got {max_zip_bytes}")
    if chunk_size <= 0 or chunk_size > CELL_LIMIT:
        raise ValueError(f"chunk_size must be between 1 and {CELL_LIMIT}, got {chunk_size}")

    snap_id = snapshot_identifier if snapshot_identifier is not None else snapshot_id

    output_dir = Path(output_dir)
    if output_dir.is_symlink():
        raise ValueError(f"output_dir must not be a symbolic link: {output_dir}")
    if output_dir.exists():
        if not output_dir.is_dir():
            raise ValueError(f"output_dir exists and is not a directory: {output_dir}")
        if any(output_dir.iterdir()):
            raise ValueError(f"output_dir must be empty: {output_dir}")
    else:
        output_dir.mkdir(parents=True, exist_ok=False)

    tables_info = _get_target_tables(source)
    schema_objects = _get_schema_objects(source)
    schema_table_names = {
        obj["name"] for obj in schema_objects if obj["type"] == "table"
    }
    exported_table_names = {name for name, _ddl in tables_info}
    if schema_table_names != exported_table_names:
        raise ValueError(
            "SQLite table-set coverage mismatch; refusing to report a complete export: "
            f"schema={sorted(schema_table_names)!r}, "
            f"export={sorted(exported_table_names)!r}"
        )

    created_paths: list[Path] = []
    index_tables: dict[str, dict] = {}
    all_pieces: list[dict] = []
    tmp_index_path: Optional[Path] = None

    try:
        for table_index, (table_name, ddl) in enumerate(tables_info, 1):
            table_xinfo = _get_table_xinfo(source, table_name)
            columns = _get_table_columns(source, table_name)
            source_rowid_kind, source_rowid_alias = _get_source_row_identity(
                source, table_name, columns
            )
            quoted_table = '"' + table_name.replace('"', '""') + '"'
            cur = source.execute(f"SELECT COUNT(*) FROM {quoted_table}")
            source_row_count = cur.fetchone()[0]

            overhead = _calc_zip_overhead(table_name)
            part_index = 1
            current_part_rows: list[tuple] = []
            table_pieces: list[dict] = []
            total_table_chunks = 0
            read_stats = {"read_row_count": 0, "read_cell_count": 0}
            buffered_xml_bytes = 0

            compressor = zlib.compressobj(level=6, wbits=-15)
            compressed_bytes = len(compressor.compress(SHEET_PREFIX))

            def _flush_current():
                nonlocal part_index, current_part_rows, compressor, compressed_bytes
                nonlocal buffered_xml_bytes
                if not current_part_rows and len(table_pieces) > 0:
                    return
                remaining = current_part_rows
                while remaining or (len(table_pieces) == 0 and not remaining):
                    piece_info, remaining = _write_part_with_retry(
                        output_dir,
                        table_name,
                        part_index,
                        remaining,
                        max_zip_bytes,
                        table_index=table_index,
                    )
                    created_paths.append(output_dir / piece_info["name"])
                    table_pieces.append(piece_info)
                    all_pieces.append(piece_info)
                    part_index += 1
                    if len(table_pieces) > 0 and not remaining:
                        break
                current_part_rows = []
                compressor = zlib.compressobj(level=6, wbits=-15)
                compressed_bytes = len(compressor.compress(SHEET_PREFIX))
                buffered_xml_bytes = 0

            for chunk_row in _iter_table_chunks(
                source,
                table_name,
                columns,
                chunk_size,
                stats=read_stats,
                source_rowid_kind=source_rowid_kind,
                source_rowid_alias=source_rowid_alias,
            ):
                total_table_chunks += 1
                next_row_idx = len(current_part_rows) + 2
                row_xml = _format_row_xml(next_row_idx, chunk_row).encode("utf-8")

                if (
                    current_part_rows
                    and buffered_xml_bytes + len(row_xml) > MAX_BUFFERED_XML_BYTES
                ):
                    _flush_current()
                    row_xml = _format_row_xml(2, chunk_row).encode("utf-8")

                compressed_bytes += len(compressor.compress(row_xml))
                current_part_rows.append(chunk_row)
                buffered_xml_bytes += len(row_xml)

                should_flush = (
                    len(current_part_rows) >= max_rows_per_sheet
                    or buffered_xml_bytes >= MAX_BUFFERED_XML_BYTES
                )
                if not should_flush and compressed_bytes + overhead >= max_zip_bytes - 64 * 1024:
                    probe = compressor.copy()
                    exact_size = (
                        compressed_bytes
                        + len(probe.compress(SHEET_FOOTER))
                        + len(probe.flush(zlib.Z_FINISH))
                        + overhead
                    )
                    should_flush = exact_size >= max_zip_bytes
                if should_flush:
                    _flush_current()

            if current_part_rows or len(table_pieces) == 0:
                _flush_current()

            source_cell_count = source_row_count * len(columns)
            if read_stats["read_row_count"] != source_row_count:
                raise ValueError(
                    f"Source row count changed while exporting {table_name!r}: "
                    f"COUNT(*)={source_row_count}, SELECT read={read_stats['read_row_count']}"
                )
            if read_stats["read_cell_count"] != source_cell_count:
                raise ValueError(
                    f"Source cell count mismatch for {table_name!r}: "
                    f"expected {source_cell_count}, read {read_stats['read_cell_count']}"
                )
            piece_chunk_count = sum(piece["chunk_count"] for piece in table_pieces)
            if piece_chunk_count != total_table_chunks:
                raise AssertionError(
                    f"Chunk count mismatch for {table_name!r}: "
                    f"generated {total_table_chunks}, pieces {piece_chunk_count}"
                )

            index_tables[table_name] = {
                "table_index": table_index,
                "table": table_name,
                "ddl": ddl,
                "table_ddl": ddl,
                "column_names": columns,
                "columns": columns,
                "column_schema": table_xinfo,
                "source_rowid_alias": source_rowid_alias,
                "source_rowid_kind": source_rowid_kind,
                "source_row_count": source_row_count,
                "source_rows": source_row_count,
                "read_row_count": read_stats["read_row_count"],
                "source_cell_count": source_cell_count,
                "read_cell_count": read_stats["read_cell_count"],
                "total_chunks": total_table_chunks,
                "piece_names": [p["name"] for p in table_pieces],
                "chunk_counts": [p["chunk_count"] for p in table_pieces],
                "sha256": [p["sha256"] for p in table_pieces],
                "pieces": table_pieces,
            }

        index_doc = {
            "version": 2,
            "row_identity_format": ROW_IDENTITY_FORMAT,
            "snapshot_identifier": snap_id,
            "snapshot_id": snap_id,
            "schema_objects": schema_objects,
            "tables": index_tables,
            "piece_names": [p["name"] for p in all_pieces],
            "pieces": all_pieces,
        }

        index_path = output_dir / "index.json"
        fd, tmp_index_name = tempfile.mkstemp(prefix="index.json.", suffix=".tmp", dir=output_dir)
        os.close(fd)
        tmp_index_path = Path(tmp_index_name)
        try:
            with open(tmp_index_path, "w", encoding="utf-8") as f:
                json.dump(index_doc, f, indent=2, sort_keys=True)
                f.write("\n")
            tmp_index_path.replace(index_path)
            tmp_index_path = None
            created_paths.append(index_path)
            index_path.chmod(0o600)
        finally:
            if tmp_index_path is not None:
                tmp_index_path.unlink(missing_ok=True)

        return index_doc

    except Exception:
        # 失敗時は作成中ファイルを全削除してフェイルクローズする
        if tmp_index_path is not None:
            tmp_index_path.unlink(missing_ok=True)
        for p in created_paths:
            p.unlink(missing_ok=True)
        raise


def _iter_lossless_xlsx_rows(xlsx_path: Union[str, Path]) -> Generator[dict[str, Any], None, None]:
    """XLSXピースを1行ずつ読み、チャンク行を返す。"""
    xlsx_path = Path(xlsx_path)
    with zipfile.ZipFile(xlsx_path, "r") as z:
        with z.open("xl/worksheets/sheet1.xml") as sf:
            for event, elem in ET.iterparse(sf, events=("end",)):
                tag = elem.tag.split("}", 1)[1] if elem.tag.startswith("{") else elem.tag
                if tag == "row":
                    r_attr = elem.get("r")
                    if r_attr == "1":
                        elem.clear()
                        continue
                    cells: dict[str, Any] = {}
                    cell_types: dict[str, Optional[str]] = {}
                    for c in elem:
                        c_tag = c.tag.split("}", 1)[1] if c.tag.startswith("{") else c.tag
                        if c_tag != "c":
                            continue
                        ref = c.get("r", "")
                        ref_match = re.fullmatch(r"([A-Z]+)([0-9]+)", ref)
                        if ref_match is None or ref_match.group(2) != r_attr:
                            raise ValueError(
                                f"Invalid cell reference {ref!r} in row {r_attr!r} of {xlsx_path.name}"
                            )
                        col = ref_match.group(1)
                        if col in cells:
                            raise ValueError(f"Duplicate cell {ref} in {xlsx_path.name}")
                        t = c.get("t")
                        cell_types[col] = t
                        val = None
                        if t == "inlineStr":
                            found_text = False
                            for child in c:
                                child_tag = (
                                    child.tag.split("}", 1)[1]
                                    if child.tag.startswith("{")
                                    else child.tag
                                )
                                if child_tag == "is":
                                    for tchild in child:
                                        t_tag = (
                                            tchild.tag.split("}", 1)[1]
                                            if tchild.tag.startswith("{")
                                            else tchild.tag
                                        )
                                        if t_tag == "t":
                                            if found_text:
                                                raise ValueError(
                                                    f"Duplicate inline text in cell {ref} of {xlsx_path.name}"
                                                )
                                            found_text = True
                                            val = tchild.text or ""
                            if not found_text:
                                raise ValueError(
                                    f"Missing inline text in cell {ref} of {xlsx_path.name}"
                                )
                        else:
                            found_value = False
                            for child in c:
                                child_tag = (
                                    child.tag.split("}", 1)[1]
                                    if child.tag.startswith("{")
                                    else child.tag
                                )
                                if child_tag == "v":
                                    if found_value:
                                        raise ValueError(
                                            f"Duplicate numeric value in cell {ref} of {xlsx_path.name}"
                                        )
                                    found_value = True
                                    val = child.text
                        cells[col] = val

                    payload_encoding = _excel_xstring_unescape(
                        str(cells.get("H", "plain"))
                    )
                    if payload_encoding not in ("plain", "utf8-base64"):
                        raise ValueError(
                            f"Unknown payload_encoding {payload_encoding!r} in {xlsx_path.name}"
                        )
                    sq_type = _excel_xstring_unescape(str(cells.get("D", "")))
                    val_chunk = cells.get("G")
                    if payload_encoding == "plain" and val_chunk is not None:
                        val_chunk = _excel_xstring_unescape(str(val_chunk))
                    if sq_type == "null":
                        val_chunk = None
                    elif val_chunk is None:
                        val_chunk = ""

                    row_result = {
                        "table": _excel_xstring_unescape(str(cells.get("A", ""))),
                        "row_number": int(cells.get("B", 0)),
                        "column_name": _excel_xstring_unescape(str(cells.get("C", ""))),
                        "sqlite_type": sq_type,
                        "chunk_number": int(cells.get("E", 1)),
                        "total_chunks": int(cells.get("F", 1)),
                        "value_chunk": val_chunk,
                        "payload_encoding": payload_encoding,
                        "_present_columns": frozenset(cells),
                        "_cell_types": cell_types,
                    }
                    if "I" in cells:
                        if cell_types.get("I") != "inlineStr":
                            raise ValueError(
                                f"source_rowid must be an inline string in {xlsx_path.name}"
                            )
                        source_rowid_text = _excel_xstring_unescape(
                            str(cells.get("I", ""))
                        )
                        if source_rowid_text == "":
                            source_rowid = None
                        else:
                            if not re.fullmatch(r"(?:0|-?[1-9][0-9]*)", source_rowid_text):
                                raise ValueError(
                                    f"Non-canonical source_rowid in {xlsx_path.name}"
                                )
                            source_rowid = int(source_rowid_text)
                            if not _MIN_ROWID <= source_rowid <= _MAX_ROWID:
                                raise ValueError(
                                    f"source_rowid is outside SQLite int64 range in {xlsx_path.name}"
                                )
                        row_result["source_rowid"] = source_rowid
                    yield row_result
                    elem.clear()


def read_lossless_xlsx(xlsx_path: Union[str, Path]) -> list[dict[str, Any]]:
    """ロスレスXLSXピースを読み込み、チャンク行の辞書リストを返す。"""
    return [
        {key: value for key, value in row.items() if not key.startswith("_")}
        for row in _iter_lossless_xlsx_rows(xlsx_path)
    ]


def _reconstruct_value(
    sqlite_type: str,
    chunks: list[Any],
    payload_encoding: str = "plain",
) -> Any:
    if not chunks:
        raise ValueError("A value must contain at least one chunk")
    st = sqlite_type
    if st == "null":
        if len(chunks) != 1 or chunks[0] is not None or payload_encoding != "plain":
            raise ValueError("Invalid null payload")
        return None
    if st == "integer":
        if payload_encoding != "plain" or len(chunks) != 1:
            raise ValueError("Integer payload cannot use encoded text")
        return int(chunks[0])
    if st == "real":
        if payload_encoding != "plain" or len(chunks) != 1:
            raise ValueError("Real payload cannot use encoded text")
        return float.fromhex(str(chunks[0]))
    if st == "text":
        full_text = "".join(str(c) for c in chunks)
        if payload_encoding == "utf8-base64":
            try:
                value = base64.b64decode(full_text.encode("ascii"), validate=True).decode(
                    "utf-8", errors="strict"
                )
            except (UnicodeEncodeError, ValueError, UnicodeDecodeError) as exc:
                raise ValueError("Invalid utf8-base64 text payload") from exc
            _utf8_length_and_validate(value)
            return value
        if payload_encoding != "plain":
            raise ValueError(f"Unknown text payload encoding: {payload_encoding}")
        value = full_text
        _utf8_length_and_validate(value)
        return value
    if st == "blob":
        if payload_encoding != "plain":
            raise ValueError("BLOB payload must use the established base64 representation")
        full_b64 = "".join(str(c) for c in chunks)
        try:
            return base64.b64decode(full_b64.encode("ascii"), validate=True)
        except (UnicodeEncodeError, ValueError) as exc:
            raise ValueError("Invalid BLOB base64 payload") from exc
    raise ValueError(f"Unknown sqlite_type: {sqlite_type}")


def _quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _sqlite_values_equal(expected: Any, actual: Any) -> bool:
    if type(expected) is not type(actual):
        return False
    if isinstance(expected, float):
        return expected.hex() == actual.hex()
    return expected == actual


def _read_and_verify_piece(
    base_dir: Path,
    piece: dict[str, Any],
    *,
    require_bytes: bool,
) -> tuple[Path, int]:
    name = piece.get("name")
    if not isinstance(name, str) or not name or Path(name).name != name:
        raise ValueError(f"Unsafe or missing piece name: {name!r}")
    path = base_dir / name
    if path.is_symlink() or not path.is_file():
        raise ValueError(f"Missing or symbolic-link piece: {name}")
    size = path.stat().st_size
    expected_size = piece.get("bytes")
    if require_bytes and (not isinstance(expected_size, int) or size != expected_size):
        raise ValueError(
            f"Byte count mismatch for piece {name}: expected {expected_size!r}, got {size}"
        )
    expected_sha = piece.get("sha256")
    if not isinstance(expected_sha, str) or len(expected_sha) != 64:
        raise ValueError(f"Missing or invalid SHA-256 for piece {name}")
    digest = hashlib.sha256()
    with open(path, "rb") as stream:
        while block := stream.read(65536):
            digest.update(block)
    actual_sha = digest.hexdigest()
    if actual_sha != expected_sha:
        raise ValueError(f"SHA-256 mismatch for piece {name}")
    return path, size


def _restore_table_rows(
    target_conn: sqlite3.Connection,
    table_name: str,
    table_info: dict[str, Any],
    pieces: list[dict[str, Any]],
    base_dir: Path,
    *,
    strict_v2: bool,
    row_identity_format: Optional[str],
) -> int:
    columns = table_info.get("columns", table_info.get("column_names"))
    if not isinstance(columns, list) or any(not isinstance(c, str) for c in columns):
        raise ValueError(f"Invalid column list for table {table_name!r}")
    if "column_names" in table_info and table_info["column_names"] != columns:
        raise ValueError(f"column_names/columns aliases disagree for {table_name!r}")
    source_count = table_info.get("source_row_count", table_info.get("source_rows"))
    if not isinstance(source_count, int) or source_count < 0:
        raise ValueError(f"Invalid source row count for table {table_name!r}")
    if "source_rows" in table_info and table_info["source_rows"] != source_count:
        raise ValueError(f"source_rows/source_row_count aliases disagree for {table_name!r}")
    if table_info.get("read_row_count", source_count) != source_count:
        raise ValueError(f"Export read row count mismatch for table {table_name!r}")
    expected_cell_count = source_count * len(columns)
    if table_info.get("source_cell_count", expected_cell_count) != expected_cell_count:
        raise ValueError(f"Source cell count mismatch for table {table_name!r}")

    quoted_table = _quote_identifier(table_name)
    xinfo = _get_table_xinfo(target_conn, table_name)
    selected_xinfo = [column for column in xinfo if column["hidden"] != 1]
    target_columns = [column["name"] for column in selected_xinfo]
    if target_columns != columns:
        raise ValueError(
            f"Target columns mismatch for {table_name!r}: expected {columns!r}, got {target_columns!r}"
        )
    target_cursor = target_conn.execute(f"SELECT * FROM {quoted_table} LIMIT 0")
    target_select_columns = [
        description[0] for description in target_cursor.description or ()
    ]
    if target_select_columns != columns:
        raise ValueError(
            f"Target SELECT * columns mismatch for {table_name!r}: "
            f"expected {columns!r}, got {target_select_columns!r}"
        )
    saved_xinfo = table_info.get("column_schema")
    if saved_xinfo is not None and saved_xinfo != xinfo:
        raise ValueError(f"PRAGMA table_xinfo mismatch for table {table_name!r}")

    with_source_rowid = row_identity_format == ROW_IDENTITY_FORMAT
    source_rowid_kind: Optional[str] = None
    source_rowid_alias: Optional[str] = None
    if with_source_rowid:
        source_rowid_kind = table_info.get("source_rowid_kind")
        source_rowid_alias = table_info.get("source_rowid_alias")
        if source_rowid_kind not in ("rowid", "without_rowid", "shadowed"):
            raise ValueError(f"Invalid source_rowid_kind for table {table_name!r}")
        actual_kind, actual_alias = _get_source_row_identity(
            target_conn, table_name, columns
        )
        if (source_rowid_kind, source_rowid_alias) != (actual_kind, actual_alias):
            raise ValueError(
                f"Source row identity metadata disagrees with DDL for {table_name!r}"
            )
        if source_rowid_kind == "rowid" and source_rowid_alias not in _ROWID_ALIASES:
            raise ValueError(f"Invalid source_rowid_alias for table {table_name!r}")
        if source_rowid_kind != "rowid" and source_rowid_alias is not None:
            raise ValueError(f"Unexpected source_rowid_alias for table {table_name!r}")

    generated_columns = [
        column["name"] for column in selected_xinfo if column["hidden"] in (2, 3)
    ]
    ordinary_columns = [column for column in columns if column not in generated_columns]
    insert_column_names = (
        ([source_rowid_alias] if source_rowid_kind == "rowid" else [])
        + ordinary_columns
    )
    if insert_column_names:
        insert_columns_sql = ", ".join(_quote_identifier(c) for c in insert_column_names)
        placeholders = ", ".join("?" for _ in insert_column_names)
        insert_sql = f"INSERT INTO {quoted_table} ({insert_columns_sql}) VALUES ({placeholders})"
    else:
        insert_sql = f"INSERT INTO {quoted_table} DEFAULT VALUES"
    if generated_columns:
        returned = ", ".join(_quote_identifier(c) for c in generated_columns)
        insert_sql += f" RETURNING {returned}"

    expected_piece_names = [p.get("name") for p in pieces]
    alias_piece_names = table_info.get("piece_names")
    if alias_piece_names is not None and alias_piece_names != expected_piece_names:
        raise ValueError(f"piece_names/pieces aliases disagree for table {table_name!r}")
    declared_chunk_counts = table_info.get("chunk_counts")
    if declared_chunk_counts is not None and declared_chunk_counts != [
        p.get("chunk_count") for p in pieces
    ]:
        raise ValueError(f"chunk_counts/pieces aliases disagree for table {table_name!r}")

    expected_columns = {"A", "B", "C", "D", "E", "F", "G", "H"}
    if with_source_rowid:
        expected_columns.add("I")
    row_number = 1
    row_values: dict[str, Any] = {}
    cell_chunks: dict[str, dict[str, Any]] = {}
    actual_rows = 0
    actual_cells = 0
    actual_chunks = 0
    row_source_rowid: Optional[int] = None

    def finish_row() -> None:
        nonlocal row_number, row_values, actual_rows, row_source_rowid
        if cell_chunks:
            raise ValueError(f"Incomplete cell chunks in {table_name!r} row {row_number}")
        if set(row_values) != set(columns):
            missing = [name for name in columns if name not in row_values]
            extra = [name for name in row_values if name not in columns]
            raise ValueError(
                f"Cell coverage mismatch in {table_name!r} row {row_number}: "
                f"missing={missing!r}, extra={extra!r}"
            )
        ordered_values = tuple(row_values[name] for name in columns)
        if table_name == "sqlite_sequence":
            if columns != ["name", "seq"] or len(ordered_values) != 2:
                raise ValueError("Unexpected sqlite_sequence schema")
            if source_rowid_kind == "rowid":
                target_conn.execute(
                    f"INSERT INTO sqlite_sequence ({_quote_identifier(source_rowid_alias)}, name, seq) "
                    "VALUES (?, ?, ?)",
                    (row_source_rowid, *ordered_values),
                )
            else:
                target_conn.execute(
                    "INSERT INTO sqlite_sequence (name, seq) VALUES (?, ?)", ordered_values
                )
        else:
            params = (
                ((row_source_rowid,) if source_rowid_kind == "rowid" else ())
                + tuple(row_values[name] for name in ordinary_columns)
            )
            if generated_columns:
                returned_row = target_conn.execute(insert_sql, params).fetchone()
                if returned_row is None or len(returned_row) != len(generated_columns):
                    raise ValueError(f"Generated column result missing for {table_name!r}")
                for index, column_name in enumerate(generated_columns):
                    expected_value = row_values[column_name]
                    actual_value = returned_row[index]
                    if not _sqlite_values_equal(expected_value, actual_value):
                        raise ValueError(
                            f"Generated value mismatch for {table_name}.{column_name} "
                            f"row {row_number}: expected {expected_value!r}, got {actual_value!r}"
                        )
            elif insert_column_names:
                target_conn.execute(insert_sql, params)
            else:
                target_conn.execute(insert_sql)
        actual_rows += 1
        row_number += 1
        row_values = {}
        row_source_rowid = None

    for piece_index, piece in enumerate(pieces):
        path, _ = _read_and_verify_piece(
            base_dir, piece, require_bytes=strict_v2
        )
        piece_rows = 0
        for entry in _iter_lossless_xlsx_rows(path):
            piece_rows += 1
            actual_chunks += 1
            if strict_v2 and entry["_present_columns"] != expected_columns:
                raise ValueError(f"Missing or extra XLSX columns in piece {path.name}")
            if strict_v2:
                cell_types = entry["_cell_types"]
                if any(cell_types.get(col) != "inlineStr" for col in ("A", "C", "D", "H")):
                    raise ValueError(f"Invalid metadata cell type in piece {path.name}")
                if any(cell_types.get(col) is not None for col in ("B", "E", "F")):
                    raise ValueError(f"Invalid numeric cell type in piece {path.name}")
                expected_value_cell_type = (
                    None if entry["sqlite_type"] == "null" else "inlineStr"
                )
                if cell_types.get("G") != expected_value_cell_type:
                    raise ValueError(f"Invalid payload cell type in piece {path.name}")
                if with_source_rowid and cell_types.get("I") != "inlineStr":
                    raise ValueError(f"Invalid source_rowid cell type in piece {path.name}")
            if entry["table"] != table_name:
                raise ValueError(
                    f"Table name mismatch in {path.name}: {entry['table']!r} != {table_name!r}"
                )
            entry_row = entry["row_number"]
            if entry_row < 1 or entry_row > source_count:
                raise ValueError(f"Out-of-range row number {entry_row} in {path.name}")
            entry_source_rowid = entry.get("source_rowid") if with_source_rowid else None
            if with_source_rowid:
                if source_rowid_kind == "rowid":
                    if type(entry_source_rowid) is not int or not _MIN_ROWID <= entry_source_rowid <= _MAX_ROWID:
                        raise ValueError(f"Missing or invalid source_rowid in {path.name}")
                elif entry_source_rowid is not None:
                    raise ValueError(f"Unexpected source_rowid for {source_rowid_kind} table {table_name!r}")
            if not row_values and not cell_chunks:
                if entry_row != row_number:
                    raise ValueError(
                        f"Row number gap or reordering in {table_name!r}: "
                        f"expected {row_number}, got {entry_row}"
                    )
                row_source_rowid = entry_source_rowid
            elif entry_row != row_number:
                finish_row()
                if entry_row != row_number:
                    raise ValueError(
                        f"Row number gap or reordering in {table_name!r}: "
                        f"expected {row_number}, got {entry_row}"
                    )
                row_source_rowid = entry_source_rowid
            elif with_source_rowid and entry_source_rowid != row_source_rowid:
                raise ValueError(
                    f"Conflicting source_rowid values in {table_name!r} row {entry_row}"
                )

            column_name = entry["column_name"]
            if column_name not in columns:
                raise ValueError(f"Unknown column {column_name!r} in {table_name!r}")
            if column_name in row_values:
                raise ValueError(
                    f"Duplicate cell {table_name}.{column_name} in row {entry_row}"
                )
            chunk_no = entry["chunk_number"]
            total = entry["total_chunks"]
            if total < 1 or chunk_no < 1 or chunk_no > total:
                raise ValueError(f"Invalid chunk numbering for {table_name}.{column_name}")
            encoding = entry["payload_encoding"]
            state = cell_chunks.get(column_name)
            if state is None:
                if chunk_no != 1:
                    raise ValueError(
                        f"Missing first chunk for {table_name}.{column_name} row {entry_row}"
                    )
                state = {
                    "sqlite_type": entry["sqlite_type"],
                    "encoding": encoding,
                    "total": total,
                    "next": 1,
                    "values": [],
                }
                cell_chunks[column_name] = state
            if (
                entry["sqlite_type"] != state["sqlite_type"]
                or encoding != state["encoding"]
                or total != state["total"]
                or chunk_no != state["next"]
            ):
                raise ValueError(
                    f"Chunk type, encoding, or numbering mismatch for "
                    f"{table_name}.{column_name} row {entry_row}"
                )
            if entry["sqlite_type"] not in ("null", "integer", "real", "text", "blob"):
                raise ValueError(f"Unknown SQLite type {entry['sqlite_type']!r}")
            if entry["sqlite_type"] == "text" and encoding not in ("plain", "utf8-base64"):
                raise ValueError(f"Invalid text encoding {encoding!r}")
            if entry["sqlite_type"] != "text" and encoding != "plain":
                raise ValueError("Only text payloads may use utf8-base64")
            state["values"].append(entry["value_chunk"])
            state["next"] += 1
            if chunk_no == total:
                value = _reconstruct_value(
                    state["sqlite_type"], state["values"], state["encoding"]
                )
                row_values[column_name] = value
                del cell_chunks[column_name]
                actual_cells += 1
        declared_piece_chunks = piece.get("chunk_count")
        if not isinstance(declared_piece_chunks, int) or piece_rows != declared_piece_chunks:
            raise ValueError(
                f"Piece chunk count mismatch for {path.name}: "
                f"expected {declared_piece_chunks!r}, read {piece_rows}"
            )
        if piece_index < len(pieces) - 1 and (row_values or cell_chunks):
            # Pieces may split a row between chunks, so only forbid a row from
            # being closed and then reintroduced; the state may continue here.
            continue

    if row_values or cell_chunks:
        finish_row()
    if actual_rows != source_count:
        raise ValueError(
            f"Restored row count mismatch for {table_name!r}: "
            f"expected {source_count}, got {actual_rows}"
        )
    if actual_cells != expected_cell_count:
        raise ValueError(
            f"Restored cell count mismatch for {table_name!r}: "
            f"expected {expected_cell_count}, got {actual_cells}"
        )
    expected_chunks = table_info.get("total_chunks")
    if not isinstance(expected_chunks, int) or actual_chunks != expected_chunks:
        raise ValueError(
            f"Restored chunk count mismatch for {table_name!r}: "
            f"expected {expected_chunks!r}, got {actual_chunks}"
        )
    if "read_cell_count" in table_info and table_info["read_cell_count"] != actual_cells:
        raise ValueError(f"Export read cell count mismatch for table {table_name!r}")
    return actual_rows


def _schema_rows(connection: sqlite3.Connection) -> list[dict[str, Any]]:
    rows = connection.execute(
        "SELECT type, name, tbl_name, sql FROM sqlite_master ORDER BY type, name"
    ).fetchall()
    return [
        {"name": name, "type": obj_type, "tbl_name": tbl_name, "sql": sql}
        for obj_type, name, tbl_name, sql in rows
    ]


def _validate_internal_table_metadata(
    tables: dict[str, dict[str, Any]],
    schema_objects: Optional[list[dict[str, Any]]],
    *,
    strict_v2: bool,
) -> set[str]:
    unsupported_tables = sorted(
        name
        for name in tables
        if isinstance(name, str)
        and name.startswith("sqlite_")
        and name not in _SUPPORTED_INTERNAL_TABLES
    )
    if unsupported_tables:
        raise ValueError(
            "Unsupported internal SQLite table metadata: "
            f"{unsupported_tables!r}"
        )

    schema_tables = [
        item
        for item in (schema_objects or [])
        if item["type"] == "table" and item["name"].startswith("sqlite_")
    ]
    unsupported_schema_tables = sorted(
        item["name"]
        for item in schema_tables
        if item["name"] not in _SUPPORTED_INTERNAL_TABLES
    )
    if unsupported_schema_tables:
        raise ValueError(
            "Unsupported internal SQLite schema table metadata: "
            f"{unsupported_schema_tables!r}"
        )

    table_stats = {
        name for name in tables if isinstance(name, str) and name in _SQLITE_STAT_TABLES
    }
    schema_stats = {
        item["name"]: item
        for item in schema_tables
        if item["name"] in _SQLITE_STAT_TABLES
    }
    if table_stats != set(schema_stats):
        raise ValueError(
            "SQLite statistics table coverage mismatch: "
            f"tables={sorted(table_stats)!r}, schema={sorted(schema_stats)!r}"
        )
    if table_stats and not strict_v2:
        raise ValueError(
            "SQLite statistics tables require version 2 schema metadata"
        )

    for table_name in sorted(table_stats):
        table_info = tables[table_name]
        schema_item = schema_stats[table_name]
        if not isinstance(table_info, dict):
            raise ValueError(
                f"Invalid SQLite statistics table metadata for {table_name!r}"
            )
        ddl = table_info.get("ddl", table_info.get("table_ddl"))
        if (
            not isinstance(ddl, str)
            or not ddl.strip()
            or schema_item["sql"] != ddl
            or (table_info.get("ddl") is not None and table_info.get("table_ddl", ddl) != ddl)
        ):
            raise ValueError(
                f"SQLite statistics table DDL metadata mismatch for {table_name!r}"
            )
        if not isinstance(table_info.get("column_schema"), list):
            raise ValueError(
                f"SQLite statistics table xinfo metadata is missing for {table_name!r}"
            )
    return table_stats


def _bootstrap_statistics_tables(
    target_conn: sqlite3.Connection,
    tables: dict[str, dict[str, Any]],
    schema_objects: Optional[list[dict[str, Any]]],
    expected_stats: set[str],
) -> None:
    """Generate SQLite-owned statistics schemas, then restore their exact rows."""
    if not expected_stats:
        return

    target_conn.execute("ANALYZE")
    generated_stats = {
        row[0]
        for row in target_conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'sqlite_stat%'"
        )
    }
    missing_stats = expected_stats - generated_stats
    if missing_stats:
        raise ValueError(
            "Target SQLite build cannot recreate required statistics tables: "
            f"{sorted(missing_stats)!r}"
        )
    unsupported_generated = generated_stats - _SQLITE_STAT_TABLES
    if unsupported_generated:
        raise ValueError(
            "Target SQLite generated unsupported statistics tables: "
            f"{sorted(unsupported_generated)!r}"
        )

    for table_name in sorted(generated_stats - expected_stats):
        target_conn.execute(f"DROP TABLE {_quote_identifier(table_name)}")

    schema_stats = {
        item["name"]: item
        for item in (schema_objects or [])
        if item["type"] == "table" and item["name"] in _SQLITE_STAT_TABLES
    }
    for table_name in sorted(expected_stats):
        table_info = tables[table_name]
        actual = target_conn.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name=?",
            (table_name,),
        ).fetchone()
        expected_ddl = table_info.get("ddl", table_info.get("table_ddl"))
        if (
            actual is None
            or actual[0] != expected_ddl
            or actual[0] != schema_stats[table_name]["sql"]
        ):
            raise ValueError(
                f"SQLite-generated statistics table DDL mismatch for {table_name!r}"
            )
        target_xinfo = _get_table_xinfo(target_conn, table_name)
        if target_xinfo != table_info["column_schema"]:
            raise ValueError(
                f"SQLite-generated statistics table xinfo mismatch for {table_name!r}"
            )
        columns = _get_table_columns(target_conn, table_name)
        saved_columns = table_info.get("columns", table_info.get("column_names"))
        if columns != saved_columns:
            raise ValueError(
                f"SQLite-generated statistics table columns mismatch for {table_name!r}"
            )
        target_conn.execute(f"DELETE FROM {_quote_identifier(table_name)}")


def reconstruct_sqlite_tables(
    source_dir_or_index: Union[str, Path, dict],
    target_conn: sqlite3.Connection,
) -> dict[str, int]:
    """検証しながらXLSX群を空のSQLite接続へ原子的に復元する。"""
    if not isinstance(target_conn, sqlite3.Connection):
        raise TypeError("target_conn must be a sqlite3.Connection")
    if isinstance(source_dir_or_index, dict):
        index_doc = source_dir_or_index
        base_dir = Path(".")
    else:
        base_dir = Path(source_dir_or_index)
        with open(base_dir / "index.json", "r", encoding="utf-8") as f:
            index_doc = json.load(f)
    if not isinstance(index_doc, dict):
        raise ValueError("XLSX index must be a JSON object")
    version = index_doc.get("version", 1)
    if version not in (1, 2):
        raise ValueError(f"Unsupported lossless XLSX index version: {version!r}")
    strict_v2 = version >= 2
    row_identity_format = index_doc.get("row_identity_format")
    if row_identity_format not in (None, ROW_IDENTITY_FORMAT):
        raise ValueError(f"Unsupported row_identity_format: {row_identity_format!r}")
    if row_identity_format is not None and not strict_v2:
        raise ValueError("source row identity columns require version 2 index metadata")
    tables: dict[str, dict] = index_doc.get("tables", {})
    if not isinstance(tables, dict):
        raise ValueError("index tables must be an object")
    schema_objects = index_doc.get("schema_objects")
    if strict_v2 and not isinstance(schema_objects, list):
        raise ValueError("version 2 index is missing schema_objects")
    if schema_objects is not None:
        if not isinstance(schema_objects, list):
            raise ValueError("schema_objects must be a list")
        for item in schema_objects:
            if (
                not isinstance(item, dict)
                or set(item) != {"name", "type", "tbl_name", "sql"}
                or item["type"] not in ("table", "index", "view", "trigger")
                or not isinstance(item["name"], str)
                or not isinstance(item["tbl_name"], str)
                or (item["sql"] is not None and not isinstance(item["sql"], str))
            ):
                raise ValueError(f"Invalid schema object metadata: {item!r}")

    statistics_tables = _validate_internal_table_metadata(
        tables, schema_objects, strict_v2=strict_v2
    )

    piece_name_seen: set[str] = set()
    for table_name, table_info in tables.items():
        if not isinstance(table_name, str) or not isinstance(table_info, dict):
            raise ValueError("Invalid table metadata")
        if table_info.get("table", table_name) != table_name:
            raise ValueError(f"Table name aliases disagree for {table_name!r}")
        pieces = table_info.get("pieces")
        if not isinstance(pieces, list):
            raise ValueError(f"Invalid pieces list for {table_name!r}")
        piece_names = []
        total = 0
        for piece in pieces:
            if not isinstance(piece, dict):
                raise ValueError(f"Invalid piece metadata for {table_name!r}")
            piece_name = piece.get("name")
            if not isinstance(piece_name, str) or piece_name in piece_name_seen:
                raise ValueError(f"Duplicate or invalid piece name: {piece_name!r}")
            piece_name_seen.add(piece_name)
            piece_names.append(piece_name)
            chunk_count = piece.get("chunk_count")
            if not isinstance(chunk_count, int) or chunk_count < 0:
                raise ValueError(f"Invalid chunk count for piece {piece_name!r}")
            total += chunk_count
        if table_info.get("total_chunks") != total:
            raise ValueError(f"Total chunk count mismatch for table {table_name!r}")
        if table_info.get("piece_names", piece_names) != piece_names:
            raise ValueError(f"Piece name aliases disagree for {table_name!r}")
        declared_sha = table_info.get("sha256")
        if declared_sha is not None and declared_sha != [p.get("sha256") for p in pieces]:
            raise ValueError(f"SHA-256 aliases disagree for {table_name!r}")

    global_pieces = index_doc.get("pieces")
    if global_pieces is not None:
        if not isinstance(global_pieces, list):
            raise ValueError("index pieces must be a list")
        expected_global_pieces = [
            piece for table_info in tables.values() for piece in table_info["pieces"]
        ]
        if global_pieces != expected_global_pieces:
            raise ValueError("Global piece index does not match table piece lists")
    expected_piece_names = [
        piece["name"] for table_info in tables.values() for piece in table_info["pieces"]
    ]
    if index_doc.get("piece_names", expected_piece_names) != expected_piece_names:
        raise ValueError("Global piece_names alias does not match table piece lists")

    table_objects = (
        [item for item in schema_objects if item["type"] == "table"]
        if schema_objects is not None
        else []
    )
    expected_data_tables = {
        item["name"]
        for item in table_objects
    }
    if schema_objects is not None and expected_data_tables != set(tables):
        raise ValueError(
            f"Schema/table index mismatch: schema={sorted(expected_data_tables)!r}, "
            f"tables={sorted(tables)!r}"
        )

    existing_objects = target_conn.execute(
        "SELECT name FROM sqlite_master WHERE type IN ('table','index','view','trigger')"
    ).fetchall()
    existing_temp_objects = target_conn.execute(
        "SELECT name FROM sqlite_temp_master WHERE type IN ('table','index','view','trigger')"
    ).fetchall()
    if existing_objects or existing_temp_objects:
        raise ValueError("target_conn must refer to an empty SQLite database")

    outer_transaction = target_conn.in_transaction
    foreign_keys = int(target_conn.execute("PRAGMA foreign_keys").fetchone()[0])
    original_defer = int(target_conn.execute("PRAGMA defer_foreign_keys").fetchone()[0])
    changed_defer = False
    own_transaction = not outer_transaction
    savepoint = "_ikarchive_lossless_restore"
    if outer_transaction:
        target_conn.execute(f"SAVEPOINT {savepoint}")
        if foreign_keys and not original_defer:
            target_conn.execute("PRAGMA defer_foreign_keys = ON")
            changed_defer = True
    else:
        if foreign_keys:
            target_conn.execute("PRAGMA foreign_keys = OFF")
        target_conn.execute("BEGIN")

    try:
        # Create tables first. sqlite_sequence is materialized automatically by
        # AUTOINCREMENT tables. SQLite statistics tables are bootstrapped by
        # ANALYZE below because their schemas are SQLite-owned.
        for table_name, table_info in tables.items():
            if table_name == "sqlite_sequence" or table_name in _SQLITE_STAT_TABLES:
                continue
            ddl = table_info.get("ddl", table_info.get("table_ddl"))
            if not isinstance(ddl, str) or not ddl.strip():
                raise ValueError(f"Missing CREATE TABLE SQL for {table_name!r}")
            if table_info.get("ddl") is not None and table_info.get("table_ddl", ddl) != ddl:
                raise ValueError(f"DDL aliases disagree for {table_name!r}")
            target_conn.execute(ddl)
            target_xinfo = _get_table_xinfo(target_conn, table_name)
            saved_xinfo = table_info.get("column_schema")
            if saved_xinfo is not None and saved_xinfo != target_xinfo:
                raise ValueError(f"PRAGMA table_xinfo mismatch for table {table_name!r}")

        # SQLite owns the CREATE TABLE statements for statistics tables. Create
        # their supported schemas, remove any generated tables absent from the
        # source, and clear generated rows before restoring source bytes.
        _bootstrap_statistics_tables(
            target_conn, tables, schema_objects, statistics_tables
        )

        counts: dict[str, int] = {}
        for table_name, table_info in tables.items():
            if table_name == "sqlite_sequence":
                continue
            counts[table_name] = _restore_table_rows(
                target_conn,
                table_name,
                table_info,
                table_info["pieces"],
                base_dir,
                strict_v2=strict_v2,
                row_identity_format=row_identity_format,
            )
        if "sqlite_sequence" in tables:
            seq_info = tables["sqlite_sequence"]
            target_conn.execute("DELETE FROM sqlite_sequence")
            counts["sqlite_sequence"] = _restore_table_rows(
                target_conn,
                "sqlite_sequence",
                seq_info,
                seq_info["pieces"],
                base_dir,
                strict_v2=strict_v2,
                row_identity_format=row_identity_format,
            )

        # Explicit indexes, views, and triggers are installed only after all
        # table data is present, so restoration cannot fire user triggers.
        if schema_objects is not None:
            for obj_type in ("index", "view", "trigger"):
                for item in schema_objects:
                    if item["type"] != obj_type or item["sql"] is None:
                        continue
                    if obj_type == "index" and item["name"].startswith("sqlite_autoindex_"):
                        continue
                    target_conn.execute(item["sql"])
            actual_schema = _schema_rows(target_conn)
            if actual_schema != schema_objects:
                raise ValueError("Restored sqlite_master objects do not match schema_objects")

        if foreign_keys:
            violations = target_conn.execute("PRAGMA foreign_key_check").fetchall()
            if violations:
                raise ValueError(f"Restored database has foreign-key violations: {len(violations)}")

        if outer_transaction:
            if changed_defer:
                target_conn.execute("PRAGMA defer_foreign_keys = OFF")
            target_conn.execute(f"RELEASE SAVEPOINT {savepoint}")
        else:
            target_conn.commit()
        return counts
    except Exception:
        if outer_transaction:
            try:
                target_conn.execute(f"ROLLBACK TO SAVEPOINT {savepoint}")
            finally:
                target_conn.execute(f"RELEASE SAVEPOINT {savepoint}")
                if changed_defer:
                    target_conn.execute(
                        f"PRAGMA defer_foreign_keys = {original_defer}"
                    )
        elif target_conn.in_transaction:
            target_conn.rollback()
        raise
    finally:
        if own_transaction and foreign_keys:
            target_conn.execute("PRAGMA foreign_keys = ON")
