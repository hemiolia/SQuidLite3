import copy
import contextlib
import base64
import hashlib
import io
import json
import os
from pathlib import Path
import sqlite3
import sys
import tempfile
import unittest
import xml.etree.ElementTree as ET
import zipfile
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT / "src/python"))

import export_full_xlsx as xlsx_cli  # noqa: E402
from export_full_xlsx import (  # noqa: E402
    ExportVerificationError,
    export_full_xlsx,
    main,
    verify_export,
)


MAIN_NS = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
ET.register_namespace("", MAIN_NS)


def _file_digest(path: Path) -> tuple[int, str]:
    digest = hashlib.sha256()
    size = 0
    with path.open("rb") as stream:
        while block := stream.read(65536):
            size += len(block)
            digest.update(block)
    return size, digest.hexdigest()


def _cell_text(row: ET.Element, column: str) -> str | None:
    cell = next(
        (
            item
            for item in row.findall(f"{{{MAIN_NS}}}c")
            if (item.get("r") or "").startswith(column)
        ),
        None,
    )
    if cell is None:
        return None
    inline = cell.find(f"{{{MAIN_NS}}}is")
    if inline is not None:
        return "".join(item.text or "" for item in inline.iter(f"{{{MAIN_NS}}}t"))
    value = cell.find(f"{{{MAIN_NS}}}v")
    return None if value is None else value.text


def _rewrite_piece(path: Path, mutate) -> None:
    temporary = path.with_suffix(".tampered.xlsx")
    with zipfile.ZipFile(path, "r") as source:
        members = {name: source.read(name) for name in source.namelist()}
    root = ET.fromstring(members["xl/worksheets/sheet1.xml"])
    sheet_data = root.find(f"{{{MAIN_NS}}}sheetData")
    assert sheet_data is not None
    mutate(sheet_data)
    members["xl/worksheets/sheet1.xml"] = ET.tostring(
        root, encoding="utf-8", xml_declaration=True
    )
    with zipfile.ZipFile(temporary, "w", compression=zipfile.ZIP_DEFLATED) as target:
        for name, data in members.items():
            target.writestr(name, data)
    os.replace(temporary, path)


def _refresh_piece_hash(index_path: Path, piece_name: str) -> None:
    index_doc = json.loads(index_path.read_text(encoding="utf-8"))
    size, digest = _file_digest(index_path.parent / piece_name)
    matches = [piece for piece in index_doc["pieces"] if piece["name"] == piece_name]
    for table in index_doc["tables"].values():
        matches.extend(piece for piece in table["pieces"] if piece["name"] == piece_name)
    for piece in matches:
        piece["bytes"] = size
        piece["sha256"] = digest
    index_path.write_text(json.dumps(index_doc, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


class FullXlsxExportTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name).resolve()
        self.db_path = self.root / "snapshot.sqlite3"
        conn = sqlite3.connect(self.db_path)
        try:
            conn.executescript(
                """
                CREATE TABLE records (
                    id INTEGER PRIMARY KEY,
                    huge INTEGER NOT NULL,
                    payload BLOB,
                    note TEXT,
                    empty_text TEXT,
                    nullable TEXT,
                    controls TEXT,
                    literal_escape TEXT,
                    generated_note TEXT GENERATED ALWAYS AS (note || '!') STORED
                );
                CREATE INDEX records_note_index ON records(note);
                CREATE TABLE sequence_rows (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    label TEXT
                );
                CREATE TABLE empty_table (value TEXT, ordinal INTEGER);
                CREATE TABLE other_table (key TEXT PRIMARY KEY, value REAL);
                CREATE TABLE explicit_rowids (label TEXT, payload TEXT);
                CREATE TABLE without_rowid (key TEXT PRIMARY KEY, payload BLOB) WITHOUT ROWID;
                CREATE TABLE shadowed_rowids (_rowid_ TEXT, rowid TEXT, oid TEXT, payload TEXT);
                CREATE TABLE empty_without_rowid (key TEXT PRIMARY KEY) WITHOUT ROWID;
                INSERT INTO records (
                    id, huge, payload, note, empty_text, nullable, controls, literal_escape
                ) VALUES (
                    1,
                    9223372036854775807,
                    X'000102030405060708090A0B0C0D0E0F',
                    'plain note',
                    '',
                    NULL,
                    'before' || char(0) || char(1) || 'after',
                    '_x0001_'
                );
                INSERT INTO sequence_rows(label) VALUES ('one');
                INSERT INTO other_table VALUES ('k', 0.125);
                """
            )
            conn.execute(
                "UPDATE records SET payload = ?, note = ? WHERE id = 1",
                (bytes(range(256)) * 180, "large note " + "𠀋" * 9000),
            )
            conn.execute(
                "INSERT INTO explicit_rowids(rowid,label,payload) VALUES(?,?,?)",
                (-(1 << 63), "minimum", "rowid chunk " * 2000),
            )
            conn.execute(
                "INSERT INTO explicit_rowids(rowid,label,payload) VALUES(?,?,?)",
                ((1 << 63) - 1, "maximum", "last row"),
            )
            conn.execute(
                "INSERT INTO without_rowid VALUES(?,?)",
                ("stable-key", b"without-rowid-payload"),
            )
            conn.execute(
                "INSERT INTO shadowed_rowids(rowid,_rowid_,oid,payload) VALUES(?,?,?,?)",
                ("named-rowid", "named-hidden-rowid", "named-oid", "kept columns"),
            )
            conn.commit()
        finally:
            conn.close()
        self.output = self.root / "xlsx"

    def tearDown(self):
        self.temp.cleanup()

    def test_independent_xstring_decoder_fast_path_and_single_pass_tokens(self):
        base64_text = base64.b64encode(bytes(range(256)) * 16).decode("ascii")
        prefix_free = [
            "",
            '{"int64":9223372036854775807,"decimal":1.2300e+04,"unknown":true}',
            base64_text,
            "日本語・漢字・𠮷・𠀋",
            "text with NUL\x00 and controls\x01\x1f",
            "_X0041_",
            "uppercase prefix _X and punctuation !?",
        ]
        for text in prefix_free:
            with self.subTest(text_kind="prefix-free", length=len(text)):
                self.assertIs(xlsx_cli._excel_xstring_unescape(text), text)

        token_cases = {
            "_x0041_": "A",
            "_x0000_": "\x00",
            "_xD800_": "\ud800",
            "_x12_": "_x12_",
            "_xGGGG_": "_xGGGG_",
            "_x0041": "_x0041",
            "_x005F_x0041_": "_x0041_",
            "_x_x0041_": "_xA",
            "__x0041_": "_A",
            "_x0041__x0042_": "AB",
        }
        for encoded, expected in token_cases.items():
            with self.subTest(encoded=encoded):
                self.assertEqual(xlsx_cli._excel_xstring_unescape(encoded), expected)

        class TextSubclass(str):
            pass

        subclass_text = TextSubclass("plain subclass text")
        decoded_subclass = xlsx_cli._excel_xstring_unescape(subclass_text)
        self.assertEqual(decoded_subclass, subclass_text)
        self.assertIs(type(decoded_subclass), str)

    def export(self, *, max_rows_per_sheet=200_000):
        return export_full_xlsx(
            self.db_path,
            self.output,
            snapshot_identifier="opaque/snapshot:値/../id",
            max_rows_per_sheet=max_rows_per_sheet,
        )

    def open_source(self):
        conn = sqlite3.connect(
            f"file:{self.db_path.as_posix()}?mode=ro&immutable=1", uri=True
        )
        conn.execute("BEGIN")
        return conn

    def test_full_export_has_verified_receipt_and_preserves_source_bytes(self):
        before = _file_digest(self.db_path)
        result = self.export(max_rows_per_sheet=6)
        after = _file_digest(self.db_path)
        self.assertEqual(after, before)

        index = json.loads((self.output / "index.json").read_text(encoding="utf-8"))
        manifest = json.loads((self.output / "manifest.json").read_text(encoding="utf-8"))
        receipt = json.loads((self.output / "verification.json").read_text(encoding="utf-8"))
        self.assertEqual(index["status"], "verified")
        self.assertEqual(manifest["status"], "verified")
        self.assertEqual(receipt["status"], "verified")
        self.assertEqual(index["snapshot_identifier"], "opaque/snapshot:値/../id")
        self.assertEqual(index["version"], 2)
        self.assertEqual(index["row_identity_format"], "source_rowid_column_v1")
        self.assertEqual(index["source"]["sha256"], before[1])
        self.assertEqual(index["source"]["bytes"], before[0])
        self.assertNotIn("snapshot", " ".join(path.name for path in self.output.glob("*.xlsx")))
        self.assertIn("records", index["tables"])
        self.assertIn("other_table", index["tables"])
        self.assertIn("empty_table", index["tables"])
        self.assertIn("sequence_rows", index["tables"])
        self.assertIn("sqlite_sequence", index["tables"])
        self.assertEqual(index["tables"]["explicit_rowids"]["source_rowid_kind"], "rowid")
        self.assertEqual(index["tables"]["explicit_rowids"]["source_rowid_alias"], "_rowid_")
        self.assertEqual(index["tables"]["without_rowid"]["source_rowid_kind"], "without_rowid")
        self.assertIsNone(index["tables"]["without_rowid"]["source_rowid_alias"])
        self.assertEqual(index["tables"]["shadowed_rowids"]["source_rowid_kind"], "shadowed")
        self.assertIsNone(index["tables"]["shadowed_rowids"]["source_rowid_alias"])
        self.assertEqual(index["tables"]["empty_without_rowid"]["source_rowid_kind"], "without_rowid")
        self.assertEqual(result["counts"]["exported_tables"], 9)
        self.assertGreater(result["counts"]["chunks"], result["counts"]["exported_cells"])
        self.assertGreater(result["counts"]["pieces"], 5)
        self.assertFalse((self.output / "verification.failed.json").exists())
        self.assertTrue(result["counts"]["source_rowids_verified"])
        self.assertTrue(receipt["source_rowids_verified"])
        self.assertTrue(receipt["counts"]["source_rowids_verified"])

        source = self.open_source()
        try:
            counts = verify_export(source, self.output)
            self.assertEqual(counts["exported_rows"], 8)
            self.assertEqual(counts["source_bytes"], before[0])
            self.assertTrue(counts["source_rowids_verified"])
        finally:
            source.close()

    def _export_for_tamper(self):
        self.export(max_rows_per_sheet=10_000)
        index_path = self.output / "index.json"
        index = json.loads(index_path.read_text(encoding="utf-8"))
        records_piece = index["tables"]["records"]["pieces"][0]
        return index_path, records_piece["name"]

    def test_missing_chunk_is_rejected_against_source_cursor(self):
        index_path, piece_name = self._export_for_tamper()
        piece_path = self.output / piece_name

        def remove_second_blob_chunk(sheet_data):
            for row in list(sheet_data.findall(f"{{{MAIN_NS}}}row"))[1:]:
                if (
                    _cell_text(row, "C") == "payload"
                    and _cell_text(row, "D") == "blob"
                    and _cell_text(row, "E") == "2"
                ):
                    sheet_data.remove(row)
                    return
            raise AssertionError("second payload chunk not found")

        _rewrite_piece(piece_path, remove_second_blob_chunk)
        _refresh_piece_hash(index_path, piece_name)
        source = self.open_source()
        try:
            with self.assertRaises(ExportVerificationError):
                verify_export(source, self.output)
        finally:
            source.close()

    def test_missing_source_rowid_is_rejected_against_source_cursor(self):
        index_path, piece_name = self._export_for_tamper()
        piece_path = self.output / piece_name

        def remove_source_rowid(sheet_data):
            row = next(iter(sheet_data.findall(f"{{{MAIN_NS}}}row")[1:]))
            cell = next(
                item for item in row.findall(f"{{{MAIN_NS}}}c")
                if (item.get("r") or "").startswith("I")
            )
            cell.find(f"{{{MAIN_NS}}}is/{{{MAIN_NS}}}t").text = ""

        _rewrite_piece(piece_path, remove_source_rowid)
        _refresh_piece_hash(index_path, piece_name)
        source = self.open_source()
        try:
            with self.assertRaisesRegex(ExportVerificationError, "source_rowid"):
                verify_export(source, self.output)
        finally:
            source.close()

    def test_conflicting_source_rowid_across_chunks_is_rejected(self):
        index_path, piece_name = self._export_for_tamper()
        piece_path = self.output / piece_name

        def change_second_blob_chunk_rowid(sheet_data):
            for row in sheet_data.findall(f"{{{MAIN_NS}}}row")[1:]:
                if (
                    _cell_text(row, "C") == "payload"
                    and _cell_text(row, "D") == "blob"
                    and _cell_text(row, "E") == "2"
                ):
                    cell = next(
                        item for item in row.findall(f"{{{MAIN_NS}}}c")
                        if (item.get("r") or "").startswith("I")
                    )
                    cell.find(f"{{{MAIN_NS}}}is/{{{MAIN_NS}}}t").text = "2"
                    return
            raise AssertionError("second payload chunk not found")

        _rewrite_piece(piece_path, change_second_blob_chunk_rowid)
        _refresh_piece_hash(index_path, piece_name)
        source = self.open_source()
        try:
            with self.assertRaisesRegex(ExportVerificationError, "source_rowid"):
                verify_export(source, self.output)
        finally:
            source.close()

    def test_duplicate_chunk_is_rejected(self):
        index_path, piece_name = self._export_for_tamper()
        piece_path = self.output / piece_name

        def append_duplicate(sheet_data):
            data_rows = list(sheet_data.findall(f"{{{MAIN_NS}}}row"))[1:]
            duplicate = copy.deepcopy(data_rows[-1])
            duplicate.set("r", str(int(data_rows[-1].get("r")) + 1))
            sheet_data.append(duplicate)

        _rewrite_piece(piece_path, append_duplicate)
        _refresh_piece_hash(index_path, piece_name)
        source = self.open_source()
        try:
            with self.assertRaises(ExportVerificationError):
                verify_export(source, self.output)
        finally:
            source.close()

    def test_missing_piece_and_hash_mismatch_are_rejected(self):
        index_path, piece_name = self._export_for_tamper()
        source = self.open_source()
        try:
            (self.output / piece_name).unlink()
            with self.assertRaises(ExportVerificationError):
                verify_export(source, self.output)
        finally:
            source.close()

        self.output = self.root / "xlsx-hash"
        self.export()
        index_path = self.output / "index.json"
        index = json.loads(index_path.read_text(encoding="utf-8"))
        name = index["pieces"][0]["name"]
        for piece in index["pieces"]:
            if piece["name"] == name:
                piece["sha256"] = "0" * 64
        for table in index["tables"].values():
            for piece in table["pieces"]:
                if piece["name"] == name:
                    piece["sha256"] = "0" * 64
        index_path.write_text(json.dumps(index, ensure_ascii=False), encoding="utf-8")
        source = self.open_source()
        try:
            with self.assertRaises(ExportVerificationError):
                verify_export(source, self.output)
        finally:
            source.close()

    def test_schema_mismatch_is_rejected(self):
        self._export_for_tamper()
        index_path = self.output / "index.json"
        index = json.loads(index_path.read_text(encoding="utf-8"))
        index["schema_objects"][0]["sql"] = "CREATE TABLE altered(x)"
        index_path.write_text(json.dumps(index, ensure_ascii=False), encoding="utf-8")
        source = self.open_source()
        try:
            with self.assertRaises(ExportVerificationError):
                verify_export(source, self.output)
        finally:
            source.close()

    def test_verification_failure_marks_index_failed_and_writes_failure_receipt(self):
        real_export = xlsx_cli.export_sqlite_tables

        def write_bad_schema(*args, **kwargs):
            result = real_export(*args, **kwargs)
            index_path = Path(args[1]) / "index.json"
            index = json.loads(index_path.read_text(encoding="utf-8"))
            index["schema_objects"] = []
            index_path.write_text(json.dumps(index), encoding="utf-8")
            return result

        with patch.object(xlsx_cli, "export_sqlite_tables", side_effect=write_bad_schema):
            with self.assertRaises(ExportVerificationError):
                self.export()
        index = json.loads((self.output / "index.json").read_text(encoding="utf-8"))
        failure = json.loads(
            (self.output / "verification.failed.json").read_text(encoding="utf-8")
        )
        self.assertEqual(index["status"], "failed")
        self.assertEqual(index["verification_status"], "failed")
        self.assertEqual(failure["status"], "failed")
        self.assertEqual(failure["phase"], "independent_verification")
        self.assertFalse((self.output / "verification.json").exists())

    def test_overlap_and_nonempty_sidecar_fail_without_success_evidence(self):
        with self.assertRaises(ValueError):
            export_full_xlsx(
                self.db_path,
                self.db_path.parent,
                snapshot_identifier="overlap",
            )
        self.assertFalse((self.db_path.parent / "verification.json").exists())

        self.db_path.with_name(self.db_path.name + "-wal").write_bytes(b"pending")
        try:
            with self.assertRaises(ValueError):
                self.export()
            failure = json.loads((self.output / "verification.failed.json").read_text(encoding="utf-8"))
            self.assertEqual(failure["status"], "failed")
            self.assertEqual(failure["phase"], "source_validation")
            self.assertFalse((self.output / "verification.json").exists())
        finally:
            self.db_path.with_name(self.db_path.name + "-wal").unlink(missing_ok=True)

    def test_source_symlink_is_rejected(self):
        link = self.root / "linked.sqlite3"
        link.symlink_to(self.db_path)
        with self.assertRaises(ValueError):
            export_full_xlsx(
                link,
                self.output,
                snapshot_identifier="symlink",
            )
        failure = json.loads((self.output / "verification.failed.json").read_text(encoding="utf-8"))
        self.assertEqual(failure["status"], "failed")

    def test_source_symlink_ancestor_is_rejected(self):
        alias = self.root / "alias"
        alias.symlink_to(self.root, target_is_directory=True)
        aliased_db = alias / self.db_path.name
        output = self.root / "ancestor-symlink-output"
        with self.assertRaises(ValueError):
            export_full_xlsx(
                aliased_db,
                output,
                snapshot_identifier="ancestor-symlink",
            )
        failure = json.loads((output / "verification.failed.json").read_text(encoding="utf-8"))
        self.assertEqual(failure["status"], "failed")

    def test_nonempty_output_is_left_untouched(self):
        output = self.root / "already-used"
        output.mkdir()
        sentinel = output / "keep.txt"
        sentinel.write_text("existing", encoding="utf-8")
        with self.assertRaises(ValueError):
            export_full_xlsx(
                self.db_path,
                output,
                snapshot_identifier="nonempty-output",
            )
        self.assertEqual(sentinel.read_text(encoding="utf-8"), "existing")
        self.assertFalse((output / "verification.failed.json").exists())

    def test_cli_stdout_contains_only_summary_metadata(self):
        stdout = io.StringIO()
        stderr = io.StringIO()
        with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            status = main(
                [
                    "--db",
                    str(self.db_path),
                    "--output",
                    str(self.output),
                    "--snapshot-id",
                    "opaque-private-id",
                ]
            )
        self.assertEqual(status, 0)
        summary = json.loads(stdout.getvalue())
        self.assertEqual(summary["status"], "verified")
        self.assertEqual(
            set(summary),
            {"status", "output", "source_bytes", "source_sha256", "counts", "files"},
        )
        self.assertNotIn("opaque-private-id", stdout.getvalue())
        self.assertNotIn("large note", stdout.getvalue())
        self.assertNotIn("opaque-private-id", stderr.getvalue())
        self.assertNotIn("large note", stderr.getvalue())


if __name__ == "__main__":
    unittest.main()
