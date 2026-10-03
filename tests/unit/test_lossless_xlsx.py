import base64
import hashlib
import json
import math
import os
from pathlib import Path
import sqlite3
import sys
import tempfile
import unittest
import xml.etree.ElementTree as ET
import zipfile

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src/python"))

from ikarchive.lossless_xlsx import (
    CELL_LIMIT,
    CHUNK_SIZE,
    _encode_cell_chunks,
    _write_part_with_retry,
    _excel_xstring_escape,
    _excel_xstring_unescape,
    check_xml_safe,
    escape_xml,
    export_sqlite_tables,
    read_lossless_xlsx,
    reconstruct_sqlite_tables,
)


def _piece_path(output_dir: Path, manifest: dict, table: str, index: int = 0) -> Path:
    return output_dir / manifest["tables"][table]["piece_names"][index]


class TestLosslessXlsxRoundtrip(unittest.TestCase):
    def test_comprehensive_positive_roundtrip(self):
        source = sqlite3.connect(":memory:")
        try:
            # 1. 複合型テーブル
            source.execute(
                """CREATE TABLE records (
                    id INTEGER PRIMARY KEY,
                    long_text TEXT,
                    large_blob BLOB,
                    empty_text TEXT,
                    empty_blob BLOB,
                    null_val TEXT,
                    float_val REAL,
                    float_tiny REAL,
                    float_inf REAL,
                    float_neginf REAL,
                    int_pos INTEGER,
                    int_neg INTEGER,
                    int_zero INTEGER,
                    complex_text TEXT,
                    unicode_text TEXT
                )"""
            )

            # 2. 自動採番テーブル（sqlite_sequenceを発生させる）
            source.execute(
                """CREATE TABLE seq_items (
                    seq_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    title TEXT
                )"""
            )

            # 3. 空テーブル（0行）
            source.execute(
                """CREATE TABLE empty_table (
                    col_a TEXT,
                    col_b INTEGER
                )"""
            )

            # 50,000文字超のテキスト
            long_text = "イカリングアーカイブ完全保存用テキスト。" * 2600  # 52,000文字
            self.assertGreater(len(long_text), 50000)

            # 200,000バイトのBLOB
            large_blob = bytes(range(256)) * (200000 // 256) + bytes(range(200000 % 256))
            self.assertEqual(len(large_blob), 200000)

            # 複雑な改行、制御、XML記号を含むテキスト
            complex_text = "ライン1\r\nライン2\rライン3\nライン4\t<tag attr='val'> & \"quote\""

            # Unicode・絵文字
            unicode_text = "日本語・漢字・ひらがな・カタカナ・𠀋・𠮷・Splatoon3"

            # レコード挿入
            source.execute(
                """INSERT INTO records VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    1,
                    long_text,
                    large_blob,
                    "",
                    b"",
                    None,
                    3.141592653589793,
                    1.23456789e-300,
                    float("inf"),
                    float("-inf"),
                    9223372036854775807,
                    -9223372036854775808,
                    0,
                    complex_text,
                    unicode_text,
                ),
            )

            # seq_itemsへ2行挿入
            source.execute("INSERT INTO seq_items (title) VALUES (?)", ("Item Alpha",))
            source.execute("INSERT INTO seq_items (title) VALUES (?)", ("Item Beta",))

            with tempfile.TemporaryDirectory() as tmpdir:
                out_dir = Path(tmpdir)
                manifest = export_sqlite_tables(
                    source,
                    out_dir,
                    snapshot_identifier="snapshot_20261002_001",
                )

                # index.jsonの検証
                index_path = out_dir / "index.json"
                self.assertTrue(index_path.exists())
                with open(index_path, "r", encoding="utf-8") as f:
                    index_on_disk = json.load(f)

                self.assertEqual(manifest, index_on_disk)
                self.assertEqual(manifest["snapshot_identifier"], "snapshot_20261002_001")
                self.assertIn("records", manifest["tables"])
                self.assertIn("seq_items", manifest["tables"])
                self.assertIn("empty_table", manifest["tables"])
                self.assertIn("sqlite_sequence", manifest["tables"])

                # テーブル別メタデータの検証
                rec_meta = manifest["tables"]["records"]
                self.assertEqual(rec_meta["source_row_count"], 1)
                self.assertGreater(rec_meta["total_chunks"], 1)
                self.assertTrue(rec_meta["piece_names"][0].startswith("t"))
                self.assertTrue(_piece_path(out_dir, manifest, "records").exists())

                empty_meta = manifest["tables"]["empty_table"]
                self.assertEqual(empty_meta["source_row_count"], 0)
                self.assertEqual(empty_meta["total_chunks"], 0)
                self.assertEqual(len(empty_meta["piece_names"]), 1)
                self.assertTrue((out_dir / empty_meta["piece_names"][0]).exists())

                # 再構成
                target = sqlite3.connect(":memory:")
                try:
                    reconstructed_counts = reconstruct_sqlite_tables(out_dir, target)

                    # 行数一致の検証
                    self.assertEqual(reconstructed_counts["records"], 1)
                    self.assertEqual(reconstructed_counts["seq_items"], 2)
                    self.assertEqual(reconstructed_counts["empty_table"], 0)
                    self.assertEqual(reconstructed_counts["sqlite_sequence"], 1)

                    # 値の完全一致検証 (records)
                    rec_row = target.execute("SELECT * FROM records WHERE id = 1").fetchone()
                    self.assertIsNotNone(rec_row)

                    self.assertEqual(rec_row[0], 1)
                    self.assertEqual(rec_row[1], long_text)
                    self.assertEqual(rec_row[2], large_blob)
                    self.assertEqual(rec_row[3], "")
                    self.assertEqual(rec_row[4], b"")
                    self.assertIsNone(rec_row[5])
                    self.assertEqual(rec_row[6], 3.141592653589793)
                    self.assertEqual(rec_row[7], 1.23456789e-300)
                    self.assertEqual(rec_row[8], float("inf"))
                    self.assertEqual(rec_row[9], float("-inf"))
                    self.assertEqual(rec_row[10], 9223372036854775807)
                    self.assertEqual(rec_row[11], -9223372036854775808)
                    self.assertEqual(rec_row[12], 0)
                    self.assertEqual(rec_row[13], complex_text)
                    self.assertEqual(rec_row[14], unicode_text)

                    # seq_items の検証
                    seq_rows = target.execute(
                        "SELECT seq_id, title FROM seq_items ORDER BY seq_id"
                    ).fetchall()
                    self.assertEqual(seq_rows, [(1, "Item Alpha"), (2, "Item Beta")])

                    # sqlite_sequence の検証
                    seq_val = target.execute(
                        "SELECT seq FROM sqlite_sequence WHERE name = 'seq_items'"
                    ).fetchone()
                    self.assertEqual(seq_val[0], 2)
                    source_sequence_rows = source.execute(
                        "SELECT _rowid_, name, seq FROM sqlite_sequence ORDER BY _rowid_"
                    ).fetchall()
                    target_sequence_rows = target.execute(
                        "SELECT _rowid_, name, seq FROM sqlite_sequence ORDER BY _rowid_"
                    ).fetchall()
                    self.assertEqual(target_sequence_rows, source_sequence_rows)
                finally:
                    target.close()
        finally:
            source.close()

    def test_xstring_escape_and_unescape_fast_path_boundaries(self):
        all_ascii = "".join(chr(codepoint) for codepoint in range(128))
        expected_ascii = "".join(
            f"_x{codepoint:04X}_"
            if codepoint in (*range(0x00, 0x09), 0x0B, 0x0C, *range(0x0E, 0x20))
            else chr(codepoint)
            for codepoint in range(128)
        )
        escaped_ascii = _excel_xstring_escape(all_ascii)
        self.assertEqual(escaped_ascii, expected_ascii)
        self.assertEqual(_excel_xstring_unescape(escaped_ascii), all_ascii)
        self.assertEqual(escaped_ascii[-1], "\x7f")

        safe_ascii = "plain\ttab\nline\rreturn\x7fDEL"
        self.assertIs(_excel_xstring_escape(safe_ascii), safe_ascii)
        self.assertIs(_excel_xstring_unescape(safe_ascii), safe_ascii)
        self.assertEqual(
            escape_xml(safe_ascii), "plain\ttab\nline&#xD;return\x7fDEL"
        )

        literal_tokens = "_x0000_ _x0041_ _x005F_ _X0041_"
        escaped_tokens = _excel_xstring_escape(literal_tokens)
        self.assertIn("_x005F_x0000_", escaped_tokens)
        self.assertIn("_x005F_x0041_", escaped_tokens)
        self.assertIn("_x005F_x005F_", escaped_tokens)
        self.assertEqual(_excel_xstring_unescape(escaped_tokens), literal_tokens)

        non_ascii = "日本語\r\n𠀋 _x0041_"
        self.assertEqual(
            _excel_xstring_unescape(_excel_xstring_escape(non_ascii)), non_ascii
        )

    def test_ascii_fast_path_full_xlsx_roundtrip_preserves_values_and_source_rowids(self):
        source = sqlite3.connect(":memory:")
        try:
            source.execute(
                "CREATE TABLE fast_values(label TEXT, value, payload BLOB)"
            )
            ascii_json = (
                '{"int64":9223372036854775807,"decimal":1.2300e+04,'
                '"empty":"","unknown":true}'
            )
            ascii_base64 = base64.b64encode(bytes(range(256)) * 24).decode("ascii")
            values = [
                (5, "base64", ascii_base64, b"\x00\xffpayload"),
                (23, "json", ascii_json, b""),
                (77, "unicode-token-crlf", "日本語\r\n_x0041_𠀋", None),
                (500, "all-ascii-controls", "".join(chr(i) for i in range(128)), bytes(range(256))),
            ]
            source.executemany(
                "INSERT INTO fast_values(rowid,label,value,payload) VALUES(?,?,?,?)",
                values,
            )
            expected = source.execute(
                "SELECT rowid,label,value,typeof(value),payload,typeof(payload) "
                "FROM fast_values ORDER BY rowid"
            ).fetchall()

            with tempfile.TemporaryDirectory() as tmpdir:
                out_dir = Path(tmpdir)
                manifest = export_sqlite_tables(source, out_dir)
                pieces = manifest["tables"]["fast_values"]["pieces"]
                self.assertTrue(pieces)
                chunks = []
                for piece in pieces:
                    chunks.extend(read_lossless_xlsx(out_dir / piece["name"]))
                self.assertEqual(
                    {row["source_rowid"] for row in chunks}, {5, 23, 77, 500}
                )

                target = sqlite3.connect(":memory:")
                try:
                    reconstruct_sqlite_tables(out_dir, target)
                    restored = target.execute(
                        "SELECT rowid,label,value,typeof(value),payload,typeof(payload) "
                        "FROM fast_values ORDER BY rowid"
                    ).fetchall()
                    self.assertEqual(restored, expected)
                finally:
                    target.close()
        finally:
            source.close()

    def test_ascii_fast_path_cell_limit_boundaries_roundtrip(self):
        source = sqlite3.connect(":memory:")
        try:
            source.execute("CREATE TABLE \"boundary \"\" table\" (label TEXT, value TEXT)")
            value = "A" * (2 * CELL_LIMIT + 3) + "_x0041_\r\n\t\x7f"
            source.execute(
                'INSERT INTO "boundary "" table"(rowid,label,value) VALUES(?,?,?)',
                (41, "ascii-boundary", value),
            )
            source_bytes_before = source.serialize()
            schema_before = source.execute(
                "SELECT type,name,tbl_name,sql FROM sqlite_master ORDER BY type,name"
            ).fetchall()
            source_rows = source.execute(
                'SELECT rowid,label,value,typeof(value) FROM "boundary "" table" ORDER BY rowid'
            ).fetchall()

            for chunk_size in (CELL_LIMIT - 1, CELL_LIMIT):
                with self.subTest(chunk_size=chunk_size), tempfile.TemporaryDirectory() as tmpdir:
                    out_dir = Path(tmpdir) / "xlsx"
                    out_dir.mkdir()
                    manifest = export_sqlite_tables(
                        source,
                        out_dir,
                        chunk_size=chunk_size,
                        snapshot_identifier="ascii-cell-boundary",
                    )
                    meta = manifest["tables"]['boundary " table']
                    records = []
                    for piece in meta["pieces"]:
                        records.extend(read_lossless_xlsx(out_dir / piece["name"]))
                    payload = [row for row in records if row["column_name"] == "value"]
                    self.assertEqual([row["chunk_number"] for row in payload], [1, 2, 3])
                    self.assertEqual({row["total_chunks"] for row in payload}, {3})
                    self.assertTrue(all(len(row["value_chunk"]) <= chunk_size for row in payload))
                    self.assertTrue(all(row["payload_encoding"] == "plain" for row in payload))
                    self.assertTrue(all(row["source_rowid"] == 41 for row in payload))
                    self.assertEqual("".join(row["value_chunk"] for row in payload), value)

                    target = sqlite3.connect(":memory:")
                    try:
                        reconstruct_sqlite_tables(out_dir, target)
                        restored = target.execute(
                            'SELECT rowid,label,value,typeof(value) FROM "boundary "" table" ORDER BY rowid'
                        ).fetchall()
                        self.assertEqual(restored, source_rows)
                        target_schema = target.execute(
                            "SELECT type,name,tbl_name,sql FROM sqlite_master ORDER BY type,name"
                        ).fetchall()
                        self.assertEqual(target_schema, schema_before)
                    finally:
                        target.close()

            oversized = list(
                _encode_cell_chunks(
                    'boundary " table',
                    1,
                    "value",
                    value,
                    CELL_LIMIT + 1,
                    source_rowid=41,
                )
            )
            self.assertEqual([row[4] for row in oversized], [1, 2, 3])
            self.assertEqual({row[5] for row in oversized}, {3})
            self.assertTrue(all(len(row[6]) <= CELL_LIMIT for row in oversized))
            self.assertEqual("".join(row[6] for row in oversized), value)

            with tempfile.TemporaryDirectory() as tmpdir:
                rejected_dir = Path(tmpdir) / "rejected"
                rejected_dir.mkdir()
                with self.assertRaisesRegex(ValueError, "chunk_size"):
                    export_sqlite_tables(source, rejected_dir, chunk_size=CELL_LIMIT + 1)
                self.assertEqual(list(rejected_dir.iterdir()), [])
            self.assertEqual(source.serialize(), source_bytes_before)
            self.assertEqual(
                source.execute(
                    "SELECT type,name,tbl_name,sql FROM sqlite_master ORDER BY type,name"
                ).fetchall(),
                schema_before,
            )
        finally:
            source.close()

    def test_huge_integer_inline_str_and_roundtrip(self):
        source = sqlite3.connect(":memory:")
        try:
            source.execute(
                """CREATE TABLE ints (
                    id INTEGER PRIMARY KEY,
                    huge_pos INTEGER,
                    huge_neg INTEGER,
                    zero_val INTEGER,
                    norm_val INTEGER
                )"""
            )
            huge_pos = 9223372036854775807
            huge_neg = -9223372036854775808
            zero_val = 0
            norm_val = 42
            source.execute(
                "INSERT INTO ints VALUES (?, ?, ?, ?, ?)",
                (1, huge_pos, huge_neg, zero_val, norm_val),
            )

            with tempfile.TemporaryDirectory() as tmpdir:
                out_dir = Path(tmpdir)
                manifest = export_sqlite_tables(source, out_dir)

                xlsx_file = _piece_path(out_dir, manifest, "ints")
                self.assertTrue(xlsx_file.exists())

                with zipfile.ZipFile(xlsx_file, "r") as z:
                    xml_content = z.read("xl/worksheets/sheet1.xml").decode("utf-8")

                # G列（ペイロード）が <c r="G..." t="inlineStr"><is><t xml:space="preserve">9223372036854775807</t></is></c>
                self.assertIn('t="inlineStr"><is><t xml:space="preserve">9223372036854775807</t></is></c>', xml_content)
                self.assertIn('t="inlineStr"><is><t xml:space="preserve">-9223372036854775808</t></is></c>', xml_content)
                self.assertIn('t="inlineStr"><is><t xml:space="preserve">0</t></is></c>', xml_content)
                self.assertIn('t="inlineStr"><is><t xml:space="preserve">42</t></is></c>', xml_content)

                # G列で <v>9223372036854775807</v> 形式が使われていないことを確認（Excel数値パースによる15桁超精度落ち防止）
                self.assertNotIn("<v>9223372036854775807</v>", xml_content)
                self.assertNotIn("<v>-9223372036854775808</v>", xml_content)

                # B列（row_number）およびE列（chunk_number）が数値であることを確認
                root = ET.fromstring(xml_content)
                ns = {"m": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
                rows = root.findall(".//m:row", ns)
                data_rows = [r for r in rows if r.get("r") != "1"]
                self.assertGreater(len(data_rows), 0)
                for r in data_rows:
                    b_cell = r.find("m:c[@r='B" + r.get("r") + "']", ns)
                    self.assertIsNotNone(b_cell)
                    self.assertIsNone(b_cell.get("t"))  # numeric default
                    self.assertIsNotNone(b_cell.find("m:v", ns))

                    e_cell = r.find("m:c[@r='E" + r.get("r") + "']", ns)
                    self.assertIsNotNone(e_cell)
                    self.assertIsNone(e_cell.get("t"))
                    self.assertIsNotNone(e_cell.find("m:v", ns))

                # 再構成検証
                target = sqlite3.connect(":memory:")
                try:
                    reconstruct_sqlite_tables(out_dir, target)
                    res = target.execute("SELECT * FROM ints WHERE id = 1").fetchone()
                    self.assertIsNotNone(res)
                    self.assertEqual(res[0], 1)
                    self.assertEqual(res[1], huge_pos)
                    self.assertIsInstance(res[1], int)
                    self.assertEqual(res[2], huge_neg)
                    self.assertIsInstance(res[2], int)
                    self.assertEqual(res[3], zero_val)
                    self.assertIsInstance(res[3], int)
                    self.assertEqual(res[4], norm_val)
                    self.assertIsInstance(res[4], int)
                finally:
                    target.close()
        finally:
            source.close()


class TestLosslessXlsxChunking(unittest.TestCase):
    def test_text_chunking_boundary(self):
        source = sqlite3.connect(":memory:")
        try:
            source.execute("CREATE TABLE text_chunks (id INT, txt TEXT)")

            txt_exact = "A" * 16000
            txt_plus1 = "B" * 16001
            txt_50k = "C" * 50000

            source.execute("INSERT INTO text_chunks VALUES (1, ?)", (txt_exact,))
            source.execute("INSERT INTO text_chunks VALUES (2, ?)", (txt_plus1,))
            source.execute("INSERT INTO text_chunks VALUES (3, ?)", (txt_50k,))

            with tempfile.TemporaryDirectory() as tmpdir:
                out_dir = Path(tmpdir)
                manifest = export_sqlite_tables(source, out_dir)

                rows = read_lossless_xlsx(_piece_path(out_dir, manifest, "text_chunks"))
                txt_rows = [r for r in rows if r["column_name"] == "txt"]

                # id=1: 1 chunk (16000)
                id1_chunks = [r for r in txt_rows if r["row_number"] == 1]
                self.assertEqual(len(id1_chunks), 1)
                self.assertEqual(id1_chunks[0]["total_chunks"], 1)
                self.assertEqual(len(id1_chunks[0]["value_chunk"]), 16000)

                # id=2: 2 chunks (16000 + 1)
                id2_chunks = [r for r in txt_rows if r["row_number"] == 2]
                self.assertEqual(len(id2_chunks), 2)
                self.assertEqual(id2_chunks[0]["total_chunks"], 2)
                self.assertEqual(len(id2_chunks[0]["value_chunk"]), 16000)
                self.assertEqual(len(id2_chunks[1]["value_chunk"]), 1)

                # id=3: 4 chunks (16000 * 3 + 2000)
                id3_chunks = [r for r in txt_rows if r["row_number"] == 3]
                self.assertEqual(len(id3_chunks), 4)
                self.assertEqual(id3_chunks[0]["total_chunks"], 4)
                self.assertEqual(len(id3_chunks[0]["value_chunk"]), 16000)
                self.assertEqual(len(id3_chunks[1]["value_chunk"]), 16000)
                self.assertEqual(len(id3_chunks[2]["value_chunk"]), 16000)
                self.assertEqual(len(id3_chunks[3]["value_chunk"]), 2000)

                # 再構成検証
                target = sqlite3.connect(":memory:")
                try:
                    reconstruct_sqlite_tables(out_dir, target)
                    res = target.execute("SELECT id, txt FROM text_chunks ORDER BY id").fetchall()
                    self.assertEqual(res[0][1], txt_exact)
                    self.assertEqual(res[1][1], txt_plus1)
                    self.assertEqual(res[2][1], txt_50k)
                finally:
                    target.close()
        finally:
            source.close()


class TestLosslessXlsxMultiPiece(unittest.TestCase):
    def test_max_rows_per_sheet_splitting(self):
        source = sqlite3.connect(":memory:")
        try:
            source.execute("CREATE TABLE split_table (id INT, val TEXT)")
            for i in range(1, 21):
                source.execute("INSERT INTO split_table VALUES (?, ?)", (i, f"value_{i}"))

            with tempfile.TemporaryDirectory() as tmpdir:
                out_dir = Path(tmpdir)
                # 20 rows * 2 columns = 40 chunks
                # max_rows_per_sheet = 8 -> 40 / 8 = 5 pieces
                manifest = export_sqlite_tables(source, out_dir, max_rows_per_sheet=8)
                pieces = manifest["tables"]["split_table"]["pieces"]
                self.assertEqual(len(pieces), 5)
                for idx, p in enumerate(pieces, 1):
                    digest = hashlib.sha256(b"split_table").hexdigest()[:12]
                    self.assertEqual(p["name"], f"t0001_{digest}_{idx:04d}.xlsx")
                    self.assertEqual(p["chunk_count"], 8)
                    self.assertTrue((out_dir / p["name"]).exists())

                target = sqlite3.connect(":memory:")
                try:
                    reconstruct_sqlite_tables(out_dir, target)
                    res = target.execute("SELECT id, val FROM split_table ORDER BY id").fetchall()
                    self.assertEqual(len(res), 20)
                    self.assertEqual(res[0], (1, "value_1"))
                    self.assertEqual(res[-1], (20, "value_20"))
                finally:
                    target.close()
        finally:
            source.close()

    def test_1000_incompressible_chunks_adaptive_splitting(self):
        source = sqlite3.connect(":memory:")
        try:
            source.execute("CREATE TABLE big_blob (payload BLOB)")
            # 1000 chunks of 16,000 base64 chars = 12,000,000 raw bytes
            # os.urandom provides incompressible data
            raw_blob = os.urandom(12000 * 1000)
            source.execute("INSERT INTO big_blob VALUES (?)", (raw_blob,))

            with tempfile.TemporaryDirectory() as tmpdir:
                out_dir = Path(tmpdir)
                max_zip_bytes = 40000
                manifest = export_sqlite_tables(source, out_dir, max_zip_bytes=max_zip_bytes)

                tbl_meta = manifest["tables"]["big_blob"]
                self.assertEqual(tbl_meta["total_chunks"], 1000)
                pieces = tbl_meta["pieces"]
                self.assertGreater(len(pieces), 1)

                total_chunks_counted = 0
                for p in pieces:
                    p_path = out_dir / p["name"]
                    self.assertTrue(p_path.exists())
                    file_size = p_path.stat().st_size
                    self.assertLessEqual(file_size, max_zip_bytes)
                    self.assertEqual(file_size, p["bytes"])
                    # SHA256検証
                    h = hashlib.sha256(p_path.read_bytes()).hexdigest()
                    self.assertEqual(h, p["sha256"])
                    total_chunks_counted += p["chunk_count"]

                self.assertEqual(total_chunks_counted, 1000)

                # 再構成検証
                target = sqlite3.connect(":memory:")
                try:
                    reconstruct_sqlite_tables(out_dir, target)
                    rec_blob = target.execute("SELECT payload FROM big_blob").fetchone()[0]
                    self.assertEqual(rec_blob, raw_blob)
                finally:
                    target.close()
        finally:
            source.close()

    def test_halves_retry_mechanism(self):
        # 4 incompressible 16k rows
        # Each row takes ~13.8 KB in zip
        # 4 rows take ~51 KB > 40 KB
        # 2 rows take ~26 KB <= 40 KB
        table_name = "half_tbl"
        rows = [
            (table_name, i, "col", "blob", 1, 1, base64.b64encode(os.urandom(12000)).decode("ascii")[:16000])
            for i in range(1, 5)
        ]

        with tempfile.TemporaryDirectory() as tmpdir:
            out_dir = Path(tmpdir)
            # Test that _write_part_with_retry splits 4 rows down to 2 rows when limit is 40,000
            piece1, rem1 = _write_part_with_retry(out_dir, table_name, 1, rows, 40000)
            self.assertEqual(piece1["chunk_count"], 2)
            self.assertLessEqual(piece1["bytes"], 40000)
            self.assertEqual(len(rem1), 2)

            piece2, rem2 = _write_part_with_retry(out_dir, table_name, 2, rem1, 40000)
            self.assertEqual(piece2["chunk_count"], 2)
            self.assertLessEqual(piece2["bytes"], 40000)
            self.assertEqual(len(rem2), 0)

            # If limit is 5000, halving down to 1 fails with ValueError
            with self.assertRaises(ValueError) as ctx:
                _write_part_with_retry(out_dir, table_name, 3, rows, 5000)
            self.assertIn("exceeds limit max_zip_bytes", str(ctx.exception))


class TestLosslessXlsxTableFiltering(unittest.TestCase):
    def test_unpopulated_sqlite_sequence_is_preserved_as_an_empty_table(self):
        source = sqlite3.connect(":memory:")
        try:
            # AUTOINCREMENTを宣言するが0行 -> sqlite_sequenceは空
            source.execute("CREATE TABLE auto_empty (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT)")
            with tempfile.TemporaryDirectory() as tmpdir:
                manifest = export_sqlite_tables(source, Path(tmpdir))
                self.assertIn("sqlite_sequence", manifest["tables"])
                self.assertEqual(
                    manifest["tables"]["sqlite_sequence"]["source_row_count"], 0
                )
                self.assertIn("auto_empty", manifest["tables"])
                self.assertEqual(
                    {obj["name"] for obj in manifest["schema_objects"] if obj["type"] == "table"},
                    set(manifest["tables"]),
                )

                target = sqlite3.connect(":memory:")
                try:
                    reconstruct_sqlite_tables(Path(tmpdir), target)
                    self.assertEqual(
                        target.execute("SELECT * FROM sqlite_sequence").fetchall(), []
                    )
                finally:
                    target.close()
        finally:
            source.close()

    def test_internal_objects_and_views_excluded(self):
        source = sqlite3.connect(":memory:")
        try:
            source.execute("CREATE TABLE actual_table (id INT)")
            source.execute("INSERT INTO actual_table VALUES (1)")
            source.execute("CREATE VIEW sample_view AS SELECT id FROM actual_table")

            with tempfile.TemporaryDirectory() as tmpdir:
                manifest = export_sqlite_tables(source, Path(tmpdir))
                self.assertIn("actual_table", manifest["tables"])
                self.assertNotIn("sample_view", manifest["tables"])
                self.assertIn(
                    {"name": "sample_view", "type": "view", "tbl_name": "sample_view", "sql": "CREATE VIEW sample_view AS SELECT id FROM actual_table"},
                    manifest["schema_objects"],
                )
        finally:
            source.close()

    def test_virtual_and_internal_stat_tables_fail_explicitly(self):
        for kind in ("virtual", "stats"):
            source = sqlite3.connect(":memory:")
            try:
                if kind == "virtual":
                    try:
                        source.execute("CREATE VIRTUAL TABLE search_docs USING fts5(content)")
                    except sqlite3.OperationalError as exc:
                        self.skipTest(f"SQLite FTS5 is unavailable: {exc}")
                else:
                    source.execute("CREATE TABLE analyzed (value TEXT)")
                    source.execute("INSERT INTO analyzed VALUES ('value')")
                    source.execute("ANALYZE")

                with tempfile.TemporaryDirectory() as tmpdir:
                    out_dir = Path(tmpdir)
                    with self.assertRaisesRegex(
                        ValueError,
                        "not supported by lossless XLSX reconstruction",
                    ):
                        export_sqlite_tables(source, out_dir)
                    self.assertEqual(list(out_dir.iterdir()), [])
            finally:
                source.close()


class TestLosslessXlsxDeterminism(unittest.TestCase):
    def test_deterministic_output(self):
        source = sqlite3.connect(":memory:")
        try:
            source.execute("CREATE TABLE det (id INT, note TEXT, num REAL)")
            source.execute("INSERT INTO det VALUES (10, 'deterministic test', 1.25)")

            with tempfile.TemporaryDirectory() as dir1, tempfile.TemporaryDirectory() as dir2:
                p1 = Path(dir1)
                p2 = Path(dir2)
                m1 = export_sqlite_tables(source, p1)
                m2 = export_sqlite_tables(source, p2)
                f1 = _piece_path(p1, m1, "det").read_bytes()
                f2 = _piece_path(p2, m2, "det").read_bytes()
                self.assertEqual(f1, f2)
                self.assertEqual(hashlib.sha256(f1).hexdigest(), hashlib.sha256(f2).hexdigest())

                idx1 = (p1 / "index.json").read_bytes()
                idx2 = (p2 / "index.json").read_bytes()
                self.assertEqual(idx1, idx2)
        finally:
            source.close()


class TestLosslessXlsxNegativeAndFailClosed(unittest.TestCase):
    def test_max_zip_bytes_exceeded_fails_closed(self):
        source = sqlite3.connect(":memory:")
        try:
            source.execute("CREATE TABLE t (id INT, data TEXT)")
            source.execute("INSERT INTO t VALUES (1, 'some payload')")

            with tempfile.TemporaryDirectory() as tmpdir:
                out_dir = Path(tmpdir)
                with self.assertRaises(ValueError) as ctx:
                    export_sqlite_tables(source, out_dir, max_zip_bytes=500)

                self.assertIn("exceeds limit max_zip_bytes", str(ctx.exception))
                # フェイルクローズ: 作成途中の残骸ファイルが残っていないこと
                self.assertEqual(list(out_dir.iterdir()), [])
        finally:
            source.close()

    def test_too_small_single_chunk_fails_cleanly(self):
        source = sqlite3.connect(":memory:")
        try:
            source.execute("CREATE TABLE t (id INT, data BLOB)")
            # 16k chunk of incompressible data
            raw_chunk = os.urandom(12000)
            source.execute("INSERT INTO t VALUES (1, ?)", (raw_chunk,))

            with tempfile.TemporaryDirectory() as tmpdir:
                out_dir = Path(tmpdir)
                # 1 chunk zip requires ~13.8 KB, so max_zip_bytes=5000 is too small for even one chunk
                with self.assertRaises(ValueError) as ctx:
                    export_sqlite_tables(source, out_dir, max_zip_bytes=5000)

                self.assertIn("exceeds limit max_zip_bytes", str(ctx.exception))
                # フェイルクローズ: 作成途中の残骸ファイルが残っていないこと
                self.assertEqual(list(out_dir.iterdir()), [])
        finally:
            source.close()

    def test_xml_invalid_text_roundtrips_without_xml_controls(self):
        source = sqlite3.connect(":memory:")
        try:
            source.execute("CREATE TABLE t (id INT, bad TEXT, literal TEXT, astral TEXT)")
            bad = "nul\x00control\x01end"
            literal = "_x0041_ / _x005F_ / 日本語\r終端"
            astral = "𠀋" * 20000
            source.execute("INSERT INTO t VALUES (1, ?, ?, ?)", (bad, literal, astral))

            with tempfile.TemporaryDirectory() as tmpdir:
                out_dir = Path(tmpdir)
                manifest = export_sqlite_tables(source, out_dir)
                rows = read_lossless_xlsx(_piece_path(out_dir, manifest, "t"))
                bad_rows = [row for row in rows if row["column_name"] == "bad"]
                self.assertEqual({row["sqlite_type"] for row in bad_rows}, {"text"})
                self.assertEqual(
                    {row["payload_encoding"] for row in bad_rows}, {"utf8-base64"}
                )
                with zipfile.ZipFile(_piece_path(out_dir, manifest, "t")) as zipped:
                    sheet_xml = zipped.read("xl/worksheets/sheet1.xml")
                self.assertIn(b"_x005F_x0041_", sheet_xml)
                for row in rows:
                    if row["sqlite_type"] == "text" and row["payload_encoding"] == "plain":
                        self.assertLessEqual(len(row["value_chunk"]), CHUNK_SIZE)
                        self.assertLessEqual(
                            len(row["value_chunk"].encode("utf-16-le")) // 2, 32767
                        )

                target = sqlite3.connect(":memory:")
                try:
                    reconstruct_sqlite_tables(out_dir, target)
                    restored = target.execute(
                        "SELECT bad, literal, astral FROM t"
                    ).fetchone()
                    self.assertEqual(restored, (bad, literal, astral))
                finally:
                    target.close()
        finally:
            source.close()

    def test_invalid_parameters(self):
        source = sqlite3.connect(":memory:")
        try:
            with tempfile.TemporaryDirectory() as tmpdir:
                out_dir = Path(tmpdir)
                with self.assertRaises(ValueError):
                    export_sqlite_tables(source, out_dir, max_rows_per_sheet=0)

                with self.assertRaises(ValueError):
                    export_sqlite_tables(source, out_dir, max_zip_bytes=-100)

                with self.assertRaises(TypeError):
                    export_sqlite_tables("invalid_conn", out_dir)
        finally:
            source.close()


class TestLosslessXlsxHardening(unittest.TestCase):
    def test_source_rowids_gaps_extremes_and_explicit_unavailable_kinds_roundtrip(self):
        source = sqlite3.connect(":memory:")
        try:
            source.execute("CREATE TABLE explicit_rowids(label TEXT, payload BLOB)")
            source.execute(
                "INSERT INTO explicit_rowids(rowid,label,payload) VALUES(?,?,?)",
                (-(1 << 63), "minimum", bytes(range(256)) * 100),
            )
            source.execute(
                "INSERT INTO explicit_rowids(rowid,label,payload) VALUES(?,?,?)",
                (-17, "negative-gap", b"middle"),
            )
            source.execute(
                "INSERT INTO explicit_rowids(rowid,label,payload) VALUES(?,?,?)",
                ((1 << 63) - 1, "maximum", b"last"),
            )
            source.execute(
                "CREATE TABLE without_rowid(key TEXT PRIMARY KEY, value TEXT) WITHOUT ROWID"
            )
            source.execute("INSERT INTO without_rowid VALUES('key', 'value')")
            source.execute(
                "CREATE TABLE shadowed(_rowid_ TEXT, rowid TEXT, oid TEXT, payload TEXT)"
            )
            source.execute(
                "INSERT INTO shadowed VALUES('named-1', 'named-2', 'named-3', 'payload')"
            )
            source.execute(
                "CREATE TABLE empty_without_rowid(key TEXT PRIMARY KEY) WITHOUT ROWID"
            )

            with tempfile.TemporaryDirectory() as tmpdir:
                out_dir = Path(tmpdir)
                manifest = export_sqlite_tables(source, out_dir)
                self.assertEqual(manifest["version"], 2)
                self.assertEqual(
                    manifest["row_identity_format"], "source_rowid_column_v1"
                )
                self.assertEqual(
                    manifest["tables"]["explicit_rowids"]["source_rowid_kind"], "rowid"
                )
                self.assertEqual(
                    manifest["tables"]["explicit_rowids"]["source_rowid_alias"], "_rowid_"
                )
                self.assertEqual(
                    manifest["tables"]["without_rowid"]["source_rowid_kind"],
                    "without_rowid",
                )
                self.assertEqual(
                    manifest["tables"]["shadowed"]["source_rowid_kind"], "shadowed"
                )
                self.assertEqual(
                    manifest["tables"]["empty_without_rowid"]["source_rowid_kind"],
                    "without_rowid",
                )

                rowid_chunks = []
                shadowed_chunks = []
                without_chunks = []
                for piece in manifest["tables"]["explicit_rowids"]["pieces"]:
                    rowid_chunks.extend(read_lossless_xlsx(out_dir / piece["name"]))
                for piece in manifest["tables"]["shadowed"]["pieces"]:
                    shadowed_chunks.extend(read_lossless_xlsx(out_dir / piece["name"]))
                for piece in manifest["tables"]["without_rowid"]["pieces"]:
                    without_chunks.extend(read_lossless_xlsx(out_dir / piece["name"]))
                ids_by_row: dict[int, set[int | None]] = {}
                for chunk in rowid_chunks:
                    ids_by_row.setdefault(chunk["row_number"], set()).add(
                        chunk["source_rowid"]
                    )
                self.assertEqual(
                    ids_by_row,
                    {
                        1: {-(1 << 63)},
                        2: {-17},
                        3: {(1 << 63) - 1},
                    },
                )
                self.assertEqual({chunk["source_rowid"] for chunk in shadowed_chunks}, {None})
                self.assertEqual({chunk["source_rowid"] for chunk in without_chunks}, {None})

                target = sqlite3.connect(":memory:")
                try:
                    reconstruct_sqlite_tables(out_dir, target)
                    source_rows = source.execute(
                        "SELECT _rowid_, label, payload FROM explicit_rowids ORDER BY _rowid_"
                    ).fetchall()
                    target_rows = target.execute(
                        "SELECT _rowid_, label, payload FROM explicit_rowids ORDER BY _rowid_"
                    ).fetchall()
                    self.assertEqual(target_rows, source_rows)
                    self.assertEqual(
                        target.execute("SELECT key, value FROM without_rowid").fetchall(),
                        source.execute("SELECT key, value FROM without_rowid").fetchall(),
                    )
                    self.assertEqual(
                        target.execute("SELECT * FROM shadowed").fetchall(),
                        source.execute("SELECT * FROM shadowed").fetchall(),
                    )
                    self.assertEqual(
                        target.execute("SELECT * FROM empty_without_rowid").fetchall(), []
                    )
                finally:
                    target.close()
        finally:
            source.close()

    def test_reconstruction_rejects_conflicting_source_rowids_within_a_row(self):
        source = sqlite3.connect(":memory:")
        try:
            source.execute("CREATE TABLE payloads(value TEXT)")
            source.execute(
                "INSERT INTO payloads(rowid,value) VALUES(?,?)",
                (73, "chunked-value-" * 2000),
            )
            with tempfile.TemporaryDirectory() as tmpdir:
                out_dir = Path(tmpdir)
                manifest = export_sqlite_tables(source, out_dir)
                piece = manifest["tables"]["payloads"]["pieces"][0]
                piece_path = out_dir / piece["name"]
                with zipfile.ZipFile(piece_path, "r") as original:
                    members = [
                        (info, original.read(info.filename))
                        for info in original.infolist()
                        if info.filename != "xl/worksheets/sheet1.xml"
                    ]
                    sheet_info = original.getinfo("xl/worksheets/sheet1.xml")
                    sheet_root = ET.fromstring(
                        original.read("xl/worksheets/sheet1.xml")
                    )
                sheet_data = sheet_root.find(
                    "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}sheetData"
                )
                self.assertIsNotNone(sheet_data)
                rows = list(sheet_data.findall(
                    "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}row"
                ))[1:]
                target_row = next(
                    row
                    for row in rows
                    if next(
                        cell for cell in row.findall(
                            "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}c"
                        )
                        if (cell.get("r") or "").startswith("E")
                    ).find(
                        "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}v"
                    ).text == "2"
                )
                rowid_cell = next(
                    cell
                    for cell in target_row.findall(
                        "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}c"
                    )
                    if (cell.get("r") or "").startswith("I")
                )
                rowid_cell.find(
                    "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}is/"
                    "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}t"
                ).text = "74"
                replacement = piece_path.with_suffix(".replacement")
                with zipfile.ZipFile(
                    replacement, "w", compression=zipfile.ZIP_DEFLATED
                ) as rebuilt:
                    for info, payload in members:
                        rebuilt.writestr(info, payload)
                    rebuilt.writestr(
                        sheet_info,
                        ET.tostring(sheet_root, encoding="utf-8", xml_declaration=True),
                    )
                replacement.replace(piece_path)

                digest = hashlib.sha256(piece_path.read_bytes()).hexdigest()
                size = piece_path.stat().st_size
                for table_info in manifest["tables"].values():
                    for item in table_info["pieces"]:
                        if item["name"] == piece["name"]:
                            item["sha256"] = digest
                            item["bytes"] = size
                    table_info["sha256"] = [
                        item["sha256"] for item in table_info["pieces"]
                    ]
                for item in manifest["pieces"]:
                    if item["name"] == piece["name"]:
                        item["sha256"] = digest
                        item["bytes"] = size
                (out_dir / "index.json").write_text(
                    json.dumps(manifest), encoding="utf-8"
                )

                target = sqlite3.connect(":memory:")
                try:
                    with self.assertRaisesRegex(ValueError, "Conflicting source_rowid"):
                        reconstruct_sqlite_tables(out_dir, target)
                    self.assertEqual(
                        target.execute("SELECT name FROM sqlite_master").fetchall(), []
                    )
                finally:
                    target.close()
        finally:
            source.close()

    def test_ten_thousand_rows_produce_deterministic_many_piece_export(self):
        source = sqlite3.connect(":memory:")
        try:
            source.execute("CREATE TABLE many_rows (value INTEGER)")
            source.executemany(
                "INSERT INTO many_rows VALUES (?)", ((n,) for n in range(10000))
            )

            with tempfile.TemporaryDirectory() as dir1, tempfile.TemporaryDirectory() as dir2:
                out1, out2 = Path(dir1), Path(dir2)
                first = export_sqlite_tables(source, out1, max_rows_per_sheet=10)
                second = export_sqlite_tables(source, out2, max_rows_per_sheet=10)
                first_pieces = first["tables"]["many_rows"]["pieces"]
                second_pieces = second["tables"]["many_rows"]["pieces"]
                self.assertEqual(len(first_pieces), 1000)
                self.assertEqual(
                    [item["sha256"] for item in first_pieces],
                    [item["sha256"] for item in second_pieces],
                )
                self.assertEqual((out1 / "index.json").read_bytes(), (out2 / "index.json").read_bytes())
                self.assertTrue(
                    all(item["bytes"] <= 20_000_000 for item in first_pieces)
                )
        finally:
            source.close()

    def test_malicious_table_name_uses_safe_unique_piece_names(self):
        source = sqlite3.connect(":memory:")
        try:
            table_name = "../../escape'\"\x01[] name"
            quoted = '"' + table_name.replace('"', '""') + '"'
            source.execute(f'CREATE TABLE {quoted} ("col\x01" TEXT)')
            source.execute(f"INSERT INTO {quoted} VALUES ('preserved')")

            with tempfile.TemporaryDirectory() as tmpdir:
                out_dir = Path(tmpdir)
                manifest = export_sqlite_tables(source, out_dir)
                piece_name = manifest["tables"][table_name]["piece_names"][0]
                self.assertRegex(piece_name, r"^t\d{4}_[0-9a-f]{12}_\d{4}\.xlsx$")
                self.assertEqual(Path(piece_name).name, piece_name)
                self.assertNotIn("..", piece_name)
                rows = read_lossless_xlsx(out_dir / piece_name)
                self.assertEqual(rows[0]["table"], table_name)
                self.assertEqual(manifest["tables"][table_name]["columns"], ["col\x01"])
        finally:
            source.close()

    def test_generated_columns_and_full_schema_objects_roundtrip(self):
        source = sqlite3.connect(":memory:")
        try:
            source.execute(
                """CREATE TABLE generated_items (
                    id INTEGER PRIMARY KEY,
                    base INTEGER NOT NULL,
                    label TEXT UNIQUE,
                    stored_value INTEGER GENERATED ALWAYS AS (base * 2) STORED,
                    virtual_value TEXT GENERATED ALWAYS AS (lower(label)) VIRTUAL
                )"""
            )
            source.execute("CREATE TABLE audit_log (item_id INTEGER, note TEXT)")
            source.execute(
                "CREATE INDEX generated_label_idx ON generated_items(virtual_value)"
            )
            source.execute(
                "CREATE VIEW generated_view AS "
                "SELECT id, stored_value, virtual_value FROM generated_items"
            )
            source.execute(
                """CREATE TRIGGER generated_audit AFTER INSERT ON generated_items
                   BEGIN INSERT INTO audit_log VALUES (NEW.id, 'inserted'); END"""
            )
            source.execute(
                "INSERT INTO generated_items(id, base, label) VALUES (7, 21, 'IKARING')"
            )

            with tempfile.TemporaryDirectory() as tmpdir:
                out_dir = Path(tmpdir)
                manifest = export_sqlite_tables(source, out_dir)
                self.assertTrue(
                    {"index", "view", "trigger"}.issubset(
                        {obj["type"] for obj in manifest["schema_objects"]}
                    )
                )
                self.assertTrue(
                    any(
                        obj["type"] == "index" and obj["sql"] is None
                        for obj in manifest["schema_objects"]
                    )
                )
                generated_meta = manifest["tables"]["generated_items"]
                self.assertEqual(
                    generated_meta["columns"],
                    ["id", "base", "label", "stored_value", "virtual_value"],
                )
                self.assertEqual(
                    [item["hidden"] for item in generated_meta["column_schema"]],
                    [0, 0, 0, 3, 2],
                )

                target = sqlite3.connect(":memory:")
                try:
                    counts = reconstruct_sqlite_tables(out_dir, target)
                    self.assertFalse(target.in_transaction)
                    self.assertEqual(counts["generated_items"], 1)
                    actual_schema = target.execute(
                        "SELECT name, type, tbl_name, sql FROM sqlite_master ORDER BY type, name"
                    ).fetchall()
                    self.assertEqual(
                        actual_schema,
                        [
                            (obj["name"], obj["type"], obj["tbl_name"], obj["sql"])
                            for obj in manifest["schema_objects"]
                        ],
                    )
                    target_xinfo = target.execute(
                        'PRAGMA table_xinfo("generated_items")'
                    ).fetchall()
                    self.assertEqual(
                        target_xinfo,
                        [
                            (
                                col["cid"],
                                col["name"],
                                col["type"],
                                col["notnull"],
                                col["dflt_value"],
                                col["pk"],
                                col["hidden"],
                            )
                            for col in generated_meta["column_schema"]
                        ],
                    )
                    self.assertEqual(
                        target.execute("SELECT * FROM generated_items").fetchone(),
                        (7, 21, "IKARING", 42, "ikaring"),
                    )
                    self.assertEqual(
                        target.execute("SELECT * FROM audit_log").fetchall(),
                        [(7, "inserted")],
                    )
                    self.assertEqual(
                        target.execute("SELECT * FROM generated_view").fetchall(),
                        [(7, 42, "ikaring")],
                    )
                finally:
                    target.close()
        finally:
            source.close()

    def test_sqlite_sequence_rows_keep_source_order_and_explicit_values(self):
        source = sqlite3.connect(":memory:")
        try:
            # Source insertion order intentionally differs from table DDL order.
            source.execute(
                "CREATE TABLE z_first (id INTEGER PRIMARY KEY AUTOINCREMENT, value TEXT)"
            )
            source.execute(
                "CREATE TABLE a_second (id INTEGER PRIMARY KEY AUTOINCREMENT, value TEXT)"
            )
            source.execute("INSERT INTO z_first(value) VALUES ('z')")
            source.execute("INSERT INTO a_second(value) VALUES ('a')")
            source.execute("UPDATE sqlite_sequence SET seq=91 WHERE name='z_first'")
            source.execute("UPDATE sqlite_sequence SET seq=47 WHERE name='a_second'")
            expected = source.execute(
                "SELECT name, seq FROM sqlite_sequence ORDER BY rowid"
            ).fetchall()
            self.assertEqual(expected, [("z_first", 91), ("a_second", 47)])

            with tempfile.TemporaryDirectory() as tmpdir:
                out_dir = Path(tmpdir)
                manifest = export_sqlite_tables(source, out_dir)
                target = sqlite3.connect(":memory:")
                try:
                    reconstruct_sqlite_tables(out_dir, target)
                    actual = target.execute(
                        "SELECT name, seq FROM sqlite_sequence ORDER BY rowid"
                    ).fetchall()
                    self.assertEqual(actual, expected)
                    self.assertEqual(
                        target.execute("INSERT INTO z_first(value) VALUES ('next')").lastrowid,
                        92,
                    )
                    self.assertEqual(
                        target.execute("INSERT INTO a_second(value) VALUES ('next')").lastrowid,
                        48,
                    )
                    self.assertEqual(
                        {obj["name"] for obj in manifest["schema_objects"] if obj["type"] == "table"},
                        set(manifest["tables"]),
                    )
                finally:
                    target.close()
        finally:
            source.close()

    def test_nonempty_or_symlink_output_is_preserved(self):
        source = sqlite3.connect(":memory:")
        try:
            source.execute("CREATE TABLE t (value TEXT)")
            with tempfile.TemporaryDirectory() as tmpdir, tempfile.TemporaryDirectory() as linked:
                output = Path(tmpdir) / "out"
                output.mkdir()
                sentinel = output / "keep.txt"
                sentinel.write_text("original", encoding="utf-8")
                with self.assertRaisesRegex(ValueError, "must be empty"):
                    export_sqlite_tables(source, output)
                self.assertEqual(sentinel.read_text(encoding="utf-8"), "original")

                linked_dir = Path(linked)
                linked_sentinel = linked_dir / "keep.txt"
                linked_sentinel.write_text("linked original", encoding="utf-8")
                output_link = Path(tmpdir) / "linked-output"
                output_link.symlink_to(linked_dir, target_is_directory=True)
                with self.assertRaisesRegex(ValueError, "symbolic link"):
                    export_sqlite_tables(source, output_link)
                self.assertEqual(
                    linked_sentinel.read_text(encoding="utf-8"), "linked original"
                )
                self.assertEqual(list(linked_dir.iterdir()), [linked_sentinel])
        finally:
            source.close()

    def test_restore_rejects_nonempty_target_and_rolls_back_piece_failure(self):
        source = sqlite3.connect(":memory:")
        try:
            source.execute("CREATE TABLE t (value TEXT)")
            source.execute("INSERT INTO t VALUES ('first')")
            source.execute("INSERT INTO t VALUES ('second')")
            with tempfile.TemporaryDirectory() as tmpdir:
                out_dir = Path(tmpdir)
                manifest = export_sqlite_tables(source, out_dir, max_rows_per_sheet=1)

                nonempty = sqlite3.connect(":memory:")
                try:
                    nonempty.execute("CREATE TABLE keep (value TEXT)")
                    nonempty.execute("INSERT INTO keep VALUES ('untouched')")
                    with self.assertRaisesRegex(ValueError, "empty SQLite database"):
                        reconstruct_sqlite_tables(out_dir, nonempty)
                    self.assertEqual(
                        nonempty.execute("SELECT value FROM keep").fetchone()[0],
                        "untouched",
                    )
                finally:
                    nonempty.close()

                broken = json.loads(json.dumps(manifest))
                piece = broken["tables"]["t"]["pieces"][1]
                piece["sha256"] = "0" * 64
                broken["tables"]["t"]["sha256"][1] = piece["sha256"]
                broken["pieces"][1]["sha256"] = piece["sha256"]
                (out_dir / "index.json").write_text(
                    json.dumps(broken), encoding="utf-8"
                )
                target = sqlite3.connect(":memory:")
                try:
                    target.execute("BEGIN")
                    with self.assertRaisesRegex(ValueError, "SHA-256 mismatch"):
                        reconstruct_sqlite_tables(out_dir, target)
                    self.assertTrue(target.in_transaction)
                    self.assertEqual(
                        target.execute("SELECT name FROM sqlite_master").fetchall(), []
                    )
                finally:
                    target.close()
        finally:
            source.close()

    def test_restore_respects_callers_outer_transaction(self):
        source = sqlite3.connect(":memory:")
        try:
            source.execute("CREATE TABLE t (value TEXT)")
            source.execute("INSERT INTO t VALUES ('preserved')")
            with tempfile.TemporaryDirectory() as tmpdir:
                out_dir = Path(tmpdir)
                export_sqlite_tables(source, out_dir)
                target = sqlite3.connect(":memory:")
                try:
                    target.execute("PRAGMA foreign_keys = ON")
                    target.execute("BEGIN")
                    reconstruct_sqlite_tables(out_dir, target)
                    self.assertTrue(target.in_transaction)
                    self.assertEqual(target.execute("PRAGMA foreign_keys").fetchone()[0], 1)
                    self.assertEqual(
                        target.execute("PRAGMA defer_foreign_keys").fetchone()[0], 0
                    )
                    self.assertEqual(
                        target.execute("SELECT value FROM t").fetchone()[0], "preserved"
                    )
                    target.rollback()
                    self.assertEqual(
                        target.execute("SELECT name FROM sqlite_master").fetchall(), []
                    )
                finally:
                    target.close()
        finally:
            source.close()

    def test_restore_rejects_missing_column_and_duplicate_cell_chunks(self):
        for duplicate in (False, True):
            source = sqlite3.connect(":memory:")
            try:
                source.execute("CREATE TABLE t (id INTEGER, value TEXT)")
                value = "x" * 16001 if duplicate else "present"
                source.execute("INSERT INTO t VALUES (1, ?)", (value,))
                with tempfile.TemporaryDirectory() as tmpdir:
                    out_dir = Path(tmpdir)
                    manifest = export_sqlite_tables(source, out_dir)
                    piece_path = _piece_path(out_dir, manifest, "t")
                    with zipfile.ZipFile(piece_path, "r") as original:
                        sheet_xml = original.read("xl/worksheets/sheet1.xml").decode("utf-8")
                        entries = [
                            (info, original.read(info.filename))
                            for info in original.infolist()
                            if info.filename != "xl/worksheets/sheet1.xml"
                        ]
                    if duplicate:
                        start = sheet_xml.index('<row r="3">')
                        end = sheet_xml.index("</row>", start) + len("</row>")
                        duplicate_row = sheet_xml[start:end]
                        sheet_xml = sheet_xml.replace(
                            "</sheetData>", duplicate_row + "</sheetData>", 1
                        )
                        expected_error = "Duplicate cell"
                    else:
                        start = sheet_xml.index('<c r="C2"')
                        end = sheet_xml.index("</c>", start) + len("</c>")
                        sheet_xml = sheet_xml[:start] + sheet_xml[end:]
                        expected_error = "Missing or extra XLSX columns"

                    replacement = piece_path.with_suffix(".replacement")
                    with zipfile.ZipFile(replacement, "w", compression=zipfile.ZIP_DEFLATED) as rebuilt:
                        for info, payload in entries:
                            rebuilt.writestr(info, payload)
                        with zipfile.ZipFile(piece_path, "r") as original:
                            sheet_info = original.getinfo("xl/worksheets/sheet1.xml")
                        rebuilt.writestr(sheet_info, sheet_xml.encode("utf-8"))
                    replacement.replace(piece_path)

                    piece_meta = manifest["tables"]["t"]["pieces"][0]
                    piece_meta["bytes"] = piece_path.stat().st_size
                    piece_meta["sha256"] = hashlib.sha256(piece_path.read_bytes()).hexdigest()
                    manifest["tables"]["t"]["sha256"][0] = piece_meta["sha256"]
                    manifest["pieces"][0]["bytes"] = piece_meta["bytes"]
                    manifest["pieces"][0]["sha256"] = piece_meta["sha256"]
                    (out_dir / "index.json").write_text(
                        json.dumps(manifest), encoding="utf-8"
                    )

                    target = sqlite3.connect(":memory:")
                    try:
                        with self.assertRaisesRegex(ValueError, expected_error):
                            reconstruct_sqlite_tables(out_dir, target)
                        self.assertEqual(
                            target.execute("SELECT name FROM sqlite_master").fetchall(), []
                        )
                    finally:
                        target.close()
            finally:
                source.close()

    def test_surrogate_text_is_rejected_explicitly(self):
        source = sqlite3.connect(":memory:")
        try:
            source.execute("CREATE TABLE t (value TEXT)")
            source.execute("INSERT INTO t VALUES (CAST(X'EDA080' AS TEXT))")
            source.text_factory = lambda raw: raw.decode("utf-8", "surrogateescape")
            with tempfile.TemporaryDirectory() as tmpdir:
                with self.assertRaisesRegex(ValueError, r"Surrogate U\+"):
                    export_sqlite_tables(source, Path(tmpdir))
                self.assertEqual(list(Path(tmpdir).iterdir()), [])
        finally:
            source.close()


if __name__ == "__main__":
    unittest.main()
