import hashlib
import json
from pathlib import Path
import resource
import sqlite3
import sys
import tempfile
import time
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src/python"))

from ikarchive.lossless_sqlite import (
    DEFAULT_MAX_BYTES,
    MAX_ALLOWED_BYTES,
    VALUE_CHUNK_BYTES,
    export_sqlite_shards,
    iter_table_rows,
    iter_table_rows_with_identity,
    verify_sqlite_shards,
)
from ikarchive.slice_selectors import export_selectors, verify_selectors


def _sha(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _quote(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _save_manifest(root: Path, manifest: dict) -> None:
    (root / "manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, sort_keys=True, indent=2) + "\n",
        encoding="utf-8",
    )


def _source_sha_for_selectors(source: sqlite3.Connection) -> str:
    schema = list(source.execute(
        "SELECT type,name,tbl_name,sql FROM sqlite_master ORDER BY type,name"
    ))
    raw = json.dumps(schema, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(raw).hexdigest()


def _selector_source(row_count: int = 4) -> sqlite3.Connection:
    source = sqlite3.connect(":memory:")
    source.executescript(
        """
        CREATE TABLE matches (
            account TEXT NOT NULL,
            kind TEXT NOT NULL,
            match_key TEXT NOT NULL,
            payload TEXT,
            PRIMARY KEY(account,kind,match_key)
        );
        CREATE TABLE match_classification (
            account TEXT NOT NULL,
            kind TEXT NOT NULL,
            match_key TEXT NOT NULL,
            analysis_set TEXT NOT NULL,
            rule_raw TEXT,
            PRIMARY KEY(account,kind,match_key),
            FOREIGN KEY(account,kind,match_key)
              REFERENCES matches(account,kind,match_key)
        );
        CREATE TABLE extra_data (ordinal INTEGER PRIMARY KEY, raw_value BLOB);
        """
    )
    modes = ("analysis_mode_alpha", "analysis_mode_beta")
    rules = ("RULE_ALPHA", "RULE_BETA")
    match_rows = []
    classification_rows = []
    for ordinal in range(row_count):
        mode = modes[ordinal % len(modes)]
        rule = rules[(ordinal // len(modes)) % len(rules)]
        key = f"match-{ordinal:06d}"
        match_rows.append(("synthetic", "vs", key, f"source payload {ordinal}"))
        classification_rows.append(("synthetic", "vs", key, mode, rule))
    source.executemany("INSERT INTO matches VALUES(?,?,?,?)", match_rows)
    source.executemany("INSERT INTO match_classification VALUES(?,?,?,?,?)", classification_rows)
    source.executemany("INSERT INTO extra_data VALUES(?,?)", ((i, bytes([i % 256])) for i in range(row_count)))
    source.commit()
    return source


def _finalized_selector_package(source: sqlite3.Connection, root: Path) -> dict:
    manifest = export_sqlite_shards(source, root, snapshot_id="selector-finalized")
    source_sha = _source_sha_for_selectors(source)
    export_selectors(source, root, manifest, source_sha)
    manifest["source_sha256"] = source_sha
    _save_manifest(root, manifest)
    return manifest


class LosslessSqliteShardsTests(unittest.TestCase):
    def test_public_identity_stream_preserves_negative_gaps_and_key_only_rows(self):
        with tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parents[2]) as directory:
            source = sqlite3.connect(':memory:')
            source.executescript('CREATE TABLE original(value); CREATE TABLE keyed(k TEXT PRIMARY KEY, value) WITHOUT ROWID;')
            source.executemany('INSERT INTO original(rowid,value) VALUES(?,?)',
                               [(-8, b'\x00'), (9, 'raw\x00text'), (9223372036854775807, 42)])
            source.execute("INSERT INTO keyed VALUES('a',NULL)")
            root = Path(directory) / 'parts'
            manifest = export_sqlite_shards(source, root, snapshot_id='identities')
            self.assertEqual(list(iter_table_rows_with_identity(root, manifest, 'original')),
                             [(0, -8, (b'\x00',)), (1, 9, ('raw\x00text',)),
                              (2, 9223372036854775807, (42,))])
            self.assertEqual(list(iter_table_rows_with_identity(root, manifest, 'keyed')),
                             [(0, None, ('a', None))])
            source.close()

    def test_100k_rows_fragmented_at_20mib_with_time_and_peak_rss_recorded(self):
        row_total = 100_000
        with tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parents[2]) as temporary:
            base = Path(temporary)
            source_path = base / "source.sqlite3"
            source = sqlite3.connect(source_path)
            source.execute("PRAGMA journal_mode=DELETE")
            source.executescript(
                """
                CREATE TABLE matches (
                    account TEXT NOT NULL,
                    kind TEXT NOT NULL,
                    match_key TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    PRIMARY KEY(account,kind,match_key)
                );
                CREATE TABLE match_classification (
                    account TEXT NOT NULL,
                    kind TEXT NOT NULL,
                    match_key TEXT NOT NULL,
                    analysis_set TEXT NOT NULL,
                    rule_raw TEXT,
                    PRIMARY KEY(account,kind,match_key),
                    FOREIGN KEY(account,kind,match_key)
                      REFERENCES matches(account,kind,match_key)
                );
                """
            )
            source.executemany(
                "INSERT INTO matches VALUES(?,?,?,?)",
                (
                    ("benchmark", "vs", f"match-{ordinal:06d}", f"{ordinal:07d}:" + "p" * 212)
                    for ordinal in range(row_total)
                ),
            )
            source.executemany(
                "INSERT INTO match_classification VALUES(?,?,?,?,?)",
                (
                    (
                        "benchmark",
                        "vs",
                        f"match-{ordinal:06d}",
                        "analysis_mode_alpha" if ordinal % 2 == 0 else "analysis_mode_beta",
                        "RULE_ALPHA" if (ordinal // 2) % 2 == 0 else "RULE_BETA",
                    )
                    for ordinal in range(row_total)
                ),
            )
            source.commit()
            source_bytes = source_path.stat().st_size
            source_sha = _sha(source_path)
            output = base / "package"
            started = time.perf_counter()
            try:
                manifest = export_sqlite_shards(
                    source, output, snapshot_id="performance-100k", max_bytes=DEFAULT_MAX_BYTES
                )
                export_selectors(source, output, manifest, source_sha)
                manifest["source_sha256"] = source_sha
                _save_manifest(output, manifest)
                receipt = verify_sqlite_shards(source, output, manifest)
                elapsed_seconds = time.perf_counter() - started
                match_table = next(row for row in manifest["tables"] if row["name"] == "matches")
                part_count = len(match_table["parts"])
                self.assertGreaterEqual(part_count, 2)
                self.assertEqual(receipt["row_counts"]["matches"], row_total)
                self.assertTrue(all(
                    part["bytes"] <= 20 * 1024 * 1024
                    and part["page_count"] * part["page_size"] <= 20 * 1024 * 1024
                    for table in manifest["tables"]
                    for part in table["parts"]
                ))
                self.assertEqual(source_path.stat().st_size, source_bytes)
                self.assertEqual(_sha(source_path), source_sha)
                peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
                peak_rss_bytes = int(peak if sys.platform == "darwin" else peak * 1024)
                metrics = {
                    "rows": receipt["row_count"],
                    "matches_rows": row_total,
                    "max_bytes": DEFAULT_MAX_BYTES,
                    "matches_parts": part_count,
                    "all_data_parts": sum(len(table["parts"]) for table in manifest["tables"]),
                    "source_bytes": source_bytes,
                    "elapsed_seconds": round(elapsed_seconds, 3),
                    "process_peak_rss_bytes": peak_rss_bytes,
                }
                print("LOSSLESS_SQLITE_PERF " + json.dumps(metrics, sort_keys=True))
            finally:
                source.close()

    def test_finalized_selector_manifest_can_be_iterated_and_verified_in_place(self):
        source = _selector_source()
        try:
            with tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parents[2]) as temporary:
                root = Path(temporary) / "package"
                root.mkdir()
                manifest = _finalized_selector_package(source, root)
                self.assertEqual(manifest["source_sha256"], _source_sha_for_selectors(source))
                self.assertTrue(manifest["by_mode"])
                self.assertEqual(len(manifest["by_rule"]), 4)
                self.assertEqual(
                    {table["name"] for table in manifest["tables"]},
                    {"matches", "match_classification", "extra_data"},
                )
                self.assertFalse(
                    {"slice_meta", "shared_files", "archive_schema_objects"}
                    & {table["name"] for table in manifest["tables"]}
                )

                selector_receipt = verify_selectors(
                    source, root, manifest, manifest["source_sha256"]
                )
                self.assertTrue(selector_receipt["all_shared_files_reachable"])
                for table in manifest["tables"]:
                    name = table["name"]
                    columns = [row[1] for row in source.execute(f'PRAGMA table_info("{name}")')]
                    expected = [
                        tuple(row)
                        for row in source.execute(
                            f'SELECT {",".join(_quote(column) for column in columns)} '
                            f'FROM {_quote(name)} ORDER BY rowid'
                        )
                    ]
                    self.assertEqual(
                        list(iter_table_rows(root, manifest, name)),
                        list(enumerate(expected)),
                        f"source values differ for {name}",
                    )
                receipt = verify_sqlite_shards(source, root, manifest)
                self.assertEqual(receipt["status"], "verified")
                self.assertEqual(receipt["row_count"], 12)
                self.assertEqual(receipt["row_counts"], {
                    "extra_data": 4,
                    "match_classification": 4,
                    "matches": 4,
                })
        finally:
            source.close()

    def test_finalized_selector_inventory_rejects_corruption_and_unsafe_declarations(self):
        corruptions = (
            "hash",
            "undeclared",
            "unsafe-path",
            "duplicate-path",
            "oversize",
            "symlink",
            "sidecar",
        )
        for corruption in corruptions:
            with self.subTest(corruption=corruption):
                source = _selector_source()
                try:
                    with tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parents[2]) as temporary:
                        root = Path(temporary) / "package"
                        root.mkdir()
                        manifest = _finalized_selector_package(source, root)
                        first = manifest["by_mode"][0]
                        selector = root / first["file"]
                        if corruption == "hash":
                            with selector.open("r+b") as stream:
                                stream.seek(-1, 2)
                                byte = stream.read(1)
                                stream.seek(-1, 2)
                                stream.write(bytes([byte[0] ^ 1]))
                            message = "SHA-256 mismatch"
                        elif corruption == "undeclared":
                            extra = root / "by-mode" / "not-declared.sqlite3"
                            connection = sqlite3.connect(extra)
                            connection.execute("CREATE TABLE rogue(value)")
                            connection.close()
                            message = "inventory mismatch"
                        elif corruption == "unsafe-path":
                            first["file"] = "by-mode/../escape.sqlite3"
                            _save_manifest(root, manifest)
                            message = "unsafe"
                        elif corruption == "duplicate-path":
                            manifest["by_mode"].append(dict(first))
                            _save_manifest(root, manifest)
                            message = "duplicate selector path"
                        elif corruption == "oversize":
                            first["bytes"] = MAX_ALLOWED_BYTES + 1
                            _save_manifest(root, manifest)
                            message = "selector exceeds"
                        elif corruption == "symlink":
                            target = root / manifest["by_mode"][1]["file"]
                            selector.unlink()
                            selector.symlink_to(target)
                            message = "symbolic link"
                        else:
                            (root / "by-mode" / "scratch.sqlite3-wal").write_bytes(b"")
                            message = "sidecar"
                        with self.assertRaisesRegex(ValueError, message):
                            list(iter_table_rows(root, manifest, "extra_data"))
                finally:
                    source.close()

    def test_all_tables_typed_values_generated_columns_and_source_bytes(self):
        with tempfile.TemporaryDirectory() as temporary:
            base = Path(temporary)
            source_path = base / "source.sqlite3"
            source = sqlite3.connect(source_path)
            source.executescript(
                """
                CREATE TABLE typed (
                    id INTEGER PRIMARY KEY,
                    dynamic,
                    text_value TEXT,
                    blob_value BLOB,
                    empty_text TEXT,
                    empty_blob BLOB,
                    null_value TEXT,
                    real_value REAL,
                    generated TEXT GENERATED ALWAYS AS (text_value || ':' || id) STORED
                );
                CREATE TABLE composite_key (
                    left_key TEXT,
                    right_key INTEGER,
                    payload,
                    PRIMARY KEY(right_key, left_key)
                ) WITHOUT ROWID;
                CREATE TABLE empty_table (present_column, another_column);
                CREATE TABLE generated_only (left_value, right_value,
                    sum_value INTEGER GENERATED ALWAYS AS (left_value + right_value) VIRTUAL);
                CREATE TABLE autoincrement_rows (id INTEGER PRIMARY KEY AUTOINCREMENT, value);
                CREATE INDEX typed_text_index ON typed(text_value);
                CREATE VIEW typed_view AS SELECT id, text_value FROM typed;
                CREATE TRIGGER typed_trigger AFTER INSERT ON typed BEGIN
                    UPDATE typed SET dynamic = dynamic WHERE id = NEW.id;
                END;
                """
            )
            source.execute(
                "INSERT INTO typed(id,dynamic,text_value,blob_value,empty_text,empty_blob,null_value,real_value) "
                "VALUES(?,?,?,?,?,?,?,?)",
                (
                    9,
                    "00000000000000000042",
                    "first\x00line\r\n🦑",
                    b"\x00\xffblob",
                    "",
                    b"",
                    None,
                    -0.0,
                ),
            )
            source.execute(
                "INSERT INTO typed(id,dynamic,text_value,blob_value,empty_text,empty_blob,null_value,real_value) "
                "VALUES(?,?,?,?,?,?,?,?)",
                (
                    3,
                    9223372036854775807,
                    None,
                    b"second",
                    "not-empty",
                    b"not-empty",
                    "",
                    1.2345678901234567e-300,
                ),
            )
            source.executemany(
                "INSERT INTO composite_key VALUES(?,?,?)",
                [("z", 2, b"two"), ("a", 1, "one"), ("a", 3, None)],
            )
            source.execute("INSERT INTO generated_only(left_value,right_value) VALUES(4,5)")
            source.execute("INSERT INTO autoincrement_rows(value) VALUES('auto')")
            source.commit()
            before = (source_path.stat().st_size, _sha(source_path))

            output = base / "shards"
            try:
                manifest = export_sqlite_shards(source, output, snapshot_id="opaque / not a path")
                self.assertEqual(manifest["version"], 2)
                self.assertEqual(manifest["role"], "lossless_sqlite_shards")
                self.assertEqual(manifest["snapshot_identifier"], "opaque / not a path")
                self.assertEqual(
                    [item["name"] for item in manifest["tables"]],
                    [item["name"] for item in manifest["schema_objects"] if item["type"] == "table"],
                )
                self.assertIn("sqlite_sequence", [item["name"] for item in manifest["tables"]])
                typed = next(item for item in manifest["tables"] if item["name"] == "typed")
                self.assertEqual(typed["columns"], [
                    "id", "dynamic", "text_value", "blob_value", "empty_text",
                    "empty_blob", "null_value", "real_value", "generated",
                ])
                self.assertEqual(typed["column_schema"][-1]["hidden"], 3)
                self.assertEqual(typed["foreign_keys"], [])
                self.assertEqual(len(typed["parts"]), 1)
                self.assertEqual(
                    list(iter_table_rows(output, manifest, "typed")),
                    [
                        (0, (3, 9223372036854775807, None, b"second", "not-empty", b"not-empty", "", 1.2345678901234567e-300, None)),
                        (1, (9, "00000000000000000042", "first\x00line\r\n🦑", b"\x00\xffblob", "", b"", None, -0.0, "first\x00line\r\n🦑:9")),
                    ],
                )
                composite_rows = list(iter_table_rows(output, manifest, "composite_key"))
                self.assertEqual([row[1][:2] for row in composite_rows], [("a", 1), ("z", 2), ("a", 3)])
                empty_table = next(item for item in manifest["tables"] if item["name"] == "empty_table")
                self.assertEqual(empty_table["row_count"], 0)
                self.assertEqual(len(empty_table["parts"]), 1)
                self.assertEqual((empty_table["parts"][0]["row_start"], empty_table["parts"][0]["row_end"]), (0, 0))
                self.assertEqual(list(iter_table_rows(output, manifest, "empty_table")), [])

                receipt = verify_sqlite_shards(source, output, manifest)
                self.assertEqual(receipt["status"], "verified")
                self.assertEqual(receipt["coverage"], {
                    "all_tables": True,
                    "all_rows": True,
                    "all_columns": True,
                    "all_values": True,
                    "external_values": True,
                })
                self.assertEqual(receipt["row_counts"]["typed"], 2)
                self.assertTrue((output / "verification.json").is_file())
                self.assertEqual((source_path.stat().st_size, _sha(source_path)), before)
            finally:
                source.close()

    def test_large_cell_spans_bounded_value_databases_and_roundtrips(self):
        source = sqlite3.connect(":memory:")
        try:
            source.execute("CREATE TABLE payloads (id INTEGER PRIMARY KEY, binary_value, text_value)")
            blob = bytes(range(256)) * 5000
            text = "日本語🦑" * 50000
            source.execute("INSERT INTO payloads VALUES(1,?,?)", (blob, text))
            with tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary) / "shards"
                manifest = export_sqlite_shards(
                    source, root, snapshot_id="large", max_bytes=1024 * 1024
                )
                self.assertGreaterEqual(len(manifest["external_values"]), 2)
                self.assertTrue(all(item["bytes"] <= 1024 * 1024 for item in manifest["external_values"]))
                total_chunks = 0
                for item in manifest["external_values"]:
                    path = root / item["file"]
                    conn = sqlite3.connect(path)
                    try:
                        page_count = conn.execute("PRAGMA page_count").fetchone()[0]
                        page_size = conn.execute("PRAGMA page_size").fetchone()[0]
                        self.assertLessEqual(page_count * page_size, 1024 * 1024)
                        self.assertLessEqual(path.stat().st_size, 1024 * 1024)
                        self.assertEqual(
                            conn.execute("SELECT MAX(length(payload)) FROM value_chunks").fetchone()[0],
                            VALUE_CHUNK_BYTES,
                        )
                        total_chunks += conn.execute("SELECT COUNT(*) FROM value_chunks").fetchone()[0]
                    finally:
                        conn.close()
                self.assertEqual(total_chunks, manifest["counts"]["value_chunks"])
                row = list(iter_table_rows(root, manifest, "payloads"))[0][1]
                self.assertEqual(row[1], blob)
                self.assertEqual(row[2], text)
                self.assertEqual(verify_sqlite_shards(source, root, manifest)["status"], "verified")
        finally:
            source.close()

    def test_shadowed_rowid_aliases_are_not_claimed_as_preserved(self):
        source = sqlite3.connect(":memory:")
        try:
            source.execute('CREATE TABLE shadowed (rowid, _rowid_, oid, value)')
            source.executemany("INSERT INTO shadowed VALUES(?,?,?,?)", [(1, 2, 3, "a"), (4, 5, 6, "b")])
            with tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary) / "shards"
                manifest = export_sqlite_shards(source, root, snapshot_id="shadowed")
                table = manifest["tables"][0]
                self.assertNotIn("rowid_kind", table)
                self.assertTrue(table["rowid_aliases_shadowed"])
                self.assertEqual([row[0] for row in iter_table_rows(root, manifest, "shadowed")], [0, 1])
                self.assertEqual(verify_sqlite_shards(source, root, manifest)["status"], "verified")
        finally:
            source.close()

    def test_sqlite_row_factory_is_supported_and_custom_text_factory_is_rejected(self):
        source = sqlite3.connect(":memory:")
        source.row_factory = sqlite3.Row
        source.execute("CREATE TABLE row_objects (id INTEGER PRIMARY KEY, value TEXT)")
        source.execute("INSERT INTO row_objects VALUES(1,'text')")
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "row-shards"
            manifest = export_sqlite_shards(source, root, snapshot_id="row-factory")
            self.assertEqual(list(iter_table_rows(root, manifest, "row_objects")), [(0, (1, "text"))])
            self.assertEqual(verify_sqlite_shards(source, root, manifest)["status"], "verified")
            source.close()

            bytes_source = sqlite3.connect(":memory:")
            bytes_source.text_factory = bytes
            bytes_source.execute("CREATE TABLE text_values (value TEXT)")
            bytes_source.execute("INSERT INTO text_values VALUES('text')")
            blocked = Path(temporary) / "custom-text-factory-shards"
            with self.assertRaisesRegex(ValueError, "standard str text_factory"):
                export_sqlite_shards(bytes_source, blocked, snapshot_id="custom-text")
            self.assertFalse(blocked.exists())
            bytes_source.close()

    def test_foreign_key_violations_are_preserved_and_reported(self):
        source = sqlite3.connect(":memory:")
        try:
            source.execute("PRAGMA foreign_keys=OFF")
            source.executescript(
                "CREATE TABLE parent(id INTEGER PRIMARY KEY);"
                "CREATE TABLE child(id INTEGER PRIMARY KEY, parent_id REFERENCES parent(id));"
                "INSERT INTO child VALUES(1,999);"
            )
            with tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary) / "shards"
                manifest = export_sqlite_shards(source, root, snapshot_id="fk")
                child = next(item for item in manifest["tables"] if item["name"] == "child")
                self.assertEqual(child["foreign_keys"][0]["table"], "parent")
                receipt = verify_sqlite_shards(source, root, manifest)
                self.assertEqual(receipt["status"], "verified")
                self.assertEqual(receipt["known_source_foreign_key_violations"], {"child": 1})
        finally:
            source.close()

    def test_reserved_sqlite_stats_and_archive_named_tables_are_not_lost(self):
        source = sqlite3.connect(":memory:")
        try:
            source.execute("CREATE TABLE measured(value)")
            source.executemany("INSERT INTO measured VALUES(?)", [(1,), (2,)])
            source.execute("ANALYZE")
            source.execute('CREATE TABLE "_archive_rows" (value)')
            source.execute('INSERT INTO "_archive_rows" VALUES("rows")')
            source.execute('CREATE TABLE "_archive_external_cells" (value)')
            source.execute('INSERT INTO "_archive_external_cells" VALUES("external")')
            with tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary) / "shards"
                manifest = export_sqlite_shards(source, root, snapshot_id="reserved")
                names = [table["name"] for table in manifest["tables"]]
                self.assertIn("sqlite_stat1", names)
                self.assertIn("sqlite_stat4", names)
                self.assertEqual(list(iter_table_rows(root, manifest, "_archive_rows")), [(0, ("rows",))])
                self.assertEqual(
                    list(iter_table_rows(root, manifest, "_archive_external_cells")),
                    [(0, ("external",))],
                )
                self.assertEqual(verify_sqlite_shards(source, root, manifest)["status"], "verified")
        finally:
            source.close()

    def test_virtual_tables_fail_explicitly_instead_of_being_omitted(self):
        source = sqlite3.connect(":memory:")
        try:
            source.execute("CREATE VIRTUAL TABLE documents USING fts5(body)")
            with tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary) / "shards"
                with self.assertRaisesRegex(ValueError, "virtual/shadow"):
                    export_sqlite_shards(source, root, snapshot_id="unsupported")
                failure = json.loads((root / "verification.failed.json").read_text(encoding="utf-8"))
                self.assertEqual(failure["status"], "failed")
                self.assertFalse((root / "manifest.json").exists())
        finally:
            source.close()

    def test_missing_file_and_wrong_hash_are_rejected(self):
        source = sqlite3.connect(":memory:")
        source.execute("CREATE TABLE payloads (id, value)")
        source.execute("INSERT INTO payloads VALUES(1, ?)", (b"b" * 400000,))
        with tempfile.TemporaryDirectory() as temporary:
            base = Path(temporary)
            root = base / "shards"
            manifest = export_sqlite_shards(source, root, snapshot_id="corrupt")
            value_file = root / manifest["external_values"][0]["file"]
            moved = value_file.with_suffix(".missing")
            value_file.rename(moved)
            with self.assertRaisesRegex(ValueError, "inventory mismatch|missing"):
                list(iter_table_rows(root, manifest, "payloads"))
            moved.rename(value_file)
            with value_file.open("r+b") as stream:
                stream.seek(-1, 2)
                original = stream.read(1)
                stream.seek(-1, 2)
                stream.write(bytes([original[0] ^ 1]))
            with self.assertRaisesRegex(ValueError, "SHA-256 mismatch"):
                list(iter_table_rows(root, manifest, "payloads"))
        source.close()

    def test_missing_and_duplicate_chunks_are_rejected_after_indexed_file_rehash(self):
        for corruption in ("missing", "duplicate", "orphan", "wrong-size"):
            with self.subTest(corruption=corruption):
                source = sqlite3.connect(":memory:")
                source.execute("CREATE TABLE payloads (id, value)")
                source.execute("INSERT INTO payloads VALUES(1, ?)", (b"p" * 500000,))
                with tempfile.TemporaryDirectory() as temporary:
                    root = Path(temporary) / "shards"
                    manifest = export_sqlite_shards(source, root, snapshot_id=corruption, max_bytes=2 * 1024 * 1024)
                    ext = manifest["external_values"][0]
                    path = root / ext["file"]
                    conn = sqlite3.connect(path)
                    try:
                        row = conn.execute(
                            "SELECT row_ordinal,column_ordinal,chunk_index,chunk_total,sqlite_type,payload "
                            "FROM value_chunks ORDER BY row_ordinal,column_ordinal,chunk_index LIMIT 1"
                        ).fetchone()
                        if corruption == "missing":
                            conn.execute(
                                "DELETE FROM value_chunks WHERE row_ordinal=? AND column_ordinal=? AND chunk_index=?",
                                row[:3],
                            )
                            ext["chunk_count"] -= 1
                        elif corruption == "duplicate":
                            conn.execute("ALTER TABLE value_chunks RENAME TO old_chunks")
                            conn.execute(
                                "CREATE TABLE value_chunks (row_ordinal INTEGER NOT NULL, "
                                "column_ordinal INTEGER NOT NULL, chunk_index INTEGER NOT NULL, "
                                "chunk_total INTEGER NOT NULL, sqlite_type TEXT NOT NULL, payload BLOB NOT NULL)"
                            )
                            conn.execute("INSERT INTO value_chunks SELECT * FROM old_chunks")
                            conn.execute("INSERT INTO value_chunks VALUES(?,?,?,?,?,?)", row)
                            conn.execute("DROP TABLE old_chunks")
                            ext["chunk_count"] += 1
                        elif corruption == "orphan":
                            conn.execute(
                                "INSERT INTO value_chunks VALUES(?,?,?,?,?,?)",
                                (1, 99, 0, 1, "blob", b"orphan"),
                            )
                            ext["chunk_count"] += 1
                            ext["cell_count"] += 1
                        else:
                            conn.execute(
                                "UPDATE value_chunks SET payload=? WHERE row_ordinal=? "
                                "AND column_ordinal=? AND chunk_index=?",
                                (row[5][:-1], *row[:3]),
                            )
                        conn.commit()
                    finally:
                        conn.close()
                    ext["bytes"] = path.stat().st_size
                    ext["sha256"] = _sha(path)
                    check = sqlite3.connect(path)
                    try:
                        ext["page_count"] = check.execute("PRAGMA page_count").fetchone()[0]
                        ext["page_size"] = check.execute("PRAGMA page_size").fetchone()[0]
                    finally:
                        check.close()
                    _save_manifest(root, manifest)
                    with self.assertRaisesRegex(ValueError, "chunk|inventory|unreferenced"):
                        list(iter_table_rows(root, manifest, "payloads"))
                source.close()

    def test_schema_mismatch_and_nonempty_or_symlink_output_fail(self):
        source = sqlite3.connect(":memory:")
        source.execute("CREATE TABLE original (value)")
        source.execute("INSERT INTO original VALUES('kept')")
        with tempfile.TemporaryDirectory() as temporary:
            base = Path(temporary)
            root = base / "shards"
            manifest = export_sqlite_shards(source, root, snapshot_id="schema")
            source.execute("ALTER TABLE original ADD COLUMN later")
            with self.assertRaisesRegex(ValueError, "schema"):
                verify_sqlite_shards(source, root, manifest)
            self.assertTrue((root / "verification.failed.json").is_file())

            nonempty = base / "nonempty"
            nonempty.mkdir()
            (nonempty / "keep.txt").write_text("untouched", encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "empty"):
                export_sqlite_shards(source, nonempty, snapshot_id="blocked")
            self.assertEqual((nonempty / "keep.txt").read_text(encoding="utf-8"), "untouched")

            target = base / "link-target"
            target.mkdir()
            link = base / "link"
            link.symlink_to(target, target_is_directory=True)
            with self.assertRaisesRegex(ValueError, "symbolic link"):
                export_sqlite_shards(source, link, snapshot_id="blocked")
        source.close()

    def test_sibling_output_is_allowed_and_every_part_stays_bounded(self):
        with tempfile.TemporaryDirectory() as temporary:
            base = Path(temporary)
            source_path = base / "snapshot.sqlite3"
            source = sqlite3.connect(source_path)
            source.execute("CREATE TABLE many (id INTEGER PRIMARY KEY, content TEXT)")
            source.executemany(
                "INSERT INTO many VALUES(?,?)",
                ((index, "x" * 4000) for index in range(800)),
            )
            source.commit()
            before = (source_path.stat().st_size, _sha(source_path))
            output = base / "xlsx-and-sqlite-siblings"
            manifest = export_sqlite_shards(source, output, snapshot_id="siblings", max_bytes=512 * 1024)
            self.assertTrue(manifest["tables"])
            for table in manifest["tables"]:
                for part in table["parts"]:
                    path = output / part["file"]
                    self.assertLessEqual(path.stat().st_size, manifest["max_bytes"])
                    self.assertLessEqual(part["page_count"] * part["page_size"], manifest["max_bytes"])
            self.assertEqual(verify_sqlite_shards(source, output, manifest)["status"], "verified")
            self.assertEqual((source_path.stat().st_size, _sha(source_path)), before)
            source.close()


if __name__ == "__main__":
    unittest.main()
