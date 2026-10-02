from __future__ import annotations

import hashlib
import json
import os
from collections import Counter
from pathlib import Path
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[1]
REPO = ROOT.parent
sys.path.insert(0, str(REPO / "src/python"))

from ikarchive.lossless_sqlite import export_sqlite_shards, verify_sqlite_shards  # noqa: E402
from ikarchive.slice_selectors import export_selectors, rule_token, verify_selectors  # noqa: E402
from tests.unit import test_delta_reader as delta_fixtures  # noqa: E402


GENERATION = "20261002T150000Z-abc012ef"
OTHER_GENERATION = "20261002T150001Z-def345ab"
MAX_SQLITE_INTEGER = 9_223_372_036_854_775_000


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _write_json(path: Path, value: dict) -> None:
    path.write_text(
        json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2) + "\n",
        encoding="utf-8",
    )


def _quote(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _same(left, right) -> bool:
    if type(left) is not type(right):
        return False
    if isinstance(left, float):
        return left.hex() == right.hex()
    return left == right


def _decode_cell(cell):
    sqlite_type = cell["sqlite_type"]
    encoding = cell["encoding"]
    value = cell["value"]
    if sqlite_type == "null":
        if encoding != "none" or value is not None:
            raise AssertionError(f"invalid NULL representation: {cell!r}")
        return None
    if sqlite_type == "integer":
        if encoding != "decimal" or not isinstance(value, str):
            raise AssertionError(f"invalid INTEGER representation: {cell!r}")
        return int(value, 10)
    if sqlite_type == "real":
        if encoding != "float.hex" or not isinstance(value, str):
            raise AssertionError(f"invalid REAL representation: {cell!r}")
        return float.fromhex(value)
    if sqlite_type == "text":
        if encoding != "unicode" or not isinstance(value, str):
            raise AssertionError(f"invalid TEXT representation: {cell!r}")
        return value
    if sqlite_type == "blob":
        if encoding != "hex" or not isinstance(value, str):
            raise AssertionError(f"invalid BLOB representation: {cell!r}")
        return bytes.fromhex(value)
    raise AssertionError(f"unknown SQLite type: {sqlite_type!r}")


def _assert_exact_rows(test, expected, found, label):
    test.assertEqual(len(expected), len(found), label)
    for expected_row, found_row in zip(expected, found):
        test.assertEqual(len(expected_row), len(found_row), label)
        test.assertTrue(all(_same(a, b) for a, b in zip(expected_row, found_row)), label)


def _typed_key(value):
    if value is None:
        return ("null", None)
    if type(value) is int:
        return ("integer", value)
    if type(value) is float:
        return ("real", value.hex())
    if type(value) is str:
        return ("text", value)
    if type(value) is bytes:
        return ("blob", value)
    raise AssertionError(f"unsupported source SQLite value type: {type(value)!r}")


def _typed_row_key(values):
    return tuple(_typed_key(value) for value in values)


class ShardCliTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._temporary = tempfile.TemporaryDirectory(prefix=".test-shard-cli-", dir=REPO)
        cls.root = Path(cls._temporary.name)
        cls.source_path = cls.root / "source.sqlite3"
        cls.package = cls.root / "shards"
        cls._create_source()
        source_sha = _sha256_file(cls.source_path)
        source = sqlite3.connect(cls.source_path.as_uri() + "?mode=ro&immutable=1", uri=True)
        try:
            manifest = export_sqlite_shards(source, cls.package, snapshot_id=GENERATION)
            manifest["source_sha256"] = source_sha
            export_selectors(source, cls.package, manifest, source_sha)
            _write_json(cls.package / "manifest.json", manifest)
            verification = verify_sqlite_shards(source, cls.package, manifest)
            verification["source_sha256"] = source_sha
            _write_json(cls.package / "verification.json", verification)
            selector_verification = verify_selectors(source, cls.package, manifest, source_sha)
            _write_json(cls.package / "selectors-verification.json", selector_verification)
        finally:
            source.close()
        cls.manifest = json.loads((cls.package / "manifest.json").read_text(encoding="utf-8"))
        cls.source_identity = {"bytes": cls.source_path.stat().st_size, "sha256": source_sha}
        cls.source = sqlite3.connect(cls.source_path.as_uri() + "?mode=ro&immutable=1", uri=True)
        cls.source.execute("PRAGMA query_only=ON")

    @classmethod
    def tearDownClass(cls):
        cls.source.close()
        cls._temporary.cleanup()

    @classmethod
    def _create_source(cls):
        connection = sqlite3.connect(cls.source_path)
        try:
            connection.execute("PRAGMA journal_mode=DELETE")
            connection.execute("PRAGMA foreign_keys=ON")
            connection.executescript(
                """
                CREATE TABLE bodies (
                    body_id TEXT PRIMARY KEY,
                    raw_body BLOB NOT NULL,
                    note TEXT
                );
                CREATE TABLE matches (
                    account TEXT NOT NULL,
                    kind TEXT NOT NULL,
                    match_key TEXT NOT NULL,
                    raw_text TEXT,
                    newly_added_value,
                    huge_integer INTEGER,
                    real_value,
                    maybe_null TEXT,
                    empty_text TEXT,
                    preview BLOB,
                    generated_value INTEGER GENERATED ALWAYS AS (huge_integer % 101) STORED,
                    PRIMARY KEY (account,kind,match_key)
                );
                CREATE TABLE match_classification (
                    account TEXT NOT NULL,
                    kind TEXT NOT NULL,
                    match_key TEXT NOT NULL,
                    analysis_set TEXT NOT NULL,
                    rule_raw TEXT,
                    unknown_classification BLOB,
                    PRIMARY KEY (account,kind,match_key),
                    FOREIGN KEY (account,kind,match_key)
                      REFERENCES matches(account,kind,match_key)
                );
                CREATE TABLE deliberately_empty (new_unknown_column);
                CREATE INDEX matches_by_kind ON matches(kind);
                CREATE VIEW match_key_catalog AS SELECT account,kind,match_key FROM matches;
                """
            )
            big_blob = bytes(range(256)) * 1200
            connection.execute("INSERT INTO bodies VALUES(?,?,?)", ("body-1", big_blob, "large BLOB"))
            connection.executemany(
                "INSERT INTO matches(account,kind,match_key,raw_text,newly_added_value,huge_integer,"
                "real_value,maybe_null,empty_text,preview) VALUES(?,?,?,?,?,?,?,?,?,?)",
                [
                    ("cli-account", "vs", "match-area", "NUL\x00CR\r\n𠀋", "new text", MAX_SQLITE_INTEGER, 1.25, None, "", b"\x00\xffarea"),
                    ("cli-account", "vs", "match-tower", "ordinary", 9007199254740993, MAX_SQLITE_INTEGER - 1, -0.0, "present", "", b"tower"),
                    ("cli-account", "coop", "match-xmatch", "astral 雪", b"dynamic bytes", 72, 3.5, None, "empty", b"xmatch"),
                    ("cli-account", "vs", "match-unclassified", "unclassified", None, 99, 4.25, None, "", b"unclassified"),
                ],
            )
            connection.executemany(
                "INSERT INTO match_classification VALUES(?,?,?,?,?,?)",
                [
                    ("cli-account", "vs", "match-area", "bankara_open", "AREA", b"classification\x00area"),
                    ("cli-account", "vs", "match-tower", "bankara_open", "TOWER", b"classification tower"),
                    ("cli-account", "coop", "match-xmatch", "xmatch", "AREA", b"classification xmatch"),
                ],
            )
            connection.commit()
            if connection.execute("PRAGMA foreign_key_check").fetchall():
                raise AssertionError("synthetic source has foreign-key violations")
            if connection.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
                raise AssertionError("synthetic source failed integrity_check")
        finally:
            connection.close()
        for suffix in ("-wal", "-journal", "-shm"):
            sidecar = Path(str(cls.source_path) + suffix)
            if sidecar.exists() and sidecar.stat().st_size:
                raise AssertionError(f"synthetic source has sidecar {sidecar.name}")

    def _run(self, *arguments):
        env = dict(os.environ)
        env["IKARING_ARCHIVE_DATA_DIR"] = str(self.root / "unused-data-root")
        command = [sys.executable, str(REPO / "archive.py"), "--db", str(self.root / "unused-unified.sqlite3"), *arguments]
        return subprocess.run(
            command,
            cwd=REPO,
            env=env,
            capture_output=True,
            text=True,
            check=False,
        )

    def _package_file_hashes(self, package: Path) -> dict[str, tuple[int, str]]:
        return {
            path.relative_to(package).as_posix(): (path.stat().st_size, _sha256_file(path))
            for path in package.rglob("*")
            if path.is_file()
        }

    def _read_jsonl(self, result):
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stderr, "")
        rows = [json.loads(line) for line in result.stdout.splitlines()]
        self.assertGreaterEqual(len(rows), 2)
        self.assertEqual(rows[0]["type"], "header")
        self.assertEqual(rows[-1]["type"], "footer")
        return rows[0], rows[1:-1], rows[-1]

    def test_shard_list_and_full_read_roundtrip_all_native_values_readonly(self):
        source_before = (self.source_path.stat().st_size, _sha256_file(self.source_path))
        package_before = self._package_file_hashes(self.package)
        unused_store = self.root / "unused-unified.sqlite3"
        listed = self._run("shard-list", "--root", str(self.package), "--generation", GENERATION)
        self.assertEqual(listed.returncode, 0, listed.stderr)
        self.assertEqual(listed.stderr, "")
        listing = json.loads(listed.stdout)
        self.assertEqual(listing["reader_role"], "baseline_only_lossless_shard_reader")
        self.assertTrue(listing["baseline_only"])
        self.assertEqual(listing["generation"], GENERATION)
        self.assertEqual(listing["source_sha256"], self.source_identity["sha256"])
        self.assertEqual(listing["schema_objects"], self.manifest["schema_objects"])
        self.assertEqual(
            {table["name"] for table in listing["tables"]},
            {table["name"] for table in self.manifest["tables"]},
        )
        listed_matches = next(table for table in listing["tables"] if table["name"] == "matches")
        source_matches = next(table for table in self.manifest["tables"] if table["name"] == "matches")
        self.assertEqual(listed_matches["row_count"], source_matches["row_count"])
        self.assertIn("generated_value", listed_matches["columns"])
        self.assertTrue(listed_matches["foreign_keys"] == [])

        result = self._run("shard-read", "--root", str(self.package), "--table", "matches")
        header, row_objects, footer = self._read_jsonl(result)
        self.assertEqual(header["generation"], GENERATION)
        self.assertEqual(header["source_sha256"], self.source_identity["sha256"])
        self.assertEqual(header["table_full_row_count"], 4)
        self.assertEqual(header["scope"], {"kind": "full_table", "table": "matches"})
        self.assertEqual(header["column_xinfo"], source_matches["column_schema"])
        self.assertEqual(footer, {"type": "footer", "returned_rows": 4, "truncated": False, "limit": None})
        expected = [tuple(row) for row in self.source.execute("SELECT * FROM matches ORDER BY rowid")]
        self.assertEqual([row["ordinal"] for row in row_objects], list(range(4)))
        decoded = []
        for row in row_objects:
            self.assertEqual(set(row), {"ordinal", "source_rowid", "values"})
            self.assertEqual(_decode_cell(row['source_rowid']), row['ordinal'] + 1)
            self.assertEqual(len(row["values"]), len(header["columns"]))
            self.assertTrue(all(set(cell) == {"sqlite_type", "encoding", "value"} for cell in row["values"]))
            decoded.append(tuple(_decode_cell(cell) for cell in row["values"]))
        _assert_exact_rows(self, expected, decoded, "matches typed rows")
        self.assertIn("NUL\x00CR\r\n𠀋", decoded[0])
        self.assertIn(MAX_SQLITE_INTEGER, decoded[0])
        self.assertIn(b"\x00\xffarea", decoded[0])
        self.assertIsNone(decoded[0][7])
        self.assertEqual(decoded[0][8], "")
        self.assertFalse(unused_store.exists(), "shard CLI must not open or create the unified Store")

        body_result = self._run("shard-read", "--root", str(self.package), "--table", "bodies")
        body_header, body_rows, body_footer = self._read_jsonl(body_result)
        self.assertEqual(body_header["table_full_row_count"], 1)
        body_expected = [tuple(row) for row in self.source.execute("SELECT * FROM bodies ORDER BY rowid")]
        body_decoded = [tuple(_decode_cell(cell) for cell in row["values"]) for row in body_rows]
        _assert_exact_rows(self, body_expected, body_decoded, "external BLOB typed rows")
        self.assertEqual(body_footer["truncated"], False)
        self.assertEqual(source_before, (self.source_path.stat().st_size, _sha256_file(self.source_path)))
        self.assertEqual(package_before, self._package_file_hashes(self.package))

    def test_mode_rule_and_limit_preserve_full_scope_in_header_and_footer(self):
        token = rule_token("AREA")
        full = self._run(
            "shard-read", "--root", str(self.package), "--table", "matches",
            "--mode", "bankara_open",
        )
        header, rows, footer = self._read_jsonl(full)
        expected = [tuple(row) for row in self.source.execute(
            "SELECT m.* FROM matches m WHERE EXISTS (SELECT 1 FROM match_classification c "
            "WHERE c.account=m.account AND c.kind=m.kind AND c.match_key=m.match_key "
            "AND c.analysis_set=?) ORDER BY m.account,m.kind,m.match_key",
            ("bankara_open",),
        )]
        decoded = [tuple(_decode_cell(cell) for cell in row["values"]) for row in rows]
        _assert_exact_rows(self, expected, decoded, "mode selector rows")
        self.assertEqual(header["table_full_row_count"], 4)
        self.assertEqual(header["scope"]["kind"], "mode_selector")
        self.assertEqual(header["scope"]["selected_match_count"], len(expected))
        self.assertIn("all source tables and values", header["scope"]["information_scope"])
        self.assertEqual(footer, {"type": "footer", "returned_rows": len(expected), "truncated": False, "limit": None})

        limited = self._run(
            "shard-read", "--root", str(self.package), "--table", "matches",
            "--mode", "bankara_open", "--limit", "1",
        )
        limited_header, limited_rows, limited_footer = self._read_jsonl(limited)
        self.assertEqual(limited_header["table_full_row_count"], 4)
        self.assertEqual(limited_header["scope"]["selected_match_count"], 2)
        self.assertEqual(len(limited_rows), 1)
        self.assertEqual(limited_footer, {"type": "footer", "returned_rows": 1, "truncated": True, "limit": 1})

        filtered = self._run(
            "shard-read", "--root", str(self.package), "--table", "matches",
            "--mode", "bankara_open", "--rule", token,
        )
        filtered_header, filtered_rows, filtered_footer = self._read_jsonl(filtered)
        expected_rule = [tuple(row) for row in self.source.execute(
            "SELECT m.* FROM matches m WHERE EXISTS (SELECT 1 FROM match_classification c "
            "WHERE c.account=m.account AND c.kind=m.kind AND c.match_key=m.match_key "
            "AND c.analysis_set=? AND c.rule_raw IS ?) ORDER BY m.account,m.kind,m.match_key",
            ("bankara_open", "AREA"),
        )]
        filtered_decoded = [tuple(_decode_cell(cell) for cell in row["values"]) for row in filtered_rows]
        _assert_exact_rows(self, expected_rule, filtered_decoded, "mode/rule selector rows")
        self.assertEqual(filtered_header["scope"]["kind"], "mode_rule_selector")
        self.assertEqual(filtered_header["scope"]["rule_token"], token)
        self.assertEqual(filtered_footer["truncated"], False)

    def test_invalid_role_generation_proofs_and_rule_arguments_fail_by_category(self):
        bad_rule = self._run(
            "shard-read", "--root", str(self.package), "--table", "matches", "--rule", "AREA"
        )
        self.assertNotEqual(bad_rule.returncode, 0)
        self.assertEqual(bad_rule.stdout, "")
        self.assertEqual(json.loads(bad_rule.stderr), {"error": "SHARD_RULE_REQUIRES_MODE"})

        bad_limit = self._run(
            "shard-read", "--root", str(self.package), "--table", "matches", "--limit", "0"
        )
        self.assertNotEqual(bad_limit.returncode, 0)
        self.assertEqual(json.loads(bad_limit.stderr), {"error": "SHARD_LIMIT_MUST_BE_POSITIVE"})

        missing_table = self._run(
            "shard-read", "--root", str(self.package), "--table", "not_a_source_table"
        )
        self.assertNotEqual(missing_table.returncode, 0)
        self.assertEqual(missing_table.stdout, "")
        self.assertEqual(json.loads(missing_table.stderr), {"error": "SHARD_TABLE_NOT_FOUND"})

        selector_on_non_matches = self._run(
            "shard-read", "--root", str(self.package), "--table", "bodies", "--mode", "bankara_open"
        )
        self.assertNotEqual(selector_on_non_matches.returncode, 0)
        self.assertEqual(selector_on_non_matches.stdout, "")
        self.assertEqual(
            json.loads(selector_on_non_matches.stderr),
            {"error": "SHARD_SELECTOR_REQUIRES_MATCHES"},
        )

        missing_selector = self._run(
            "shard-read", "--root", str(self.package), "--table", "matches", "--mode", "not_a_mode"
        )
        self.assertNotEqual(missing_selector.returncode, 0)
        self.assertEqual(missing_selector.stdout, "")
        self.assertEqual(json.loads(missing_selector.stderr), {"error": "SHARD_SELECTOR_NOT_FOUND"})

        bad_generation = self._run(
            "shard-list", "--root", str(self.package), "--generation", OTHER_GENERATION
        )
        self.assertNotEqual(bad_generation.returncode, 0)
        self.assertEqual(bad_generation.stdout, "")
        self.assertEqual(json.loads(bad_generation.stderr), {"error": "SHARD_PACKAGE_INVALID"})

        old_role = self.root / "old-role"
        shutil.copytree(self.package, old_role)
        manifest = json.loads((old_role / "manifest.json").read_text(encoding="utf-8"))
        manifest["role"] = "analysis_slice"
        _write_json(old_role / "manifest.json", manifest)
        old_result = self._run("shard-list", "--root", str(old_role))
        self.assertNotEqual(old_result.returncode, 0)
        self.assertEqual(old_result.stdout, "")
        self.assertEqual(json.loads(old_result.stderr), {"error": "SHARD_PACKAGE_INVALID"})

        unverified = self.root / "unverified"
        shutil.copytree(self.package, unverified)
        proof_path = unverified / "verification.json"
        proof = json.loads(proof_path.read_text(encoding="utf-8"))
        proof["coverage"]["all_values"] = False
        _write_json(proof_path, proof)
        proof_result = self._run("shard-list", "--root", str(unverified))
        self.assertNotEqual(proof_result.returncode, 0)
        self.assertEqual(proof_result.stdout, "")
        self.assertEqual(json.loads(proof_result.stderr), {"error": "SHARD_PACKAGE_INVALID"})

        mixed = self.root / "mixed-generation"
        shutil.copytree(self.package, mixed)
        manifest = json.loads((mixed / "manifest.json").read_text(encoding="utf-8"))
        selector = next(item for item in manifest["by_mode"] if item["analysis_set"] == "bankara_open")
        selector_path = mixed / selector["file"]
        connection = sqlite3.connect(selector_path)
        try:
            connection.execute(
                "UPDATE slice_meta SET value=? WHERE key='snapshot_identifier'",
                (OTHER_GENERATION,),
            )
            connection.commit()
        finally:
            connection.close()
        selector["bytes"] = selector_path.stat().st_size
        selector["sha256"] = _sha256_file(selector_path)
        _write_json(mixed / "manifest.json", manifest)
        mixed_result = self._run(
            "shard-read", "--root", str(mixed), "--table", "matches", "--mode", "bankara_open"
        )
        self.assertNotEqual(mixed_result.returncode, 0)
        self.assertEqual(mixed_result.stdout, "", "selector must be validated before header emission")
        self.assertEqual(json.loads(mixed_result.stderr), {"error": "SHARD_PACKAGE_INVALID"})

    def test_delta_dirs_read_current_rows_selectors_and_schema_reset_readonly(self):
        fixture = delta_fixtures.DeltaChainReaderTests(
            methodName="test_baseline_reconciliation_preserves_rowids_types_and_blob_values"
        )
        fixture.setUp()
        try:
            writer = fixture.writer
            writer.execute(
                "UPDATE records SET payload=?,maybe_null=?,blob_value=?,large_integer=?,ratio=? WHERE id=5",
                ("updated\x00after-reconciliation", None, b"\x00delta\xff", MAX_SQLITE_INTEGER, -0.0),
            )
            writer.execute("DELETE FROM records WHERE id=-42")
            writer.execute(
                "INSERT INTO records VALUES(?,?,?,?,?,?)",
                (-101, b"negative\x00rowid", "inserted", b"", MAX_SQLITE_INTEGER - 1, 1.5),
            )
            writer.execute(
                "UPDATE match_classification SET analysis_set='bankara_open',rule_raw='AREA' "
                "WHERE account=? AND kind=? AND match_key=?",
                ("account-a", "vs", "match-a"),
            )
            writer.execute("DELETE FROM key_only WHERE kind=? AND key_value=?", ("event", b"\xffnew-key"))
            writer.execute(
                "INSERT INTO key_only VALUES(?,?,?)",
                (b"empty-blob", "delta", b"\x00new-key"),
            )
            writer.commit()
            normal_root = fixture.root / "cli-normal-delta"
            normal_plan = delta_fixtures._make_generation(
                fixture.current_db,
                fixture.baseline_db,
                normal_root,
                delta_fixtures.DELTA_A_ID,
                delta_fixtures.BASELINE_ID,
                fixture.baseline_identity["sha256"],
                kind="change_feed",
                previous=fixture.reset_plan,
            )
            first_chain = [fixture.reset_root, normal_root]

            def run_delta(command, directories, *arguments):
                options = [
                    command,
                    "--root", str(fixture.baseline_package),
                    "--generation", delta_fixtures.BASELINE_ID,
                ]
                for directory in directories:
                    options.extend(("--delta-dir", str(directory)))
                options.extend(arguments)
                return self._run(*options)

            current_listing_result = run_delta("shard-list", first_chain)
            self.assertEqual(current_listing_result.returncode, 0, current_listing_result.stderr)
            self.assertEqual(current_listing_result.stderr, "")
            current_listing = json.loads(current_listing_result.stdout)
            self.assertFalse(current_listing["baseline_only"])
            self.assertEqual(current_listing["generation"], delta_fixtures.BASELINE_ID)
            self.assertEqual(current_listing["baseline_generation"], delta_fixtures.BASELINE_ID)
            self.assertEqual(current_listing["latest_generation"], delta_fixtures.DELTA_A_ID)
            self.assertEqual(current_listing["delta_generations"], [fixture.reset_plan["generation_id"], delta_fixtures.DELTA_A_ID])
            self.assertEqual(current_listing["baseline_source_sha256"], fixture.baseline_identity["sha256"])

            normal_records_result = run_delta(
                "shard-read", first_chain, "--table", "records"
            )
            normal_header, normal_rows, normal_footer = self._read_jsonl(normal_records_result)
            self.assertEqual(normal_header["source_sha256"], fixture.baseline_identity["sha256"])
            normal_actual = Counter(
                (_typed_key(_decode_cell(row["source_rowid"])),
                 _typed_row_key(tuple(_decode_cell(cell) for cell in row["values"])))
                for row in normal_rows
            )
            normal_expected = Counter(
                (_typed_key(row[0]), _typed_row_key(row[1:]))
                for row in writer.execute("SELECT _rowid_,* FROM records")
            )
            self.assertEqual(normal_actual, normal_expected)
            self.assertIn(-101, [_decode_cell(row["source_rowid"]) for row in normal_rows])
            self.assertTrue(any(
                _decode_cell(row["values"][3]) == b"" for row in normal_rows
            ))
            self.assertEqual(normal_footer["returned_rows"], writer.execute("SELECT COUNT(*) FROM records").fetchone()[0])

            mode_result = run_delta(
                "shard-read", first_chain,
                "--table", "matches", "--mode", "bankara_open", "--rule", rule_token("AREA"),
            )
            mode_header, mode_rows, mode_footer = self._read_jsonl(mode_result)
            self.assertEqual(mode_header["latest_generation"], delta_fixtures.DELTA_A_ID)
            self.assertEqual(mode_header["scope"]["selected_match_count"], 1)
            self.assertEqual(
                mode_header["scope"]["source_value_closure"],
                "full current source remains available through unfiltered table reads",
            )
            self.assertIn("current full match_classification", mode_header["scope"]["information_scope"])
            self.assertEqual(mode_footer["returned_rows"], 1)
            self.assertEqual(mode_footer["truncated"], False)
            mode_values = [tuple(_decode_cell(cell) for cell in row["values"]) for row in mode_rows]
            self.assertEqual([row[2] for row in mode_values], ["match-a"])

            bad_chain = run_delta("shard-list", [normal_root])
            self.assertNotEqual(bad_chain.returncode, 0)
            self.assertEqual(bad_chain.stdout, "")
            self.assertEqual(json.loads(bad_chain.stderr), {"error": "SHARD_DELTA_INVALID"})

            writer.execute("ALTER TABLE records ADD COLUMN after_reset TEXT")
            writer.execute("UPDATE records SET after_reset='after-ddl' WHERE id=-101")
            writer.execute("DROP TABLE to_drop")
            writer.execute("CREATE TABLE new_empty_after_reset (new_value BLOB)")
            writer.commit()
            reset_root = fixture.root / "cli-schema-reset"
            reset_plan = delta_fixtures._make_generation(
                fixture.current_db,
                fixture.baseline_db,
                reset_root,
                delta_fixtures.RESET_B_ID,
                delta_fixtures.BASELINE_ID,
                fixture.baseline_identity["sha256"],
                kind="baseline_reconciliation",
                previous=normal_plan,
            )
            final_chain = [fixture.reset_root, normal_root, reset_root]

            source_hash_before = _sha256_file(fixture.current_db)
            baseline_hash_before = _sha256_file(fixture.baseline_db)
            package_dirs = [fixture.baseline_package, fixture.reset_root, normal_root, reset_root]
            package_hashes_before = {
                path: self._package_file_hashes(path)
                for path in package_dirs
            }

            listing_result = run_delta("shard-list", final_chain)
            self.assertEqual(listing_result.returncode, 0, listing_result.stderr)
            listing = json.loads(listing_result.stdout)
            self.assertEqual(listing["latest_generation"], delta_fixtures.RESET_B_ID)
            self.assertEqual(listing["delta_generations"], [delta_fixtures.RESET_B_ID])
            self.assertEqual(listing["source_schema_sha256"], reset_plan["source_schema_sha256"])
            self.assertNotIn("to_drop", {table["name"] for table in listing["tables"]})
            self.assertEqual(
                next(table for table in listing["tables"] if table["name"] == "new_empty_after_reset")["row_count"],
                0,
            )

            schema_rows = [
                {"type": row[0], "name": row[1], "tbl_name": row[2], "sql": row[3]}
                for row in writer.execute("SELECT type,name,tbl_name,sql FROM sqlite_master ORDER BY type,name")
            ]
            self.assertEqual(listing["schema_objects"], schema_rows)
            self.assertEqual(
                listing["source_schema_sha256"],
                hashlib.sha256(json.dumps(
                    schema_rows, ensure_ascii=False, sort_keys=True, separators=(",", ":")
                ).encode("utf-8")).hexdigest(),
            )
            table_rows = {table["name"]: table for table in listing["tables"]}
            source_table_names = {row[0] for row in writer.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )}
            self.assertEqual(set(table_rows), source_table_names)
            self.assertEqual(listing["table_count"], len(source_table_names))

            for table_name, table in table_rows.items():
                quoted = _quote(table_name)
                xinfo_rows = [dict(zip(
                    ("cid", "name", "type", "notnull", "dflt_value", "pk", "hidden"), row
                )) for row in writer.execute(f"PRAGMA table_xinfo({quoted})")]
                self.assertEqual(table["column_schema"], xinfo_rows, table_name)
                columns = [row["name"] for row in xinfo_rows if row["hidden"] != 1]
                self.assertEqual(table["columns"], columns, table_name)
                fk_rows = [dict(zip(
                    ("id", "seq", "table", "from", "to", "on_update", "on_delete", "match"), row
                )) for row in writer.execute(f"PRAGMA foreign_key_list({quoted})")]
                self.assertEqual(table["foreign_keys"], fk_rows, table_name)
                source_count = writer.execute(f"SELECT COUNT(*) FROM {quoted}").fetchone()[0]
                self.assertEqual(table["row_count"], source_count, table_name)

                read_result = run_delta("shard-read", final_chain, "--table", table_name)
                header, encoded_rows, footer = self._read_jsonl(read_result)
                self.assertEqual(header["latest_generation"], delta_fixtures.RESET_B_ID, table_name)
                self.assertEqual(header["table_full_row_count"], source_count, table_name)
                self.assertEqual(header["column_xinfo"], xinfo_rows, table_name)
                self.assertEqual(header["foreign_keys"], fk_rows, table_name)
                self.assertEqual(footer["returned_rows"], source_count, table_name)
                self.assertFalse(footer["truncated"], table_name)
                self.assertEqual([row["ordinal"] for row in encoded_rows], list(range(source_count)), table_name)

                pragma_table = next(row for row in writer.execute("PRAGMA table_list") if row[1] == table_name)
                rowid_alias = next((name for name in ("rowid", "_rowid_", "oid") if name not in columns), None)
                if pragma_table[4] == 1 or rowid_alias is None:
                    expected = Counter(
                        (_typed_key(None), _typed_row_key(row))
                        for row in writer.execute(f"SELECT * FROM {quoted}")
                    )
                else:
                    expected = Counter(
                        (_typed_key(row[0]), _typed_row_key(row[1:]))
                        for row in writer.execute(f"SELECT {rowid_alias},* FROM {quoted}")
                    )
                actual = Counter(
                    (_typed_key(_decode_cell(row["source_rowid"])),
                     _typed_row_key(tuple(_decode_cell(cell) for cell in row["values"])))
                    for row in encoded_rows
                )
                self.assertEqual(actual, expected, table_name)

            self.assertEqual(_sha256_file(fixture.current_db), source_hash_before)
            self.assertEqual(_sha256_file(fixture.baseline_db), baseline_hash_before)
            self.assertEqual(
                package_hashes_before,
                {path: self._package_file_hashes(path) for path in package_dirs},
            )
        finally:
            fixture.tearDown()


if __name__ == "__main__":
    unittest.main()
