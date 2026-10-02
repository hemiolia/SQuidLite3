from __future__ import annotations

import copy
import hashlib
import json
import os
from pathlib import Path
import shutil
import sqlite3
import sys
import tempfile
import unittest
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src/python"))

from ikarchive import lossless_sqlite as shards  # noqa: E402
from ikarchive import shard_reader as shard_reader_module  # noqa: E402
from ikarchive import verified_files  # noqa: E402
from ikarchive.lossless_sqlite import export_sqlite_shards, verify_sqlite_shards  # noqa: E402
from ikarchive.shard_reader import LosslessShardReader  # noqa: E402
from ikarchive.slice_selectors import export_selectors, rule_token, verify_selectors  # noqa: E402


GENERATION = "20261002T140000Z-abc012ef"
OTHER_GENERATION = "20261002T140001Z-def345ab"
MAX_SQLITE_INTEGER = 9_223_372_036_854_775_000


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _quote(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _write_json(path: Path, value: dict) -> None:
    path.write_text(
        json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2) + "\n",
        encoding="utf-8",
    )


def _same_value(left, right) -> bool:
    if type(left) is not type(right):
        return False
    if isinstance(left, float):
        return left.hex() == right.hex()
    return left == right


def _assert_rows_equal(test: unittest.TestCase, expected, found, context: str) -> None:
    test.assertEqual(len(expected), len(found), context)
    for expected_row, found_row in zip(expected, found):
        test.assertEqual(len(expected_row), len(found_row), context)
        test.assertTrue(
            all(_same_value(left, right) for left, right in zip(expected_row, found_row)),
            context,
        )


class LosslessShardReaderTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._temporary = tempfile.TemporaryDirectory(prefix=".test-shard-reader-", dir=ROOT)
        cls.root = Path(cls._temporary.name)
        cls.source_path = cls.root / "source.sqlite3"
        cls.package = cls.root / "package"
        cls._create_source()
        cls.source_identity = {
            "bytes": cls.source_path.stat().st_size,
            "sha256": _sha256_file(cls.source_path),
        }
        cls.source = sqlite3.connect(
            cls.source_path.as_uri() + "?mode=ro&immutable=1", uri=True
        )
        cls.source.execute("PRAGMA query_only=ON")
        cls.manifest = export_sqlite_shards(cls.source, cls.package, snapshot_id=GENERATION)
        cls.manifest["source_sha256"] = cls.source_identity["sha256"]
        export_selectors(cls.source, cls.package, cls.manifest, cls.source_identity["sha256"])
        _write_json(cls.package / "manifest.json", cls.manifest)
        verification = verify_sqlite_shards(cls.source, cls.package, cls.manifest)
        verification["source_sha256"] = cls.source_identity["sha256"]
        _write_json(cls.package / "verification.json", verification)
        selector_verification = verify_selectors(
            cls.source, cls.package, cls.manifest, cls.source_identity["sha256"]
        )
        _write_json(cls.package / "selectors-verification.json", selector_verification)

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
                    body_sha256 TEXT PRIMARY KEY,
                    raw_body BLOB NOT NULL,
                    description TEXT
                );
                CREATE TABLE responses (
                    response_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    body_sha256 TEXT REFERENCES bodies(body_sha256)
                );
                CREATE TABLE matches (
                    account TEXT NOT NULL,
                    kind TEXT NOT NULL,
                    match_key TEXT NOT NULL,
                    detail_response_id INTEGER REFERENCES responses(response_id),
                    raw_text TEXT,
                    newly_added_value,
                    huge_integer INTEGER,
                    ratio,
                    maybe_null TEXT,
                    empty_text TEXT,
                    preview BLOB,
                    generated_value INTEGER GENERATED ALWAYS AS (huge_integer % 97) STORED,
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
                CREATE TABLE deliberately_empty (unknown_column);
                CREATE TABLE identity_rows (value TEXT);
                CREATE TABLE shadowed_identity (rowid TEXT, _rowid_ TEXT, oid TEXT, value TEXT);
                CREATE TABLE without_rowid_identity (
                    first_key TEXT, second_key INTEGER, payload BLOB,
                    PRIMARY KEY(first_key,second_key)
                ) WITHOUT ROWID;
                CREATE TABLE large_lookup (
                    lookup_key TEXT PRIMARY KEY, large_blob BLOB, large_text TEXT
                );
                CREATE TABLE inline_probe (
                    probe_key TEXT PRIMARY KEY, criterion TEXT, inline_blob BLOB, maybe_null TEXT
                );
                CREATE INDEX matches_by_kind ON matches(kind);
                CREATE VIEW match_keys AS SELECT account,kind,match_key FROM matches;
                CREATE TRIGGER matches_no_delete BEFORE DELETE ON matches
                BEGIN SELECT RAISE(ABORT, 'deletion disabled'); END;
                """
            )
            blob = bytes(range(256)) * 1300
            connection.execute(
                "INSERT INTO bodies VALUES(?,?,?)",
                ("body-hash-1", blob, "large retained body"),
            )
            connection.execute("INSERT INTO responses(body_sha256) VALUES(?)", ("body-hash-1",))
            rows = [
                (
                    "synthetic-account", "vs", "match-area", 1,
                    "nul\x00line\r\n雪と𠀋", "new text field", MAX_SQLITE_INTEGER,
                    1.25, None, "", b"preview\x00area",
                ),
                (
                    "synthetic-account", "vs", "match-tower", 1,
                    "ordinary", 9007199254740993, MAX_SQLITE_INTEGER - 1,
                    -0.0, "present", "", b"preview tower",
                ),
                (
                    "synthetic-account", "vs", "match-xmatch", 1,
                    "another row", b"dynamic bytes", 77,
                    0.0, None, "empty", b"\x00\xff",
                ),
                (
                    "synthetic-account", "coop", "match-null-rule", None,
                    "rule with null", None, 88,
                    3.75, "value", "", b"null-rule",
                ),
                (
                    "synthetic-account", "vs", "match-unclassified", None,
                    "unclassified", "unknown type retained", 99,
                    4.0, None, "", b"unclassified",
                ),
            ]
            connection.executemany(
                "INSERT INTO matches(account,kind,match_key,detail_response_id,raw_text,"
                "newly_added_value,huge_integer,ratio,maybe_null,empty_text,preview) "
                "VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                rows,
            )
            connection.executemany(
                "INSERT INTO match_classification VALUES(?,?,?,?,?,?)",
                [
                    ("synthetic-account", "vs", "match-area", "bankara_open", "AREA", b"class\x00a"),
                    ("synthetic-account", "vs", "match-tower", "bankara_open", "TOWER", b"class tower"),
                    ("synthetic-account", "vs", "match-xmatch", "xmatch", "AREA", b"class xmatch"),
                    ("synthetic-account", "coop", "match-null-rule", "bankara_open", None, b"class null rule"),
                ],
            )
            connection.executemany(
                "INSERT INTO identity_rows(rowid,value) VALUES(?,?)",
                [(-19, "negative"), (9_007_199_254_740_993, "large gap"), (77, "last")],
            )
            connection.executemany(
                "INSERT INTO shadowed_identity VALUES(?,?,?,?)",
                [("visible rowid", "visible _rowid_", "visible oid", "shadowed")],
            )
            connection.executemany(
                "INSERT INTO without_rowid_identity VALUES(?,?,?)",
                [("z", 2, b"second"), ("a", 1, b"first")],
            )
            large_blob = bytes(range(256)) * 1300
            large_text = "雪" * 110_000
            connection.executemany(
                "INSERT INTO large_lookup VALUES(?,?,?)",
                [("lookup-hit", large_blob, large_text), ("other", large_blob[::-1], large_text[::-1])],
            )
            inline_blob_miss = bytes(range(256)) * 700
            inline_blob_hit = bytes(reversed(range(256))) * 700
            connection.executemany(
                "INSERT INTO inline_probe VALUES(?,?,?,?)",
                [
                    ("nonmatch-large-blob", "miss", inline_blob_miss, None),
                    ("matched-large-blob", "hit", inline_blob_hit, "present"),
                ],
            )
            connection.commit()
            if connection.execute("PRAGMA foreign_key_check").fetchall():
                raise AssertionError("synthetic fixture has foreign key violations")
            if connection.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
                raise AssertionError("synthetic fixture failed integrity_check")
        finally:
            connection.close()
        for suffix in ("-wal", "-journal", "-shm"):
            sidecar = Path(str(cls.source_path) + suffix)
            if sidecar.exists() and sidecar.stat().st_size:
                raise AssertionError(f"synthetic source left a sidecar: {sidecar.name}")

    def _copy_package(self, name: str) -> Path:
        destination = self.root / name
        if destination.exists():
            shutil.rmtree(destination)
        shutil.copytree(self.package, destination)
        return destination

    def test_external_cell_spanning_value_files_is_counted_once(self):
        package = self.root / "multi-file-value-package"
        manifest = export_sqlite_shards(
            self.source, package, max_bytes=262144, snapshot_id=GENERATION
        )
        manifest["source_sha256"] = self.source_identity["sha256"]
        export_selectors(self.source, package, manifest, self.source_identity["sha256"])
        _write_json(package / "manifest.json", manifest)
        proof = verify_sqlite_shards(self.source, package, manifest)
        proof["source_sha256"] = self.source_identity["sha256"]
        _write_json(package / "verification.json", proof)
        _write_json(package / "selectors-verification.json", verify_selectors(
            self.source, package, manifest, self.source_identity["sha256"]
        ))
        incidences = sum(row["cell_count"] for row in manifest["external_values"])
        self.assertGreater(incidences, proof["external_cell_count"])
        with LosslessShardReader(package, expected_generation=GENERATION) as reader:
            actual = list(reader.iter_rows("bodies"))
            self.assertEqual(actual[0][1][1], bytes(range(256)) * 1300)

            table = next(item for item in manifest["tables"] if item["name"] == "large_lookup")
            referenced_files = []
            for part in table["parts"]:
                connection = sqlite3.connect(package / part["file"])
                try:
                    ext_table = table["archive_metadata_tables"]["external_cells"]
                    referenced_files.extend(
                        json.loads(row[0])
                        for row in connection.execute(
                            f"SELECT value_files_json FROM {_quote(ext_table)} ORDER BY row_ordinal,column_ordinal"
                        )
                    )
                finally:
                    connection.close()
            self.assertTrue(any(len(refs) > 1 for refs in referenced_files))

            with patch.object(
                shards, "_read_external_cell", wraps=shards._read_external_cell
            ) as external_reader:
                self.assertEqual(
                    list(reader.lookup_rows_with_identity("large_lookup", {"lookup_key": "no-such-row"})),
                    [],
                )
                self.assertEqual(external_reader.call_count, 0)
                found = list(reader.lookup_rows_with_identity("large_lookup", {"lookup_key": "lookup-hit"}))
                self.assertEqual(external_reader.call_count, 2)
            expected = tuple(self.source.execute(
                "SELECT * FROM large_lookup WHERE lookup_key=?", ("lookup-hit",)
            ).fetchone())
            expected_rowid = self.source.execute(
                f"SELECT {_quote(table['source_rowid_column'])} FROM large_lookup WHERE lookup_key=?",
                ("lookup-hit",),
            ).fetchone()[0]
            self.assertEqual(len(found), 1)
            self.assertEqual(found[0][1], expected_rowid)
            _assert_rows_equal(self, [expected], [found[0][2]], "multi-file external lookup")

    def test_lookup_uses_native_exact_criteria_and_distinguishes_external_placeholder(self):
        source_before = (self.source_path.stat().st_size, _sha256_file(self.source_path))
        package_before = {
            path.relative_to(self.package).as_posix(): (path.stat().st_size, _sha256_file(path))
            for path in self.package.rglob("*") if path.is_file()
        }
        with LosslessShardReader(self.package, expected_generation=GENERATION) as reader:
            self.assertEqual(len(list(reader.lookup_rows_with_identity(
                "matches", {"huge_integer": MAX_SQLITE_INTEGER}
            ))), 1)
            self.assertEqual(list(reader.lookup_rows_with_identity("matches", {"huge_integer": True})), [])
            self.assertEqual(list(reader.lookup_rows_with_identity("matches", {"ratio": 4})), [])
            self.assertEqual(len(list(reader.lookup_rows_with_identity("matches", {"ratio": 4.0}))), 1)
            negative_zero = list(reader.lookup_rows_with_identity(
                "matches", {"kind": "vs", "ratio": -0.0}
            ))
            positive_zero = list(reader.lookup_rows_with_identity(
                "matches", {"kind": "vs", "ratio": 0.0}
            ))
            self.assertEqual([row[2][2] for row in negative_zero], ["match-tower"])
            self.assertEqual([row[2][2] for row in positive_zero], ["match-xmatch"])
            self.assertEqual(len(list(reader.lookup_rows_with_identity("matches", {"maybe_null": None}))), 3)
            self.assertEqual(list(reader.lookup_rows_with_identity(
                "matches", {"raw_text": b"ordinary"}
            )), [])
            self.assertEqual(len(list(reader.lookup_rows_with_identity(
                "matches", {"preview": b"preview\x00area"}
            ))), 1)

            with patch.object(
                shards, "_read_external_cell", wraps=shards._read_external_cell
            ) as external_reader:
                self.assertEqual(list(reader.lookup_rows_with_identity(
                    "bodies",
                    {"body_sha256": "not-the-row", "raw_body": bytes(range(256)) * 1300},
                )), [])
                self.assertEqual(external_reader.call_count, 0)

            with patch.object(
                shards, "_read_external_cell", wraps=shards._read_external_cell
            ) as external_reader:
                found = list(reader.lookup_rows_with_identity(
                    "bodies",
                    {"body_sha256": "body-hash-1", "raw_body": bytes(range(256)) * 1300},
                ))
                self.assertEqual(len(found), 1)
                self.assertEqual(external_reader.call_count, 1)

            # The placeholder in a part shard is NULL, but ext metadata proves
            # the source cell is a BLOB. It must not match source NULL.
            with patch.object(
                shards, "_read_external_cell", wraps=shards._read_external_cell
            ) as external_reader:
                self.assertEqual(list(reader.lookup_rows_with_identity("bodies", {"raw_body": None})), [])
                self.assertEqual(external_reader.call_count, 1)
        self.assertEqual(
            source_before, (self.source_path.stat().st_size, _sha256_file(self.source_path))
        )
        package_after = {
            path.relative_to(self.package).as_posix(): (path.stat().st_size, _sha256_file(path))
            for path in self.package.rglob("*") if path.is_file()
        }
        self.assertEqual(package_before, package_after)

    def test_candidate_lookup_does_not_fetch_unrelated_inline_blob_for_nonmatches(self):
        source_rows = {
            row[0]: tuple(row)
            for row in self.source.execute("SELECT * FROM inline_probe ORDER BY rowid")
        }
        miss_blob = source_rows["nonmatch-large-blob"][2]
        hit_blob = source_rows["matched-large-blob"][2]
        self.assertLess(len(miss_blob), shards._DEFAULT_EXTERNAL_THRESHOLD)
        self.assertLess(len(hit_blob), shards._DEFAULT_EXTERNAL_THRESHOLD)
        source_before = (self.source_path.stat().st_size, _sha256_file(self.source_path))
        package_before = {
            path.relative_to(self.package).as_posix(): (path.stat().st_size, _sha256_file(path))
            for path in self.package.rglob("*") if path.is_file()
        }
        selected_rows = []
        real_open = shards._open_readonly

        class TrackedCursor:
            def __init__(self, cursor, sql):
                self.cursor = cursor
                self.sql = sql

            def _record(self, row):
                if row is not None and 'from "inline_probe"' in self.sql.lower():
                    selected_rows.append((self.sql, tuple(row)))
                return row

            def fetchone(self):
                return self._record(self.cursor.fetchone())

            def fetchall(self):
                return [self._record(row) for row in self.cursor.fetchall()]

            def __iter__(self):
                for row in self.cursor:
                    yield self._record(row)

            def __getattr__(self, name):
                return getattr(self.cursor, name)

        class TrackedConnection:
            def __init__(self, connection):
                self.connection = connection

            def execute(self, sql, parameters=()):
                return TrackedCursor(self.connection.execute(sql, parameters), sql)

            def __getattr__(self, name):
                return getattr(self.connection, name)

        def tracked_open(path):
            return TrackedConnection(real_open(path))

        with LosslessShardReader(self.package, expected_generation=GENERATION) as reader:
            with patch.object(shards, "_open_readonly", side_effect=tracked_open):
                self.assertEqual(
                    list(reader.lookup_rows_with_identity("inline_probe", {"criterion": "absent"})),
                    [],
                )
                miss_reads = list(selected_rows)
                self.assertTrue(miss_reads)
                self.assertEqual(len(miss_reads), 2)
                self.assertFalse(any(sql.lstrip().lower().startswith("select *") for sql, _ in miss_reads))
                self.assertFalse(any(miss_blob in row or hit_blob in row for _, row in miss_reads))

                selected_rows.clear()
                found = list(reader.lookup_rows_with_identity("inline_probe", {"criterion": "hit"}))
                self.assertEqual(len(found), 1)
                self.assertEqual(found[0][1], self.source.execute(
                    "SELECT rowid FROM inline_probe WHERE probe_key='matched-large-blob'"
                ).fetchone()[0])
                _assert_rows_equal(self, [source_rows["matched-large-blob"]], [found[0][2]], "inline hit")
                hit_reads = list(selected_rows)
                self.assertEqual(
                    sum(sql.lstrip().lower().startswith("select *") for sql, _ in hit_reads), 1
                )
                self.assertTrue(any(hit_blob in row for _, row in hit_reads))
                self.assertFalse(any(miss_blob in row for _, row in hit_reads))

        self.assertEqual(
            source_before, (self.source_path.stat().st_size, _sha256_file(self.source_path))
        )
        package_after = {
            path.relative_to(self.package).as_posix(): (path.stat().st_size, _sha256_file(path))
            for path in self.package.rglob("*") if path.is_file()
        }
        self.assertEqual(package_before, package_after)

    def test_lookup_preserves_source_rowids_and_explicitly_missing_identity_kinds(self):
        with LosslessShardReader(self.package, expected_generation=GENERATION) as reader:
            identities = list(reader.lookup_rows_with_identity("identity_rows", {}))
            self.assertEqual([rowid for _, rowid, _ in identities], [-19, 77, 9_007_199_254_740_993])
            self.assertEqual([ordinal for ordinal, _, _ in identities], [0, 1, 2])
            self.assertEqual(
                [row[0] for row in self.source.execute("SELECT value FROM identity_rows ORDER BY rowid")],
                [values[0] for _, _, values in identities],
            )

            shadowed = next(item for item in reader.tables() if item["name"] == "shadowed_identity")
            self.assertTrue(shadowed["rowid_aliases_shadowed"])
            self.assertEqual(
                [rowid for _, rowid, _ in reader.lookup_rows_with_identity("shadowed_identity", {})],
                [None],
            )
            without_rowid = next(item for item in reader.tables() if item["name"] == "without_rowid_identity")
            self.assertEqual(without_rowid["rowid_kind"], "without_rowid_primary_key")
            self.assertEqual(
                [rowid for _, rowid, _ in reader.lookup_rows_with_identity("without_rowid_identity", {})],
                [None, None],
            )

    def test_lookup_requires_complete_process_local_package_token(self):
        with self.assertRaisesRegex(ValueError, "complete VerifiedFiles package token"):
            list(shards._iter_table_candidates(
                self.package,
                self.manifest,
                "bodies",
                {},
                verified_files=None,
                verified_records={},
            ))

        controls = {
            "manifest.json",
            "verification.json",
            "selectors-verification.json",
        }
        with LosslessShardReader(self.package, expected_generation=GENERATION) as reader:
            omitted_data_file = next(
                relative for relative in reader._verified_records if relative not in controls
            )
            for omitted in (omitted_data_file, "verification.json"):
                subset_records = {
                    relative: dict(record)
                    for relative, record in reader._verified_records.items()
                    if relative != omitted
                }
                # This token is authentic for its subset, but candidate lookup
                # requires full coverage of both data and control files.
                subset_token = verified_files.verify_files(self.package, subset_records)
                with self.subTest(omitted=omitted), self.assertRaisesRegex(
                    ValueError, "does not cover the complete package"
                ):
                    list(shards._iter_table_candidates(
                        self.package,
                        self.manifest,
                        "bodies",
                        {},
                        verified_files=subset_token,
                        verified_records=subset_records,
                    ))

    def test_lookup_early_close_releases_part_and_external_connections(self):
        opened = []
        real_open = shards._open_readonly

        class TrackedConnection:
            def __init__(self, connection):
                self.connection = connection
                self.closed = False

            def __getattr__(self, name):
                return getattr(self.connection, name)

            def close(self):
                self.closed = True
                self.connection.close()

        def tracked_open(path):
            connection = TrackedConnection(real_open(path))
            opened.append(connection)
            return connection

        with LosslessShardReader(self.package, expected_generation=GENERATION) as reader:
            with patch.object(shards, "_open_readonly", side_effect=tracked_open):
                lookup = reader.lookup_rows_with_identity(
                    "large_lookup", {"lookup_key": "lookup-hit"}
                )
                next(lookup)
                self.assertTrue(any(not item.closed for item in opened))
                lookup.close()
            self.assertTrue(opened)
            self.assertTrue(all(item.closed for item in opened))
            self.assertEqual(reader._iterators, set())
            self.assertEqual(len(list(reader.iter_rows("matches"))), 5)

    def test_selector_reuse_checks_tokens_without_rehashing_package_files(self):
        manifest = self._manifest(self.package)
        expected_hashes = (
            3
            + sum(len(table["parts"]) for table in manifest["tables"])
            + len(manifest["external_values"])
            + len(manifest["by_mode"])
            + len(manifest["by_rule"])
        )
        with patch.object(
            verified_files, "_hash_descriptor", wraps=verified_files._hash_descriptor
        ) as token_hash, patch.object(
            shards, "_sha256_file", wraps=shards._sha256_file
        ) as shard_hash:
            with LosslessShardReader(self.package, expected_generation=GENERATION) as reader:
                self.assertEqual(token_hash.call_count, expected_hashes)
                first = list(reader.iter_selected_matches("bankara_open"))
                second = list(reader.iter_selected_matches("bankara_open"))
                _assert_rows_equal(self, first, second, "selector repeated read")
                self.assertEqual(token_hash.call_count, expected_hashes)
                shard_hash.assert_not_called()

    def test_controls_are_parsed_from_the_exact_bytes_bound_by_the_token(self):
        package = self._copy_package("control-parse-hash-race")
        real_verify = shard_reader_module.verify_files

        def mutate_after_parse(root, records):
            path = Path(root) / "manifest.json"
            path.write_bytes(path.read_bytes() + b" ")
            return real_verify(root, records)

        with patch.object(shard_reader_module, "verify_files", side_effect=mutate_after_parse):
            with self.assertRaisesRegex(ValueError, "HASH_MISMATCH"):
                with LosslessShardReader(package):
                    pass

    def test_lookup_rejects_mutated_controls_shards_receipts_and_new_sqlite_files(self):
        base_manifest = self._manifest(self.package)
        part_relative = base_manifest["tables"][0]["parts"][0]["file"]
        selector_relative = base_manifest["by_mode"][0]["file"]
        external_relative = base_manifest["external_values"][0]["file"]
        tamper_cases = [
            "manifest.json",
            "verification.json",
            "selectors-verification.json",
            part_relative,
            selector_relative,
            external_relative,
        ]
        for index, relative in enumerate(tamper_cases):
            with self.subTest(relative=relative):
                package = self._copy_package(f"lookup-tamper-{index}")
                path = package / relative
                with self.assertRaisesRegex(ValueError, "VERIFIED_FILES_CHANGED"):
                    with LosslessShardReader(package) as reader:
                        before = path.stat()
                        raw = bytearray(path.read_bytes())
                        raw[0] ^= 0x01
                        path.write_bytes(raw)
                        os.utime(path, ns=(before.st_atime_ns, before.st_mtime_ns))
                        list(reader.lookup_rows_with_identity("matches", {"kind": "vs"}))

        package = self._copy_package("lookup-inode-replaced")
        manifest = self._manifest(package)
        relative = manifest["tables"][0]["parts"][0]["file"]
        path = package / relative
        with self.assertRaisesRegex(ValueError, "VERIFIED_FILES_CHANGED"):
            with LosslessShardReader(package) as reader:
                replacement = path.with_suffix(".replacement")
                replacement.write_bytes(path.read_bytes())
                os.replace(replacement, path)
                list(reader.lookup_rows_with_identity("matches", {"kind": "vs"}))

        for filename, expected_message in (
            ("new-sidecar.sqlite3-wal", "VERIFIED_FILES_CHANGED"),
            ("unlisted.sqlite3", "VERIFIED_FILES_CHANGED"),
            ("unlisted.db", "VERIFIED_FILES_CHANGED"),
        ):
            package = self._copy_package("lookup-extra-" + filename.replace(".", "-"))
            with self.assertRaisesRegex(ValueError, expected_message):
                with LosslessShardReader(package) as reader:
                    extra = package / filename
                    if filename.endswith(".db") or filename.endswith(".sqlite3"):
                        connection = sqlite3.connect(extra)
                        try:
                            connection.execute("CREATE TABLE unlisted(value)")
                            connection.commit()
                        finally:
                            connection.close()
                    else:
                        extra.write_bytes(b"sidecar")
                    list(reader.lookup_rows_with_identity("matches", {"kind": "vs"}))

    def test_default_full_stream_still_recomputes_and_rejects_row_digest_mismatch(self):
        package = self._copy_package("lookup-default-full-digest")
        manifest = self._manifest(package)
        table = next(item for item in manifest["tables"] if item["name"] == "bodies")
        part = table["parts"][0]
        connection = sqlite3.connect(package / part["file"])
        try:
            connection.execute(
                "UPDATE bodies SET body_sha256='changed-but-well-formed' WHERE body_sha256='body-hash-1'"
            )
            connection.commit()
        finally:
            connection.close()
        part["bytes"] = (package / part["file"]).stat().st_size
        part["sha256"] = _sha256_file(package / part["file"])
        _write_json(package / "manifest.json", manifest)

        with LosslessShardReader(package, expected_generation=GENERATION) as reader:
            with self.assertRaisesRegex(ValueError, "row stream SHA-256 mismatch"):
                list(reader.iter_rows("bodies"))

    def _manifest(self, package: Path) -> dict:
        return json.loads((package / "manifest.json").read_text(encoding="utf-8"))

    def test_full_tables_schema_values_and_all_selector_closures(self):
        self.assertFalse((self.package / "source.sqlite3").exists())
        self.assertTrue(LosslessShardReader.baseline_only)
        self.assertEqual(LosslessShardReader.reader_role, "baseline_only_lossless_shard_reader")
        source_before = (self.source_path.stat().st_size, _sha256_file(self.source_path))
        with LosslessShardReader(self.package, expected_generation=GENERATION) as reader:
            manifest = self._manifest(self.package)
            self.assertEqual(reader.snapshot_identifier, GENERATION)
            self.assertEqual(reader.source_sha256, self.source_identity['sha256'])
            self.assertEqual(reader.source_schema_sha256, manifest['source_schema_sha256'])
            self.assertEqual(
                {item["name"] for item in reader.tables()},
                {item["name"] for item in manifest["tables"]},
            )
            self.assertEqual(reader.schema_objects(), manifest["schema_objects"])
            for table in manifest["tables"]:
                self.assertEqual(reader.columns(table["name"]), table["column_schema"])
                self.assertEqual(reader.foreign_keys(table["name"]), table["foreign_keys"])
                if table.get("rowid_kind") == "without_rowid_primary_key":
                    key_columns = sorted(
                        (item for item in table["column_schema"] if item["pk"]),
                        key=lambda item: item["pk"],
                    )
                    order = ",".join(_quote(item["name"]) for item in key_columns)
                    query = f"SELECT * FROM {_quote(table['name'])} ORDER BY {order}"
                elif table.get("rowid_aliases_shadowed") is True:
                    query = f"SELECT * FROM {_quote(table['name'])} NOT INDEXED"
                else:
                    alias = table["source_rowid_column"]
                    query = f"SELECT * FROM {_quote(table['name'])} ORDER BY {_quote(alias)}"
                expected = [tuple(row) for row in self.source.execute(query)]
                archived = list(reader.iter_rows(table["name"]))
                self.assertEqual([ordinal for ordinal, _ in archived], list(range(len(expected))))
                _assert_rows_equal(self, expected, [values for _, values in archived], table["name"])
                identities = list(reader.iter_rows_with_identity(table['name']))
                if table.get("rowid_aliases_shadowed") is True or table.get("rowid_kind") == "without_rowid_primary_key":
                    expected_rowids = [None] * len(expected)
                else:
                    alias = table["source_rowid_column"]
                    expected_rowids = [row[0] for row in self.source.execute(
                        f"SELECT {_quote(alias)} FROM {_quote(table['name'])} ORDER BY {_quote(alias)}")]
                self.assertEqual([rowid for _, rowid, _ in identities], expected_rowids)

            self.assertEqual(len(list(reader.iter_selected_matches("bankara_open"))), 3)
            self.assertEqual(len(list(reader.iter_selected_matches("unclassified"))), 1)
            self.assertEqual(
                len(list(reader.find_rows("matches", {"huge_integer": MAX_SQLITE_INTEGER}))),
                1,
            )
            found_float = list(reader.find_rows("matches", {"ratio": 1.25}))
            self.assertEqual(len(found_float), 1)
            self.assertEqual(found_float[0][1][7].hex(), float(1.25).hex())
            found_negative_zero = list(reader.find_rows("matches", {"ratio": -0.0}))
            self.assertEqual(len(found_negative_zero), 1)
            self.assertEqual(found_negative_zero[0][1][7].hex(), (-0.0).hex())
            found_nul = list(reader.find_rows("matches", {"preview": b"preview\x00area"}))
            self.assertEqual(len(found_nul), 1)
            self.assertEqual(len(list(reader.find_rows("matches", {"maybe_null": None}))), 3)
            with self.assertRaises(KeyError):
                list(reader.find_rows("matches", {"column_that_does_not_exist": 1}))

            with self.assertRaises(KeyError):
                list(reader.iter_selected_matches("unknown_mode"))
            with self.assertRaises(KeyError):
                list(reader.iter_selected_matches("bankara_open", rule_token="missing_rule"))

            # Independently compare every mode/rule seed to source rows. Each
            # selector carries all matches columns; related tables remain
            # available through iter_rows rather than inferred joins.
            for selector in manifest["by_mode"]:
                mode = selector["analysis_set"]
                if mode == "unclassified":
                    query = (
                        "SELECT m.* FROM matches m WHERE NOT EXISTS (SELECT 1 FROM "
                        "match_classification c WHERE c.account=m.account AND c.kind=m.kind "
                        "AND c.match_key=m.match_key) ORDER BY m.account,m.kind,m.match_key"
                    )
                    params = ()
                else:
                    query = (
                        "SELECT m.* FROM matches m WHERE EXISTS (SELECT 1 FROM "
                        "match_classification c WHERE c.account=m.account AND c.kind=m.kind "
                        "AND c.match_key=m.match_key AND c.analysis_set=?) "
                        "ORDER BY m.account,m.kind,m.match_key"
                    )
                    params = (mode,)
                expected = [tuple(row) for row in self.source.execute(query, params)]
                selected = list(reader.iter_selected_matches(mode))
                _assert_rows_equal(self, expected, selected, f"mode selector {mode}")
                self.assertTrue(all(len(row) == len(reader.columns("matches")) for row in selected))

            for selector in manifest["by_rule"]:
                mode = selector["analysis_set"]
                raw_rule = selector["rule_raw"]
                token = rule_token(raw_rule)
                query = (
                    "SELECT m.* FROM matches m WHERE EXISTS (SELECT 1 FROM "
                    "match_classification c WHERE c.account=m.account AND c.kind=m.kind "
                    "AND c.match_key=m.match_key AND c.analysis_set=? AND c.rule_raw IS ?) "
                    "ORDER BY m.account,m.kind,m.match_key"
                )
                expected = [tuple(row) for row in self.source.execute(query, (mode, raw_rule))]
                selected = list(reader.iter_selected_matches(mode, rule_token=token))
                _assert_rows_equal(self, expected, selected, f"rule selector {mode}/{token}")

        self.assertEqual(source_before, (self.source_path.stat().st_size, _sha256_file(self.source_path)))
        with self.assertRaises(RuntimeError):
            reader.tables()

    def test_missing_table_record_is_rejected(self):
        package = self._copy_package("missing-table-record")
        manifest = self._manifest(package)
        manifest["tables"] = [table for table in manifest["tables"] if table["name"] != "deliberately_empty"]
        _write_json(package / "manifest.json", manifest)
        with self.assertRaisesRegex(ValueError, "table list differs"):
            with LosslessShardReader(package):
                pass

    def test_missing_and_incomplete_proofs_are_rejected(self):
        for filename in ("verification.json", "selectors-verification.json"):
            with self.subTest(filename=filename):
                package = self._copy_package("missing-proof-" + filename.replace(".", "-"))
                (package / filename).unlink()
                with self.assertRaisesRegex(ValueError, "missing"):
                    with LosslessShardReader(package):
                        pass

        package = self._copy_package("incomplete-selector-proof")
        proof_path = package / "selectors-verification.json"
        proof = json.loads(proof_path.read_text(encoding="utf-8"))
        proof["all_shared_files_reachable"] = False
        _write_json(proof_path, proof)
        with self.assertRaisesRegex(ValueError, "proof is incomplete"):
            with LosslessShardReader(package):
                pass

    def test_selector_dependency_closure_is_checked_before_seed_rows(self):
        package = self._copy_package("broken-selector-dependency")
        manifest = self._manifest(package)
        selector = next(row for row in manifest["by_mode"] if row["analysis_set"] == "bankara_open")
        dependency = manifest["tables"][0]["parts"][0]["file"]
        selector_path = package / selector["file"]
        connection = sqlite3.connect(selector_path)
        try:
            connection.execute("DELETE FROM shared_files WHERE file=?", (dependency,))
            connection.commit()
        finally:
            connection.close()
        selector["bytes"] = selector_path.stat().st_size
        selector["sha256"] = _sha256_file(selector_path)
        _write_json(package / "manifest.json", manifest)

        with LosslessShardReader(package) as reader:
            with self.assertRaisesRegex(ValueError, "dependency closure mismatch"):
                list(reader.iter_selected_matches("bankara_open"))

    def test_mutated_external_blob_is_rejected_by_file_hash(self):
        package = self._copy_package("mutated-blob")
        manifest = self._manifest(package)
        external = manifest["external_values"][0]
        path = package / external["file"]
        connection = sqlite3.connect(path)
        try:
            row = connection.execute("SELECT rowid,payload FROM value_chunks LIMIT 1").fetchone()
            payload = bytearray(row[1])
            payload[0] ^= 0x01
            connection.execute("UPDATE value_chunks SET payload=? WHERE rowid=?", (bytes(payload), row[0]))
            connection.commit()
        finally:
            connection.close()
        with self.assertRaisesRegex(ValueError, "HASH_MISMATCH"):
            with LosslessShardReader(package):
                pass

    def test_old_legacy_role_and_generation_mixes_are_rejected(self):
        package = self._copy_package("old-partial-role")
        manifest = self._manifest(package)
        manifest["role"] = "analysis_slice"
        _write_json(package / "manifest.json", manifest)
        with self.assertRaisesRegex(ValueError, "unsupported lossless"):
            with LosslessShardReader(package):
                pass

        with self.assertRaisesRegex(ValueError, "expected_generation"):
            with LosslessShardReader(self.package, expected_generation=OTHER_GENERATION):
                pass

        package = self._copy_package("mixed-selector-generation")
        manifest = self._manifest(package)
        selector = next(row for row in manifest["by_mode"] if row["analysis_set"] == "bankara_open")
        selector_path = package / selector["file"]
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
        _write_json(package / "manifest.json", manifest)
        with LosslessShardReader(package, expected_generation=GENERATION) as reader:
            with self.assertRaisesRegex(ValueError, "generation metadata mismatch"):
                list(reader.iter_selected_matches("bankara_open"))

    def test_symlink_sidecar_and_undocumented_sqlite_are_rejected(self):
        link = self.root / "package-link"
        link.symlink_to(self.package, target_is_directory=True)
        with self.assertRaisesRegex(ValueError, "symbolic link"):
            with LosslessShardReader(link):
                pass

        package = self._copy_package("sidecar-package")
        part = self._manifest(package)["tables"][0]["parts"][0]["file"]
        (package / (part + "-wal")).write_bytes(b"sidecar")
        with self.assertRaisesRegex(ValueError, "sidecar"):
            with LosslessShardReader(package):
                pass

        package = self._copy_package("unknown-sqlite-package")
        rogue = sqlite3.connect(package / "undocumented.sqlite3")
        try:
            rogue.execute("CREATE TABLE undocumented(value)")
            rogue.commit()
        finally:
            rogue.close()
        with self.assertRaisesRegex(ValueError, "inventory mismatch"):
            with LosslessShardReader(package):
                pass

        package = self._copy_package("unknown-sqlite-by-signature-package")
        rogue = sqlite3.connect(package / "undocumented.db")
        try:
            rogue.execute("CREATE TABLE undocumented(value)")
            rogue.commit()
        finally:
            rogue.close()
        with self.assertRaisesRegex(ValueError, "absent from manifest inventory"):
            with LosslessShardReader(package):
                pass


    def test_context_exit_closes_retained_part_external_and_selector_iterators(self):
        from ikarchive import lossless_sqlite as shards
        opened = []
        real_open = shards._open_readonly

        class TrackedConnection:
            def __init__(self, connection):
                self.connection = connection
                self.closed = False
            def __getattr__(self, name):
                return getattr(self.connection, name)
            def close(self):
                self.closed = True
                self.connection.close()

        def tracked_open(path):
            connection = TrackedConnection(real_open(path))
            opened.append(connection)
            return connection

        with LosslessShardReader(self.package) as reader:
            with patch.object(shards, "_open_readonly", side_effect=tracked_open):
                iterators = [
                    reader.iter_rows("bodies"),
                    reader.iter_rows_with_identity("matches"),
                    reader.find_rows("matches", {}),
                    reader.lookup_rows_with_identity("bodies", {"body_sha256": "body-hash-1"}),
                    reader.iter_selected_matches("bankara_open"),
                ]
                unstarted_lookup = reader.lookup_rows_with_identity("large_lookup", {})
                for iterator in iterators:
                    next(iterator)
                self.assertTrue(opened)
                self.assertTrue(any(not connection.closed for connection in opened))
        self.assertTrue(all(connection.closed for connection in opened))
        self.assertEqual(reader._iterators, set())
        self.assertIsNone(reader._verified_files)
        self.assertEqual(reader._verified_records, {})
        for iterator in iterators:
            with self.assertRaises(StopIteration):
                next(iterator)
        with self.assertRaises(StopIteration):
            next(unstarted_lookup)

    def test_explicit_find_close_releases_inner_iterator_and_reader_remains_usable(self):
        with LosslessShardReader(self.package) as reader:
            found = reader.find_rows("matches", {})
            next(found)
            self.assertGreater(len(reader._iterators), 1)
            found.close()
            self.assertEqual(reader._iterators, set())
            self.assertEqual(len(list(reader.iter_rows("matches"))), 5)
            self.assertEqual(reader._iterators, set())

    def test_context_exception_closes_retained_and_unstarted_iterators(self):
        reader = LosslessShardReader(self.package)
        opened = []
        real_open = shards._open_readonly

        class TrackedConnection:
            def __init__(self, connection):
                self.connection = connection
                self.closed = False

            def __getattr__(self, name):
                return getattr(self.connection, name)

            def close(self):
                self.closed = True
                self.connection.close()

        def tracked_open(path):
            connection = TrackedConnection(real_open(path))
            opened.append(connection)
            return connection

        with self.assertRaisesRegex(RuntimeError, "consumer failed"):
            with patch.object(shards, "_open_readonly", side_effect=tracked_open):
                with reader:
                    partial = reader.lookup_rows_with_identity(
                        "large_lookup", {"lookup_key": "lookup-hit"}
                    )
                    unstarted = reader.lookup_rows_with_identity("bodies", {})
                    next(partial)
                    self.assertGreaterEqual(len(opened), 2)
                    self.assertTrue(any(not connection.closed for connection in opened))
                    raise RuntimeError("consumer failed")
        self.assertTrue(all(connection.closed for connection in opened))
        self.assertEqual(reader._iterators, set())
        self.assertIsNone(reader._verified_files)
        self.assertEqual(reader._verified_records, {})
        for iterator in (partial, unstarted):
            with self.assertRaises(StopIteration):
                next(iterator)



if __name__ == "__main__":
    unittest.main()
