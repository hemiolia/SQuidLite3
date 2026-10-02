"""実SQLiteから作る全データ世代の結合回帰テスト。"""

from __future__ import annotations

import copy
import hashlib
import json
from pathlib import Path
import shutil
import sqlite3
import sys
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT / "src/python"))

import export_full_xlsx  # noqa: E402
import nas_full_data_publish as publisher  # noqa: E402
import prepare_full_data_generation as prepare  # noqa: E402
from ikarchive.lossless_sqlite import iter_table_rows, verify_sqlite_shards  # noqa: E402
from ikarchive.slice_selectors import data_files, verify_selectors  # noqa: E402


GENERATION_ID = "20261002T120000Z-abc012ef"
OTHER_GENERATION_ID = "20261002T120001Z-def345ab"
SQLITE_ONLY_GENERATION_ID = "20261002T120002Z-fed78901"
MAX_SQLITE_INTEGER = 9_223_372_036_854_775_000


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _quote(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _write_json(path: Path, value: dict) -> bytes:
    raw = (json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2) + "\n").encode("utf-8")
    path.write_bytes(raw)
    return raw


class FullDataGenerationIntegrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # macOS commonly routes the system temporary directory through a
        # /var/folders symlink; the production path validator intentionally
        # rejects every symlink component, so keep this disposable fixture
        # beneath the already checked-out repository path.
        cls._temporary = tempfile.TemporaryDirectory(
            prefix=".test-full-generation-", dir=ROOT
        )
        cls.root = Path(cls._temporary.name)
        cls.snapshot = cls.root / "source.sqlite3"
        cls.source_manifest_path = cls.root / "source-manifest.json"
        cls.generation_dir = cls.root / "generation"
        cls._create_source()
        cls.source_before = {
            "bytes": cls.snapshot.stat().st_size,
            "sha256": _sha256_file(cls.snapshot),
        }
        source_manifest = {
            "storage": "plaintext",
            "encryption": None,
            "captured_at": "2026-10-02T12:00:00Z",
            "captured_at_kind": "pinned_read_transaction",
            "raw_snapshot": {
                "basename": cls.snapshot.name,
                **cls.source_before,
                "quick_check": "ok",
            },
            "verification": {"sha256_match": True, "quick_check": "ok"},
        }
        _write_json(cls.source_manifest_path, source_manifest)
        cls.plan = prepare.prepare_generation(
            cls.snapshot,
            cls.source_manifest_path,
            cls.generation_dir,
            GENERATION_ID,
        )
        cls.plan_path = cls.generation_dir / "generation-plan.json"

    @classmethod
    def tearDownClass(cls):
        cls._temporary.cleanup()

    @classmethod
    def _create_source(cls):
        connection = sqlite3.connect(cls.snapshot)
        try:
            connection.execute("PRAGMA journal_mode=DELETE")
            connection.execute("PRAGMA foreign_keys=ON")
            connection.executescript(
                """
                CREATE TABLE matches (
                    account TEXT NOT NULL,
                    kind TEXT NOT NULL,
                    match_key TEXT NOT NULL,
                    first_seen INTEGER,
                    last_seen INTEGER,
                    newly_added_column TEXT,
                    huge_integer INTEGER,
                    payload BLOB,
                    raw_text TEXT,
                    maybe_null TEXT,
                    empty_text TEXT,
                    PRIMARY KEY (account, kind, match_key)
                );
                CREATE TABLE match_classification (
                    account TEXT NOT NULL,
                    kind TEXT NOT NULL,
                    match_key TEXT NOT NULL,
                    analysis_set TEXT NOT NULL,
                    rule_raw TEXT NOT NULL,
                    extra_unknown_value BLOB,
                    PRIMARY KEY (account, kind, match_key),
                    FOREIGN KEY (account, kind, match_key)
                      REFERENCES matches(account, kind, match_key)
                );
                CREATE TABLE deliberately_empty (
                    marker TEXT,
                    payload BLOB
                );
                """
            )
            large_blob = bytes(range(256)) * 1200
            text_with_nul = "prefix\x00middle\r\n雪だるま☃\U0001f9ed"
            rows = []
            classifications = []
            modes = ("analysis_mode_alpha", "analysis_mode_beta")
            rules = ("RULE_ALPHA", "RULE_BETA")
            for mode_index, mode in enumerate(modes):
                for rule_index, rule in enumerate(rules):
                    key = f"match-{mode_index}-{rule_index}"
                    payload = large_blob if (mode_index, rule_index) == (0, 0) else b"small\x00blob"
                    rows.append((
                        "account-synthetic",
                        "vs",
                        key,
                        1_800_000_000 + mode_index * 10 + rule_index,
                        1_800_000_100 + mode_index * 10 + rule_index,
                        f"unknown-field-{mode_index}-{rule_index}",
                        MAX_SQLITE_INTEGER - mode_index * 10 - rule_index,
                        payload,
                        text_with_nul if (mode_index, rule_index) == (0, 0) else "ordinary text",
                        None if (mode_index, rule_index) == (0, 0) else "present",
                        "",
                    ))
                    classifications.append((
                        "account-synthetic",
                        "vs",
                        key,
                        mode,
                        rule,
                        b"classification\x00" + bytes([mode_index, rule_index]),
                    ))
            connection.executemany(
                "INSERT INTO matches VALUES(?,?,?,?,?,?,?,?,?,?,?)", rows
            )
            connection.executemany(
                "INSERT INTO match_classification VALUES(?,?,?,?,?,?)", classifications
            )
            connection.commit()
            if connection.execute("PRAGMA foreign_key_check").fetchall():
                raise AssertionError("synthetic source has foreign-key violations")
            if connection.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
                raise AssertionError("synthetic source failed integrity_check")
        finally:
            connection.close()
        # Leave a static main database with no journal or WAL sidecar.
        for suffix in ("-wal", "-journal", "-shm"):
            sidecar = Path(str(cls.snapshot) + suffix)
            if sidecar.exists() and sidecar.stat().st_size:
                raise AssertionError(f"synthetic snapshot retained a sidecar: {sidecar.name}")

    def _read_plan(self, generation_dir: Path | None = None) -> dict:
        path = (generation_dir or self.generation_dir) / "generation-plan.json"
        return json.loads(path.read_text(encoding="utf-8"))

    def test_retained_attempts_and_work_receipts_are_preserved_outside_publish_package(self):
        destination = self.root / 'working-generation'
        destination.mkdir()
        (destination / 'preparation-input.json').write_text('{"working":true}')
        retained = destination / 'xlsx-retained-missing-rowid-20261002'
        retained.mkdir()
        old_piece = retained / 'old-piece.xlsx'
        old_piece.write_bytes(b'retained old attempt')
        identity = _sha256_file(old_piece)
        plan = prepare.prepare_generation(self.snapshot, self.source_manifest_path,
                                          destination, OTHER_GENERATION_ID)
        history = self.root / 'preparation-history' / destination.name
        self.assertEqual(_sha256_file(history / retained.name / old_piece.name), identity)
        self.assertEqual((history / 'preparation-input.json').read_text(), '{"working":true}')
        self.assertFalse(retained.exists())
        self._validate(destination, plan, OTHER_GENERATION_ID)
        # Arbitrary undeclared source data is not silently moved out or ignored.
        unknown = destination / 'undocumented.sqlite3'
        unknown.write_bytes(b'SQLite format 3\x00')
        prepare.preserve_preparation_history(destination)
        self.assertTrue(unknown.exists())
        with self.assertRaises(publisher.PublishError):
            self._validate(destination, plan, OTHER_GENERATION_ID)

    def _validate(self, generation_dir: Path, plan: dict, generation_id: str = GENERATION_ID):
        raw = (generation_dir / "generation-plan.json").read_bytes()
        return publisher.validate_generation(
            generation_dir,
            generation_id,
            plan,
            len(raw),
            hashlib.sha256(raw).hexdigest(),
        )

    def _assert_rejected(self, category: str, generation_dir: Path, plan: dict,
                         generation_id: str = GENERATION_ID):
        with self.assertRaises(publisher.PublishError) as raised:
            self._validate(generation_dir, plan, generation_id)
        self.assertEqual(raised.exception.category, category)

    def _copy_generation(self, label: str) -> Path:
        destination = self.root / f"negative-{label}"
        if destination.exists():
            shutil.rmtree(destination)
        shutil.copytree(self.generation_dir, destination)
        return destination

    def _refresh_plan_file(self, generation_dir: Path, plan: dict, local: str):
        path = generation_dir / local
        raw = path.read_bytes()
        matched = [row for row in plan["files"] if row["local"] == local]
        self.assertEqual(len(matched), 1, f"plan should have one entry for {local}")
        matched[0]["bytes"] = len(raw)
        matched[0]["sha256"] = hashlib.sha256(raw).hexdigest()

    def _store_plan(self, generation_dir: Path, plan: dict):
        _write_json(generation_dir / "generation-plan.json", plan)

    def test_generation_is_source_bound_complete_and_preflight_accepts_it(self):
        generation_dir = self.generation_dir
        plan = self._read_plan()
        manifest = json.loads((generation_dir / "slices/manifest.json").read_text(encoding="utf-8"))
        slice_verification = json.loads(
            (generation_dir / "slices/verification.json").read_text(encoding="utf-8")
        )
        selector_verification = json.loads(
            (generation_dir / "slices/selectors-verification.json").read_text(encoding="utf-8")
        )
        xlsx_index = json.loads((generation_dir / "xlsx/index.json").read_text(encoding="utf-8"))
        xlsx_verification = json.loads(
            (generation_dir / "xlsx/verification.json").read_text(encoding="utf-8")
        )

        self.assertEqual(plan, self.plan)
        self.assertEqual(plan["generation_id"], GENERATION_ID)
        self.assertEqual(plan["source"], self.source_before)
        self.assertEqual(manifest["snapshot_identifier"], GENERATION_ID)
        self.assertEqual(manifest["source_sha256"], self.source_before["sha256"])
        self.assertEqual(slice_verification["source_sha256"], self.source_before["sha256"])
        self.assertEqual(selector_verification["source_sha256"], self.source_before["sha256"])
        self.assertEqual(xlsx_verification["snapshot_sha256"], self.source_before["sha256"])
        self.assertEqual(
            {row["analysis_set"] for row in manifest["by_mode"] if row["matches"]},
            {"analysis_mode_alpha", "analysis_mode_beta"},
        )
        self.assertEqual(manifest["counts"]["distinct_rules"], 2)
        self.assertEqual(
            {row["rule_raw"] for row in manifest["by_rule"]},
            {"RULE_ALPHA", "RULE_BETA"},
        )
        self.assertEqual(manifest["counts"]["rule_mode_product"], 4)
        self.assertEqual(len(manifest["by_rule"]), 4)
        self.assertEqual(
            {table["name"]: table["row_count"] for table in manifest["tables"]}[
                "deliberately_empty"
            ],
            0,
        )
        self.assertEqual(
            {table["name"] for table in manifest["tables"]},
            {"matches", "match_classification", "deliberately_empty"},
        )
        self.assertTrue(manifest["external_values"], "large BLOB must use an external value dependency")
        self.assertEqual(set(xlsx_index["tables"]), {table["name"] for table in manifest["tables"]})
        self.assertEqual(xlsx_index["schema_objects"], manifest["schema_objects"])
        self.assertTrue(all(xlsx_index["tables"][name]["columns"] for name in xlsx_index["tables"]))

        expected_locals = {
            "source.sqlite3",
            "source-manifest.json",
            "slices/manifest.json",
            "slices/verification.json",
            "slices/selectors-verification.json",
            "xlsx/index.json",
            "xlsx/manifest.json",
            "xlsx/verification.json",
        }
        expected_locals.update("slices/" + row["file"] for row in data_files(manifest))
        expected_locals.update("slices/" + row["file"] for row in manifest["by_mode"])
        expected_locals.update("slices/" + row["file"] for row in manifest["by_rule"])
        expected_locals.update("xlsx/" + row["name"] for row in xlsx_index["pieces"])
        self.assertEqual({row["local"] for row in plan["files"]}, expected_locals)

        actual_locals = {
            path.relative_to(generation_dir).as_posix()
            for path in generation_dir.rglob("*")
            if path.is_file() and path.name != "generation-plan.json"
        }
        self.assertEqual(actual_locals, expected_locals)
        for entry in plan["files"]:
            path = generation_dir / entry["local"]
            self.assertEqual(path.stat().st_size, entry["bytes"], entry["local"])
            self.assertEqual(_sha256_file(path), entry["sha256"], entry["local"])

        source = sqlite3.connect(self.snapshot.as_uri() + "?mode=ro&immutable=1", uri=True)
        try:
            selector_receipt = verify_selectors(
                source, generation_dir / "slices", manifest, self.source_before["sha256"]
            )
            xlsx_receipt = export_full_xlsx.verify_export(source, generation_dir / "xlsx")

            # Copy the complete published SQLite package, including both
            # selector inventories and their proof files. The reader and
            # verifier must accept the same package that the generation plan
            # publishes, rather than a hand-trimmed subset of it.
            shard_readback = self.root / "independent-shard-readback"
            if shard_readback.exists():
                shutil.rmtree(shard_readback)
            shutil.copytree(generation_dir / "slices", shard_readback)
            shard_receipt = verify_sqlite_shards(source, shard_readback, manifest)

            self.assertEqual(shard_receipt["row_counts"], {"matches": 4, "match_classification": 4, "deliberately_empty": 0})
            self.assertTrue(all(shard_receipt["coverage"].values()))
            self.assertEqual(selector_receipt["status"], "verified")
            self.assertEqual(xlsx_receipt["exported_tables"], 3)
            self.assertEqual(xlsx_receipt["exported_rows"], 8)
            self.assertEqual(xlsx_receipt["exported_cells"], 4 * 11 + 4 * 6)

            # Independently walk every row in every shared piece and compare
            # typed values against SELECT * from the original source.
            for table in manifest["tables"]:
                columns = table["columns"]
                query = f"SELECT {','.join(_quote(name) for name in columns)} FROM {_quote(table['name'])} ORDER BY rowid"
                source_rows = [tuple(row) for row in source.execute(query)]
                archived_rows = list(iter_table_rows(shard_readback, manifest, table["name"]))
                self.assertEqual(
                    archived_rows,
                    list(enumerate(source_rows)),
                    f"typed source values differ for {table['name']}",
                )
            match = source.execute(
                "SELECT newly_added_column,huge_integer,payload,raw_text,maybe_null,empty_text "
                "FROM matches WHERE match_key='match-0-0'"
            ).fetchone()
            self.assertEqual(match[0], "unknown-field-0-0")
            self.assertEqual(match[1], MAX_SQLITE_INTEGER)
            self.assertEqual(match[2], bytes(range(256)) * 1200)
            self.assertIn("\x00", match[3])
            self.assertIsNone(match[4])
            self.assertEqual(match[5], "")
        finally:
            source.close()

        # Publisher preflight is local and read-only; no rclone/publish path is invoked.
        accepted = self._validate(generation_dir, plan)
        self.assertEqual(accepted["generation_id"], GENERATION_ID)
        self.assertEqual(accepted["source"], self.source_before)
        self.assertEqual(len(accepted["files"]), len(expected_locals))
        self.assertEqual(
            {entry["local"] for entry in accepted["files"]},
            expected_locals,
        )
        self.assertEqual(
            {"bytes": self.snapshot.stat().st_size, "sha256": _sha256_file(self.snapshot)},
            self.source_before,
            "source snapshot bytes must remain unchanged",
        )

    def test_sqlite_reader_rejects_undocumented_sqlite_file(self):
        manifest = json.loads(
            (self.generation_dir / "slices/manifest.json").read_text(encoding="utf-8")
        )
        shard_readback = self.root / "undocumented-sqlite-readback"
        if shard_readback.exists():
            shutil.rmtree(shard_readback)
        shutil.copytree(self.generation_dir / "slices", shard_readback)
        rogue = sqlite3.connect(shard_readback / "undocumented.sqlite3")
        try:
            rogue.execute("CREATE TABLE undocumented(value TEXT)")
            rogue.commit()
        finally:
            rogue.close()

        source = sqlite3.connect(self.snapshot.as_uri() + "?mode=ro&immutable=1", uri=True)
        try:
            with self.assertRaisesRegex(ValueError, "inventory mismatch"):
                verify_sqlite_shards(source, shard_readback, manifest)
        finally:
            source.close()

    def test_sqlite_only_generation_preserves_in_progress_xlsx(self):
        generation_dir = self.root / "sqlite-only-generation"
        xlsx_dir = generation_dir / "xlsx"
        partial_paths = (
            xlsx_dir / ".lossless-export.xlsx.part",
            xlsx_dir / ".writer-state" / "piece-0007.xml.part",
        )
        partial_paths[1].parent.mkdir(parents=True)
        partial_paths[0].write_bytes(b"in-progress xlsx package bytes\x00\xff")
        partial_paths[1].write_bytes(b"in-progress XML bytes\r\n\x00")

        def file_state(root: Path) -> dict[str, tuple[int, str, int]]:
            return {
                path.relative_to(root).as_posix(): (
                    path.stat().st_size,
                    _sha256_file(path),
                    path.stat().st_mtime_ns,
                )
                for path in root.rglob("*")
                if path.is_file()
            }

        xlsx_before = file_state(xlsx_dir)
        self.assertEqual(
            set(xlsx_before),
            {".lossless-export.xlsx.part", ".writer-state/piece-0007.xml.part"},
        )
        result = prepare.prepare_generation(
            self.snapshot,
            self.source_manifest_path,
            generation_dir,
            SQLITE_ONLY_GENERATION_ID,
            sqlite_only=True,
        )

        self.assertEqual(result["status"], "sqlite_prepared")
        self.assertEqual(result["generation_id"], SQLITE_ONLY_GENERATION_ID)
        self.assertEqual(result["source"], self.source_before)
        self.assertEqual(result["sqlite_verification"]["status"], "verified")
        self.assertEqual(file_state(xlsx_dir), xlsx_before)
        self.assertFalse((xlsx_dir / "index.json").exists())
        self.assertFalse((xlsx_dir / "verification.json").exists())
        self.assertFalse((generation_dir / "generation-plan.json").exists())
        receipt_path = generation_dir / "sqlite-preparation.json"
        self.assertTrue(receipt_path.is_file())
        self.assertEqual(json.loads(receipt_path.read_text(encoding="utf-8")), result)
        self.assertEqual(
            {"bytes": self.snapshot.stat().st_size, "sha256": _sha256_file(self.snapshot)},
            self.source_before,
            "SQLite-only preparation must keep the source snapshot unchanged",
        )

    def test_publisher_rejects_wrong_generation_id_and_snapshot_binding(self):
        plan = self._read_plan()
        self._assert_rejected(
            "PLAN_GENERATION_MISMATCH", self.generation_dir, plan, OTHER_GENERATION_ID
        )

        generation_dir = self._copy_generation("wrong-snapshot-id")
        plan = self._read_plan(generation_dir)
        manifest_path = generation_dir / "slices/manifest.json"
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        manifest["snapshot_identifier"] = OTHER_GENERATION_ID
        _write_json(manifest_path, manifest)
        self._refresh_plan_file(generation_dir, plan, "slices/manifest.json")
        self._store_plan(generation_dir, plan)
        self._assert_rejected("SLICE_GENERATION_MISMATCH", generation_dir, plan)

    def test_publisher_rejects_wrong_source_sha_and_missing_piece(self):
        plan = self._read_plan()
        wrong_source = copy.deepcopy(plan)
        wrong_source["source"]["sha256"] = "0" * 64
        self._assert_rejected("SOURCE_MANIFEST_INVALID", self.generation_dir, wrong_source)

        generation_dir = self._copy_generation("missing-piece")
        plan = self._read_plan(generation_dir)
        index = json.loads((generation_dir / "xlsx/index.json").read_text(encoding="utf-8"))
        self.assertTrue(index["pieces"])
        (generation_dir / "xlsx" / index["pieces"][0]["name"]).unlink()
        self._assert_rejected("PLAN_FILE_SET_MISMATCH", generation_dir, plan)

    def test_publisher_rejects_legacy_partial_and_incomplete_value_proofs(self):
        generation_dir = self._copy_generation("legacy-proof")
        plan = self._read_plan(generation_dir)
        manifest_path = generation_dir / "slices/manifest.json"
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        manifest["role"] = "analysis_slice"
        _write_json(manifest_path, manifest)
        self._refresh_plan_file(generation_dir, plan, "slices/manifest.json")
        self._store_plan(generation_dir, plan)
        self._assert_rejected("SLICE_ROLE_INVALID", generation_dir, plan)

        generation_dir = self._copy_generation("all-values-false")
        plan = self._read_plan(generation_dir)
        verification_path = generation_dir / "slices/verification.json"
        verification = json.loads(verification_path.read_text(encoding="utf-8"))
        verification["coverage"]["all_values"] = False
        _write_json(verification_path, verification)
        plan["sqlite_verification"] = verification
        self._refresh_plan_file(generation_dir, plan, "slices/verification.json")
        self._store_plan(generation_dir, plan)
        self._assert_rejected("SQLITE_COVERAGE_INCOMPLETE", generation_dir, plan)

    def test_publisher_rejects_omitted_external_value_dependency(self):
        generation_dir = self._copy_generation("external-dependency-omitted")
        plan = self._read_plan(generation_dir)
        manifest_path = generation_dir / "slices/manifest.json"
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        self.assertTrue(manifest["external_values"])
        manifest["external_values"] = []
        _write_json(manifest_path, manifest)
        self._refresh_plan_file(generation_dir, plan, "slices/manifest.json")
        self._store_plan(generation_dir, plan)
        self._assert_rejected("SQLITE_VERIFICATION_INVALID", generation_dir, plan)


if __name__ == "__main__":
    unittest.main()
