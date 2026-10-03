from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
import shutil
import sqlite3
import sys
import tempfile
import unittest
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src/python"))
sys.path.insert(0, str(ROOT / "scripts"))

from export_full_xlsx import export_full_xlsx  # noqa: E402
import ikarchive.delta_reader as delta_reader_module  # noqa: E402
import ikarchive.native_candidates as native_candidates_module  # noqa: E402
import ikarchive.verified_files as verified_files_module  # noqa: E402
from ikarchive.change_feed import install_change_feed, read_change_batch  # noqa: E402
from ikarchive.delta_reader import DeltaChainReader, DeltaReaderError  # noqa: E402
from ikarchive.delta_transport import build_delta_database, verify_delta_database  # noqa: E402
from ikarchive.lossless_sqlite import export_sqlite_shards, verify_sqlite_shards  # noqa: E402
from ikarchive.reconciliation import read_reconciliation  # noqa: E402
from ikarchive.shard_reader import LosslessShardReader  # noqa: E402
from ikarchive.slice_selectors import data_files, export_selectors, rule_token, verify_selectors  # noqa: E402
from ikarchive.writer_guards import install_writer_guards, inspect_writer_guards  # noqa: E402


BASELINE_ID = "20261003T001000Z-00000001"
RESET_ID = "20261003T001001Z-00000002"
DELTA_A_ID = "20261003T001002Z-00000003"
DELTA_B_ID = "20261003T001003Z-00000004"
RESET_B_ID = "20261003T001004Z-00000005"
DELTA_C_ID = "20261003T001005Z-00000006"
MULTI_VALUE_ID = "20261003T001006Z-00000007"
SHARDED_DDL_ID = "20261003T001007Z-00000008"
BASELINE_BLOB = bytes(range(256)) * 1100
UPDATED_BLOB = b"updated\x00payload" + bytes(range(200)) * 1400
MULTI_FILE_BLOB = bytes(range(256)) * 1600
HUGE_INTEGER = (1 << 63) - 1


def _sha256(path: Path) -> dict[str, object]:
    digest = hashlib.sha256()
    size = 0
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            size += len(block)
            digest.update(block)
    return {"bytes": size, "sha256": digest.hexdigest()}


def _write_json(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2) + "\n", encoding="utf-8")


def _read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _tree_snapshot(root: Path) -> dict[str, object]:
    snapshot: dict[str, object] = {}
    for path in sorted(root.rglob("*")):
        relative = path.relative_to(root).as_posix()
        if path.is_symlink():
            snapshot[relative] = {"symlink": str(path.readlink())}
        elif path.is_dir():
            snapshot[relative] = {"directory": True}
        elif path.is_file():
            snapshot[relative] = _sha256(path)
    return snapshot


def _readonly(path: Path, *, immutable: bool = False) -> sqlite3.Connection:
    suffix = "?mode=ro&immutable=1" if immutable else "?mode=ro"
    connection = sqlite3.connect(path.as_uri() + suffix, uri=True)
    connection.execute("PRAGMA query_only=ON")
    return connection


def _create_baseline(path: Path) -> None:
    connection = sqlite3.connect(path)
    try:
        connection.execute("PRAGMA journal_mode=DELETE")
        connection.execute("PRAGMA foreign_keys=ON")
        connection.executescript(
            """
            CREATE TABLE records (
                id INTEGER PRIMARY KEY,
                payload,
                maybe_null,
                blob_value,
                large_integer,
                ratio
            );
            CREATE TABLE key_only (payload, kind TEXT, key_value BLOB, PRIMARY KEY(kind,key_value)) WITHOUT ROWID;
            CREATE TABLE responses (response_id INTEGER PRIMARY KEY AUTOINCREMENT, body BLOB);
            CREATE TABLE matches (
                account TEXT NOT NULL, kind TEXT NOT NULL, match_key TEXT NOT NULL,
                payload, huge_integer, ratio, raw_body BLOB,
                PRIMARY KEY(account,kind,match_key)
            );
            CREATE TABLE match_classification (
                account TEXT NOT NULL, kind TEXT NOT NULL, match_key TEXT NOT NULL,
                analysis_set TEXT NOT NULL, rule_raw TEXT, raw_classification BLOB,
                PRIMARY KEY(account,kind,match_key)
            );
            CREATE TABLE deliberately_empty (untyped_value);
            CREATE TABLE to_drop (old_value TEXT);
            """
        )
        connection.executemany(
            "INSERT INTO records VALUES(?,?,?,?,?,?)",
            [
                (-11, "negative-rowid", None, b"before\x00blob", 4, -0.0),
                (5, "kept-row", "typed", BASELINE_BLOB, HUGE_INTEGER, 3.5),
                (37, "deleted-row", "gone", b"delete-me", 9, 0.0),
            ],
        )
        connection.executemany(
            "INSERT INTO key_only VALUES(?,?,?)",
            [(b"old-a", "vs", b"\x00key-a"), ("text-b", "coop", b"key-b")],
        )
        connection.execute("INSERT INTO responses(body) VALUES(?)", (b"response-body\x00",))
        connection.executemany(
            "INSERT INTO matches VALUES(?,?,?,?,?,?,?)",
            [
                ("account-a", "vs", "match-a", "one", HUGE_INTEGER, -0.0, b"m\x00a"),
                ("account-a", "vs", "match-b", "two", 12, 1.25, b"m-b"),
                ("account-a", "coop", "match-c", None, 13, 0.0, b"m-c"),
            ],
        )
        connection.executemany(
            "INSERT INTO match_classification VALUES(?,?,?,?,?,?)",
            [
                ("account-a", "vs", "match-a", "bankara_open", "AREA", b"class-a"),
                ("account-a", "vs", "match-b", "xmatch", "TOWER", b"class-b"),
            ],
        )
        connection.execute("INSERT INTO to_drop VALUES('old')")
        connection.commit()
    finally:
        connection.close()


def _make_baseline_package(source: Path, package: Path, generation: str = BASELINE_ID) -> dict:
    source_conn = _readonly(source, immutable=True)
    try:
        identity = _sha256(source)
        manifest = export_sqlite_shards(source_conn, package, snapshot_id=generation)
        manifest["source_sha256"] = identity["sha256"]
        export_selectors(source_conn, package, manifest, identity["sha256"])
        _write_json(package / "manifest.json", manifest)
        verification = verify_sqlite_shards(source_conn, package, manifest)
        verification["source_sha256"] = identity["sha256"]
        _write_json(package / "verification.json", verification)
        selectors_verification = verify_selectors(source_conn, package, manifest, identity["sha256"])
        _write_json(package / "selectors-verification.json", selectors_verification)
        return {"manifest": manifest, "identity": identity}
    finally:
        source_conn.close()


def _make_xlsx(source_db: Path, generation_root: Path, generation_id: str) -> dict:
    destination = generation_root / "xlsx"
    export_full_xlsx(source_db, destination, snapshot_identifier=generation_id)
    return {
        "index": _read_json(destination / "index.json"),
        "verification": _read_json(destination / "verification.json"),
    }


def _rewrite_native_transport_layout(
    generation_root: Path,
    plan: dict,
    current_path: Path,
    baseline_path: Path,
    *,
    gapped_reordered_table: str,
    metadata_rowid: int,
) -> dict:
    """Change only synthetic transport rowids, then refresh every bound proof."""
    database_path = generation_root / "changes.sqlite3"
    connection = sqlite3.connect(database_path)
    try:
        table_names, _xinfo, visible = delta_reader_module._delta._table_specs(plan["metadata"])
        internal = delta_reader_module._delta._internal_names(plan["metadata"], table_names)
        rowid_alias = delta_reader_module._delta._transport_rowid_alias(
            visible[gapped_reordered_table]
        )
        if rowid_alias is None:
            raise AssertionError("test table must have an unshadowed transport rowid")
        operations_table = internal["operations_table"]
        row_map = connection.execute(
            f'SELECT "row_ordinal","transport_rowid" FROM "{operations_table}" '
            'WHERE "table_name"=? AND "operation"=? ORDER BY "row_ordinal"',
            (gapped_reordered_table, "upsert"),
        ).fetchall()
        old_ids = [row[1] for row in row_map]
        temporary_ids = [(1 << 60) + index for index in range(len(old_ids))]
        new_ids = [1001 + 17 * (len(old_ids) - index) for index in range(len(old_ids))]
        quoted_alias = delta_reader_module._quote(rowid_alias)
        quoted_table = delta_reader_module._quote(gapped_reordered_table)
        for old_id, temporary_id in zip(old_ids, temporary_ids):
            connection.execute(
                f"UPDATE {quoted_table} SET {quoted_alias}=? WHERE {quoted_alias}=?",
                (temporary_id, old_id),
            )
        for (ordinal, _old_id), temporary_id, new_id in zip(row_map, temporary_ids, new_ids):
            connection.execute(
                f"UPDATE {quoted_table} SET {quoted_alias}=? WHERE {quoted_alias}=?",
                (new_id, temporary_id),
            )
            connection.execute(
                f'UPDATE "{operations_table}" SET "transport_rowid"=? '
                'WHERE "table_name"=? AND "row_ordinal"=?',
                (new_id, gapped_reordered_table, ordinal),
            )
        metadata_table = internal["metadata_table"]
        connection.execute(
            f'UPDATE "{metadata_table}" SET "_rowid_"=?', (metadata_rowid,)
        )
        connection.commit()
    finally:
        connection.close()

    document = _read_json(generation_root / "transport-document.json")
    database_digest = _sha256(database_path)
    document["database"]["bytes"] = database_digest["bytes"]
    document["database"]["sha256"] = database_digest["sha256"]
    source = _readonly(current_path, immutable=True)
    baseline = _readonly(baseline_path, immutable=True)
    try:
        source.execute("BEGIN")
        receipt = verify_delta_database(
            source,
            database_path,
            document,
            baseline_conn=baseline if plan["kind"] == "baseline_reconciliation" else None,
        )
    finally:
        source.close()
        baseline.close()
    _write_json(generation_root / "transport-document.json", document)
    _write_json(generation_root / "value-verification.json", receipt)

    shutil.rmtree(generation_root / "xlsx")
    xlsx = _make_xlsx(database_path, generation_root, plan["generation_id"])
    plan["transport_database"] = dict(document["database"])
    plan["value_verification"] = receipt
    plan["xlsx_verification"] = xlsx["verification"]
    for item in plan["files"]:
        digest = _sha256(generation_root / item["local"])
        item["bytes"] = digest["bytes"]
        item["sha256"] = digest["sha256"]
    _write_json(generation_root / "delta-plan.json", plan)
    return plan


def _make_generation(
    current_path: Path,
    baseline_path: Path,
    generation_root: Path,
    generation_id: str,
    baseline_id: str,
    baseline_sha: str,
    *,
    kind: str,
    previous: dict | None = None,
    transport_kind: str = "native_sqlite",
    shard_max_bytes: int = 1024 * 1024,
) -> dict:
    generation_root.mkdir(parents=True)
    current = _readonly(current_path)
    baseline = _readonly(baseline_path, immutable=True)
    try:
        reset = kind == "baseline_reconciliation"
        reader = read_reconciliation(current, baseline) if reset else read_change_batch(
            current, previous["through_event_id"]
        )
        with reader as batch:
            metadata = dict(batch.metadata)
            metadata["captured_at"] = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
            guard_status = inspect_writer_guards(current)
            metadata["writer_guard_status"] = guard_status
            metadata["all_writers_contract_enforced"] = guard_status["all_writers_contract_enforced"]
            parent_id = baseline_id if reset else previous["generation_id"]
            metadata.update(
                generation_id=generation_id,
                parent_generation_id=parent_id,
                baseline_generation_id=baseline_id,
                baseline_source_sha256=baseline_sha,
                captured_at_kind="pinned_read_transaction",
                schema_comparison_required=True,
                kind=kind,
                replaces_delta_chain=reset,
                supersedes_generation_id=previous["generation_id"] if reset and previous else None,
            )
            transport_db = generation_root / "changes.sqlite3"
            document = build_delta_database(batch.iter_current_changes(), metadata, transport_db)
            receipt = verify_delta_database(
                current, transport_db, document,
                baseline_conn=baseline if reset else None,
            )
        _write_json(generation_root / "transport-document.json", document)
        _write_json(generation_root / "value-verification.json", receipt)
        xlsx = _make_xlsx(transport_db, generation_root, generation_id)

        representation: dict
        if transport_kind == "native_sqlite":
            representation = {"kind": "native_sqlite", "path": "changes.sqlite3"}
        elif transport_kind == "lossless_sqlite_shards":
            package = generation_root / "transport"
            transport_conn = _readonly(transport_db, immutable=True)
            try:
                shard_manifest = export_sqlite_shards(
                    transport_conn, package, snapshot_id=generation_id, max_bytes=shard_max_bytes
                )
                shard_proof = verify_sqlite_shards(transport_conn, package, shard_manifest)
                _write_json(package / "manifest.json", shard_manifest)
                _write_json(package / "verification.json", shard_proof)
            finally:
                transport_conn.close()
            representation = {
                "kind": "lossless_sqlite_shards",
                "path": "transport/manifest.json",
                "verification": shard_proof,
            }
        else:
            raise AssertionError(f"unsupported test transport kind: {transport_kind}")

        file_entries = []

        def add(relative: str) -> None:
            digest = _sha256(generation_root / relative)
            file_entries.append({
                "local": relative,
                "remote": f"deltas/generations/{generation_id}/{relative}",
                **digest,
            })

        if transport_kind == "native_sqlite":
            add("changes.sqlite3")
        else:
            for item in data_files(shard_manifest):
                add("transport/" + item["file"])
            add("transport/manifest.json")
            add("transport/verification.json")
        add("transport-document.json")
        add("value-verification.json")
        for piece in xlsx["index"]["pieces"]:
            add("xlsx/" + piece["name"])
        add("xlsx/index.json")
        add("xlsx/verification.json")
        add("xlsx/manifest.json")
        plan = {
            "version": 1,
            "status": "prepared",
            "role": "lossless_delta_generation",
            "generation_id": generation_id,
            "baseline_generation_id": baseline_id,
            "baseline_source_sha256": baseline_sha,
            "parent_generation_id": parent_id,
            "expected_previous_generation_id": previous["generation_id"] if previous else baseline_id,
            "replaces_delta_chain": reset,
            "supersedes_generation_id": metadata["supersedes_generation_id"],
            "kind": kind,
            "captured_at": metadata["captured_at"],
            "after_event_id": metadata["after_event_id"],
            "through_event_id": metadata["through_event_id"],
            "source_schema_sha256": metadata["source_schema_sha256"],
            "source_row_counts": metadata["source_row_counts"],
            "source_table_columns": metadata["source_table_columns"],
            "source_foreign_keys": metadata["source_foreign_keys"],
            "schemas": metadata["schemas"],
            "metadata": metadata,
            "transport": representation,
            "transport_database": document["database"],
            "value_verification": receipt,
            "xlsx_verification": xlsx["verification"],
            "files": file_entries,
            "max_part_bytes": 20_000_000,
            "coverage": {
                "all_changed_values": True,
                "all_source_tables_metadata": True,
                "full_schema": True,
                "original_row_identities": True,
                "baseline_gap_reconciled": reset,
            },
        }
        _write_json(generation_root / "delta-plan.json", plan)
        return plan
    finally:
        current.close()
        baseline.close()


class DeltaChainReaderTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix=".test-delta-reader-", dir=ROOT)
        self.root = Path(self.temporary.name).resolve()
        self.baseline_db = self.root / "baseline.sqlite3"
        self.current_db = self.root / "current.sqlite3"
        self.baseline_package = self.root / "baseline-package"
        _create_baseline(self.baseline_db)
        self.baseline_package_info = _make_baseline_package(self.baseline_db, self.baseline_package)
        shutil.copy2(self.baseline_db, self.current_db)
        self.baseline_identity = self.baseline_package_info["identity"]
        self.writer = sqlite3.connect(self.current_db)
        self.writer.execute("PRAGMA foreign_keys=ON")
        self.writer.execute("PRAGMA recursive_triggers=ON")
        # Deliberately make changes before the feed exists. The full reset
        # must close the initial tracking gap by comparing every source table.
        self._apply_first_change()
        install_change_feed(self.writer)
        install_writer_guards(self.writer)
        self.writer.commit()
        self.reset_root = self.root / RESET_ID
        self.reset_plan = _make_generation(
            self.current_db, self.baseline_db, self.reset_root, RESET_ID,
            BASELINE_ID, self.baseline_identity["sha256"], kind="baseline_reconciliation",
        )

    def tearDown(self):
        self.writer.close()
        self.temporary.cleanup()

    def _apply_first_change(self):
        self.writer.execute(
            "UPDATE records SET payload=?,maybe_null=?,blob_value=?,large_integer=?,ratio=? WHERE id=5",
            ("new\x00text", None, UPDATED_BLOB, HUGE_INTEGER - 1, -0.0),
        )
        self.writer.execute("DELETE FROM records WHERE id=-11")
        self.writer.execute(
            "INSERT INTO records VALUES(?,?,?,?,?,?)",
            (-42, b"inserted\x00bytes", "new", b"inserted", 1234567890123, 0.125),
        )
        self.writer.execute(
            "UPDATE key_only SET payload=? WHERE kind=? AND key_value=?",
            (b"key\x00updated", "vs", b"\x00key-a"),
        )
        self.writer.execute("DELETE FROM key_only WHERE kind=? AND key_value=?", ("coop", b"key-b"))
        self.writer.execute(
            "INSERT INTO key_only VALUES(?,?,?)", ("new-key", "event", b"\xffnew-key")
        )
        self.writer.execute(
            "UPDATE match_classification SET analysis_set='xmatch',rule_raw='TOWER' "
            "WHERE account=? AND kind=? AND match_key=?",
            ("account-a", "vs", "match-a"),
        )
        self.writer.commit()

    def _make_delta(
        self, name: str, generation_id: str, previous: dict, *,
        transport_kind: str = "native_sqlite", shard_max_bytes: int = 1024 * 1024,
    ) -> tuple[Path, dict]:
        root = self.root / name
        plan = _make_generation(
            self.current_db, self.baseline_db, root, generation_id,
            BASELINE_ID, self.baseline_identity["sha256"], kind="change_feed", previous=previous,
            transport_kind=transport_kind, shard_max_bytes=shard_max_bytes,
        )
        return root, plan

    @staticmethod
    def _write_publisher_index(generation_root: Path, plan: dict) -> dict:
        from publish_full_data_delta import _build_delta_index

        plan_raw = (generation_root / "delta-plan.json").read_bytes()
        plan_entry = {
            "local": "delta-plan.json",
            "remote": f"deltas/generations/{plan['generation_id']}/delta-plan.json",
            "bytes": len(plan_raw),
            "sha256": hashlib.sha256(plan_raw).hexdigest(),
        }
        index = _build_delta_index(
            {"plan": plan, "plan_sha256": hashlib.sha256(plan_raw).hexdigest()},
            [*plan["files"], plan_entry],
            "2026-10-03T01:30:00Z",
        )
        _write_json(generation_root / "index.json", index)
        return index

    def _open(self, roots):
        baseline = LosslessShardReader(self.baseline_package, expected_generation=BASELINE_ID)
        return baseline

    def test_baseline_reconciliation_preserves_rowids_types_and_blob_values(self):
        before = _sha256(self.baseline_db)
        with LosslessShardReader(self.baseline_package, expected_generation=BASELINE_ID) as baseline:
            with DeltaChainReader(baseline, [self.reset_root]) as reader:
                rows = list(reader.iter_rows("records"))
                by_id = {source_rowid: values for _, source_rowid, values in rows}
                self.assertEqual(set(by_id), {5, 37, -42})
                self.assertEqual(by_id[5][1], "new\x00text")
                self.assertIsNone(by_id[5][2])
                self.assertEqual(by_id[5][3], UPDATED_BLOB)
                self.assertEqual(by_id[5][4], HUGE_INTEGER - 1)
                self.assertIs(type(by_id[5][5]), float)
                self.assertEqual(by_id[5][5].hex(), "-0x0.0p+0")
                self.assertEqual(by_id[-42][1], b"inserted\x00bytes")
                self.assertEqual(reader.row_count("records"), 3)

                key_rows = list(reader.iter_rows("key_only"))
                self.assertEqual({tuple(values) for _, source_rowid, values in key_rows}, {
                    (b"key\x00updated", "vs", b"\x00key-a"),
                    ("new-key", "event", b"\xffnew-key"),
                })
                self.assertTrue(all(source_rowid is None for _, source_rowid, _ in key_rows))
                self.assertEqual(reader.row_count("deliberately_empty"), 0)
                self.assertEqual(reader.schema_objects(), self.reset_plan["schemas"])
                self.assertEqual(reader.foreign_keys("records"), [])
        self.assertEqual(_sha256(self.baseline_db), before)
        self.assertFalse((self.baseline_package / "source.sqlite3").exists())

    def test_two_normal_deltas_update_the_current_overlay(self):
        root_a, plan_a = self._make_delta("normal-a", DELTA_A_ID, self.reset_plan)
        self.writer.execute("UPDATE records SET payload='second-state' WHERE id=5")
        self.writer.execute("DELETE FROM matches WHERE match_key='match-b'")
        self.writer.commit()
        root_b, plan_b = self._make_delta("normal-b", DELTA_B_ID, plan_a)
        self.assertEqual(plan_b["parent_generation_id"], DELTA_A_ID)
        with LosslessShardReader(self.baseline_package, expected_generation=BASELINE_ID) as baseline:
            with DeltaChainReader(baseline, [self.reset_root, root_a, root_b]) as reader:
                rows = list(reader.iter_rows("records"))
                by_id = {rowid: values for _, rowid, values in rows}
                self.assertEqual(by_id[5][1], "second-state")
                self.assertEqual(reader.row_count("matches"), 2)
                selected = list(reader.iter_selected_matches("xmatch", rule_token("TOWER")))
                self.assertEqual([row[2] for row in selected], ["match-a"])
                self.assertEqual(list(reader.iter_selected_matches("bankara_open")), [])

    def test_latest_schema_reset_restarts_from_baseline_and_keeps_empty_new_table(self):
        root_a, plan_a = self._make_delta("ddl-normal-a", DELTA_A_ID, self.reset_plan)
        self.writer.execute("UPDATE records SET payload='before-reset' WHERE id=5")
        self.writer.commit()
        root_b, plan_b = self._make_delta("ddl-normal-b", DELTA_B_ID, plan_a)
        self.writer.execute("ALTER TABLE records ADD COLUMN added_after_reset TEXT")
        self.writer.execute("UPDATE records SET added_after_reset='new-schema' WHERE id=-42")
        self.writer.execute("DROP TABLE to_drop")
        self.writer.execute("CREATE TABLE new_empty_after_reset (new_value BLOB)")
        self.writer.commit()
        reset_b_root = self.root / "schema-reset"
        reset_b = _make_generation(
            self.current_db, self.baseline_db, reset_b_root, RESET_B_ID,
            BASELINE_ID, self.baseline_identity["sha256"], kind="baseline_reconciliation",
            previous=plan_b,
        )
        with LosslessShardReader(self.baseline_package, expected_generation=BASELINE_ID) as baseline:
            with DeltaChainReader(baseline, [self.reset_root, root_a, root_b, reset_b_root]) as reader:
                self.assertEqual(reader.generation_id, RESET_B_ID)
                self.assertNotIn("to_drop", {row["name"] for row in reader.tables()})
                self.assertEqual(reader.row_count("new_empty_after_reset"), 0)
                rows = list(reader.iter_rows("records"))
                by_id = {rowid: values for _, rowid, values in rows}
                self.assertEqual(by_id[5][1], "before-reset")
                columns = [item["name"] for item in reader.columns("records") if item["hidden"] != 1]
                self.assertEqual(columns[-1], "added_after_reset")
                self.assertEqual(by_id[-42][-1], "new-schema")

    def test_sharded_schema_reset_matches_full_source_before_remote_replay(self):
        self.writer.execute("ALTER TABLE records ADD COLUMN added_after_reset TEXT")
        self.writer.execute("UPDATE records SET added_after_reset='new-schema' WHERE id=-42")
        self.writer.execute("UPDATE records SET blob_value=? WHERE id=5", (MULTI_FILE_BLOB,))
        self.writer.execute("DROP TABLE to_drop")
        self.writer.execute("CREATE TABLE new_empty_after_reset (new_value BLOB)")
        self.writer.commit()
        source_before = {
            "current": _sha256(self.current_db),
            "baseline": _sha256(self.baseline_db),
            "baseline_package": _tree_snapshot(self.baseline_package),
        }

        source_reference = self.root / "sharded-ddl-source-reference"
        _make_baseline_package(self.current_db, source_reference, SHARDED_DDL_ID)
        source_reference_before = _tree_snapshot(source_reference)

        generation_root = self.root / "sharded-ddl-generation"
        plan = _make_generation(
            self.current_db, self.baseline_db, generation_root, SHARDED_DDL_ID,
            BASELINE_ID, self.baseline_identity["sha256"], kind="baseline_reconciliation",
            transport_kind="lossless_sqlite_shards", shard_max_bytes=256 * 1024,
        )
        self.assertEqual(plan["transport"]["kind"], "lossless_sqlite_shards")
        manifest = _read_json(generation_root / "transport/manifest.json")
        records_table = next(item for item in manifest["tables"] if item["name"] == "records")
        ext_table = records_table["archive_metadata_tables"]["external_cells"]
        multi_refs = []
        large_digest = hashlib.sha256(MULTI_FILE_BLOB).hexdigest()
        for part in records_table["parts"]:
            connection = _readonly(generation_root / "transport" / part["file"], immutable=True)
            try:
                for byte_length, digest, refs_json in connection.execute(
                    f'SELECT byte_length,sha256,value_files_json FROM "{ext_table}"'
                ):
                    if byte_length == len(MULTI_FILE_BLOB) and digest == large_digest:
                        multi_refs = json.loads(refs_json)
            finally:
                connection.close()
        self.assertEqual(len(multi_refs), 2)

        publisher_index = self._write_publisher_index(generation_root, plan)
        remote_root = self.root / "sharded-ddl-remote"
        remote_root.mkdir()
        for item in publisher_index["files"]:
            source_path = generation_root / item["local"]
            target = remote_root / item["local"]
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(source_path, target)
            self.assertEqual(_sha256(target), {"bytes": item["bytes"], "sha256": item["sha256"]})
        shutil.copyfile(generation_root / "index.json", remote_root / "index.json")
        self.assertFalse((remote_root / "changes.sqlite3").exists())
        remote_before = _tree_snapshot(remote_root)
        with LosslessShardReader(self.baseline_package, expected_generation=BASELINE_ID) as baseline:
            with LosslessShardReader(source_reference, expected_generation=SHARDED_DDL_ID) as expected:
                with DeltaChainReader(baseline, [remote_root]) as reader:
                    self.assertEqual(reader.generation_id, SHARDED_DDL_ID)
                    self.assertEqual(reader.schema_objects(), expected.schema_objects())
                    self.assertEqual(reader.source_schema_sha256, expected.source_schema_sha256)
                    self.assertNotIn("to_drop", {item["name"] for item in reader.tables()})
                    for table in expected.tables():
                        name = table["name"]
                        self.assertEqual(reader.row_count(name), table["row_count"])
                        self.assertEqual(reader.columns(name), expected.columns(name))
                        self.assertEqual(reader.foreign_keys(name), expected.foreign_keys(name))
                        self.assertEqual(
                            list(reader.iter_rows(name)),
                            list(expected.iter_rows_with_identity(name)),
                        )
                    self.assertEqual(reader.row_count("new_empty_after_reset"), 0)
        self.assertEqual(_tree_snapshot(remote_root), remote_before)
        self.assertEqual(_tree_snapshot(source_reference), source_reference_before)
        self.assertEqual(
            {
                "current": _sha256(self.current_db),
                "baseline": _sha256(self.baseline_db),
                "baseline_package": _tree_snapshot(self.baseline_package),
            },
            source_before,
        )

    def test_transport_shard_representation_needs_no_unified_database_file(self):
        shards_root = self.root / "sharded-delta"
        plan = _make_generation(
            self.current_db, self.baseline_db, shards_root, "20261003T001006Z-00000007",
            BASELINE_ID, self.baseline_identity["sha256"], kind="baseline_reconciliation",
            transport_kind="lossless_sqlite_shards",
        )
        index = self._write_publisher_index(shards_root, plan)
        remote_root = self.root / "remote-sharded-delta"
        remote_root.mkdir()
        for item in index["files"]:
            source = shards_root / item["local"]
            target = remote_root / item["local"]
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(source, target)
            self.assertEqual(_sha256(target), {"bytes": item["bytes"], "sha256": item["sha256"]})
        shutil.copyfile(shards_root / "index.json", remote_root / "index.json")
        self.assertFalse((remote_root / "changes.sqlite3").exists())
        before = _tree_snapshot(remote_root)
        with LosslessShardReader(self.baseline_package, expected_generation=BASELINE_ID) as baseline:
            with DeltaChainReader(baseline, [remote_root]) as reader:
                rows = {rowid: values for _, rowid, values in reader.iter_rows("records")}
                self.assertEqual(rows[5][3], UPDATED_BLOB)
                self.assertEqual(rows[-42][1], b"inserted\x00bytes")
        self.assertEqual(_tree_snapshot(remote_root), before)

    def test_remote_only_multi_file_value_uses_unique_external_cell_receipt(self):
        self.writer.execute("UPDATE records SET blob_value=? WHERE id=5", (MULTI_FILE_BLOB,))
        self.writer.commit()
        generation_root = self.root / "multi-value-generation"
        plan = _make_generation(
            self.current_db, self.baseline_db, generation_root, MULTI_VALUE_ID,
            BASELINE_ID, self.baseline_identity["sha256"], kind="baseline_reconciliation",
            transport_kind="lossless_sqlite_shards", shard_max_bytes=256 * 1024,
        )
        index = self._write_publisher_index(generation_root, plan)
        manifest = _read_json(generation_root / "transport/manifest.json")
        proof = _read_json(generation_root / "transport/verification.json")
        large_digest = hashlib.sha256(MULTI_FILE_BLOB).hexdigest()
        records_table = next(item for item in manifest["tables"] if item["name"] == "records")
        external_table = records_table["archive_metadata_tables"]["external_cells"]
        multi_file_refs = None
        actual_external_cells = 0
        for table in manifest["tables"]:
            ext_table = table["archive_metadata_tables"]["external_cells"]
            for part in table["parts"]:
                connection = _readonly(generation_root / "transport" / part["file"], immutable=True)
                try:
                    actual_external_cells += connection.execute(
                        f'SELECT COUNT(*) FROM "{ext_table}"'
                    ).fetchone()[0]
                    if table["name"] == "records":
                        for byte_length, digest, refs_json in connection.execute(
                            f'SELECT byte_length,sha256,value_files_json FROM "{external_table}"'
                        ):
                            if byte_length == len(MULTI_FILE_BLOB) and digest == large_digest:
                                multi_file_refs = json.loads(refs_json)
                finally:
                    connection.close()
        self.assertIsNotNone(multi_file_refs)
        self.assertEqual(len(multi_file_refs), 2)
        self.assertEqual(proof["external_cell_count"], actual_external_cells)
        self.assertEqual(proof["external_cell_count"], 2)
        incidence_count = sum(
            item["cell_count"]
            for item in manifest["external_values"]
            if item["file"] in multi_file_refs
        )
        self.assertEqual(incidence_count, 2)
        self.assertEqual(sum(item["cell_count"] for item in manifest["external_values"]), 3)

        def copy_remote_tree(source: Path, destination: Path, publisher_index: dict) -> None:
            destination.mkdir()
            for item in publisher_index["files"]:
                source_path = source / item["local"]
                target = destination / item["local"]
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(source_path, target)
                self.assertEqual(_sha256(target), {"bytes": item["bytes"], "sha256": item["sha256"]})
            shutil.copyfile(source / "index.json", destination / "index.json")

        remote_root = self.root / "multi-value-remote"
        copy_remote_tree(generation_root, remote_root, index)
        self.assertFalse((remote_root / "changes.sqlite3").exists())
        source_before = {
            "baseline": _sha256(self.baseline_db),
            "current": _sha256(self.current_db),
            "baseline_package": _tree_snapshot(self.baseline_package),
        }
        before = _tree_snapshot(remote_root)

        def source_rows(table: str) -> dict[int, tuple[tuple[type, object], ...]]:
            connection = _readonly(self.current_db, immutable=True)
            try:
                return {
                    rowid: tuple((type(value), value) for value in values)
                    for rowid, *values in connection.execute(
                        f'SELECT rowid,* FROM "{table}" ORDER BY rowid'
                    )
                }
            finally:
                connection.close()

        expected_records = source_rows("records")
        expected_feed = source_rows("archive_change_feed")

        def assert_rows_match(table: str, rows: list[tuple[int, int | None, tuple]]) -> None:
            self.assertEqual([ordinal for ordinal, _rowid, _values in rows], list(range(len(rows))))
            actual = {
                rowid: tuple((type(value), value) for value in values)
                for _ordinal, rowid, values in rows
            }
            self.assertEqual(actual, source_rows(table))

        scratch_connections = []
        original_connect = sqlite3.connect

        def track_scratch_connection(database, *args, **kwargs):
            connection = original_connect(database, *args, **kwargs)
            if str(database).startswith(str(reader._temp_root / "transport-cache-")):
                scratch_connections.append(connection)
            return connection

        original_shard_iterator = delta_reader_module._lossless.iter_table_rows_with_identity

        def fail_delta_transport_only(package_root, manifest, table):
            if Path(package_root) == remote_root / "transport":
                raise RuntimeError("injected shard read failure")
            return original_shard_iterator(package_root, manifest, table)

        with LosslessShardReader(self.baseline_package, expected_generation=BASELINE_ID) as baseline:
            with DeltaChainReader(baseline, [remote_root]) as reader:
                temp_root = reader._temp_root
                with patch("sqlite3.connect", side_effect=track_scratch_connection):
                    with patch.object(
                        delta_reader_module._lossless,
                        "iter_table_rows_with_identity",
                        side_effect=fail_delta_transport_only,
                    ):
                        with self.assertRaises(DeltaReaderError):
                            reader.row_count("records")
                    failed_connections = scratch_connections[:]
                    self.assertTrue(failed_connections)
                    for connection in failed_connections:
                        with self.assertRaises(sqlite3.ProgrammingError):
                            connection.execute("SELECT 1")
                    self.assertEqual(list(temp_root.glob("transport-cache-*")), [])

                    # Exercise a later successful scratch connection too.
                    scratch_connections.clear()
                    first = reader.row_count("records")
                    self.assertEqual(first, len(expected_records))
                    successful_connections = scratch_connections[:]
                    self.assertTrue(successful_connections)
                    for connection in successful_connections:
                        with self.assertRaises(sqlite3.ProgrammingError):
                            connection.execute("SELECT 1")
                    self.assertEqual(list(temp_root.glob("transport-cache-*")), [])

                # A partial public SELECT must survive materializing another
                # table through the sharded transport.
                first_rows = []
                with DeltaChainReader(baseline, [remote_root]) as reader:
                    records = reader.iter_rows("records")
                    first_rows.append(next(records))
                    self.assertEqual(
                        reader.row_count("archive_change_feed"),
                        plan["source_row_counts"]["archive_change_feed"],
                    )
                    first_rows.extend(records)
                    assert_rows_match("records", first_rows)
                    self.assertEqual(
                        next(values for _ordinal, rowid, values in first_rows if rowid == 5)[3],
                        MULTI_FILE_BLOB,
                    )
                    self.assertEqual(
                        reader.row_count("records"),
                        plan["source_row_counts"]["records"],
                    )

                # Repeat with an actual iterator for table B while A remains
                # suspended, then finish A and compare every native value and
                # original rowid against the source database.
                with DeltaChainReader(baseline, [remote_root]) as reader:
                    records = reader.iter_rows("records")
                    first = next(records)
                    feed_rows = list(reader.iter_rows("archive_change_feed"))
                    remaining = list(records)
                    assert_rows_match("records", [first, *remaining])
                    assert_rows_match("archive_change_feed", feed_rows)
                    self.assertEqual(
                        {rowid: values for _ord, rowid, values in feed_rows},
                        {rowid: tuple(value for _type, value in typed) for rowid, typed in expected_feed.items()},
                    )

                # Selector temporary tables are iterator-local: two partial
                # selectors can coexist, close independently, and be rerun.
                with DeltaChainReader(baseline, [remote_root]) as reader:
                    by_rule = reader.iter_selected_matches("xmatch", rule_token("TOWER"))
                    first_rule = next(by_rule)
                    unclassified = reader.iter_selected_matches("unclassified")
                    first_unclassified = next(unclassified)
                    self.assertIn(first_rule[2], {"match-a", "match-b"})
                    self.assertEqual(first_unclassified[2], "match-c")
                    by_rule.close()
                    unclassified.close()

                    full_rule = list(reader.iter_selected_matches("xmatch", rule_token("TOWER")))
                    full_unclassified = list(reader.iter_selected_matches("unclassified"))
                    self.assertEqual({row[2] for row in full_rule}, {"match-a", "match-b"})
                    self.assertEqual({row[2] for row in full_unclassified}, {"match-c"})
                    expected_matches = source_rows("matches")
                    expected_by_key = {
                        typed[2][1]: typed for typed in expected_matches.values()
                    }
                    for rows in (full_rule, full_unclassified):
                        for row in rows:
                            expected = expected_by_key[row[2]]
                            self.assertEqual(
                                tuple((type(value), value) for value in row), expected,
                            )
                    selector_names = [
                        row[0] for row in reader._require_open().execute(
                            "SELECT name FROM sqlite_master WHERE type='table' "
                            "AND name GLOB '_selected_match_keys_*' ORDER BY name"
                        )
                    ]
                    self.assertEqual(len(selector_names), 4)
                    self.assertEqual(len(set(selector_names)), 4)
                    self.assertEqual([
                        reader._require_open().execute(
                            f'SELECT COUNT(*) FROM "{name}"'
                        ).fetchone()[0]
                        for name in selector_names
                    ], [0, 0, 0, 0])
        self.assertEqual(_tree_snapshot(remote_root), before)

        tampered_generation = self.root / "multi-value-tampered-generation"
        shutil.copytree(generation_root, tampered_generation)
        tampered_proof = _read_json(tampered_generation / "transport/verification.json")
        tampered_proof["external_cell_count"] += 1
        _write_json(tampered_generation / "transport/verification.json", tampered_proof)
        tampered_plan = _read_json(tampered_generation / "delta-plan.json")
        tampered_plan["transport"]["verification"] = tampered_proof
        proof_entry = next(item for item in tampered_plan["files"]
                           if item["local"] == "transport/verification.json")
        proof_entry.update(_sha256(tampered_generation / "transport/verification.json"))
        _write_json(tampered_generation / "delta-plan.json", tampered_plan)
        tampered_index = self._write_publisher_index(tampered_generation, tampered_plan)
        tampered_remote = self.root / "multi-value-tampered-remote"
        copy_remote_tree(tampered_generation, tampered_remote, tampered_index)
        self.assertFalse((tampered_remote / "changes.sqlite3").exists())
        before = _tree_snapshot(tampered_remote)
        with LosslessShardReader(self.baseline_package, expected_generation=BASELINE_ID) as baseline:
            with self.assertRaises(DeltaReaderError):
                with DeltaChainReader(baseline, [tampered_remote]):
                    pass
        self.assertEqual(_tree_snapshot(tampered_remote), before)
        self.assertEqual(
            {
                "baseline": _sha256(self.baseline_db),
                "current": _sha256(self.current_db),
                "baseline_package": _tree_snapshot(self.baseline_package),
            },
            source_before,
        )

    def test_publisher_index_is_strict_and_never_mutates_generation_input(self):
        indexed = self.root / "indexed-reset"
        shutil.copytree(self.reset_root, indexed)
        plan = _read_json(indexed / "delta-plan.json")
        self._write_publisher_index(indexed, plan)

        cases = (
            "wrong_plan_sha",
            "missing_plan_entry",
            "unlisted_index_file",
            "readback_false",
            "file_count_mismatch",
            "duplicate_json_key",
            "symlink_index",
            "unlisted_physical_file",
        )
        for case in cases:
            with self.subTest(case=case):
                bad_root = self.root / f"indexed-reset-{case}"
                shutil.copytree(indexed, bad_root)
                index_path = bad_root / "index.json"
                index = _read_json(index_path)
                if case == "wrong_plan_sha":
                    index["plan_sha256"] = "0" * 64
                    _write_json(index_path, index)
                elif case == "missing_plan_entry":
                    index["files"].pop()
                    _write_json(index_path, index)
                elif case == "unlisted_index_file":
                    index["files"].append({
                        "local": "rogue.sqlite3",
                        "remote": f"deltas/generations/{plan['generation_id']}/rogue.sqlite3",
                        "bytes": 1,
                        "sha256": "0" * 64,
                    })
                    _write_json(index_path, index)
                elif case == "readback_false":
                    index["verification"]["full_readback"] = False
                    _write_json(index_path, index)
                elif case == "file_count_mismatch":
                    index["verification"]["file_count"] += 1
                    _write_json(index_path, index)
                elif case == "duplicate_json_key":
                    index_path.write_text('{"version":1,"version":1}\n', encoding="utf-8")
                elif case == "symlink_index":
                    index_path.unlink()
                    index_path.symlink_to(indexed / "index.json")
                elif case == "unlisted_physical_file":
                    (bad_root / "rogue.bin").write_bytes(b"not declared")

                before = _tree_snapshot(bad_root)
                with LosslessShardReader(self.baseline_package, expected_generation=BASELINE_ID) as baseline:
                    with self.assertRaises(DeltaReaderError):
                        with DeltaChainReader(baseline, [bad_root]):
                            pass
                self.assertEqual(_tree_snapshot(bad_root), before)

    def test_published_delta_requirement_and_default_prepared_compatibility(self):
        with self.assertRaises(TypeError):
            DeltaChainReader(None, require_published_deltas=1)

        with LosslessShardReader(self.baseline_package, expected_generation=BASELINE_ID) as baseline:
            with DeltaChainReader(baseline, []) as reader:
                self.assertFalse(reader.published_deltas_verified)
                self.assertEqual(reader.generation_id, BASELINE_ID)

            with DeltaChainReader(baseline, [self.reset_root]) as reader:
                self.assertFalse(reader.published_deltas_verified)
            with self.assertRaises(RuntimeError):
                _ = reader.published_deltas_verified

            with self.assertRaises(DeltaReaderError) as missing:
                with DeltaChainReader(
                    baseline, [self.reset_root], require_published_deltas=True,
                ):
                    pass
            self.assertEqual(missing.exception.code, "DELTA_PUBLISHED_INDEX_REQUIRED")

    def test_published_delta_indexes_are_required_and_bound_to_raw_plan(self):
        indexed = self.root / "published-reset"
        shutil.copytree(self.reset_root, indexed)
        plan = _read_json(indexed / "delta-plan.json")
        self._write_publisher_index(indexed, plan)

        with LosslessShardReader(self.baseline_package, expected_generation=BASELINE_ID) as baseline:
            with DeltaChainReader(baseline, [indexed]) as reader:
                self.assertTrue(reader.published_deltas_verified)
            with DeltaChainReader(
                baseline, [indexed], require_published_deltas=True,
            ) as reader:
                self.assertTrue(reader.published_deltas_verified)

        malformed = self.root / "published-reset-malformed-index"
        shutil.copytree(indexed, malformed)
        bad_index = _read_json(malformed / "index.json")
        bad_index["verification"]["full_readback"] = False
        _write_json(malformed / "index.json", bad_index)
        with LosslessShardReader(self.baseline_package, expected_generation=BASELINE_ID) as baseline:
            with self.assertRaises(DeltaReaderError) as invalid:
                with DeltaChainReader(
                    baseline, [malformed], require_published_deltas=True,
                ):
                    pass
            self.assertEqual(invalid.exception.code, "DELTA_INDEX_PROOF_INCOMPLETE")

        corrupt_plan = self.root / "published-reset-corrupt-raw-plan"
        shutil.copytree(indexed, corrupt_plan)
        plan_path = corrupt_plan / "delta-plan.json"
        plan_path.write_bytes(plan_path.read_bytes() + b"\n")
        with LosslessShardReader(self.baseline_package, expected_generation=BASELINE_ID) as baseline:
            with self.assertRaises(DeltaReaderError) as invalid:
                with DeltaChainReader(
                    baseline, [corrupt_plan], require_published_deltas=True,
                ):
                    pass
            self.assertEqual(invalid.exception.code, "DELTA_INDEX_PLAN_MISMATCH")

    def test_published_requirement_covers_only_effective_chain_after_latest_reset(self):
        indexed_reset = self.root / "published-chain-reset"
        shutil.copytree(self.reset_root, indexed_reset)
        self._write_publisher_index(indexed_reset, _read_json(indexed_reset / "delta-plan.json"))
        root_a, plan_a = self._make_delta("prepared-chain-a", DELTA_A_ID, self.reset_plan)

        with LosslessShardReader(self.baseline_package, expected_generation=BASELINE_ID) as baseline:
            with self.assertRaises(DeltaReaderError) as missing_one:
                with DeltaChainReader(
                    baseline, [indexed_reset, root_a], require_published_deltas=True,
                ):
                    pass
            self.assertEqual(missing_one.exception.code, "DELTA_PUBLISHED_INDEX_REQUIRED")

        indexed_root_a = self.root / "published-chain-a"
        shutil.copytree(root_a, indexed_root_a)
        self._write_publisher_index(indexed_root_a, _read_json(indexed_root_a / "delta-plan.json"))
        with LosslessShardReader(self.baseline_package, expected_generation=BASELINE_ID) as baseline:
            with DeltaChainReader(
                baseline, [indexed_reset, indexed_root_a], require_published_deltas=True,
            ) as reader:
                self.assertEqual(reader.generation_id, DELTA_A_ID)
                self.assertTrue(reader.published_deltas_verified)

        latest_reset_root = self.root / "published-latest-reset"
        latest_reset = _make_generation(
            self.current_db,
            self.baseline_db,
            latest_reset_root,
            RESET_B_ID,
            BASELINE_ID,
            self.baseline_identity["sha256"],
            kind="baseline_reconciliation",
            previous=plan_a,
        )
        self._write_publisher_index(latest_reset_root, latest_reset)
        with LosslessShardReader(self.baseline_package, expected_generation=BASELINE_ID) as baseline:
            with DeltaChainReader(
                baseline,
                [self.reset_root, root_a, latest_reset_root],
                require_published_deltas=True,
            ) as reader:
                self.assertEqual(reader.generation_id, RESET_B_ID)
                self.assertTrue(reader.published_deltas_verified)

    def test_published_delta_status_detects_post_open_input_changes(self):
        for case in ("index_changed", "index_deleted", "index_replaced", "plan_changed", "artifact_changed"):
            with self.subTest(case=case):
                indexed = self.root / f"published-post-open-{case}"
                shutil.copytree(self.reset_root, indexed)
                self._write_publisher_index(indexed, _read_json(indexed / "delta-plan.json"))
                with LosslessShardReader(self.baseline_package, expected_generation=BASELINE_ID) as baseline:
                    with self.assertRaises(DeltaReaderError) as exited_changed:
                        with DeltaChainReader(
                            baseline, [indexed], require_published_deltas=True,
                        ) as reader:
                            self.assertTrue(reader.published_deltas_verified)
                            index_path = indexed / "index.json"
                            if case == "index_changed":
                                index_path.write_bytes(index_path.read_bytes() + b" ")
                            elif case == "index_deleted":
                                index_path.unlink()
                            elif case == "index_replaced":
                                replacement = indexed / "index.replacement"
                                replacement.write_bytes(index_path.read_bytes())
                                replacement.replace(index_path)
                            elif case == "plan_changed":
                                plan_path = indexed / "delta-plan.json"
                                plan_path.write_bytes(plan_path.read_bytes() + b"\n")
                            else:
                                artifact = indexed / "transport-document.json"
                                artifact.write_bytes(artifact.read_bytes() + b" ")
                            with self.assertRaises(DeltaReaderError) as changed:
                                _ = reader.published_deltas_verified
                            self.assertEqual(changed.exception.code, "DELTA_PUBLISHED_INPUT_CHANGED")
                    self.assertEqual(exited_changed.exception.code, "DELTA_PUBLISHED_INPUT_CHANGED")

    def test_published_inputs_guard_metadata_and_iterator_entry_points(self):
        cases = (
            ("index_delete", "tables"),
            ("index_same_bytes_inode_replace", "row_count"),
            ("plan_changed", "generation_id"),
            ("artifact_changed", "iter_rows"),
        )
        for case, api in cases:
            with self.subTest(case=case, api=api):
                indexed = self.root / f"published-entry-guard-{case}"
                shutil.copytree(self.reset_root, indexed)
                self._write_publisher_index(indexed, _read_json(indexed / "delta-plan.json"))
                with LosslessShardReader(self.baseline_package, expected_generation=BASELINE_ID) as baseline:
                    reader = DeltaChainReader(
                        baseline, [indexed], require_published_deltas=True,
                    )
                    reader.__enter__()
                    self.assertTrue(reader.published_deltas_verified)
                    index_path = indexed / "index.json"
                    if case in {"index_delete", "index_same_bytes_inode_replace"}:
                        old_bytes = index_path.read_bytes()
                        if case == "index_delete":
                            index_path.unlink()
                        else:
                            replacement = indexed / "index.replacement"
                            replacement.write_bytes(old_bytes)
                            replacement.replace(index_path)
                    elif case == "plan_changed":
                        path = indexed / "delta-plan.json"
                        path.write_bytes(path.read_bytes() + b"\n")
                    else:
                        path = indexed / "transport-document.json"
                        path.write_bytes(path.read_bytes() + b" ")

                    with self.assertRaises(DeltaReaderError) as rejected:
                        if api == "tables":
                            reader.tables()
                        elif api == "row_count":
                            reader.row_count("records")
                        elif api == "generation_id":
                            _ = reader.generation_id
                        else:
                            reader.iter_rows("records")
                    self.assertEqual(rejected.exception.code, "DELTA_PUBLISHED_INPUT_CHANGED")
                    with self.assertRaises(DeltaReaderError) as exit_rejected:
                        reader.__exit__(None, None, None)
                    self.assertEqual(exit_rejected.exception.code, "DELTA_PUBLISHED_INPUT_CHANGED")
                    self.assertIsNone(reader._connection)
                    self.assertEqual(reader._iterators, set())

    def test_keyboard_interrupt_during_enter_cleans_partial_resources(self):
        indexed = self.root / "published-enter-interrupt"
        shutil.copytree(self.reset_root, indexed)
        self._write_publisher_index(indexed, _read_json(indexed / "delta-plan.json"))
        baseline_before = _tree_snapshot(self.baseline_package)
        generation_before = _tree_snapshot(indexed)

        with LosslessShardReader(self.baseline_package, expected_generation=BASELINE_ID) as baseline:
            baseline_tables_before = baseline.tables()
            reader = DeltaChainReader(baseline, [indexed], require_published_deltas=True)
            opened = {}

            def interrupt_after_resources_open():
                opened["active"] = reader._active
                opened["connection"] = reader._connection
                opened["temp"] = reader._temp
                opened["temp_root"] = reader._temp_root
                opened["temp_root_existed"] = reader._temp_root.exists()
                raise KeyboardInterrupt("controlled delta enter interrupt")

            with patch.object(
                reader, "_assert_published_inputs_unchanged",
                side_effect=interrupt_after_resources_open,
            ):
                with self.assertRaisesRegex(
                    KeyboardInterrupt, "controlled delta enter interrupt"
                ):
                    reader.__enter__()

            self.assertTrue(opened["active"])
            self.assertIsNotNone(opened["connection"])
            self.assertIsNotNone(opened["temp"])
            self.assertTrue(opened["temp_root_existed"])
            self.assertFalse(reader._active)
            self.assertIsNone(reader._connection)
            self.assertIsNone(reader._temp)
            self.assertIsNone(reader._temp_root)
            self.assertEqual(reader._iterators, set())
            self.assertEqual(reader._cache_names, {})
            with self.assertRaises(sqlite3.ProgrammingError):
                opened["connection"].execute("SELECT 1")
            self.assertFalse(opened["temp_root"].exists())

            # The failed overlay enter does not close or mutate its caller-owned baseline.
            self.assertTrue(baseline._active)
            self.assertEqual(baseline.tables(), baseline_tables_before)
            self.assertEqual(_tree_snapshot(self.baseline_package), baseline_before)
            self.assertEqual(_tree_snapshot(indexed), generation_before)

    def test_published_partial_iterator_close_detects_change_and_closes_connections(self):
        indexed = self.root / "published-partial-close-guard"
        shutil.copytree(self.reset_root, indexed)
        self._write_publisher_index(indexed, _read_json(indexed / "delta-plan.json"))
        original_connect = sqlite3.connect
        opened_connections = []

        def track_connect(*args, **kwargs):
            connection = original_connect(*args, **kwargs)
            opened_connections.append(connection)
            return connection

        with LosslessShardReader(self.baseline_package, expected_generation=BASELINE_ID) as baseline:
            reader = DeltaChainReader(baseline, [indexed], require_published_deltas=True)
            reader.__enter__()
            overlay_connection = reader._connection
            with patch("sqlite3.connect", side_effect=track_connect):
                rows = reader.iter_rows("records")
                next(rows)
                index_path = indexed / "index.json"
                index_path.write_bytes(index_path.read_bytes() + b" ")
                with self.assertRaises(DeltaReaderError) as rejected:
                    rows.close()
            self.assertEqual(rejected.exception.code, "DELTA_PUBLISHED_INPUT_CHANGED")
            self.assertTrue(opened_connections)
            self.assertEqual(reader._iterators, set())
            for connection in opened_connections:
                with self.assertRaises(sqlite3.ProgrammingError):
                    connection.execute("SELECT 1")
            self.assertIsNotNone(overlay_connection)
            with self.assertRaises(DeltaReaderError) as exit_rejected:
                reader.__exit__(None, None, None)
            self.assertEqual(exit_rejected.exception.code, "DELTA_PUBLISHED_INPUT_CHANGED")
            self.assertIsNone(reader._connection)
            with self.assertRaises(sqlite3.ProgrammingError):
                overlay_connection.execute("SELECT 1")

    def test_published_chain_rechecks_its_baseline_package(self):
        indexed = self.root / "published-baseline-guard"
        shutil.copytree(self.reset_root, indexed)
        self._write_publisher_index(indexed, _read_json(indexed / "delta-plan.json"))
        baseline = LosslessShardReader(self.baseline_package, expected_generation=BASELINE_ID)
        baseline.__enter__()
        reader = DeltaChainReader(baseline, [indexed], require_published_deltas=True)
        reader.__enter__()
        self.assertTrue(reader.published_deltas_verified)
        manifest = self.baseline_package / "manifest.json"
        manifest.write_bytes(manifest.read_bytes() + b" ")
        with self.assertRaises(DeltaReaderError) as rejected:
            reader.tables()
        self.assertEqual(rejected.exception.code, "DELTA_PUBLISHED_INPUT_CHANGED")
        with self.assertRaises(DeltaReaderError) as reader_exit:
            reader.__exit__(None, None, None)
        self.assertEqual(reader_exit.exception.code, "DELTA_PUBLISHED_INPUT_CHANGED")
        self.assertIsNone(reader._connection)
        with self.assertRaises(ValueError):
            baseline.__exit__(None, None, None)

    def test_iterator_resources_close_on_exception_and_unstarted_reader_exit(self):
        indexed = self.root / "published-iterator-cleanup"
        shutil.copytree(self.reset_root, indexed)
        self._write_publisher_index(indexed, _read_json(indexed / "delta-plan.json"))
        with LosslessShardReader(self.baseline_package, expected_generation=BASELINE_ID) as baseline:
            reader = DeltaChainReader(baseline, [indexed], require_published_deltas=True)
            with self.assertRaisesRegex(RuntimeError, "consumer stopped"):
                with reader:
                    rows = reader.iter_rows("records")
                    next(rows)
                    selector = reader.iter_selected_matches("xmatch")
                    raise RuntimeError("consumer stopped")
            self.assertIsNone(reader._connection)
            self.assertEqual(reader._iterators, set())
            with self.assertRaises(StopIteration):
                next(rows)
            with self.assertRaises(StopIteration):
                next(selector)

            unstarted = DeltaChainReader(baseline, [indexed], require_published_deltas=True)
            unstarted.__enter__()
            pending_rows = unstarted.iter_rows("records")
            pending_selector = unstarted.iter_selected_matches("xmatch")
            connection = unstarted._connection
            unstarted.__exit__(None, None, None)
            self.assertIsNone(unstarted._connection)
            self.assertEqual(unstarted._iterators, set())
            with self.assertRaises(sqlite3.ProgrammingError):
                connection.execute("SELECT 1")
            with self.assertRaises(StopIteration):
                next(pending_rows)
            with self.assertRaises(StopIteration):
                next(pending_selector)

    def test_exit_preserves_body_baseexception_and_keeps_success_guard_precedence(self):
        indexed = self.root / "published-exit-exception-priority"
        shutil.copytree(self.reset_root, indexed)
        self._write_publisher_index(indexed, _read_json(indexed / "delta-plan.json"))

        class SyntheticInterrupt(BaseException):
            pass

        class ClosingFailure:
            def __init__(self, error):
                self.error = error
                self.closed = False

            def close(self):
                self.closed = True
                raise self.error

        body_error = SyntheticInterrupt("original body interruption")
        close_error = RuntimeError("iterator close failure")
        guard_error = DeltaReaderError("DELTA_PUBLISHED_INPUT_CHANGED")
        cleanup_error = RuntimeError("cleanup follow-up failure")

        with LosslessShardReader(self.baseline_package, expected_generation=BASELINE_ID) as baseline:
            reader = DeltaChainReader(baseline, [indexed], require_published_deltas=True)
            body_iterator = ClosingFailure(close_error)
            body_cleanup_calls = []
            real_body_cleanup = reader._cleanup

            def body_cleanup_then_fail():
                body_cleanup_calls.append(True)
                real_body_cleanup()
                raise cleanup_error

            with (
                patch.object(reader, "_assert_public_read_state", side_effect=guard_error) as body_guard,
                patch.object(reader, "_cleanup", side_effect=body_cleanup_then_fail),
            ):
                with self.assertRaises(SyntheticInterrupt) as caught:
                    with reader:
                        reader._iterators.add(body_iterator)
                        raise body_error
            self.assertIs(caught.exception, body_error)
            self.assertTrue(body_iterator.closed)
            self.assertEqual(body_guard.call_count, 1)
            self.assertEqual(body_cleanup_calls, [True])
            self.assertIsNone(reader._connection)
            self.assertEqual(reader._iterators, set())

            successful_reader = DeltaChainReader(
                baseline, [indexed], require_published_deltas=True,
            )
            success_iterator = ClosingFailure(close_error)
            success_cleanup_calls = []
            real_success_cleanup = successful_reader._cleanup

            def success_cleanup_then_fail():
                success_cleanup_calls.append(True)
                real_success_cleanup()
                raise cleanup_error

            with (
                patch.object(
                    successful_reader, "_assert_public_read_state", side_effect=guard_error,
                ) as success_guard,
                patch.object(successful_reader, "_cleanup", side_effect=success_cleanup_then_fail),
            ):
                with self.assertRaises(DeltaReaderError) as guard_caught:
                    with successful_reader:
                        successful_reader._iterators.add(success_iterator)
                self.assertIs(guard_caught.exception, guard_error)
            self.assertTrue(success_iterator.closed)
            self.assertEqual(success_guard.call_count, 1)
            self.assertEqual(success_cleanup_calls, [True])
            self.assertIsNone(successful_reader._connection)
            self.assertEqual(successful_reader._iterators, set())

    def test_wrong_parent_or_generation_and_missing_chain_are_rejected(self):
        normal_root, normal_plan = self._make_delta("wrong-parent-base", DELTA_A_ID, self.reset_plan)
        self.assertTrue(normal_plan["metadata"]["all_writers_contract_enforced"])
        bad_root = self.root / "bad-parent"
        shutil.copytree(normal_root, bad_root)
        bad_plan = _read_json(bad_root / "delta-plan.json")
        bad_plan["parent_generation_id"] = "20261003T001009Z-00000009"
        _write_json(bad_root / "delta-plan.json", bad_plan)
        with LosslessShardReader(self.baseline_package, expected_generation=BASELINE_ID) as baseline:
            with self.assertRaises(DeltaReaderError):
                with DeltaChainReader(baseline, [bad_root]):
                    pass
        with LosslessShardReader(self.baseline_package, expected_generation=BASELINE_ID) as baseline:
            with self.assertRaises(DeltaReaderError) as missing_parent:
                with DeltaChainReader(baseline, [normal_root]):
                    pass
            self.assertIn(missing_parent.exception.code, {"DELTA_PARENT_CHAIN_INVALID", "DELTA_PLAN_METADATA_INVALID"})

        wrong_generation = self.root / "wrong-generation"
        shutil.copytree(normal_root, wrong_generation)
        plan = _read_json(wrong_generation / "delta-plan.json")
        plan["generation_id"] = "20261003T001007Z-00000008"
        _write_json(wrong_generation / "delta-plan.json", plan)
        with LosslessShardReader(self.baseline_package, expected_generation=BASELINE_ID) as baseline:
            with self.assertRaises(DeltaReaderError):
                with DeltaChainReader(baseline, [wrong_generation]):
                    pass

        unguarded_root = self.root / "unguarded-normal"
        shutil.copytree(normal_root, unguarded_root)
        unguarded_plan = _read_json(unguarded_root / "delta-plan.json")
        unguarded_plan["metadata"]["all_writers_contract_enforced"] = False
        unguarded_plan["metadata"]["writer_guard_status"]["all_writers_contract_enforced"] = False
        unguarded_plan["metadata"]["writer_guard_status"]["status"] = "missing"
        _write_json(unguarded_root / "delta-plan.json", unguarded_plan)
        with LosslessShardReader(self.baseline_package, expected_generation=BASELINE_ID) as baseline:
            with self.assertRaises(DeltaReaderError) as unguarded:
                with DeltaChainReader(baseline, [self.reset_root, unguarded_root]):
                    pass
            self.assertEqual(unguarded.exception.code, "DELTA_WRITER_GUARDS_UNVERIFIED")

    def test_generation_paths_reject_parent_traversal_and_symlink_roots(self):
        with LosslessShardReader(self.baseline_package, expected_generation=BASELINE_ID) as baseline:
            traversal = self.root / ".." / self.root.name
            with self.assertRaises(DeltaReaderError) as unsafe_path:
                with DeltaChainReader(baseline, [traversal]):
                    pass
            self.assertEqual(unsafe_path.exception.code, "DELTA_GENERATION_PATH_INVALID")

        link = self.root / "reset-link"
        link.symlink_to(self.reset_root, target_is_directory=True)
        with LosslessShardReader(self.baseline_package, expected_generation=BASELINE_ID) as baseline:
            with self.assertRaises(DeltaReaderError) as unsafe_link:
                with DeltaChainReader(baseline, [link]):
                    pass
            self.assertEqual(unsafe_link.exception.code, "DELTA_GENERATION_PATH_INVALID")

    def test_omitted_plan_file_and_tampered_file_are_rejected(self):
        bad_missing = self.root / "missing-file"
        shutil.copytree(self.reset_root, bad_missing)
        missing_plan = _read_json(bad_missing / "delta-plan.json")
        missing_plan["files"] = [item for item in missing_plan["files"] if item["local"] != "value-verification.json"]
        _write_json(bad_missing / "delta-plan.json", missing_plan)
        with LosslessShardReader(self.baseline_package, expected_generation=BASELINE_ID) as baseline:
            with self.assertRaises(DeltaReaderError):
                with DeltaChainReader(baseline, [bad_missing]):
                    pass

        bad_tamper = self.root / "tampered-file"
        shutil.copytree(self.reset_root, bad_tamper)
        target = bad_tamper / "transport-document.json"
        target.write_bytes(target.read_bytes() + b" ")
        with LosslessShardReader(self.baseline_package, expected_generation=BASELINE_ID) as baseline:
            with self.assertRaises(DeltaReaderError) as tampered:
                with DeltaChainReader(baseline, [bad_tamper]):
                    pass
            self.assertEqual(tampered.exception.code, "DELTA_FILE_HASH_MISMATCH")

    def test_unique_lookup_sharded_reset_and_two_deltas_skip_superseded_external_values(self):
        large_text = "t\x00𠮷" * 50_000
        blob_a = b"A" + bytes(range(256)) * 1500
        blob_b = b"B" + bytes(range(255, -1, -1)) * 1500
        root_reset = self.root / "lookup-sharded-reset"
        plan_reset = _make_generation(
            self.current_db, self.baseline_db, root_reset, RESET_ID,
            BASELINE_ID, self.baseline_identity["sha256"], kind="baseline_reconciliation",
            transport_kind="lossless_sqlite_shards", shard_max_bytes=256 * 1024,
        )
        self.writer.execute(
            "UPDATE matches SET payload=?, huge_integer=?, ratio=?, raw_body=? "
            "WHERE account=? AND kind=? AND match_key=?",
            (large_text, HUGE_INTEGER, -0.0, blob_a, "account-a", "vs", "match-a"),
        )
        self.writer.commit()
        root_a, plan_a = self._make_delta(
            "lookup-sharded-a", DELTA_A_ID, plan_reset,
            transport_kind="lossless_sqlite_shards", shard_max_bytes=256 * 1024,
        )
        self.writer.execute(
            "UPDATE matches SET payload=?, huge_integer=?, ratio=?, raw_body=? "
            "WHERE account=? AND kind=? AND match_key=?",
            (large_text, HUGE_INTEGER - 1, 0.0, blob_b, "account-a", "vs", "match-a"),
        )
        self.writer.commit()
        root_b, _plan_b = self._make_delta(
            "lookup-sharded-b", DELTA_B_ID, plan_a,
            transport_kind="lossless_sqlite_shards", shard_max_bytes=256 * 1024,
        )

        expected_row = self.writer.execute(
            "SELECT rowid, * FROM matches WHERE account=? AND kind=? AND match_key=?",
            ("account-a", "vs", "match-a"),
        ).fetchone()
        expected_rowid, *expected_values = expected_row
        matches_table_id = next(
            item["table_id"] for item in _read_json(root_b / "transport/manifest.json")["tables"]
            if item["name"] == "matches"
        )
        original_decode = delta_reader_module._lossless._read_external_cell
        decoded_roots: list[str] = []

        def count_external_reads(root, *args, **kwargs):
            resolved = Path(root).resolve()
            if len(args) > 1 and args[1] == matches_table_id and resolved == (root_a / "transport").resolve():
                decoded_roots.append("superseded")
            elif len(args) > 1 and args[1] == matches_table_id and resolved == (root_b / "transport").resolve():
                decoded_roots.append("current")
            return original_decode(root, *args, **kwargs)

        baseline_before = _tree_snapshot(self.baseline_package)
        source_before = _sha256(self.current_db)
        with patch.object(
            delta_reader_module._lossless, "_read_external_cell", side_effect=count_external_reads
        ):
            with LosslessShardReader(self.baseline_package, expected_generation=BASELINE_ID) as baseline:
                with DeltaChainReader(baseline, [root_reset, root_a, root_b]) as reader:
                    self.assertIsNone(
                        reader.get_unique_row("matches", {"match_key": "not-present"})
                    )
                    self.assertEqual(decoded_roots, [])
                    lookup_criteria = {
                        "account": "account-a",
                        "kind": "vs",
                        "match_key": "match-a",
                        "payload": large_text,
                        "raw_body": blob_b,
                    }
                    result = reader.get_unique_row("matches", lookup_criteria)
                    self.assertEqual(result, {"source_rowid": expected_rowid,
                                              "values": tuple(expected_values)})
                    self.assertIs(type(result["values"][4]), int)
                    self.assertEqual(result["values"][4], HUGE_INTEGER - 1)
                    self.assertIs(type(result["values"][5]), float)
                    self.assertEqual(result["values"][5].hex(), "0x0.0p+0")
                    self.assertEqual(result["values"][3], large_text)
                    self.assertEqual(result["values"][6], blob_b)
                    self.assertEqual(reader._materialized, set())
                    operation_cache = reader._lookup_operations_cache
                    self.assertTrue(reader._lookup_operations_cache_ready)
                    second = reader.get_unique_row(
                        "matches", {"account": "account-a", "match_key": "match-a"}
                    )
                    self.assertEqual(second, result)
                    self.assertEqual(reader._lookup_operations_cache, operation_cache)
                    self.assertEqual(reader._materialized, set())
        self.assertNotIn("superseded", decoded_roots)
        self.assertIn("current", decoded_roots)
        self.assertEqual(_tree_snapshot(self.baseline_package), baseline_before)
        self.assertEqual(_sha256(self.current_db), source_before)

    def test_unique_lookup_native_reset_and_two_deltas_use_projected_candidates(self):
        nonmatch_text = "unselected\x00𠮷" * 45_000
        nonmatch_blob = b"N" + bytes(range(256)) * 3_500
        selected_text = "selected\x00𠮷" * 55_000
        selected_blob = b"S" + bytes(range(255, -1, -1)) * 4_000
        self.writer.execute(
            "UPDATE matches SET payload=?,raw_body=? WHERE match_key=?",
            (nonmatch_text, nonmatch_blob, "match-b"),
        )
        self.writer.commit()
        root_reset = self.root / "lookup-native-reset"
        plan_reset = _make_generation(
            self.current_db, self.baseline_db, root_reset,
            "20261003T010010Z-00000011", BASELINE_ID,
            self.baseline_identity["sha256"], kind="baseline_reconciliation",
            transport_kind="native_sqlite",
        )
        self.writer.execute(
            "UPDATE matches SET payload=?,raw_body=? WHERE match_key=?",
            ("superseded-value", b"superseded\x00blob", "match-a"),
        )
        self.writer.commit()
        root_a, plan_a = self._make_delta(
            "lookup-native-a", "20261003T010011Z-00000012", plan_reset,
            transport_kind="native_sqlite",
        )
        self.writer.execute(
            "UPDATE matches SET payload=?,huge_integer=?,ratio=?,raw_body=? WHERE match_key=?",
            (selected_text, HUGE_INTEGER, float("-inf"), selected_blob, "match-a"),
        )
        self.writer.commit()
        root_b, _plan_b = self._make_delta(
            "lookup-native-b", "20261003T010012Z-00000013", plan_a,
            transport_kind="native_sqlite",
        )

        expected = self.writer.execute(
            "SELECT rowid,* FROM matches WHERE match_key=?", ("match-a",)
        ).fetchone()
        expected_rowid, *expected_values = expected
        transport_connection = sqlite3.connect(root_b / "changes.sqlite3")
        try:
            expected_transport_rowid = transport_connection.execute(
                'SELECT "_rowid_" FROM "matches" WHERE "match_key"=?', ("match-a",)
            ).fetchone()[0]
        finally:
            transport_connection.close()
        source_before = _sha256(self.current_db)
        baseline_before = _tree_snapshot(self.baseline_package)
        read_external = delta_reader_module._lossless._read_external_cell
        baseline_external_reads: list[str] = []

        def count_baseline_external_reads(root, table_id, *args, **kwargs):
            if Path(root).resolve() == self.baseline_package.resolve():
                baseline_external_reads.append(table_id)
            return read_external(root, table_id, *args, **kwargs)

        real_connect = native_candidates_module._connect_readonly
        query_log: list[tuple[Path, str, tuple[Any, ...]]] = []

        class TrackedConnection:
            def __init__(self, connection, path):
                self.connection = connection
                self.path = path

            def execute(self, sql, parameters=()):
                query_log.append((self.path, sql, tuple(parameters)))
                return self.connection.execute(sql, parameters)

            def close(self):
                return self.connection.close()

        def tracked_connect(path):
            return TrackedConnection(real_connect(path), Path(path).resolve())

        original_iter = native_candidates_module.NativeTransportCandidates.iter_table_candidates
        candidate_calls: list[tuple[str, str]] = []

        def tracked_iter(adapter, table, criteria_by_column, *, candidate_row_filter=None):
            candidate_calls.append((str(adapter._root_input), table))
            return original_iter(
                adapter, table, criteria_by_column,
                candidate_row_filter=candidate_row_filter,
            )

        with LosslessShardReader(self.baseline_package, expected_generation=BASELINE_ID) as baseline:
            with DeltaChainReader(baseline, [root_reset, root_a, root_b]) as reader:
                with (
                    patch.object(native_candidates_module, "_connect_readonly", side_effect=tracked_connect),
                    patch.object(
                        native_candidates_module.NativeTransportCandidates,
                        "iter_table_candidates", new=tracked_iter,
                    ),
                    patch.object(
                        delta_reader_module._lossless,
                        "_read_external_cell", side_effect=count_baseline_external_reads,
                    ),
                    patch.object(
                        verified_files_module, "_hash_descriptor",
                        wraps=verified_files_module._hash_descriptor,
                    ) as hash_file,
                ):
                    self.assertIsNone(
                        reader.get_unique_row("matches", {"match_key": "no-such-row"})
                    )
                    result = reader.get_unique_row("matches", {"match_key": "match-a"})
                    self.assertEqual(result, {
                        "source_rowid": expected_rowid,
                        "values": tuple(expected_values),
                    })
                    self.assertIs(type(result["values"][4]), int)
                    self.assertEqual(result["values"][4], HUGE_INTEGER)
                    self.assertIs(type(result["values"][5]), float)
                    self.assertEqual(result["values"][5].hex(), "-inf")
                    self.assertEqual(
                        reader.get_unique_row("matches", {"ratio": float("-inf")}), result
                    )
                    self.assertIsNone(reader.get_unique_row("matches", {"ratio": 0}))
                    self.assertIsNone(reader.get_unique_row(
                        "matches", {"huge_integer": float(HUGE_INTEGER)}
                    ))
                    self.assertEqual(
                        reader.get_unique_row("matches", {"huge_integer": HUGE_INTEGER}), result
                    )
                    with self.assertRaises(DeltaReaderError) as bool_criteria:
                        reader.get_unique_row("matches", {"huge_integer": True})
                    self.assertEqual(bool_criteria.exception.code, "DELTA_LOOKUP_CRITERIA_INVALID")
                    self.assertEqual(reader.get_unique_row(
                        "matches", {"match_key": "match-a"},
                    ), result)
                    self.assertEqual(reader._materialized, set())
                    self.assertEqual(baseline_external_reads, [])
                    self.assertEqual(hash_file.call_count, 0)

                    self.assertEqual(
                        [package["kind"] for package in reader._lookup_transport_packages],
                        ["native_sqlite", "native_sqlite", "native_sqlite"],
                    )
                    for package in reader._lookup_transport_packages:
                        operation_table = package["operations_table"]
                        self.assertEqual(
                            sum(root == str(package["root"]) and name == operation_table
                                for root, name in candidate_calls),
                            1,
                        )

                    match_scans = [
                        sql.lower() for _path, sql, _params in query_log
                        if 'left join "matches" as v' in sql.lower()
                    ]
                    self.assertTrue(match_scans)
                    for sql in match_scans:
                        self.assertNotIn('"payload"', sql)
                        self.assertNotIn('"raw_body"', sql)
                    full_match_reads = [
                        (path, params) for path, sql, params in query_log
                        if path == (root_b / "changes.sqlite3").resolve()
                        and sql.lower().startswith('select * from "matches"')
                    ]
                    self.assertEqual(len(full_match_reads), 4)
                    self.assertTrue(all(
                        params == (expected_transport_rowid,)
                        for _path, params in full_match_reads
                    ))

                    sibling = root_a / "xlsx" / "manifest.json"
                    sibling.write_bytes(sibling.read_bytes() + b" ")
                    with self.assertRaises(DeltaReaderError) as changed:
                        reader.get_unique_row("matches", {"match_key": "match-a"})
                    self.assertEqual(changed.exception.code, "DELTA_PUBLISHED_INPUT_CHANGED")

        self.assertEqual(_sha256(self.current_db), source_before)
        self.assertEqual(_tree_snapshot(self.baseline_package), baseline_before)

    def test_unique_lookup_mixes_native_and_sharded_transports(self):
        self.writer.execute(
            "UPDATE records SET payload=?,blob_value=? WHERE id=?",
            ("middle-state", b"M" + bytes(range(256)) * 1_500, 5),
        )
        self.writer.commit()
        root_a, plan_a = self._make_delta(
            "lookup-mixed-a", "20261003T010013Z-00000014", self.reset_plan,
            transport_kind="lossless_sqlite_shards", shard_max_bytes=256 * 1024,
        )
        self.writer.execute(
            "UPDATE records SET payload=?,large_integer=?,ratio=? WHERE id=?",
            ("final-state\x00𠮷", HUGE_INTEGER, -0.0, 5),
        )
        self.writer.commit()
        root_b, _plan_b = self._make_delta(
            "lookup-mixed-b", "20261003T010014Z-00000015", plan_a,
            transport_kind="native_sqlite",
        )
        expected = self.writer.execute(
            "SELECT rowid,* FROM records WHERE id=?", (5,)
        ).fetchone()
        expected_rowid, *expected_values = expected

        with LosslessShardReader(self.baseline_package, expected_generation=BASELINE_ID) as baseline:
            with DeltaChainReader(baseline, [self.reset_root, root_a, root_b]) as reader:
                result = reader.get_unique_row("records", {"id": 5})
                self.assertEqual(result, {
                    "source_rowid": expected_rowid,
                    "values": tuple(expected_values),
                })
                self.assertEqual(
                    [package["kind"] for package in reader._lookup_transport_packages],
                    ["native_sqlite", "lossless_sqlite_shards", "native_sqlite"],
                )
                self.assertEqual(reader._materialized, set())

        with LosslessShardReader(self.baseline_package, expected_generation=BASELINE_ID) as baseline:
            with DeltaChainReader(baseline, [self.reset_root, root_a, root_b]) as full_reader:
                full_rows = list(full_reader.iter_rows("records"))
        full_match = next(row for row in full_rows if row[2][0] == 5)
        self.assertEqual((full_match[1], full_match[2]), (result["source_rowid"], result["values"]))

    def test_native_lookup_accepts_gapped_reordered_transport_rowids_and_metadata_rowid(self):
        for index, metadata_rowid in enumerate((0, -7)):
            with self.subTest(metadata_rowid=metadata_rowid):
                self.writer.execute(
                    "UPDATE matches SET payload=? WHERE match_key=?",
                    (f"layout-state-{index}", "match-a"),
                )
                self.writer.commit()
                root = self.root / f"native-layout-{index}"
                generation_id = f"20261003T01002{index}Z-0000001{index}"
                plan = _make_generation(
                    self.current_db, self.baseline_db, root, generation_id,
                    BASELINE_ID, self.baseline_identity["sha256"],
                    kind="baseline_reconciliation", transport_kind="native_sqlite",
                )
                plan = _rewrite_native_transport_layout(
                    root, plan, self.current_db, self.baseline_db,
                    gapped_reordered_table="matches", metadata_rowid=metadata_rowid,
                )
                document = _read_json(root / "transport-document.json")
                internal = document["internal_names"]
                transport = sqlite3.connect(root / "changes.sqlite3")
                try:
                    ops = internal["operations_table"]
                    operation_rowids = [row[0] for row in transport.execute(
                        f'SELECT "transport_rowid" FROM "{ops}" '
                        'WHERE "table_name"=? AND "operation"=? ORDER BY "row_ordinal"',
                        ("matches", "upsert"),
                    )]
                    physical_rowids = [row[0] for row in transport.execute(
                        'SELECT "_rowid_" FROM "matches" ORDER BY "_rowid_"'
                    )]
                    actual_metadata_rowid = transport.execute(
                        f'SELECT "_rowid_" FROM "{internal["metadata_table"]}"'
                    ).fetchone()[0]
                finally:
                    transport.close()
                self.assertEqual(sorted(operation_rowids), physical_rowids)
                self.assertEqual(operation_rowids, sorted(operation_rowids, reverse=True))
                self.assertEqual(actual_metadata_rowid, metadata_rowid)

                expected = self.writer.execute(
                    "SELECT rowid,* FROM matches WHERE match_key=?", ("match-a",)
                ).fetchone()
                with LosslessShardReader(
                    self.baseline_package, expected_generation=BASELINE_ID,
                ) as baseline:
                    with DeltaChainReader(baseline, [root]) as reader:
                        result = reader.get_unique_row("matches", {"match_key": "match-a"})
                        self.assertEqual(result, {
                            "source_rowid": expected[0], "values": tuple(expected[1:]),
                        })
                        package = reader._lookup_transport_packages[0]
                        adapter = package["adapter"]
                        ordinal_index = package["visible"]["matches"].index("match_key")
                        candidates = list(adapter.iter_table_candidates(
                            "matches", {ordinal_index: "match-a"},
                        ))
                        self.assertEqual(len(candidates), 1)
                        source_ordinal, physical_rowid, values = candidates[0]
                        self.assertEqual(values, tuple(expected[1:]))
                        reference_connection = sqlite3.connect(root / "changes.sqlite3")
                        try:
                            expected_reference = reference_connection.execute(
                                f'SELECT "row_ordinal","transport_rowid" FROM "{ops}" '
                                'WHERE "table_name"=? AND "source_rowid"=? AND "operation"=?',
                                ("matches", expected[0], "upsert"),
                            ).fetchone()
                        finally:
                            reference_connection.close()
                        self.assertEqual(
                            (source_ordinal, physical_rowid), expected_reference,
                        )
                        self.assertEqual(reader._materialized, set())
                        self.assertEqual(reader.columns("matches"), plan["source_table_columns"]["matches"])

    def test_native_lookup_rejects_bad_superseded_operation_reference(self):
        self.writer.execute("UPDATE records SET payload=? WHERE id=?", ("older", 5))
        self.writer.commit()
        root_a, plan_a = self._make_delta(
            "lookup-native-reference-a", "20261003T010030Z-00000020",
            self.reset_plan, transport_kind="native_sqlite",
        )
        self.writer.execute("UPDATE records SET payload=? WHERE id=?", ("newer", 5))
        self.writer.commit()
        root_b, _plan_b = self._make_delta(
            "lookup-native-reference-b", "20261003T010031Z-00000021",
            plan_a, transport_kind="native_sqlite",
        )
        original_iter = native_candidates_module.NativeTransportCandidates.iter_table_candidates

        def corrupt_historical_reference(
            adapter, table, criteria_by_column, *, candidate_row_filter=None,
        ):
            if (Path(adapter._root_input) == root_a.resolve()
                    and table == "records" and candidate_row_filter is not None):
                original_filter = candidate_row_filter

                def wrong_transport_rowid(ordinal, transport_rowid):
                    return original_filter(ordinal, transport_rowid + 10_000)

                candidate_row_filter = wrong_transport_rowid
            return original_iter(
                adapter, table, criteria_by_column,
                candidate_row_filter=candidate_row_filter,
            )

        with LosslessShardReader(self.baseline_package, expected_generation=BASELINE_ID) as baseline:
            with DeltaChainReader(baseline, [self.reset_root, root_a, root_b]) as reader:
                with patch.object(
                    native_candidates_module.NativeTransportCandidates,
                    "iter_table_candidates", new=corrupt_historical_reference,
                ):
                    with self.assertRaises(DeltaReaderError) as invalid:
                        reader.get_unique_row("records", {"id": 999_999})
                self.assertEqual(
                    invalid.exception.code, "DELTA_LOOKUP_TRANSPORT_REFERENCE_INVALID"
                )
                self.assertNotIn("records", reader._lookup_map_tables)
                self.assertEqual(reader._materialized, set())

    def test_unique_lookup_validation_and_baseline_without_rowid_fallback(self):
        baseline = LosslessShardReader(self.baseline_package, expected_generation=BASELINE_ID)
        with baseline:
            reader = DeltaChainReader(baseline, [])
            with reader:
                result = reader.get_unique_row("records", {"id": 5})
                self.assertEqual(result["source_rowid"], 5)
                self.assertEqual(result["values"][1], "kept-row")
                self.assertEqual(reader._materialized, set())
                self.assertIsNone(reader.get_unique_row("records", {"id": 999_999}))

                invalid_cases = [
                    ("unknown-table", "missing_table", {}, "DELTA_LOOKUP_TABLE_UNKNOWN"),
                    ("unknown-column", "records", {"missing_column": 1}, "DELTA_LOOKUP_COLUMN_UNKNOWN"),
                    ("bool", "records", {"id": True}, "DELTA_LOOKUP_CRITERIA_INVALID"),
                    ("nan", "records", {"ratio": float("nan")}, "DELTA_LOOKUP_CRITERIA_INVALID"),
                    ("criteria-shape", "records", [], "DELTA_LOOKUP_CRITERIA_INVALID"),
                ]
                for _label, table, criteria, code in invalid_cases:
                    with self.subTest(case=_label):
                        with self.assertRaises(DeltaReaderError) as error:
                            reader.get_unique_row(table, criteria)
                        self.assertEqual(error.exception.code, code)
                with self.assertRaises(DeltaReaderError) as missing:
                    reader.get_unique_row("records", {})
                self.assertEqual(missing.exception.code, "DELTA_LOOKUP_NOT_UNIQUE")

                key_only = reader.get_unique_row(
                    "key_only", {"kind": "vs", "key_value": b"\x00key-a"}
                )
                self.assertIsNone(key_only["source_rowid"])
                self.assertEqual(key_only["values"], (b"old-a", "vs", b"\x00key-a"))
                self.assertIn("key_only", reader._materialized)
                self.assertIsNone(reader.get_unique_row("deliberately_empty", {}))
                self.assertEqual(reader._materialized, {"key_only"})
            with self.assertRaises(RuntimeError):
                reader.get_unique_row("records", {"id": 5})

        with LosslessShardReader(self.baseline_package, expected_generation=BASELINE_ID) as baseline:
            with DeltaChainReader(baseline, [self.reset_root]) as reader:
                current_key = reader.get_unique_row(
                    "key_only", {"kind": "vs", "key_value": b"\x00key-a"}
                )
                self.assertIsNone(current_key["source_rowid"])
                self.assertEqual(
                    current_key["values"], (b"key\x00updated", "vs", b"\x00key-a")
                )
                self.assertEqual(reader._materialized, {"key_only"})

    def test_unique_lookup_falls_back_for_schema_changes_and_shadowed_rowid(self):
        self.writer.execute("ALTER TABLE records ADD COLUMN lookup_added TEXT")
        self.writer.execute("UPDATE records SET lookup_added='schema-reset' WHERE id=5")
        self.writer.commit()
        schema_root = self.root / "lookup-schema-reset"
        _make_generation(
            self.current_db, self.baseline_db, schema_root, SHARDED_DDL_ID,
            BASELINE_ID, self.baseline_identity["sha256"], kind="baseline_reconciliation",
            transport_kind="lossless_sqlite_shards", shard_max_bytes=256 * 1024,
        )
        with LosslessShardReader(self.baseline_package, expected_generation=BASELINE_ID) as baseline:
            with DeltaChainReader(baseline, [schema_root]) as reader:
                result = reader.get_unique_row("records", {"id": 5})
                self.assertEqual(result["values"][-1], "schema-reset")
                self.assertEqual(reader._materialized, {"records"})
                self.assertIsNone(reader._lookup_operations_cache)

        shadow_source = self.root / "shadowed-rowid.sqlite3"
        shadow_current = self.root / "shadowed-rowid-current.sqlite3"
        shadow_package = self.root / "shadowed-rowid-package"
        connection = sqlite3.connect(shadow_source)
        try:
            connection.execute(
                "CREATE TABLE shadowed (id TEXT PRIMARY KEY, rowid TEXT, _rowid_ TEXT, oid TEXT, payload BLOB)"
            )
            connection.execute(
                "CREATE TABLE matches (account TEXT, kind TEXT, match_key TEXT, payload)"
            )
            connection.execute(
                "CREATE TABLE match_classification (account TEXT, kind TEXT, match_key TEXT, "
                "analysis_set TEXT, rule_raw TEXT, raw_classification BLOB)"
            )
            connection.execute(
                "INSERT INTO shadowed VALUES(?,?,?,?,?)",
                ("key-a", "a", "b", "c", b"shadowed\x00row"),
            )
            connection.commit()
        finally:
            connection.close()
        shadow_baseline_info = _make_baseline_package(shadow_source, shadow_package)
        with LosslessShardReader(shadow_package, expected_generation=BASELINE_ID) as baseline:
            with DeltaChainReader(baseline, []) as reader:
                result = reader.get_unique_row("shadowed", {"payload": b"shadowed\x00row"})
                self.assertIsNone(result["source_rowid"])
                self.assertEqual(
                    result["values"], ("key-a", "a", "b", "c", b"shadowed\x00row")
                )
                self.assertEqual(reader._materialized, {"shadowed"})

        shutil.copy2(shadow_source, shadow_current)
        writer = sqlite3.connect(shadow_current)
        try:
            writer.execute("PRAGMA recursive_triggers=ON")
            install_change_feed(writer)
            install_writer_guards(writer)
            writer.execute(
                "UPDATE shadowed SET payload=? WHERE id=?", (b"native\x00updated", "key-a")
            )
            writer.commit()
        finally:
            writer.close()
        shadow_delta_root = self.root / "shadowed-native-reset"
        _make_generation(
            shadow_current, shadow_source, shadow_delta_root,
            "20261003T010040Z-00000022", BASELINE_ID,
            shadow_baseline_info["identity"]["sha256"],
            kind="baseline_reconciliation", transport_kind="native_sqlite",
        )
        with LosslessShardReader(shadow_package, expected_generation=BASELINE_ID) as baseline:
            with DeltaChainReader(baseline, [shadow_delta_root]) as reader:
                self.assertFalse(reader._lookup_fast_eligible("shadowed"))
                current_shadowed = reader.get_unique_row(
                    "shadowed", {"payload": b"native\x00updated"}
                )
                self.assertIsNone(current_shadowed["source_rowid"])
                self.assertEqual(
                    current_shadowed["values"],
                    ("key-a", "a", "b", "c", b"native\x00updated"),
                )
                self.assertEqual(reader._materialized, {"shadowed"})

    def test_unique_lookup_rechecks_prepared_and_published_tokens_and_closes_candidate_generators(self):
        root = self.root / "lookup-token-reset"
        plan = _make_generation(
            self.current_db, self.baseline_db, root, RESET_ID,
            BASELINE_ID, self.baseline_identity["sha256"], kind="baseline_reconciliation",
            transport_kind="lossless_sqlite_shards", shard_max_bytes=256 * 1024,
        )
        for published in (False, True):
            generation_root = self.root / ("lookup-token-published" if published else "lookup-token-prepared")
            shutil.copytree(root, generation_root)
            generation_plan = _read_json(generation_root / "delta-plan.json")
            if published:
                self._write_publisher_index(generation_root, generation_plan)
            with LosslessShardReader(self.baseline_package, expected_generation=BASELINE_ID) as baseline:
                try:
                    with DeltaChainReader(
                        baseline, [generation_root], require_published_deltas=published,
                    ) as reader:
                        first = reader.get_unique_row("records", {"id": 5})
                        self.assertEqual(first["source_rowid"], 5)
                        data_entry = next(
                            item for item in generation_plan["files"]
                            if item["local"].startswith("transport/")
                            and item["local"].endswith(".sqlite3")
                        )
                        target = generation_root / data_entry["local"]
                        temporary = target.with_name(target.name + ".replacement")
                        temporary.write_bytes(target.read_bytes())
                        temporary.replace(target)
                        with self.assertRaises(DeltaReaderError) as changed:
                            reader.get_unique_row("records", {"id": 5})
                        self.assertEqual(changed.exception.code, "DELTA_PUBLISHED_INPUT_CHANGED")
                except DeltaReaderError as changed_on_exit:
                    if not published:
                        raise
                    self.assertEqual(changed_on_exit.code, "DELTA_PUBLISHED_INPUT_CHANGED")

        with LosslessShardReader(self.baseline_package, expected_generation=BASELINE_ID) as baseline:
            with DeltaChainReader(baseline, []) as reader:
                closed: list[bool] = []
                original = delta_reader_module._lossless._iter_table_candidates

                def tracked_candidates(*args, **kwargs):
                    underlying = original(*args, **kwargs)

                    def entries():
                        try:
                            yield from underlying
                        finally:
                            closed.append(True)
                            underlying.close()

                    return entries()

                with patch.object(
                    delta_reader_module._lossless,
                    "_iter_table_candidates",
                    side_effect=tracked_candidates,
                ):
                    with self.assertRaises(DeltaReaderError) as nonunique:
                        reader.get_unique_row("records", {})
                self.assertEqual(nonunique.exception.code, "DELTA_LOOKUP_NOT_UNIQUE")
                self.assertTrue(closed)

    def test_unique_lookup_operation_cache_rejects_bad_metadata_and_cleans_up(self):
        root = self.root / "lookup-operation-validation"
        _make_generation(
            self.current_db, self.baseline_db, root, RESET_ID,
            BASELINE_ID, self.baseline_identity["sha256"], kind="baseline_reconciliation",
            transport_kind="lossless_sqlite_shards", shard_max_bytes=256 * 1024,
        )
        plan = _read_json(root / "delta-plan.json")
        table_names, _xinfo, _visible = delta_reader_module._delta._table_specs(plan["metadata"])
        operations_table = delta_reader_module._delta._internal_names(
            plan["metadata"], table_names
        )["operations_table"]
        original = delta_reader_module._lossless._iter_table_candidates

        for corruption in ("operation-ordinal", "identity-key", "source-rowid", "missing-operation"):
            with self.subTest(corruption=corruption):
                with LosslessShardReader(self.baseline_package, expected_generation=BASELINE_ID) as baseline:
                    with DeltaChainReader(baseline, [root]) as reader:
                        def corrupt_candidates(package_root, manifest, tablename, criteria, **kwargs):
                            candidates = original(package_root, manifest, tablename, criteria, **kwargs)
                            if Path(package_root) != root / "transport" or tablename != operations_table:
                                return candidates

                            def rows():
                                try:
                                    for ordinal, source_rowid, values in candidates:
                                        if corruption == "missing-operation" and ordinal == 0:
                                            continue
                                        if ordinal == 0 and corruption in (
                                            "operation-ordinal", "identity-key", "source-rowid"
                                        ):
                                            changed = list(values)
                                            if corruption == "operation-ordinal":
                                                changed[0] = 10_000
                                            elif corruption == "identity-key":
                                                changed[-1] = "incorrect-identity-key"
                                            else:
                                                source_rowid = 10_000
                                            values = tuple(changed)
                                        yield ordinal, source_rowid, values
                                finally:
                                    candidates.close()
                            return rows()

                        with patch.object(
                            delta_reader_module._lossless,
                            "_iter_table_candidates",
                            side_effect=corrupt_candidates,
                        ):
                            with self.assertRaises(DeltaReaderError) as invalid:
                                reader.get_unique_row("records", {"id": 5})
                        self.assertEqual(invalid.exception.code, "DELTA_LOOKUP_TRANSPORT_INVALID")
                        self.assertIsNone(reader._lookup_operations_cache)
                        self.assertFalse(reader._lookup_operations_cache_ready)
                        self.assertEqual(reader._lookup_transport_packages, [])
                        self.assertEqual(reader._materialized, set())

        with LosslessShardReader(self.baseline_package, expected_generation=BASELINE_ID) as baseline:
            with DeltaChainReader(baseline, [root]) as reader:
                def interrupt_candidates(package_root, manifest, tablename, criteria, **kwargs):
                    if Path(package_root) == root / "transport" and tablename == operations_table:
                        def interrupt():
                            raise KeyboardInterrupt
                            yield
                        return interrupt()
                    return original(package_root, manifest, tablename, criteria, **kwargs)

                with patch.object(
                    delta_reader_module._lossless,
                    "_iter_table_candidates",
                    side_effect=interrupt_candidates,
                ):
                    with self.assertRaises(KeyboardInterrupt):
                        reader.get_unique_row("records", {"id": 5})
                self.assertIsNone(reader._lookup_operations_cache)
                self.assertEqual(reader._lookup_transport_packages, [])
                self.assertEqual(reader._materialized, set())

    def test_unique_lookup_preserves_iterator_baseexception_when_close_also_fails(self):
        class SyntheticInterrupt(BaseException):
            pass

        class InterruptingIterator:
            def __init__(self, body_error, close_error):
                self.body_error = body_error
                self.close_error = close_error
                self.closed = False

            def __iter__(self):
                return self

            def __next__(self):
                raise self.body_error

            def close(self):
                self.closed = True
                raise self.close_error

        body_error = SyntheticInterrupt("original operation scan interruption")
        close_error = RuntimeError("secondary iterator close failure")
        original = native_candidates_module.NativeTransportCandidates.iter_table_candidates
        interrupting_iterator = InterruptingIterator(body_error, close_error)

        def interrupt_operations(adapter, table, criteria_by_column, *, candidate_row_filter=None):
            if table == adapter.operations_table:
                return interrupting_iterator
            return original(
                adapter, table, criteria_by_column,
                candidate_row_filter=candidate_row_filter,
            )

        with LosslessShardReader(self.baseline_package, expected_generation=BASELINE_ID) as baseline:
            with DeltaChainReader(baseline, [self.reset_root]) as reader:
                with patch.object(
                    native_candidates_module.NativeTransportCandidates,
                    "iter_table_candidates",
                    new=interrupt_operations,
                ):
                    with self.assertRaises(SyntheticInterrupt) as caught:
                        reader.get_unique_row("records", {"id": 5})

                self.assertIs(caught.exception, body_error)
                self.assertTrue(interrupting_iterator.closed)
                self.assertIsNone(reader._lookup_operations_cache)
                self.assertFalse(reader._lookup_operations_cache_ready)
                self.assertEqual(reader._lookup_transport_packages, [])
                self.assertNotIn("records", reader._lookup_map_tables)
                self.assertEqual(reader._materialized, set())
                self.assertFalse(any(
                    name.startswith("_lookup_operations_")
                    or name.startswith("_lookup_rows_")
                    for (name,) in reader._connection.execute(
                        "SELECT name FROM sqlite_master WHERE type='table'"
                    )
                ))

    def test_unique_lookup_pk_only_clear_fallback_drops_map_after_operation_cursor_closes(self):
        root = self.root / "lookup-pk-only-clear"
        plan = _make_generation(
            self.current_db, self.baseline_db, root, RESET_B_ID,
            BASELINE_ID, self.baseline_identity["sha256"], kind="baseline_reconciliation",
            transport_kind="lossless_sqlite_shards", shard_max_bytes=256 * 1024,
        )
        _table_names, _xinfo, _visible = delta_reader_module._delta._table_specs(plan["metadata"])
        operations_table = delta_reader_module._delta._internal_names(
            plan["metadata"], _table_names
        )["operations_table"]
        original = delta_reader_module._lossless._iter_table_candidates
        synthetic_operations_used = False
        original_materialize = None
        original_operation_counts = None

        def clear_without_row_identity(package_root, manifest, tablename, criteria, **kwargs):
            candidates = original(package_root, manifest, tablename, criteria, **kwargs)
            nonlocal synthetic_operations_used
            if (Path(package_root) != root / "transport" or tablename != operations_table
                    or synthetic_operations_used):
                return candidates
            synthetic_operations_used = True

            def rows():
                first_upsert_reference = None
                replaced_delete = False
                try:
                    for ordinal, rowid, values in candidates:
                        values = list(values)
                        if values[1] == "records" and values[2] == "upsert" \
                                and first_upsert_reference is None:
                            first_upsert_reference = (values[5], values[6])
                            values[2] = "clear_table"
                            changed = list(values)
                            changed[3] = None
                            changed[4] = None
                            changed[5] = None
                            changed[6] = None
                            changed[7] = None
                            yield ordinal, rowid, tuple(changed)
                            continue
                        if (values[1] == "records" and values[2] == "delete"
                                and first_upsert_reference is not None and not replaced_delete):
                            values[2] = "upsert"
                            values[3] = None
                            values[4] = None
                            values[5], values[6] = first_upsert_reference
                            values[7] = None
                            replaced_delete = True
                            yield ordinal, rowid, tuple(values)
                            continue
                        if values[1] == "records" and values[2] == "upsert":
                            values[3] = None
                            values[4] = None
                            values[7] = None
                        yield ordinal, rowid, tuple(values)
                finally:
                    candidates.close()
            return rows()

        def materialize_after_synthetic_metadata(table, criteria_indexes):
            reader._documents[0]["operation_counts"].clear()
            reader._documents[0]["operation_counts"].update(original_operation_counts)
            return original_materialize(table, criteria_indexes)

        with LosslessShardReader(self.baseline_package, expected_generation=BASELINE_ID) as baseline:
            with DeltaChainReader(baseline, [root]) as reader:
                original_materialize = reader._lookup_materialized
                original_operation_counts = dict(reader._documents[0]["operation_counts"])
                with patch.object(
                    delta_reader_module._lossless,
                    "_iter_table_candidates",
                    side_effect=clear_without_row_identity,
                ), patch.object(
                    reader, "_lookup_materialized", side_effect=materialize_after_synthetic_metadata
                ) as materialized:
                    reader._documents[0]["operation_counts"]["delete"] -= 1
                    reader._documents[0]["operation_counts"]["clear_table"] += 1
                    result = reader.get_unique_row("records", {"id": 5})
                    self.assertEqual(materialized.call_count, 1)
                self.assertEqual(result["source_rowid"], 5)
                self.assertEqual(result["values"][1], "new\x00text")
                self.assertIn("records", reader._lookup_fallback_tables)
                self.assertEqual(reader._materialized, {"records"})
                self.assertFalse(any(
                    row[0].startswith("_lookup_rows_")
                    for row in reader._connection.execute(
                        "SELECT name FROM sqlite_master WHERE type='table'"
                    )
                ))

    def test_unique_lookup_checks_superseded_transport_references_without_decoding_values(self):
        reset_root = self.root / "lookup-reference-reset"
        reset_plan = _make_generation(
            self.current_db, self.baseline_db, reset_root, RESET_B_ID,
            BASELINE_ID, self.baseline_identity["sha256"], kind="baseline_reconciliation",
            transport_kind="lossless_sqlite_shards", shard_max_bytes=256 * 1024,
        )
        self.writer.execute("UPDATE records SET payload=? WHERE id=5", ("older-value",))
        self.writer.commit()
        delta_a_root, delta_a_plan = self._make_delta(
            "lookup-reference-a", DELTA_C_ID, reset_plan,
            transport_kind="lossless_sqlite_shards", shard_max_bytes=256 * 1024,
        )
        self.writer.execute("UPDATE records SET payload=? WHERE id=5", ("newer-value",))
        self.writer.commit()
        delta_b_root, _delta_b_plan = self._make_delta(
            "lookup-reference-b", "20261003T001008Z-00000009", delta_a_plan,
            transport_kind="lossless_sqlite_shards", shard_max_bytes=256 * 1024,
        )
        _table_names, _xinfo, _visible = delta_reader_module._delta._table_specs(
            delta_a_plan["metadata"]
        )
        operations_table = delta_reader_module._delta._internal_names(
            delta_a_plan["metadata"], _table_names
        )["operations_table"]
        records_table_id = next(
            item["table_id"] for item in _read_json(delta_a_root / "transport/manifest.json")["tables"]
            if item["name"] == "records"
        )
        original_candidates = delta_reader_module._lossless._iter_table_candidates
        original_external_read = delta_reader_module._lossless._read_external_cell
        decoded_records: list[str] = []

        def corrupt_old_reference(package_root, manifest, tablename, criteria, **kwargs):
            candidates = original_candidates(package_root, manifest, tablename, criteria, **kwargs)
            if Path(package_root) != delta_a_root / "transport" or tablename != operations_table:
                return candidates

            def rows():
                changed_reference = False
                try:
                    for ordinal, rowid, values in candidates:
                        if (not changed_reference and values[1] == "records"
                                and values[2] == "upsert" and values[3] == 5):
                            changed = list(values)
                            changed[6] = changed[6] + 100_000
                            values = tuple(changed)
                            changed_reference = True
                        yield ordinal, rowid, values
                finally:
                    candidates.close()
            return rows()

        def count_record_payload_reads(package_root, table_id, *args, **kwargs):
            if (Path(package_root) == delta_a_root / "transport"
                    and table_id == records_table_id):
                decoded_records.append("decoded")
            return original_external_read(package_root, table_id, *args, **kwargs)

        with LosslessShardReader(self.baseline_package, expected_generation=BASELINE_ID) as baseline:
            with DeltaChainReader(baseline, [reset_root, delta_a_root, delta_b_root]) as reader:
                with patch.object(
                    delta_reader_module._lossless,
                    "_iter_table_candidates",
                    side_effect=corrupt_old_reference,
                ), patch.object(
                    delta_reader_module._lossless,
                    "_read_external_cell",
                    side_effect=count_record_payload_reads,
                ):
                    with self.assertRaises(DeltaReaderError) as invalid:
                        reader.get_unique_row("records", {"id": 999_999})
                self.assertEqual(
                    invalid.exception.code, "DELTA_LOOKUP_TRANSPORT_REFERENCE_INVALID"
                )
                self.assertEqual(decoded_records, [])
                self.assertNotIn("records", reader._lookup_map_tables)

    def test_unique_lookup_rejects_per_generation_row_count_mismatch_and_discards_map(self):
        for transport_kind in ("native_sqlite", "lossless_sqlite_shards"):
            with self.subTest(transport_kind=transport_kind):
                root = self.root / f"lookup-row-count-mismatch-{transport_kind}"
                _make_generation(
                    self.current_db, self.baseline_db, root, RESET_B_ID,
                    BASELINE_ID, self.baseline_identity["sha256"],
                    kind="baseline_reconciliation", transport_kind=transport_kind,
                    shard_max_bytes=256 * 1024,
                )
                with LosslessShardReader(
                    self.baseline_package, expected_generation=BASELINE_ID,
                ) as baseline:
                    with DeltaChainReader(baseline, [root]) as reader:
                        reader._plans[0]["source_row_counts"]["records"] += 1
                        with self.assertRaises(DeltaReaderError) as mismatch:
                            reader.get_unique_row("records", {"id": 5})
                        self.assertEqual(
                            mismatch.exception.code, "DELTA_LOOKUP_ROW_COUNT_MISMATCH"
                        )
                        self.assertNotIn("records", reader._lookup_map_tables)
                        self.assertFalse(any(
                            row[0].startswith("_lookup_rows_")
                            for row in reader._connection.execute(
                                "SELECT name FROM sqlite_master WHERE type='table'"
                            )
                        ))

    def test_unique_lookup_delete_then_reinsert_replaces_tombstone(self):
        reset_root = self.root / "lookup-delete-reset"
        reset_plan = _make_generation(
            self.current_db, self.baseline_db, reset_root, RESET_B_ID,
            BASELINE_ID, self.baseline_identity["sha256"], kind="baseline_reconciliation",
            transport_kind="lossless_sqlite_shards", shard_max_bytes=256 * 1024,
        )
        self.writer.execute("DELETE FROM records WHERE id=37")
        self.writer.commit()
        deleted_root, deleted_plan = self._make_delta(
            "lookup-delete", DELTA_C_ID, reset_plan,
            transport_kind="lossless_sqlite_shards", shard_max_bytes=256 * 1024,
        )
        self.writer.execute(
            "INSERT INTO records VALUES(?,?,?,?,?,?)",
            (37, "reinserted", "new", b"new\x00blob", 123, -0.0),
        )
        self.writer.commit()
        reinsert_root, _reinsert_plan = self._make_delta(
            "lookup-reinsert", "20261003T001008Z-00000009", deleted_plan,
            transport_kind="lossless_sqlite_shards", shard_max_bytes=256 * 1024,
        )

        with LosslessShardReader(self.baseline_package, expected_generation=BASELINE_ID) as baseline:
            with DeltaChainReader(baseline, [reset_root, deleted_root]) as reader:
                self.assertIsNone(reader.get_unique_row("records", {"id": 37}))
                self.assertIsNone(reader.get_unique_row("records", {"id": -11}))
        with LosslessShardReader(self.baseline_package, expected_generation=BASELINE_ID) as baseline:
            with DeltaChainReader(baseline, [reset_root, deleted_root, reinsert_root]) as reader:
                result = reader.get_unique_row("records", {"id": 37})
                self.assertEqual(result["source_rowid"], 37)
                self.assertEqual(result["values"][1:4], ("reinserted", "new", b"new\x00blob"))
                self.assertEqual(result["values"][5].hex(), "-0x0.0p+0")

    def test_unique_lookup_retains_foreign_key_and_schema_metadata(self):
        baseline_db = self.root / "lookup-fk-baseline.sqlite3"
        current_db = self.root / "lookup-fk-current.sqlite3"
        baseline_package = self.root / "lookup-fk-package"
        baseline_connection = sqlite3.connect(baseline_db)
        try:
            baseline_connection.execute("PRAGMA foreign_keys=ON")
            baseline_connection.executescript(
                "CREATE TABLE parents (parent_id INTEGER PRIMARY KEY, label TEXT NOT NULL);"
                "CREATE TABLE children (child_id INTEGER PRIMARY KEY, "
                "parent_id INTEGER NOT NULL REFERENCES parents(parent_id), "
                "body BLOB, note TEXT);"
                "CREATE TABLE matches (account TEXT, kind TEXT, match_key TEXT, payload);"
                "CREATE TABLE match_classification (account TEXT, kind TEXT, match_key TEXT, "
                "analysis_set TEXT, rule_raw TEXT, raw_classification BLOB);"
                "INSERT INTO parents VALUES(7, 'parent');"
                "INSERT INTO children VALUES(44, 7, x'6265666f7265', 'before');"
            )
            baseline_connection.commit()
        finally:
            baseline_connection.close()
        shutil.copy2(baseline_db, current_db)
        baseline_info = _make_baseline_package(baseline_db, baseline_package)
        writer = sqlite3.connect(current_db)
        try:
            writer.execute("PRAGMA foreign_keys=ON")
            writer.execute("PRAGMA recursive_triggers=ON")
            install_change_feed(writer)
            install_writer_guards(writer)
            large_body = b"\x00\xff" * 180_000
            large_note = "note\x00𠮷" * 40_000
            writer.execute(
                "UPDATE children SET body=?, note=? WHERE child_id=44",
                (large_body, large_note),
            )
            writer.commit()
        finally:
            writer.close()
        for transport_kind in ("native_sqlite", "lossless_sqlite_shards"):
            with self.subTest(transport_kind=transport_kind):
                root = self.root / f"lookup-fk-generation-{transport_kind}"
                plan = _make_generation(
                    current_db, baseline_db, root, RESET_B_ID,
                    BASELINE_ID, baseline_info["identity"]["sha256"],
                    kind="baseline_reconciliation", transport_kind=transport_kind,
                    shard_max_bytes=256 * 1024,
                )

                with LosslessShardReader(
                    baseline_package, expected_generation=BASELINE_ID,
                ) as baseline:
                    with DeltaChainReader(baseline, [root]) as reader:
                        result = reader.get_unique_row("children", {"parent_id": 7})
                        self.assertEqual(result["source_rowid"], 44)
                        self.assertEqual(result["values"], (44, 7, large_body, large_note))
                        self.assertEqual(
                            reader.columns("children"), plan["source_table_columns"]["children"]
                        )
                        self.assertEqual(
                            reader.foreign_keys("children"),
                            plan["source_foreign_keys"]["children"],
                        )
                        self.assertEqual(reader.schema_objects(), plan["schemas"])
                        self.assertEqual(reader._materialized, set())


if __name__ == "__main__":
    unittest.main()
