import hashlib
import json
import os
from pathlib import Path
import resource
import sqlite3
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

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
from ikarchive import lossless_sqlite as shards
from ikarchive.slice_selectors import export_selectors, verify_selectors
from ikarchive.verified_files import verify_files


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


def _write_json_file(path: Path, value: dict) -> None:
    path.write_text(
        json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2) + "\n",
        encoding="utf-8",
    )


def _package_records(root: Path) -> dict[str, dict[str, object]]:
    return {
        path.relative_to(root).as_posix(): {
            "bytes": path.stat().st_size,
            "sha256": _sha(path),
        }
        for path in sorted(root.rglob("*"))
        if path.is_file() and not path.is_symlink()
    }


def _external_candidate_source(path: Path) -> sqlite3.Connection:
    writer = sqlite3.connect(path)
    try:
        writer.execute("PRAGMA journal_mode=DELETE")
        writer.execute(
            "CREATE TABLE candidate_rows ("
            "source_id INTEGER PRIMARY KEY, selector TEXT NOT NULL, large_text TEXT NOT NULL, "
            "large_blob BLOB NOT NULL, ratio, nullable_text TEXT, empty_text TEXT)"
        )
        selected_text = "selected:" + "s" * (300 * 1024)
        other_text = "other:" + "o" * (300 * 1024)
        blob_bytes = 300 * 1024
        writer.executemany(
            "INSERT INTO candidate_rows VALUES(?,?,?,?,?,?,?)",
            [
                (10, "no", other_text, b"a" * blob_bytes, 1.25, None, ""),
                (20, "yes", other_text, b"b" * blob_bytes, 2.5, "text", ""),
                (40, "yes", selected_text, b"c" * blob_bytes, 3.75, None, ""),
                (99, "no", selected_text, b"d" * blob_bytes, 4.5, "last", ""),
            ],
        )
        writer.commit()
    finally:
        writer.close()
    source = sqlite3.connect(path.as_uri() + "?mode=ro&immutable=1", uri=True)
    source.execute("PRAGMA query_only=ON")
    return source


def _candidate_transport_package(base: Path):
    source_path = base / "source.sqlite3"
    source = _external_candidate_source(source_path)
    package = base / "transport"
    manifest = export_sqlite_shards(
        source, package, snapshot_id="candidate-transport", max_bytes=MAX_ALLOWED_BYTES
    )
    receipt = verify_sqlite_shards(source, package, manifest)
    if receipt["status"] != "verified":
        raise AssertionError("synthetic transport package did not verify")
    records = _package_records(package)
    token = verify_files(package, records)
    return source, source_path, package, manifest, records, token


class _TrackedConnection:
    def __init__(self, connection: sqlite3.Connection, opened: list, full_fetches: list):
        self._connection = connection
        self._opened = opened
        self._full_fetches = full_fetches
        self.closed = False
        opened.append(self)

    def execute(self, sql, *args):
        normalized = sql.lstrip().upper()
        if normalized.startswith('SELECT * FROM "CANDIDATE_ROWS" NOT INDEXED'):
            self._full_fetches.append(sql)
        return self._connection.execute(sql, *args)

    def close(self):
        self.closed = True
        self._connection.close()

    def __getattr__(self, name):
        return getattr(self._connection, name)


class LosslessSqliteShardsTests(unittest.TestCase):
    def test_transport_candidate_filter_skips_payload_fetch_for_rejected_rows(self):
        with tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parents[2]) as temporary:
            source, source_path, package, manifest, records, token = _candidate_transport_package(
                Path(temporary)
            )
            source_sha = _sha(source_path)
            opened = []
            full_fetches = []
            callback_rows = []
            stats = {}
            real_open = shards._open_readonly

            def tracked_open(path):
                return _TrackedConnection(real_open(path), opened, full_fetches)

            try:
                with patch.object(shards, "_open_readonly", side_effect=tracked_open), patch.object(
                    shards, "_read_external_cell", wraps=shards._read_external_cell
                ) as external_read:
                    rows = list(shards._iter_table_entries(
                        package,
                        manifest,
                        "candidate_rows",
                        stats=stats,
                        candidate_criteria={1: "yes"},
                        verified_files=token,
                        verified_records=records,
                        candidate_package_kind="transport",
                        candidate_row_filter=lambda ordinal, rowid: (
                            callback_rows.append((ordinal, rowid)) or False
                        ),
                    ))
                self.assertEqual(rows, [])
                self.assertEqual(callback_rows, [(1, 20), (2, 40)])
                external_read.assert_not_called()
                self.assertEqual(full_fetches, [])
                self.assertEqual(stats["rows"], 4)
                self.assertEqual(stats["cells"], 0)
                self.assertEqual(stats["external_cells"], 8)
                self.assertTrue(opened)
                self.assertTrue(all(connection.closed for connection in opened))
                self.assertEqual(_sha(source_path), source_sha)
            finally:
                source.close()

    def test_transport_candidate_filter_preserves_external_and_native_values(self):
        with tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parents[2]) as temporary:
            source, source_path, package, manifest, records, token = _candidate_transport_package(
                Path(temporary)
            )
            source_sha = _sha(source_path)
            opened = []
            full_fetches = []
            callback_rows = []
            real_open = shards._open_readonly

            def tracked_open(path):
                return _TrackedConnection(real_open(path), opened, full_fetches)

            selected_text = "selected:" + "s" * (300 * 1024)
            selected_blob = b"c" * (300 * 1024)

            def keep_not_twenty(ordinal, rowid):
                callback_rows.append((ordinal, rowid))
                return rowid != 20

            try:
                with patch.object(shards, "_open_readonly", side_effect=tracked_open), patch.object(
                    shards, "_read_external_cell", wraps=shards._read_external_cell
                ) as external_read:
                    rows = list(shards._iter_table_candidates(
                        package,
                        manifest,
                        "candidate_rows",
                        {1: "yes", 2: selected_text},
                        verified_files=token,
                        verified_records=records,
                        candidate_package_kind="transport",
                        candidate_row_filter=keep_not_twenty,
                    ))
                self.assertEqual(callback_rows, [(1, 20), (2, 40)])
                self.assertEqual(
                    rows,
                    [(2, 40, (40, "yes", selected_text, selected_blob, 3.75, None, ""))],
                )
                self.assertEqual(external_read.call_count, 2)
                self.assertEqual(len(full_fetches), 1)
                self.assertTrue(opened)
                self.assertTrue(all(connection.closed for connection in opened))
                self.assertEqual(_sha(source_path), source_sha)
            finally:
                source.close()

    def test_candidate_filter_errors_are_stable_and_release_connections(self):
        with tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parents[2]) as temporary:
            source, _source_path, package, manifest, records, token = _candidate_transport_package(
                Path(temporary)
            )
            real_open = shards._open_readonly

            def raise_runtime(_ordinal, _rowid):
                raise RuntimeError("private callback detail")

            def raise_interrupt(_ordinal, _rowid):
                raise KeyboardInterrupt()

            cases = (
                ("exception", raise_runtime, ValueError, "CANDIDATE_ROW_FILTER_FAILED"),
                ("result", lambda _ordinal, _rowid: 1, ValueError,
                 "CANDIDATE_ROW_FILTER_RESULT_INVALID"),
                ("base", raise_interrupt, KeyboardInterrupt, None),
            )
            try:
                for name, callback, exception_type, category in cases:
                    with self.subTest(name=name):
                        opened = []
                        full_fetches = []

                        def tracked_open(path):
                            return _TrackedConnection(real_open(path), opened, full_fetches)

                        with patch.object(shards, "_open_readonly", side_effect=tracked_open):
                            generator = shards._iter_table_candidates(
                                package,
                                manifest,
                                "candidate_rows",
                                {1: "yes"},
                                verified_files=token,
                                verified_records=records,
                                candidate_package_kind="transport",
                                candidate_row_filter=callback,
                            )
                            with self.assertRaises(exception_type) as raised:
                                list(generator)
                        if category is not None:
                            self.assertEqual(str(raised.exception), category)
                            self.assertIsNone(raised.exception.__cause__)
                            if category == "CANDIDATE_ROW_FILTER_FAILED":
                                self.assertTrue(raised.exception.__suppress_context__)
                        self.assertTrue(opened)
                        self.assertTrue(all(connection.closed for connection in opened))
                        self.assertEqual(full_fetches, [])
            finally:
                source.close()

    def test_candidate_package_controls_and_lookup_options_are_exact(self):
        with tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parents[2]) as temporary:
            base = Path(temporary)
            source, _source_path, package, manifest, records, token = _candidate_transport_package(base)
            try:
                self.assertEqual(
                    {"manifest.json", "verification.json"} & set(records),
                    {"manifest.json", "verification.json"},
                )
                self.assertNotIn("selectors-verification.json", records)
                with self.assertRaisesRegex(ValueError, "does not cover the complete package"):
                    missing_control = {
                        relative: record for relative, record in records.items()
                        if relative != "verification.json"
                    }
                    missing_token = verify_files(package, missing_control)
                    list(shards._iter_table_candidates(
                        package, manifest, "candidate_rows", {},
                        verified_files=missing_token,
                        verified_records=missing_control,
                        candidate_package_kind="transport",
                    ))

                with self.assertRaisesRegex(ValueError, "CANDIDATE_PACKAGE_KIND_INVALID"):
                    list(shards._iter_table_candidates(
                        package, manifest, "candidate_rows", {},
                        verified_files=token, verified_records=records,
                        candidate_package_kind=[],
                    ))
                with self.assertRaisesRegex(ValueError, "CANDIDATE_OPTIONS_REQUIRE_LOOKUP"):
                    list(shards._iter_table_entries(
                        package, manifest, "candidate_rows", candidate_package_kind="transport"
                    ))
                with self.assertRaisesRegex(ValueError, "CANDIDATE_OPTIONS_REQUIRE_LOOKUP"):
                    list(shards._iter_table_entries(
                        package, manifest, "candidate_rows", candidate_row_filter=lambda *_: True
                    ))
                with self.assertRaisesRegex(ValueError, "CANDIDATE_ROW_FILTER_INVALID"):
                    list(shards._iter_table_candidates(
                        package, manifest, "candidate_rows", {},
                        verified_files=token, verified_records=records,
                        candidate_row_filter=object(),
                    ))
            finally:
                source.close()

    def test_baseline_candidates_require_selector_control_and_transport_rejects_selectors(self):
        source = _selector_source()
        try:
            with tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parents[2]) as temporary:
                package = Path(temporary) / "baseline"
                manifest = _finalized_selector_package(source, package)
                verify_sqlite_shards(source, package, manifest)
                selector_receipt = verify_selectors(
                    source, package, manifest, manifest["source_sha256"]
                )
                _write_json_file(package / "selectors-verification.json", selector_receipt)
                records = _package_records(package)
                token = verify_files(package, records)

                rows = list(shards._iter_table_candidates(
                    package, manifest, "matches", {2: "match-000000"},
                    verified_files=token, verified_records=records,
                ))
                self.assertEqual(len(rows), 1)
                self.assertEqual(rows[0][2][2], "match-000000")

                missing_selector_control = {
                    relative: record for relative, record in records.items()
                    if relative != "selectors-verification.json"
                }
                subset_token = verify_files(package, missing_selector_control)
                with self.assertRaisesRegex(ValueError, "does not cover the complete package"):
                    list(shards._iter_table_candidates(
                        package, manifest, "matches", {2: "match-000000"},
                        verified_files=subset_token,
                        verified_records=missing_selector_control,
                    ))

                with self.assertRaisesRegex(
                    ValueError, "CANDIDATE_TRANSPORT_SELECTORS_FORBIDDEN"
                ):
                    list(shards._iter_table_candidates(
                        package, manifest, "matches", {2: "match-000000"},
                        verified_files=token, verified_records=records,
                        candidate_package_kind="transport",
                    ))
        finally:
            source.close()

    def test_transport_candidate_early_close_releases_part_and_value_connections(self):
        with tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parents[2]) as temporary:
            source, _source_path, package, manifest, records, token = _candidate_transport_package(
                Path(temporary)
            )
            opened = []
            full_fetches = []
            real_open = shards._open_readonly

            def tracked_open(path):
                return _TrackedConnection(real_open(path), opened, full_fetches)

            try:
                with patch.object(shards, "_open_readonly", side_effect=tracked_open):
                    rows = shards._iter_table_candidates(
                        package, manifest, "candidate_rows", {1: "yes"},
                        verified_files=token, verified_records=records,
                        candidate_package_kind="transport",
                        candidate_row_filter=lambda _ordinal, _rowid: True,
                    )
                    row = next(rows)
                    self.assertEqual(row[1], 20)
                    self.assertTrue(any(not connection.closed for connection in opened))
                    rows.close()
                self.assertGreaterEqual(len(opened), 2)
                self.assertTrue(all(connection.closed for connection in opened))
            finally:
                source.close()

    def test_transport_candidate_rejects_same_bytes_with_replaced_file_identity(self):
        with tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parents[2]) as temporary:
            source, _source_path, package, manifest, records, token = _candidate_transport_package(
                Path(temporary)
            )
            try:
                relative = manifest["tables"][0]["parts"][0]["file"]
                target = package / relative
                original_bytes = target.read_bytes()
                replacement = target.with_name(target.name + ".replacement")
                replacement.write_bytes(original_bytes)
                self.assertEqual(_sha(replacement), records[relative]["sha256"])
                os.replace(replacement, target)
                with self.assertRaisesRegex(ValueError, "VERIFIED_FILES_CHANGED"):
                    list(shards._iter_table_candidates(
                        package,
                        manifest,
                        "candidate_rows",
                        {},
                        verified_files=token,
                        verified_records=records,
                        candidate_package_kind="transport",
                    ))
            finally:
                source.close()

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
