"""Read complete lossless SQLite shard generations without the source database.

This is the baseline reader for immutable generations. It exposes each source
table and its schema metadata directly; it does not infer related values by
joining primary/foreign keys, and it does not implement delta/change-feed
resolution.
"""

from __future__ import annotations

from contextlib import closing
import copy
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import stat
from typing import Any, Callable, Generator, Iterator, Optional

from . import lossless_sqlite as _shards
from . import slice_selectors as _selectors
from .verified_files import VerifiedFiles, verify_files


_SHA256 = re.compile(r"[0-9a-f]{64}\Z", re.ASCII)
_MODE = re.compile(r"[a-z0-9_]{1,64}\Z", re.ASCII)
_RULE_TOKEN = re.compile(r"[A-Za-z0-9_]{1,64}\Z", re.ASCII)
_COVERAGE_KEYS = ("all_tables", "all_rows", "all_columns", "all_values", "external_values")


def _quote(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _reject_duplicate_json_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def _control_fingerprint(info: os.stat_result) -> tuple[int, int, int, int, int]:
    return (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns)


def _read_control_json(path: Path, label: str) -> tuple[dict[str, Any], dict[str, Any]]:
    """Read and hash the exact stable bytes later bound into the package token."""
    try:
        before_path = os.lstat(path)
    except OSError as exc:
        raise ValueError(f"{label} is missing or unsafe") from exc
    if not stat.S_ISREG(before_path.st_mode):
        raise ValueError(f"{label} is not a regular file")
    nofollow = getattr(os, "O_NOFOLLOW", None)
    if nofollow is None:
        raise ValueError("verified control reads require O_NOFOLLOW")
    try:
        descriptor = os.open(
            path,
            os.O_RDONLY | nofollow | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NONBLOCK", 0),
        )
    except OSError as exc:
        raise ValueError(f"{label} cannot be opened safely") from exc
    try:
        before_fd = os.fstat(descriptor)
        fingerprint = _control_fingerprint(before_path)
        if not stat.S_ISREG(before_fd.st_mode) or _control_fingerprint(before_fd) != fingerprint:
            raise ValueError(f"{label} changed while opening")
        chunks: list[bytes] = []
        digest = hashlib.sha256()
        count = 0
        while True:
            block = os.read(descriptor, 1024 * 1024)
            if not block:
                break
            chunks.append(block)
            count += len(block)
            digest.update(block)
        after_fd = os.fstat(descriptor)
        after_path = os.lstat(path)
        if (_control_fingerprint(after_fd) != fingerprint
                or _control_fingerprint(after_path) != fingerprint
                or count != fingerprint[2]):
            raise ValueError(f"{label} changed while reading")
    except OSError as exc:
        raise ValueError(f"{label} cannot be read") from exc
    finally:
        os.close(descriptor)
    raw = b"".join(chunks)
    try:
        value = json.loads(raw, object_pairs_hook=_reject_duplicate_json_keys)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"{label} cannot be parsed") from exc
    if not isinstance(value, dict):
        raise ValueError(f"{label} must contain a JSON object")
    return value, {"bytes": count, "sha256": digest.hexdigest()}


def _safe_root(value: str | Path) -> Path:
    absolute = Path(os.path.abspath(os.fspath(value)))
    for candidate in reversed((absolute, *absolute.parents)):
        try:
            mode = candidate.lstat().st_mode
        except FileNotFoundError:
            continue
        if stat.S_ISLNK(mode):
            raise ValueError(f"shard root path contains a symbolic link: {candidate}")
    if not absolute.is_dir():
        raise ValueError("shard root is not a directory")
    return absolute.resolve(strict=True)


def _require_int(value: Any, label: str, *, minimum: int = 0) -> int:
    if type(value) is not int or value < minimum:
        raise ValueError(f"{label} is invalid")
    return value


def _require_sha256(value: Any, label: str) -> str:
    if not isinstance(value, str) or not _SHA256.fullmatch(value):
        raise ValueError(f"{label} is invalid")
    return value


def _canonical(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _reject_unknown_sqlite_headers(root: Path, declared: set[str]) -> None:
    signature = b"SQLite format 3\x00"
    for current, directories, files in os.walk(root, followlinks=False):
        current_path = Path(current)
        for directory in directories:
            if (current_path / directory).is_symlink():
                raise ValueError(f"shard tree contains a symbolic link: {(current_path / directory).relative_to(root)}")
        for filename in files:
            path = current_path / filename
            if path.is_symlink():
                raise ValueError(f"shard tree contains a symbolic link: {path.relative_to(root)}")
            relative = path.relative_to(root).as_posix()
            if relative in declared or not path.is_file():
                continue
            try:
                with path.open("rb") as stream:
                    if stream.read(len(signature)) == signature:
                        raise ValueError(f"SQLite file is absent from manifest inventory: {relative}")
            except OSError as exc:
                raise ValueError(f"shard file cannot be inspected: {relative}") from exc


def _same_sqlite_value(left: Any, right: Any) -> bool:
    if type(left) is not type(right):
        return False
    if isinstance(left, float):
        return left.hex() == right.hex()
    return left == right


class LosslessShardReader:
    """Stream a verified baseline shard package.

    The reader is deliberately baseline-only. Opening it validates package
    proofs, all declared file hashes, and the complete SQLite file inventory;
    full iteration retains per-table stream-digest verification. Candidate
    lookup reuses the open-time process-local hash token and decodes external
    payloads only for matched rows.
    """

    reader_role = "baseline_only_lossless_shard_reader"
    baseline_only = True

    def __init__(self, root: str | Path, expected_generation: Optional[str] = None):
        if expected_generation is not None and (
            not isinstance(expected_generation, str) or not expected_generation
        ):
            raise ValueError("expected_generation must be non-empty text or None")
        self._requested_root = Path(root)
        self._expected_generation = expected_generation
        self._root: Optional[Path] = None
        self._manifest: Optional[dict[str, Any]] = None
        self._verification: Optional[dict[str, Any]] = None
        self._selector_verification: Optional[dict[str, Any]] = None
        self._part_files: dict[str, dict[str, Any]] = {}
        self._external_files: dict[str, dict[str, Any]] = {}
        self._selector_files: dict[str, dict[str, Any]] = {}
        self._verified_files: Optional[VerifiedFiles] = None
        self._verified_records: dict[str, dict[str, Any]] = {}
        self._manifest_object_sha256: Optional[str] = None
        self._active = False
        self._iterators: set[Generator] = set()

    def __enter__(self) -> "LosslessShardReader":
        if self._active:
            raise RuntimeError("LosslessShardReader is already open")
        self._validate_package()
        self._active = True
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        close_error = None
        try:
            for iterator in tuple(self._iterators):
                try:
                    iterator.close()
                except BaseException as error:
                    if close_error is None:
                        close_error = error
            if self._active and self._verified_files is not None:
                try:
                    self._assert_verified_package()
                except BaseException as error:
                    if close_error is None:
                        close_error = error
        finally:
            self._iterators.clear()
            self._active = False
            self._root = None
            self._manifest = None
            self._verification = None
            self._selector_verification = None
            self._part_files = {}
            self._external_files = {}
            self._selector_files = {}
            self._verified_files = None
            self._verified_records = {}
            self._manifest_object_sha256 = None
        if close_error is not None:
            raise close_error

    def _track_iterator(self, factory: Callable[[], Iterator]) -> Generator:
        """Close nested SQLite iterators on early stop or reader context exit.

        Closing early releases resources; it does not claim that the table's
        end-of-stream digest and inventory checks have run to completion.
        """
        def rows():
            inner = None
            try:
                self._require_open()
                inner = iter(factory())
                while True:
                    self._require_open()
                    try:
                        item = next(inner)
                    except StopIteration:
                        return
                    yield item
            finally:
                try:
                    if inner is not None and hasattr(inner, "close"):
                        inner.close()
                finally:
                    self._iterators.discard(outer)

        outer = rows()
        self._iterators.add(outer)
        return outer

    def _require_open(self) -> tuple[Path, dict[str, Any]]:
        if not self._active or self._root is None or self._manifest is None:
            raise RuntimeError("LosslessShardReader must be used inside a with block")
        return self._root, self._manifest

    def _assert_verified_package(self) -> None:
        root, manifest = self._require_open()
        if self._verified_files is None:
            raise RuntimeError("verified package token is unavailable")
        current_manifest_sha = hashlib.sha256(_canonical(manifest).encode("utf-8")).hexdigest()
        if current_manifest_sha != self._manifest_object_sha256:
            raise ValueError("in-memory manifest differs from the verified control file")
        self._verified_files.assert_matches(root, self._verified_records)
        declared_sqlite = set(self._part_files) | set(self._external_files) | set(self._selector_files)
        _shards._check_unlisted_sqlite_files(root, declared_sqlite)
        _reject_unknown_sqlite_headers(root, declared_sqlite)

    def _validate_manifest_tables(self, manifest: dict[str, Any], tables: list[dict[str, Any]]) -> None:
        names: list[str] = []
        for table in tables:
            name = table["name"]
            names.append(name)
            columns = table.get("columns")
            xinfo = table.get("column_schema")
            if not isinstance(columns, list) or not all(isinstance(item, str) for item in columns):
                raise ValueError(f"invalid source columns for {name!r}")
            if not isinstance(xinfo, list):
                raise ValueError(f"invalid table_xinfo for {name!r}")
            required_xinfo = {"cid", "name", "type", "notnull", "dflt_value", "pk", "hidden"}
            if any(not isinstance(column, dict) or set(column) != required_xinfo for column in xinfo):
                raise ValueError(f"invalid table_xinfo columns for {name!r}")
            if any(
                type(column["cid"]) is not int
                or not isinstance(column["name"], str)
                or (column["type"] is not None and not isinstance(column["type"], str))
                or type(column["notnull"]) is not int
                or (column["dflt_value"] is not None and not isinstance(column["dflt_value"], (str, int, float)))
                or type(column["pk"]) is not int
                or type(column["hidden"]) is not int
                for column in xinfo
            ):
                raise ValueError(f"invalid table_xinfo value for {name!r}")
            visible = [column["name"] for column in xinfo if column["hidden"] != 1]
            if columns != visible or len(columns) != len(set(columns)):
                raise ValueError(f"columns do not match table_xinfo for {name!r}")
            _require_int(table.get("row_count"), f"row_count for {name!r}")
            rowid_kind = table.get("rowid_kind")
            rowid_column = table.get("source_rowid_column")
            shadowed_aliases = table.get("rowid_aliases_shadowed")
            if rowid_kind == "rowid":
                if (not isinstance(rowid_column, str)
                        or rowid_column not in {"_rowid_", "rowid", "oid"}
                        or rowid_column.casefold() in {column.casefold() for column in columns}
                        or shadowed_aliases is not None):
                    raise ValueError(f"source rowid metadata is invalid for {name!r}")
            elif rowid_kind == "without_rowid_primary_key":
                if rowid_column is not None or shadowed_aliases is not None or not any(
                    column["pk"] for column in xinfo
                ):
                    raise ValueError(f"WITHOUT ROWID metadata is invalid for {name!r}")
            elif rowid_kind is None and shadowed_aliases is True:
                if rowid_column is not None:
                    raise ValueError(f"shadowed rowid metadata is invalid for {name!r}")
            else:
                raise ValueError(f"source row identity kind is missing or unsupported for {name!r}")
            _require_sha256(table.get("row_stream_sha256"), f"row stream hash for {name!r}")
            foreign_keys = table.get("foreign_keys")
            if not isinstance(foreign_keys, list) or any(
                not isinstance(row, dict)
                or set(row) != {"id", "seq", "table", "from", "to", "on_update", "on_delete", "match"}
                for row in foreign_keys
            ):
                raise ValueError(f"foreign key metadata is invalid for {name!r}")
        if names != [row["name"] for row in manifest["schema_objects"] if row["type"] == "table"]:
            raise ValueError("manifest table list differs from schema_objects")
        if "matches" not in names or "match_classification" not in names:
            raise ValueError("selector source tables are missing from the manifest")

    def _selector_summary(self, manifest: dict[str, Any]) -> dict[str, int]:
        by_mode = manifest.get("by_mode")
        by_rule = manifest.get("by_rule")
        if not isinstance(by_mode, list) or not isinstance(by_rule, list):
            raise ValueError("complete by_mode and by_rule selector inventories are required")

        mode_names: list[str] = []
        for row in by_mode:
            if not isinstance(row, dict):
                raise ValueError("by_mode selector metadata is invalid")
            mode = row.get("analysis_set")
            if not isinstance(mode, str) or not _MODE.fullmatch(mode):
                raise ValueError("by_mode selector name is invalid")
            _require_int(row.get("matches"), f"matches count for mode {mode!r}")
            mode_names.append(mode)
        if len(mode_names) != len(set(mode_names)):
            raise ValueError("by_mode selector inventory contains duplicates")

        rule_pairs: list[tuple[str, str]] = []
        populated_modes: set[str] = set()
        rules: set[str] = set()
        for row in by_rule:
            if not isinstance(row, dict):
                raise ValueError("by_rule selector metadata is invalid")
            mode = row.get("analysis_set")
            raw_rule = row.get("rule_raw")
            if not isinstance(mode, str) or not _MODE.fullmatch(mode):
                raise ValueError("by_rule selector name is invalid")
            if raw_rule is not None and not isinstance(raw_rule, str):
                raise ValueError("by_rule selector rule_raw must be text or null")
            _require_int(row.get("matches"), f"matches count for rule selector {mode!r}")
            raw_key = _canonical(raw_rule)
            rule_pairs.append((mode, raw_key))
            populated_modes.add(mode)
            rules.add(raw_key)
        if len(rule_pairs) != len(set(rule_pairs)):
            raise ValueError("by_rule selector inventory contains duplicates")

        expected_modes = set(_selectors.MODE_SLICES) | populated_modes | {_selectors.UNCLASSIFIED}
        if set(mode_names) != expected_modes:
            raise ValueError("by_mode selector inventory is incomplete")
        expected_pairs = {
            (mode, raw)
            for mode in populated_modes
            for raw in rules
        }
        if set(rule_pairs) != expected_pairs:
            raise ValueError("by_rule selector inventory is incomplete")

        expected = {
            "mode_files": len(by_mode),
            "rule_files": len(by_rule),
            "distinct_modes_with_matches": len(populated_modes),
            "distinct_rules": len(rules),
            "rule_mode_product": len(expected_pairs),
        }
        if manifest.get("counts") != expected:
            raise ValueError("manifest selector counts are inconsistent")
        return expected

    def _validate_verification(
        self,
        root: Path,
        receipt: dict[str, Any],
        manifest: dict[str, Any],
        tables: list[dict[str, Any]],
        part_files: dict[str, dict[str, Any]],
        external_files: dict[str, dict[str, Any]],
    ) -> None:
        source_sha = manifest["source_sha256"]
        snapshot_id = manifest["snapshot_identifier"]
        schema_sha = manifest["source_schema_sha256"]
        if (
            receipt.get("version") != _shards.MANIFEST_VERSION
            or receipt.get("role") != _shards.MANIFEST_ROLE
            or receipt.get("status") != "verified"
            or receipt.get("snapshot_identifier") != snapshot_id
            or receipt.get("source_sha256") != source_sha
            or receipt.get("source_schema_sha256") != schema_sha
        ):
            raise ValueError("full SQLite verification proof does not match the generation")
        coverage = receipt.get("coverage")
        if not isinstance(coverage, dict) or any(coverage.get(key) is not True for key in _COVERAGE_KEYS):
            raise ValueError("full SQLite verification proof does not cover every value")

        expected_rows = {table["name"]: table["row_count"] for table in tables}
        expected_row_count = sum(expected_rows.values())
        expected_cell_count = sum(table["row_count"] * len(table["columns"]) for table in tables)
        # One external cell can span several value files. Their cell_count
        # fields count incidences per file, not unique original source cells.
        expected_external_cells = 0
        for table in tables:
            metadata_table = table.get("archive_metadata_tables", {}).get("external_cells")
            if not isinstance(metadata_table, str) or not metadata_table:
                raise ValueError("full SQLite external cell metadata table is missing")
            for part in table["parts"]:
                # Per-row metadata stores one record per original cell even
                # when that cell's chunks span several value shard files.
                path = _shards._safe_file(root, part["file"])
                with closing(_shards._open_readonly(path)) as conn:
                    expected_external_cells += conn.execute(
                        f"SELECT COUNT(*) FROM {_quote(metadata_table)}"
                    ).fetchone()[0]
        external_incidences = sum(record["cell_count"] for record in external_files.values())
        if (type(expected_external_cells) is not int
                or expected_external_cells < 0
                or expected_external_cells > external_incidences
                or (external_incidences > 0 and expected_external_cells == 0)):
            raise ValueError("full SQLite external cell summary is invalid")
        expected_chunks = sum(record["chunk_count"] for record in external_files.values())
        expected_values = {
            "table_count": len(tables),
            "row_counts": expected_rows,
            "row_count": expected_row_count,
            "cell_count": expected_cell_count,
            "external_cell_count": expected_external_cells,
            "value_chunk_count": expected_chunks,
            "file_count": len(part_files) + len(external_files),
        }
        receipt_rows = receipt.get("row_counts")
        if (
            not isinstance(receipt_rows, dict)
            or set(receipt_rows) != set(expected_rows)
            or any(type(value) is not int or value < 0 for value in receipt_rows.values())
            or receipt_rows != expected_rows
        ):
            raise ValueError("full SQLite verification row counts differ from the manifest")
        for key, expected in expected_values.items():
            found = receipt.get(key)
            if key == "row_counts":
                continue
            if type(found) is not int or found != expected:
                raise ValueError(f"full SQLite verification {key} differs from the manifest")
        fk_violations = receipt.get("known_source_foreign_key_violations")
        if not isinstance(fk_violations, dict) or any(
            not isinstance(name, str) or type(count) is not int or count < 0
            for name, count in fk_violations.items()
        ):
            raise ValueError("full SQLite verification foreign key summary is invalid")

    def _validate_selector_verification(
        self, receipt: dict[str, Any], manifest: dict[str, Any], expected: dict[str, int]
    ) -> None:
        if (
            receipt.get("status") != "verified"
            or receipt.get("snapshot_identifier") != manifest["snapshot_identifier"]
            or receipt.get("source_sha256") != manifest["source_sha256"]
            or receipt.get("all_shared_files_reachable") is not True
            or receipt.get("all_mode_matches") is not True
            or receipt.get("all_rule_matches") is not True
        ):
            raise ValueError("selector verification proof is incomplete or cross-generation")
        for key, value in expected.items():
            if type(receipt.get(key)) is not int or receipt[key] != value:
                raise ValueError(f"selector verification {key} differs from the manifest")

    def _validate_package(self) -> None:
        root = _safe_root(self._requested_root)
        manifest, manifest_record = _read_control_json(root / "manifest.json", "manifest.json")
        verification, verification_record = _read_control_json(
            root / "verification.json", "verification.json"
        )
        selector_verification, selector_verification_record = _read_control_json(
            root / "selectors-verification.json", "selectors-verification.json"
        )
        tables = _shards._manifest_tables(manifest)
        if self._expected_generation is not None and manifest.get("snapshot_identifier") != self._expected_generation:
            raise ValueError("shard generation does not match expected_generation")
        snapshot_id = manifest.get("snapshot_identifier")
        if not isinstance(snapshot_id, str) or not snapshot_id:
            raise ValueError("manifest snapshot_identifier is invalid")
        source_sha = _require_sha256(manifest.get("source_sha256"), "manifest source_sha256")
        schema_sha = _require_sha256(
            manifest.get("source_schema_sha256"), "manifest source_schema_sha256"
        )
        if _shards._schema_sha256(manifest.get("schema_objects")) != schema_sha:
            raise ValueError("manifest schema SHA-256 mismatch")
        max_bytes = manifest.get("max_bytes")
        if type(max_bytes) is not int or not _shards.MIN_MAX_BYTES <= max_bytes <= _shards.MAX_ALLOWED_BYTES:
            raise ValueError("manifest max_bytes is invalid")
        self._validate_manifest_tables(manifest, tables)

        part_files, external_files, selector_files = _shards._declared_files(
            manifest, root, verify_hashes=False
        )
        declared = set(part_files) | set(external_files) | set(selector_files)
        _shards._check_unlisted_sqlite_files(root, declared)
        _reject_unknown_sqlite_headers(root, declared)

        # Validate every declared file's size/cap before binding the whole
        # package to one process-local full-SHA token.
        for relative, record in part_files.items():
            _shards._check_file_record(root, relative, record, max_bytes, verify_hash=False)
        for relative, record in external_files.items():
            _shards._check_file_record(root, relative, record, max_bytes, verify_hash=False)
        for relative, record in selector_files.items():
            _shards._check_file_record(
                root, relative, record, _shards.MAX_SELECTOR_BYTES, verify_hash=False
            )

        records: dict[str, dict[str, Any]] = {
            "manifest.json": manifest_record,
            "verification.json": verification_record,
            "selectors-verification.json": selector_verification_record,
        }
        for declared_group in (part_files, external_files, selector_files):
            for relative, record in declared_group.items():
                if relative in records:
                    raise ValueError("control and shard file paths overlap")
                records[relative] = {"bytes": record["bytes"], "sha256": record["sha256"]}
        token = verify_files(root, records)

        self._validate_verification(
            root, verification, manifest, tables, part_files, external_files
        )
        selector_summary = self._selector_summary(manifest)
        self._validate_selector_verification(selector_verification, manifest, selector_summary)

        token.assert_matches(root, records)
        _shards._check_unlisted_sqlite_files(root, declared)
        _reject_unknown_sqlite_headers(root, declared)

        self._root = root
        self._manifest = manifest
        self._verification = verification
        self._selector_verification = selector_verification
        self._part_files = part_files
        self._external_files = external_files
        self._selector_files = selector_files
        self._verified_files = token
        self._verified_records = records
        self._manifest_object_sha256 = hashlib.sha256(
            _canonical(manifest).encode("utf-8")
        ).hexdigest()

    def _table_metadata(self, table: str) -> dict[str, Any]:
        _, manifest = self._require_open()
        if not isinstance(table, str):
            raise TypeError("table name must be text")
        for item in manifest["tables"]:
            if item["name"] == table:
                return item
        raise KeyError(f"table is not in manifest: {table}")

    def tables(self) -> list[dict[str, Any]]:
        """Return metadata for every source table, including empty/internal tables."""
        _, manifest = self._require_open()
        return copy.deepcopy(manifest["tables"])

    @property
    def snapshot_identifier(self) -> str:
        return self._require_open()[1]['snapshot_identifier']

    @property
    def source_sha256(self) -> str:
        return self._require_open()[1]['source_sha256']

    @property
    def source_schema_sha256(self) -> str:
        return self._require_open()[1]['source_schema_sha256']

    def schema_objects(self) -> list[dict[str, Any]]:
        """Return the complete sqlite_master object inventory from the source."""
        _, manifest = self._require_open()
        return copy.deepcopy(manifest["schema_objects"])

    def columns(self, table: str) -> list[dict[str, Any]]:
        """Return source PRAGMA table_xinfo definitions for ``table``."""
        return copy.deepcopy(self._table_metadata(table)["column_schema"])

    def foreign_keys(self, table: str) -> list[dict[str, Any]]:
        """Return the source foreign_key_list definitions without joining rows."""
        return copy.deepcopy(self._table_metadata(table)["foreign_keys"])

    def iter_rows(self, table: str) -> Iterator[tuple[int, tuple[Any, ...]]]:
        """Yield every typed source row as ``(ordinal, values)`` in archive order."""
        root, manifest = self._require_open()
        self._table_metadata(table)

        def rows() -> Generator[tuple[int, tuple[Any, ...]], None, None]:
            self._require_open()
            yield from _shards.iter_table_rows(root, manifest, table)

        return self._track_iterator(rows)

    def iter_rows_with_identity(self, table: str) -> Iterator[tuple[int, Any, tuple[Any, ...]]]:
        """Yield archive ordinal, original hidden rowid, and native values."""
        root, manifest = self._require_open()
        self._table_metadata(table)
        def rows():
            self._require_open()
            yield from _shards.iter_table_rows_with_identity(root, manifest, table)
        return self._track_iterator(rows)

    def find_rows(
        self, table: str, criteria: dict[str, Any]
    ) -> Iterator[tuple[int, tuple[Any, ...]]]:
        """Stream rows whose named values match with exact SQLite types."""
        self._require_open()
        metadata = self._table_metadata(table)
        if not isinstance(criteria, dict):
            raise TypeError("criteria must be a dict of column names to native SQLite values")
        columns = metadata["columns"]
        unknown = set(criteria) - set(columns)
        if unknown:
            raise KeyError(f"unknown column(s) for {table!r}: {', '.join(sorted(map(str, unknown)))}")
        indexes = {name: columns.index(name) for name in criteria}

        def rows() -> Generator[tuple[int, tuple[Any, ...]], None, None]:
            with closing(self.iter_rows(table)) as source_rows:
                for ordinal, values in source_rows:
                    if all(_same_sqlite_value(values[indexes[name]], expected) for name, expected in criteria.items()):
                        yield ordinal, values

        return self._track_iterator(rows)

    def lookup_rows_with_identity(
        self, table: str, criteria: dict[str, Any]
    ) -> Iterator[tuple[int, Any, tuple[Any, ...]]]:
        """Find rows with native values and source identity from a verified package.

        The package's full file hashes and baseline proof were checked when
        this reader opened. This lookup rechecks the complete token and SQLite
        inventory, scans row/column/external-cell metadata, and decodes large
        external values only for candidate rows. It does not recompute the
        complete row-stream digest or recount every value-chunk file.
        """
        self._require_open()
        metadata = self._table_metadata(table)
        if not isinstance(criteria, dict):
            raise TypeError("criteria must be a dict of column names to native SQLite values")
        columns = metadata["columns"]
        unknown = set(criteria) - set(columns)
        if unknown:
            raise KeyError(f"unknown column(s) for {table!r}: {', '.join(sorted(map(str, unknown)))}")
        indexes = {columns.index(name): value for name, value in criteria.items()}
        self._assert_verified_package()

        def rows() -> Generator[tuple[int, Any, tuple[Any, ...]], None, None]:
            root, manifest = self._require_open()
            try:
                with closing(_shards._iter_table_candidates(
                    root,
                    manifest,
                    table,
                    indexes,
                    verified_files=self._verified_files,
                    verified_records=self._verified_records,
                )) as candidates:
                    for entry in candidates:
                        self._require_open()
                        self._assert_verified_package()
                        yield entry
            finally:
                # This also runs when the consumer closes the generator early.
                self._assert_verified_package()

        return self._track_iterator(rows)

    def _resolve_selector(self, mode: str, rule_token: Optional[str]) -> tuple[str, dict[str, Any], str]:
        _, manifest = self._require_open()
        if not isinstance(mode, str) or not _MODE.fullmatch(mode):
            raise ValueError("analysis mode is invalid")
        if rule_token is None:
            matches = [row for row in manifest["by_mode"] if row["analysis_set"] == mode]
            relative = f"by-mode/{mode}.sqlite3"
        else:
            if not isinstance(rule_token, str) or not _RULE_TOKEN.fullmatch(rule_token):
                raise ValueError("rule_token is invalid")
            relative = f"by-rule/{mode}__{rule_token}.sqlite3"
            matches = [row for row in manifest["by_rule"] if row["file"] == relative]
        if len(matches) != 1:
            raise KeyError(f"selector is not present: {relative}")
        return ("by_rule" if rule_token is not None else "by_mode", matches[0], relative)

    def _verify_selector_dependencies(self, row: dict[str, Any]) -> None:
        self._require_open()
        self._assert_verified_package()
        selector_relative = row["file"]
        record = self._selector_files.get(selector_relative)
        if (record is None or record.get("bytes") != row.get("bytes")
                or record.get("sha256") != row.get("sha256")):
            raise ValueError("selector is not bound to the verified package token")

    def _validate_selector_database(
        self,
        connection: sqlite3.Connection,
        axis: str,
        row: dict[str, Any],
        relative: str,
    ) -> None:
        root, manifest = self._require_open()
        if connection.execute("PRAGMA quick_check").fetchone() != ("ok",):
            raise ValueError(f"selector integrity check failed: {relative}")
        actual_tables = {
            item[0]
            for item in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }
        expected_tables = {
            "slice_meta", "shared_files", "archive_schema_objects",
            "archive_table_columns", "archive_foreign_keys", "matches", "match_classification",
        }
        if actual_tables != expected_tables:
            raise ValueError(f"selector table inventory mismatch: {relative}")
        try:
            meta = dict(connection.execute("SELECT key,value FROM slice_meta"))
        except sqlite3.DatabaseError as exc:
            raise ValueError(f"selector metadata is invalid: {relative}") from exc
        expected_meta = {
            "role": "lossless_slice_selector",
            "snapshot_identifier": manifest["snapshot_identifier"],
            "source_sha256": manifest["source_sha256"],
            "source_schema_sha256": manifest["source_schema_sha256"],
            "axis": "rule" if axis == "by_rule" else "mode",
            "analysis_set": row["analysis_set"],
            "rule_raw_json": json.dumps(row.get("rule_raw"), ensure_ascii=False),
            "manifest": "manifest.json",
            "root_from_selector": "..",
            "information_scope": "all_source_tables_and_values_via_shared_files",
        }
        if meta != expected_meta:
            raise ValueError(f"selector generation metadata mismatch: {relative}")

        dependencies: dict[str, dict[str, Any]] = {}
        dependencies.update(self._part_files)
        dependencies.update(self._external_files)
        expected_dependencies = [
            (name, record["bytes"], record["sha256"])
            for name, record in sorted(dependencies.items())
        ]
        found_dependencies = list(
            connection.execute("SELECT file,bytes,sha256 FROM shared_files ORDER BY file")
        )
        if found_dependencies != expected_dependencies:
            raise ValueError(f"selector shared_files dependency closure mismatch: {relative}")

        expected_schema = [
            (item["type"], item["name"], item["tbl_name"], item["sql"])
            for item in manifest["schema_objects"]
        ]
        found_schema = list(
            connection.execute(
                "SELECT type,name,tbl_name,sql FROM archive_schema_objects ORDER BY type,name"
            )
        )
        if found_schema != expected_schema:
            raise ValueError(f"selector schema object inventory mismatch: {relative}")

        expected_columns: list[tuple[str, int, str]] = []
        expected_foreign_keys: list[tuple[str, int, int, str]] = []
        for table in manifest["tables"]:
            for ordinal, column in enumerate(table["column_schema"]):
                expected_columns.append(
                    (table["name"], ordinal, json.dumps(column, ensure_ascii=False, sort_keys=True))
                )
            for foreign_key in table["foreign_keys"]:
                definition = tuple(
                    foreign_key[key]
                    for key in ("id", "seq", "table", "from", "to", "on_update", "on_delete", "match")
                )
                expected_foreign_keys.append(
                    (
                        table["name"],
                        foreign_key["id"],
                        foreign_key["seq"],
                        json.dumps(definition, ensure_ascii=False),
                    )
                )
        found_columns = list(
            connection.execute(
                "SELECT table_name,column_ordinal,definition_json FROM archive_table_columns "
                "ORDER BY table_name,column_ordinal"
            )
        )
        if found_columns != sorted(expected_columns):
            raise ValueError(f"selector table column metadata mismatch: {relative}")
        found_foreign_keys = list(
            connection.execute(
                "SELECT table_name,id,seq,definition_json FROM archive_foreign_keys "
                "ORDER BY table_name,id,seq"
            )
        )
        if found_foreign_keys != sorted(expected_foreign_keys):
            raise ValueError(f"selector foreign key metadata mismatch: {relative}")

        by_table = {table["name"]: table for table in manifest["tables"]}
        for table_name in ("matches", "match_classification"):
            info = connection.execute(f"PRAGMA table_xinfo({_quote(table_name)})").fetchall()
            expected_names = by_table[table_name]["columns"]
            if [item[1] for item in info] != expected_names or any(item[2] not in (None, "") for item in info):
                raise ValueError(f"selector seed columns differ for {table_name!r}: {relative}")

        classification = by_table["match_classification"]["columns"]
        if axis == "by_mode" and row["analysis_set"] == _selectors.UNCLASSIFIED:
            if connection.execute("SELECT 1 FROM match_classification LIMIT 1").fetchone() is not None:
                raise ValueError(f"unclassified selector contains classifications: {relative}")
        else:
            count_sql = f"SELECT COUNT(*) FROM match_classification WHERE {_quote('analysis_set')} IS NOT ?"
            params: tuple[Any, ...] = (row["analysis_set"],)
            if axis == "by_rule":
                count_sql += f" OR {_quote('rule_raw')} IS NOT ?"
                params += (row["rule_raw"],)
            if int(connection.execute(count_sql, params).fetchone()[0]) != 0:
                raise ValueError(f"selector classification scope mismatch: {relative}")

        matches_count = int(connection.execute("SELECT COUNT(*) FROM matches").fetchone()[0])
        if matches_count != row.get("matches"):
            raise ValueError(f"selector match count mismatch: {relative}")

    def iter_selected_matches(
        self, mode: str, rule_token: Optional[str] = None
    ) -> Iterator[tuple[Any, ...]]:
        """Yield complete ``matches`` seed rows from one verified selector.

        ``rule_token`` is the canonical path token (for example the value
        returned by ``slice_selectors.rule_token(raw_rule)``), not the raw rule.
        The selected database also has to reference and verify every shared
        source table/value artifact before its seed rows are returned.
        """
        self._require_open()
        axis, row, relative = self._resolve_selector(mode, rule_token)

        def rows() -> Generator[tuple[Any, ...], None, None]:
            root, manifest = self._require_open()
            try:
                self._verify_selector_dependencies(row)
                path = root.joinpath(*Path(relative).parts)
                with closing(_shards._open_readonly(path)) as connection:
                    self._validate_selector_database(connection, axis, row, relative)
                    cursor = connection.execute(
                        f"SELECT * FROM {_quote('matches')} "
                        f"ORDER BY {_quote('account')},{_quote('kind')},{_quote('match_key')}"
                    )
                    for values in cursor:
                        yield tuple(values)
            finally:
                self._assert_verified_package()

        return self._track_iterator(rows)


__all__ = ["LosslessShardReader"]
