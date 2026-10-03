"""Read typed candidate rows from a verified native SQLite delta transport.

The adapter binds the transport database to a complete process-local
``VerifiedFiles`` package token. It validates the transport document, physical
table inventory, and every table count once at construction. Candidate scans
project only hidden rowid plus requested columns; complete rows are fetched only
after exact typed criteria and the caller's strict-boolean filter both match.
The filter can therefore reject a candidate without materializing unrelated
large BLOB or TEXT cells in Python.
"""

from __future__ import annotations

import copy
import os
from pathlib import Path
import sqlite3
from types import MappingProxyType
from typing import Any, Callable, Iterator, Mapping, Optional

from . import delta_transport as _delta
from . import lossless_sqlite as _lossless
from .verified_files import VerifiedFiles


_OPERATION_COLUMNS = tuple(item[0] for item in _delta._OPS_COLUMNS)


def _fail(code: str) -> ValueError:
    return ValueError(code)


def _native_value_ok(value: Any) -> bool:
    return _delta._type_ok(value)


def _criteria_value_ok(value: Any) -> bool:
    return _native_value_ok(value)


def _same_native_value(left: Any, right: Any) -> bool:
    return _lossless._same_sqlite_value(left, right)


def _close_preserving_active_exception(connection: Any) -> None:
    try:
        connection.close()
    except BaseException:
        pass


def _reject_sidecars(path: Path) -> None:
    for suffix in ("-wal", "-shm", "-journal"):
        sidecar = Path(str(path) + suffix)
        try:
            sidecar.lstat()
        except FileNotFoundError:
            continue
        except OSError as exc:
            raise _fail("NATIVE_TRANSPORT_SIDECAR_INVALID") from exc
        raise _fail("NATIVE_TRANSPORT_SIDECAR_PRESENT")


def _connect_readonly(path: Path) -> sqlite3.Connection:
    """Open one guarded immutable reader connection, closing on setup failure."""
    safe_path = _delta._check_path_components(path, require_exists=True)
    _reject_sidecars(safe_path)
    connection = sqlite3.connect(
        safe_path.as_uri() + "?mode=ro&immutable=1", uri=True, timeout=30,
    )
    try:
        _execute_and_close(connection, "PRAGMA query_only=ON")
        _execute_and_close(connection, "PRAGMA trusted_schema=OFF")
    except BaseException:
        _close_preserving_active_exception(connection)
        raise
    return connection


def _execute_and_close(connection: Any, sql: str) -> None:
    cursor = connection.execute(sql)
    cursor.close()


def _safe_close_cursor(cursor: Any) -> None:
    if cursor is not None:
        cursor.close()


def _fetch_one(connection: Any, sql: str, parameters: tuple[Any, ...] = ()) -> Any:
    cursor = connection.execute(sql, parameters)
    primary: Optional[BaseException] = None
    try:
        return cursor.fetchone()
    except BaseException as exc:
        primary = exc
        raise
    finally:
        try:
            _safe_close_cursor(cursor)
        except BaseException:
            if primary is None:
                raise


def _fetch_all(connection: Any, sql: str, parameters: tuple[Any, ...] = ()) -> list[Any]:
    cursor = connection.execute(sql, parameters)
    primary: Optional[BaseException] = None
    try:
        return cursor.fetchall()
    except BaseException as exc:
        primary = exc
        raise
    finally:
        try:
            _safe_close_cursor(cursor)
        except BaseException:
            if primary is None:
                raise


def _unused_projection_alias(prefix: str, columns: tuple[str, ...]) -> str:
    used = {name.casefold() for name in columns}
    alias = prefix
    suffix = 0
    while alias.casefold() in used:
        suffix += 1
        alias = f"{prefix}_{suffix}"
    return alias


class NativeTransportCandidates:
    """Typed candidate access for one transport DB in a full verified package.

    Each iterator opens and owns its own read-only SQLite connection. Call its
    ``close()`` method when stopping early; exhaustion and all error paths also
    release the cursor and connection. The full parent file token is checked
    before and after construction and every iterator, without rehashing files.
    """

    def __init__(
        self,
        root: str | os.PathLike[str],
        database_relative: str,
        document: Mapping[str, Any],
        *,
        verified_files: VerifiedFiles,
        verified_records: Mapping[str, Mapping[str, Any]],
    ) -> None:
        self._verified_files = verified_files
        self._records: dict[str, dict[str, Any]] = {}
        self._root_input = root
        self._database_relative = database_relative

        connection: Optional[sqlite3.Connection] = None
        primary: Optional[BaseException] = None
        cleanup_error: Optional[BaseException] = None
        try:
            if type(verified_files) is not VerifiedFiles:
                raise _fail("NATIVE_TRANSPORT_VERIFIED_FILES_INVALID")
            self._records = copy.deepcopy(dict(verified_records))
            self._root_input = str(verified_files.root)
            verified_files.assert_matches(root, self._records)

            copied = copy.deepcopy(dict(document)) if isinstance(document, Mapping) else document
            database = copied.get("database") if isinstance(copied, dict) else None
            if not isinstance(database, dict):
                raise _fail("NATIVE_TRANSPORT_DOCUMENT_INVALID")
            database_record = {
                "bytes": database.get("bytes"),
                "sha256": database.get("sha256"),
            }
            database_path = verified_files.checked_path(database_relative, database_record)

            # delta_transport binds the document's original path to the
            # database field. Here only the copy is relocated to this package.
            relocated = copy.deepcopy(copied)
            relocated["database"]["path"] = str(database_path)
            metadata, internal, table_names, _xinfo, visible, validated_path = (
                _delta._validate_document(relocated, database_path)
            )
            if validated_path != database_path:
                raise _fail("NATIVE_TRANSPORT_DATABASE_PATH_MISMATCH")

            connection = _connect_readonly(database_path)
            _delta._validate_database_inventory(
                connection, metadata, internal, table_names, _xinfo, visible,
            )

            operation_counts = relocated["operation_counts"]
            expected_counts = {
                table: relocated["upsert_counts_by_table"].get(table, 0)
                for table in table_names
            }
            operations_table = internal["operations_table"]
            metadata_table = internal["metadata_table"]
            expected_counts[operations_table] = sum(operation_counts.values())
            expected_counts[metadata_table] = 1
            for table, expected in expected_counts.items():
                row = _fetch_one(
                    connection,
                    f"SELECT COUNT(*) FROM {_delta._quote(table)}",
                )
                if (row is None or len(row) != 1 or type(row[0]) is not int
                        or row[0] != expected):
                    raise _fail("NATIVE_TRANSPORT_TABLE_COUNT_MISMATCH")

            reference_rows = _fetch_all(
                connection,
                f"SELECT \"table_name\",COUNT(*),COUNT(DISTINCT \"transport_rowid\"),"
                "SUM(CASE WHEN typeof(\"transport_rowid\")='integer' "
                "AND \"transport_rowid\">0 THEN 1 ELSE 0 END) "
                f"FROM {_delta._quote(operations_table)} "
                "WHERE \"operation\"=? GROUP BY \"table_name\"",
                ("upsert",),
            )
            reference_counts: dict[str, tuple[int, int, int]] = {}
            for row in reference_rows:
                if (len(row) != 4 or not isinstance(row[0], str)
                        or row[0] not in table_names
                        or any(type(value) is not int for value in row[1:])):
                    raise _fail("NATIVE_TRANSPORT_SOURCE_REFERENCES_INVALID")
                reference_counts[row[0]] = (row[1], row[2], row[3])
            for table in table_names:
                expected = expected_counts[table]
                if reference_counts.get(table, (0, 0, 0)) != (
                    expected, expected, expected,
                ):
                    raise _fail("NATIVE_TRANSPORT_SOURCE_REFERENCES_INVALID")

            columns_by_table: dict[str, tuple[str, ...]] = {
                table: tuple(visible[table]) for table in table_names
            }
            columns_by_table[operations_table] = _OPERATION_COLUMNS
            columns_by_table[metadata_table] = ("name", "value")
            documents: dict[str, Mapping[str, Any]] = {}
            for table, columns in columns_by_table.items():
                documents[table] = MappingProxyType({
                    "name": table,
                    "columns": columns,
                    "row_count": expected_counts[table],
                })

            self._table_names = tuple(table_names)
            self._visible = MappingProxyType(columns_by_table)
            self._table_documents = MappingProxyType(documents)
            self._operations_table = operations_table
            self._metadata_table = metadata_table
            self._expected_counts = MappingProxyType(dict(expected_counts))
            self._database_record = MappingProxyType(database_record)
        except sqlite3.Error:
            primary = _fail("NATIVE_TRANSPORT_DATABASE_INVALID")
            raise primary from None
        except BaseException as exc:
            primary = exc
            raise
        finally:
            if connection is not None:
                try:
                    connection.close()
                except BaseException as exc:
                    if primary is None:
                        cleanup_error = exc
            try:
                if type(verified_files) is VerifiedFiles:
                    verified_files.assert_matches(root, self._records)
            except BaseException as exc:
                if primary is None and cleanup_error is None:
                    cleanup_error = exc
            if primary is None and cleanup_error is not None:
                if isinstance(cleanup_error, sqlite3.Error):
                    raise _fail("NATIVE_TRANSPORT_DATABASE_INVALID") from None
                raise cleanup_error

    @property
    def table_names(self) -> list[str]:
        return list(self._table_names)

    @property
    def visible(self) -> dict[str, list[str]]:
        return {table: list(columns) for table, columns in self._visible.items()
                if table in self._table_names}

    @property
    def operations_table(self) -> str:
        return self._operations_table

    @property
    def metadata_table(self) -> str:
        return self._metadata_table

    @property
    def table_documents(self) -> dict[str, dict[str, Any]]:
        return {
            table: {
                "name": document["name"],
                "columns": list(document["columns"]),
                "row_count": document["row_count"],
            }
            for table, document in self._table_documents.items()
        }

    def iter_table_candidates(
        self,
        table: str,
        criteria_by_column: Mapping[int, Any],
        *,
        candidate_row_filter: Optional[Callable[[int, int], bool]] = None,
    ) -> Iterator[tuple[int, int, tuple[Any, ...]]]:
        """Yield exact native rows matching selected columns and row filter.

        ``criteria_by_column`` keys are zero-based visible-column ordinals.
        The returned second element is the transport table's actual hidden
        SQLite rowid, not a reconstructed source-row identity.
        """
        return self._iter_table_candidates(table, criteria_by_column,
                                           candidate_row_filter=candidate_row_filter)

    def _assert_parent(self) -> None:
        self._verified_files.assert_matches(self._root_input, self._records)

    def _iter_table_candidates(
        self,
        table: str,
        criteria_by_column: Mapping[int, Any],
        *,
        candidate_row_filter: Optional[Callable[[int, int], bool]],
    ) -> Iterator[tuple[int, int, tuple[Any, ...]]]:
        connection: Optional[sqlite3.Connection] = None
        scan_cursor: Any = None
        primary: Optional[BaseException] = None
        cleanup_error: Optional[BaseException] = None
        callback_baseexception = False
        try:
            self._assert_parent()
            if not isinstance(table, str) or table not in self._visible:
                raise _fail("NATIVE_TRANSPORT_TABLE_UNKNOWN")
            columns = self._visible[table]
            if (not isinstance(criteria_by_column, Mapping)
                    or any(type(index) is not int or index < 0 or index >= len(columns)
                           for index in criteria_by_column)):
                raise _fail("NATIVE_TRANSPORT_CRITERIA_INVALID")
            criteria = dict(criteria_by_column)
            if any(not _criteria_value_ok(value) for value in criteria.values()):
                raise _fail("NATIVE_TRANSPORT_CRITERIA_VALUE_INVALID")
            if candidate_row_filter is not None and not callable(candidate_row_filter):
                raise _fail("NATIVE_TRANSPORT_CANDIDATE_FILTER_INVALID")

            rowid_alias = _delta._transport_rowid_alias(list(columns))
            if rowid_alias is None:
                raise _fail("NATIVE_TRANSPORT_ROWID_UNAVAILABLE")
            is_source_table = table in self._table_names
            database_path = self._verified_files.checked_path(
                self._database_relative, self._database_record,
            )
            connection = _connect_readonly(database_path)

            selected_indexes = sorted(criteria)
            if is_source_table:
                ordinal_alias = _unused_projection_alias(
                    "__ikarchive_row_ordinal", columns,
                )
                source_rowid_alias = _unused_projection_alias(
                    "__ikarchive_transport_rowid", columns + (ordinal_alias,),
                )
                projection = [
                    f"o.{_delta._quote('row_ordinal')} AS {_delta._quote(ordinal_alias)}",
                    f"v.{_delta._quote(rowid_alias)} AS {_delta._quote(source_rowid_alias)}",
                ]
                projection.extend(
                    f"v.{_delta._quote(columns[index])}" for index in selected_indexes
                )
                scan_sql = (
                    f"SELECT {','.join(projection)} "
                    f"FROM {_delta._quote(self._operations_table)} AS o "
                    f"LEFT JOIN {_delta._quote(table)} AS v NOT INDEXED "
                    f"ON v.{_delta._quote(rowid_alias)}="
                    f"o.{_delta._quote('transport_rowid')} "
                    f"WHERE o.{_delta._quote('table_name')}=? "
                    f"AND o.{_delta._quote('operation')}=? "
                    f"AND o.{_delta._quote('row_ordinal')} IS NOT NULL "
                    f"ORDER BY o.{_delta._quote('row_ordinal')}"
                )
                scan_parameters: tuple[Any, ...] = (table, "upsert")
                expected_description = [ordinal_alias, source_rowid_alias] + [
                    columns[index] for index in selected_indexes
                ]
            else:
                projection = [_delta._quote(rowid_alias)]
                projection.extend(
                    _delta._quote(columns[index]) for index in selected_indexes
                )
                scan_sql = (
                    f"SELECT {','.join(projection)} FROM {_delta._quote(table)} NOT INDEXED "
                    f"ORDER BY {_delta._quote(rowid_alias)}"
                )
                scan_parameters = ()
                rowid_description = (
                    "operation_ordinal" if table == self._operations_table else "rowid"
                )
                expected_description = [rowid_description] + [
                    columns[index] for index in selected_indexes
                ]
            scan_cursor = connection.execute(scan_sql, scan_parameters)
            if [item[0] for item in (scan_cursor.description or ())] != expected_description:
                raise _fail("NATIVE_TRANSPORT_COLUMNS_INVALID")

            expected_count = self._expected_counts[table]
            ordinal = 0
            while True:
                projected = scan_cursor.fetchone()
                if projected is None:
                    break
                prefix_width = 2 if is_source_table else 1
                if len(projected) != prefix_width + len(selected_indexes):
                    raise _fail("NATIVE_TRANSPORT_ROW_INVALID")
                if is_source_table:
                    scanned_ordinal = projected[0]
                    transport_rowid = projected[1]
                    criteria_values = projected[2:]
                    if type(scanned_ordinal) is not int or scanned_ordinal != ordinal:
                        raise _fail("NATIVE_TRANSPORT_ROWID_SEQUENCE_INVALID")
                    if type(transport_rowid) is not int or transport_rowid <= 0:
                        raise _fail("NATIVE_TRANSPORT_ROWID_INVALID")
                else:
                    transport_rowid = projected[0]
                    criteria_values = projected[1:]
                if type(transport_rowid) is not int:
                    raise _fail("NATIVE_TRANSPORT_ROWID_INVALID")
                if table == self._operations_table:
                    expected_rowid = ordinal
                else:
                    expected_rowid = None
                if expected_rowid is not None and transport_rowid != expected_rowid:
                    raise _fail("NATIVE_TRANSPORT_ROWID_SEQUENCE_INVALID")
                if any(not _native_value_ok(value) for value in criteria_values):
                    raise _fail("NATIVE_TRANSPORT_VALUE_INVALID")

                matches = all(
                    _same_native_value(value, criteria[index])
                    for index, value in zip(selected_indexes, criteria_values)
                )
                if matches and candidate_row_filter is not None:
                    try:
                        filter_result = candidate_row_filter(ordinal, transport_rowid)
                    except Exception:
                        raise _fail("NATIVE_TRANSPORT_CANDIDATE_FILTER_FAILED") from None
                    except BaseException:
                        callback_baseexception = True
                        raise
                    if type(filter_result) is not bool:
                        raise _fail("NATIVE_TRANSPORT_CANDIDATE_FILTER_RESULT_INVALID")
                    matches = filter_result

                if matches:
                    full_sql = (
                        f"SELECT * FROM {_delta._quote(table)} NOT INDEXED "
                        f"WHERE {_delta._quote(rowid_alias)}=?"
                    )
                    full_cursor = connection.execute(full_sql, (transport_rowid,))
                    full_primary: Optional[BaseException] = None
                    try:
                        if [item[0] for item in (full_cursor.description or ())] != list(columns):
                            raise _fail("NATIVE_TRANSPORT_COLUMNS_INVALID")
                        full_row = full_cursor.fetchone()
                        if full_row is None or len(full_row) != len(columns):
                            raise _fail("NATIVE_TRANSPORT_ROW_INVALID")
                        values = tuple(full_row)
                    except BaseException as exc:
                        full_primary = exc
                        raise
                    finally:
                        try:
                            _safe_close_cursor(full_cursor)
                        except BaseException:
                            if full_primary is None:
                                raise
                    if any(not _native_value_ok(value) for value in values):
                        raise _fail("NATIVE_TRANSPORT_VALUE_INVALID")
                    if any(not _same_native_value(values[index], expected)
                           for index, expected in criteria.items()):
                        raise _fail("NATIVE_TRANSPORT_CRITERIA_CHANGED")
                    yield ordinal, transport_rowid, values
                ordinal += 1
            if ordinal != expected_count:
                raise _fail("NATIVE_TRANSPORT_TABLE_COUNT_MISMATCH")
        except sqlite3.Error:
            primary = _fail("NATIVE_TRANSPORT_DATABASE_INVALID")
            raise primary from None
        except BaseException as exc:
            if not isinstance(exc, GeneratorExit) or callback_baseexception:
                primary = exc
            raise
        finally:
            if scan_cursor is not None:
                try:
                    scan_cursor.close()
                except BaseException as exc:
                    if primary is None:
                        cleanup_error = exc
            if connection is not None:
                try:
                    connection.close()
                except BaseException as exc:
                    if primary is None and cleanup_error is None:
                        cleanup_error = exc
            try:
                self._assert_parent()
            except BaseException as exc:
                if primary is None and cleanup_error is None:
                    cleanup_error = exc
            if primary is None and cleanup_error is not None:
                if isinstance(cleanup_error, sqlite3.Error):
                    raise _fail("NATIVE_TRANSPORT_DATABASE_INVALID") from None
                raise cleanup_error
