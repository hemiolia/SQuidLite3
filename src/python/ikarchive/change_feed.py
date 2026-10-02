"""Transactional SQLite change-feed installation and snapshot readers.

The feed records identities rather than row bodies.  A reader pins one SQLite
snapshot and resolves every changed identity to the current full row in that
same snapshot.  System tables which SQLite will not allow us to trigger are
represented as streamed clear-and-replace sequences.

Every writer connection must enable ``PRAGMA recursive_triggers=ON``. SQLite's
implicit DELETE triggers for ``INSERT OR REPLACE`` run only under that writer
setting; otherwise replacement can omit the removed row from the feed. This
module never changes the pragma, and a read-only batch cannot verify the
settings used by past writers.
"""

from __future__ import annotations

from contextlib import AbstractContextManager
import hashlib
import json
import re
import sqlite3
from typing import Any, Iterator, Optional, Sequence

from .writer_guards import inspect_writer_guards


CHANGE_TABLE = "archive_change_feed"
_TRIGGER_PREFIX = "ia_change_"
_CHANGE_TABLE_SQL = (
    "CREATE TABLE archive_change_feed ("
    "event_id INTEGER PRIMARY KEY, table_name TEXT, operation TEXT, "
    "old_rowid INTEGER, new_rowid INTEGER, old_key_json TEXT, "
    "new_key_json TEXT, changed_at TEXT)"
)
_CHANGE_TABLE_COLUMNS = [
    ("event_id", "INTEGER", 1),
    ("table_name", "TEXT", 0),
    ("operation", "TEXT", 0),
    ("old_rowid", "INTEGER", 0),
    ("new_rowid", "INTEGER", 0),
    ("old_key_json", "TEXT", 0),
    ("new_key_json", "TEXT", 0),
    ("changed_at", "TEXT", 0),
]
_FEED_GUARD_MESSAGE = "ARCHIVE_CHANGE_FEED_APPEND_ONLY"
_WRITER_REQUIREMENTS = {"recursive_triggers": True}
_WRITER_REQUIREMENTS_NOTE = (
    "Every writer must use PRAGMA recursive_triggers=ON for INSERT OR REPLACE "
    "to record the implicit DELETE; installer and read-only batch connections "
    "do not set or verify that writer setting."
)


def _quote(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _schema_objects(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    return [
        {"type": row[0], "name": row[1], "tbl_name": row[2], "sql": row[3]}
        for row in conn.execute(
            "SELECT type,name,tbl_name,sql FROM sqlite_master ORDER BY type,name"
        )
    ]


def _schema_sha256(objects: list[dict[str, Any]]) -> str:
    encoded = json.dumps(
        objects, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _table_xinfo(conn: sqlite3.Connection, table: str) -> list[dict[str, Any]]:
    return [
        {
            "cid": row[0],
            "name": row[1],
            "type": row[2],
            "notnull": row[3],
            "dflt_value": row[4],
            "pk": row[5],
            "hidden": row[6],
        }
        for row in conn.execute(f"PRAGMA table_xinfo({_quote(table)})")
    ]


def _visible_columns(xinfo: list[dict[str, Any]]) -> list[str]:
    return [column["name"] for column in xinfo if column["hidden"] != 1]


def _table_list(conn: sqlite3.Connection) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for row in conn.execute("PRAGMA table_list"):
        if len(row) >= 6:
            result[row[1]] = {
                "type": row[2],
                "ncol": row[3],
                "without_rowid": bool(row[4]),
                "strict": bool(row[5]),
            }
    return result


def _table_names(objects: list[dict[str, Any]]) -> list[str]:
    return [item["name"] for item in objects if item["type"] == "table"]


def _is_archive_internal(name: str) -> bool:
    return name.casefold().startswith("sqlite_")


def _check_source_connection(conn: sqlite3.Connection) -> None:
    if not isinstance(conn, sqlite3.Connection):
        raise TypeError("conn must be a sqlite3.Connection")
    if conn.text_factory is not str:
        raise ValueError("source connection must use sqlite3's standard str text_factory")
    attached = [row[1] for row in conn.execute("PRAGMA database_list") if row[1] not in {"main", "temp"}]
    if attached:
        raise ValueError("attached databases are unsupported: " + ", ".join(attached))
    if conn.execute("SELECT 1 FROM sqlite_temp_master LIMIT 1").fetchone() is not None:
        raise ValueError("temporary schema objects are unsupported")


def _check_supported_tables(
    conn: sqlite3.Connection, objects: list[dict[str, Any]]
) -> dict[str, dict[str, Any]]:
    flags = _table_list(conn)
    unsupported = [
        name
        for name in _table_names(objects)
        if flags.get(name, {}).get("type") in {"virtual", "shadow"}
    ]
    unsupported.extend(
        item["name"]
        for item in objects
        if item["type"] == "table"
        and item.get("sql")
        and re.match(r"\s*CREATE\s+VIRTUAL\s+TABLE\b", item["sql"], re.IGNORECASE)
        and item["name"] not in unsupported
    )
    if unsupported:
        raise ValueError(
            "virtual/shadow tables cannot be tracked without loss: "
            + ", ".join(sorted(unsupported))
        )
    return flags


def _identity_plan(
    conn: sqlite3.Connection,
    table: str,
    xinfo: list[dict[str, Any]],
    flags: dict[str, dict[str, Any]],
    *,
    require_identity: bool,
) -> dict[str, Any]:
    visible = _visible_columns(xinfo)
    table_kind = flags.get(table, {}).get("type", "table")
    if table_kind in {"virtual", "shadow"}:
        raise ValueError(f"virtual/shadow table has no supported identity: {table}")
    if not flags.get(table, {}).get("without_rowid", False):
        aliases = {name.casefold() for name in visible}
        rowid_alias = next(
            (name for name in ("_rowid_", "rowid", "oid") if name.casefold() not in aliases),
            None,
        )
        if rowid_alias is not None:
            return {"kind": "rowid", "rowid_alias": rowid_alias, "primary_key": []}

    primary_key = sorted(
        (column for column in xinfo if column["pk"]), key=lambda column: column["pk"]
    )
    if not primary_key:
        if require_identity:
            raise ValueError(
                f"table has no usable rowid or primary key identity: {table}"
            )
        return {"kind": "none", "rowid_alias": None, "primary_key": []}

    columns = [column["name"] for column in primary_key]
    if require_identity:
        null_test = " OR ".join(f"{_quote(name)} IS NULL" for name in columns)
        if conn.execute(
            f"SELECT 1 FROM {_quote(table)} WHERE {null_test} LIMIT 1"
        ).fetchone() is not None:
            raise ValueError(f"primary key is not a complete identity (NULL key): {table}")
        group = ", ".join(_quote(name) for name in columns)
        if conn.execute(
            f"SELECT 1 FROM {_quote(table)} GROUP BY {group} HAVING COUNT(*)>1 LIMIT 1"
        ).fetchone() is not None:
            raise ValueError(f"primary key is not a unique identity: {table}")
    return {"kind": "primary_key", "rowid_alias": None, "primary_key": columns}


def _value_expression(prefix: str, column: str) -> str:
    value = f"{prefix}.{_quote(column)}"
    kind = f"typeof({value})"
    return (
        f"CASE {kind} "
        f"WHEN 'integer' THEN CAST({value} AS TEXT) "
        f"WHEN 'real' THEN printf('%!.17g', {value}) "
        f"WHEN 'text' THEN {value} "
        f"WHEN 'blob' THEN hex({value}) "
        "ELSE NULL END"
    )


def _typed_key_expression(prefix: str, columns: Sequence[str]) -> str:
    values = []
    for column in columns:
        value = f"{prefix}.{_quote(column)}"
        values.append(
            "json_object('column', "
            + _literal(column)
            + f", 'sqlite_type', typeof({value}), 'value', {_value_expression(prefix, column)})"
        )
    return "json_array(" + ",".join(values) + ")"


def _identity_sql_parts(plan: dict[str, Any], prefix: str, *, new: bool) -> tuple[str, str]:
    if plan["kind"] == "rowid":
        alias = plan["rowid_alias"]
        return ("NULL", f"{prefix}.{_quote(alias)}")
    if plan["kind"] == "primary_key":
        return (_typed_key_expression(prefix, plan["primary_key"]), "NULL")
    raise ValueError("cannot build trigger identities without a key")


def _trigger_name(table: str, operation: str) -> str:
    digest = hashlib.sha256((table + "\0" + operation).encode("utf-8", "surrogatepass")).hexdigest()
    return _TRIGGER_PREFIX + digest


def _trigger_sql(table: str, operation: str, plan: dict[str, Any]) -> str:
    trigger = _quote(_trigger_name(table, operation))
    target = _quote(table)
    old_key, old_rowid = ("NULL", "NULL")
    new_key, new_rowid = ("NULL", "NULL")
    if operation in {"UPDATE", "DELETE"}:
        old_key, old_rowid = _identity_sql_parts(plan, "OLD", new=False)
    if operation in {"INSERT", "UPDATE"}:
        new_key, new_rowid = _identity_sql_parts(plan, "NEW", new=True)
    changed_at = "strftime('%Y-%m-%dT%H:%M:%fZ','now')"
    return (
        f"CREATE TRIGGER {trigger} AFTER {operation} ON {target} BEGIN "
        f"INSERT INTO {_quote(CHANGE_TABLE)} "
        "(table_name,operation,old_rowid,new_rowid,old_key_json,new_key_json,changed_at) "
        f"VALUES({_literal(table)},{_literal(operation)},{old_rowid},{new_rowid},"
        f"{old_key},{new_key},{changed_at}); END"
    )


def _trigger_create_if_missing(ddl: str) -> str:
    prefix = "CREATE TRIGGER "
    if not ddl.startswith(prefix):
        raise ValueError("internal trigger DDL does not start with CREATE TRIGGER")
    return "CREATE TRIGGER IF NOT EXISTS " + ddl[len(prefix) :]


def _normalized_sql(sql: Optional[str]) -> str:
    return " ".join((sql or "").split()).casefold()


def _feed_guard_trigger_map() -> dict[str, str]:
    return {
        "ia_feed_guard_before_update": (
            f"CREATE TRIGGER {_quote('ia_feed_guard_before_update')} "
            f"BEFORE UPDATE ON {_quote(CHANGE_TABLE)} BEGIN "
            f"SELECT RAISE(ABORT,{_literal(_FEED_GUARD_MESSAGE)}); END"
        ),
        "ia_feed_guard_before_delete": (
            f"CREATE TRIGGER {_quote('ia_feed_guard_before_delete')} "
            f"BEFORE DELETE ON {_quote(CHANGE_TABLE)} BEGIN "
            f"SELECT RAISE(ABORT,{_literal(_FEED_GUARD_MESSAGE)}); END"
        ),
    }


def _feed_integrity_guard_status(conn: sqlite3.Connection) -> dict[str, Any]:
    expected = _feed_guard_trigger_map()
    actual = [
        (row[0], row[1])
        for row in conn.execute("SELECT name,sql FROM sqlite_master WHERE type='trigger'")
    ]
    missing: list[str] = []
    mismatched: list[str] = []
    for name, ddl in expected.items():
        matches = [(actual_name, sql) for actual_name, sql in actual if actual_name.casefold() == name.casefold()]
        if not matches:
            missing.append(name)
        elif len(matches) != 1 or matches[0][0] != name or _normalized_sql(matches[0][1]) != _normalized_sql(ddl):
            mismatched.append(name)
    if not missing and not mismatched:
        status = "verified"
    elif missing and mismatched:
        status = "missing_and_mismatched"
    elif missing:
        status = "missing"
    else:
        status = "mismatched"
    return {
        "status": status,
        "expected_triggers": sorted(expected),
        "missing_triggers": sorted(missing),
        "mismatched_triggers": sorted(mismatched),
    }


def _expected_feed_trigger_map(
    conn: sqlite3.Connection,
    objects: list[dict[str, Any]],
    flags: dict[str, dict[str, Any]],
) -> dict[str, str]:
    result: dict[str, str] = {}
    for table in _table_names(objects):
        if table.casefold() == CHANGE_TABLE.casefold() or _is_archive_internal(table):
            continue
        xinfo = _table_xinfo(conn, table)
        plan = _identity_plan(conn, table, xinfo, flags, require_identity=True)
        for operation in ("INSERT", "UPDATE", "DELETE"):
            result[_trigger_name(table, operation)] = _trigger_sql(table, operation, plan)
    return result


def _object_by_name(conn: sqlite3.Connection, name: str) -> Optional[tuple[str, str, Optional[str]]]:
    row = conn.execute(
        "SELECT type,name,sql FROM sqlite_master WHERE name=? COLLATE NOCASE",
        (name,),
    ).fetchone()
    return (row[0], row[1], row[2]) if row else None


def _source_row_counts(conn: sqlite3.Connection, tables: Sequence[str]) -> dict[str, int]:
    return {
        table: int(conn.execute(f"SELECT COUNT(*) FROM {_quote(table)}").fetchone()[0])
        for table in tables
    }


def _source_table_columns(
    conn: sqlite3.Connection, tables: Sequence[str]
) -> dict[str, list[dict[str, Any]]]:
    """Return complete table_xinfo metadata for every archived table."""
    return {table: _table_xinfo(conn, table) for table in tables}


def _source_foreign_keys(conn: sqlite3.Connection, tables: Sequence[str]) -> dict[str, list[dict[str, Any]]]:
    fields = ('id', 'seq', 'table', 'from', 'to', 'on_update', 'on_delete', 'match')
    return {table: [dict(zip(fields, tuple(row))) for row in conn.execute(
        f'PRAGMA foreign_key_list({_quote(table)})')] for table in tables}


def install_change_feed(conn: sqlite3.Connection, *, include_row_counts: bool = False) -> dict[str, Any]:
    """Install identity-only AFTER triggers without replacing user objects.

    Work is enclosed in a savepoint.  If the caller owns an outer transaction,
    this routine never commits or rolls it back; a failed installation rolls
    back only its own savepoint.
    Full row counts are disabled by default: scanning a multi-gigabyte table
    while DDL holds the writer lock would delay the collector. The pinned
    read_change_batch context always returns complete counts without that lock.

    The change-feed table gets canonical BEFORE UPDATE and BEFORE DELETE
    guards. This installer does not change ``PRAGMA recursive_triggers``;
    every connection that writes source tables must enable it so
    ``INSERT OR REPLACE`` records the implicitly deleted row.
    """
    _check_source_connection(conn)
    savepoint = "ikarchive_install_change_feed"
    conn.execute(f"SAVEPOINT {_quote(savepoint)}")
    try:
        before_objects = _schema_objects(conn)
        before_table_names = _table_names(before_objects)
        flags = _check_supported_tables(conn, before_objects)
        existing_feed_object = _object_by_name(conn, CHANGE_TABLE)
        if existing_feed_object is not None and existing_feed_object[0] != "table":
            raise ValueError(f"reserved change-feed name already belongs to a {existing_feed_object[0]}")
        if existing_feed_object is not None and existing_feed_object[1] != CHANGE_TABLE:
            raise ValueError("reserved change-feed table name has incompatible capitalization")
        expected_sql = _normalized_sql(_CHANGE_TABLE_SQL)
        if existing_feed_object is not None and _normalized_sql(existing_feed_object[2]) != expected_sql:
            raise ValueError("existing archive_change_feed table has different identity SQL")

        guard_status_before = _feed_integrity_guard_status(conn)
        if guard_status_before["mismatched_triggers"]:
            raise ValueError(
                "existing archive_change_feed guard has a different identity SQL: "
                + ", ".join(guard_status_before["mismatched_triggers"])
            )

        # Validate every source identity before adding the journal table or any trigger.
        planned = _expected_feed_trigger_map(conn, before_objects, flags)
        conn.execute(_CHANGE_TABLE_SQL.replace("CREATE TABLE", "CREATE TABLE IF NOT EXISTS", 1))
        feed_info = [
            (row[1], row[2], row[5])
            for row in conn.execute(f"PRAGMA table_xinfo({_quote(CHANGE_TABLE)})")
        ]
        if feed_info != _CHANGE_TABLE_COLUMNS:
            raise ValueError("archive_change_feed column layout does not match the required schema")
        actual_feed_sql = conn.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name=?",
            (CHANGE_TABLE,),
        ).fetchone()[0]
        if _normalized_sql(actual_feed_sql) != expected_sql:
            raise ValueError("archive_change_feed table SQL identity mismatch")

        existing_triggers = {
            row[0]: row[1]
            for row in conn.execute("SELECT name,sql FROM sqlite_master WHERE type='trigger'")
        }
        missing_tables: set[str] = set()
        trigger_names: list[str] = []
        for name, ddl in planned.items():
            trigger_names.append(name)
            actual_sql = existing_triggers.get(name)
            if actual_sql is not None:
                if _normalized_sql(actual_sql) != _normalized_sql(ddl):
                    raise ValueError(f"existing change trigger has different identity SQL: {name}")
                continue
            table_name = next(
                table
                for table in before_table_names
                if name in {_trigger_name(table, op) for op in ("INSERT", "UPDATE", "DELETE")}
            )
            if existing_feed_object is not None:
                missing_tables.add(table_name)
            conn.execute(_trigger_create_if_missing(ddl))
            created_sql = conn.execute(
                "SELECT sql FROM sqlite_master WHERE type='trigger' AND name=?", (name,)
            ).fetchone()
            if created_sql is None or _normalized_sql(created_sql[0]) != _normalized_sql(ddl):
                raise ValueError(f"created change trigger identity SQL mismatch: {name}")

        guard_trigger_map = _feed_guard_trigger_map()
        created_guard_triggers: list[str] = []
        for name in guard_status_before["missing_triggers"]:
            conn.execute(_trigger_create_if_missing(guard_trigger_map[name]))
            created_sql = conn.execute(
                "SELECT sql FROM sqlite_master WHERE type='trigger' AND name=?", (name,)
            ).fetchone()
            if created_sql is None or _normalized_sql(created_sql[0]) != _normalized_sql(guard_trigger_map[name]):
                raise ValueError(f"created archive_change_feed guard identity SQL mismatch: {name}")
            created_guard_triggers.append(name)
        guard_status_after = _feed_integrity_guard_status(conn)
        if guard_status_after["status"] != "verified":
            raise ValueError("archive_change_feed append-only guards are not verified")

        objects = _schema_objects(conn)
        writer_guard_status = inspect_writer_guards(conn)
        tables = _table_names(objects)
        schema_sha_before = _schema_sha256(before_objects)
        schema_sha_after = _schema_sha256(objects)
        requires_reconciliation = not writer_guard_status["all_writers_contract_enforced"] or bool(missing_tables) or (
            existing_feed_object is not None and guard_status_before["status"] != "verified"
        )
        metadata = {
            "table": CHANGE_TABLE,
            "trigger_names": sorted(trigger_names),
            "tracked_tables": sorted(
                name for name in before_table_names
                if name.casefold() != CHANGE_TABLE.casefold() and not _is_archive_internal(name)
            ),
            "replace_tables": sorted(
                name for name in before_table_names
                if name.casefold() != CHANGE_TABLE.casefold() and _is_archive_internal(name)
            ),
            "source_schema_sha256": _schema_sha256(objects),
            "source_schema_sha256_before_installation": schema_sha_before,
            "source_schema_changed_by_installation": schema_sha_before != schema_sha_after,
            "feed_guard_schema_changed_by_installation": bool(created_guard_triggers),
            "schemas": objects,
            "source_table_columns": _source_table_columns(conn, tables),
            "source_foreign_keys": _source_foreign_keys(conn, tables),
            "source_row_counts": _source_row_counts(conn, tables) if include_row_counts else None,
            "row_counts_status": "collected" if include_row_counts else "deferred_to_read_batch",
            "requires_baseline_reconciliation": requires_reconciliation,
            "untracked_tables": sorted(missing_tables),
            "feed_integrity_guard_status_before_installation": guard_status_before,
            "feed_integrity_guard_status": guard_status_after,
            "writer_requirements": dict(_WRITER_REQUIREMENTS),
            "writer_requirements_verified": False,
            "writer_requirements_note": _WRITER_REQUIREMENTS_NOTE,
            "writer_guard_status": writer_guard_status,
            "all_writers_contract_enforced": writer_guard_status["all_writers_contract_enforced"],
            "schema_comparison_required": True,
            "initial_baseline_required": existing_feed_object is None,
        }
        conn.execute(f"RELEASE SAVEPOINT {_quote(savepoint)}")
        return metadata
    except BaseException:
        conn.execute(f"ROLLBACK TO SAVEPOINT {_quote(savepoint)}")
        conn.execute(f"RELEASE SAVEPOINT {_quote(savepoint)}")
        raise


def _decode_typed_key(identity_json: str, expected_columns: Sequence[str]) -> tuple[Any, ...]:
    try:
        items = json.loads(identity_json)
    except (TypeError, json.JSONDecodeError) as exc:
        raise ValueError("change-feed primary-key JSON is invalid") from exc
    if not isinstance(items, list) or len(items) != len(expected_columns):
        raise ValueError("change-feed primary-key JSON has an invalid shape")
    values: list[Any] = []
    for expected_name, item in zip(expected_columns, items):
        if (
            not isinstance(item, dict)
            or set(item) != {"column", "sqlite_type", "value"}
            or item["column"] != expected_name
        ):
            raise ValueError("change-feed primary-key column metadata is inconsistent")
        kind = item["sqlite_type"]
        value = item["value"]
        if kind == "null":
            if value is not None:
                raise ValueError("NULL primary-key value has a non-NULL encoding")
            values.append(None)
        elif kind == "integer":
            if not isinstance(value, str) or not re.fullmatch(r"-?\d+", value):
                raise ValueError("integer primary-key value is invalid")
            values.append(int(value))
        elif kind == "real":
            if not isinstance(value, str):
                raise ValueError("real primary-key value is invalid")
            try:
                values.append(float(value))
            except ValueError as exc:
                raise ValueError("real primary-key value is invalid") from exc
        elif kind == "text":
            if not isinstance(value, str):
                raise ValueError("TEXT primary-key value is invalid")
            values.append(value)
        elif kind == "blob":
            if not isinstance(value, str) or len(value) % 2:
                raise ValueError("BLOB primary-key value is invalid")
            try:
                values.append(bytes.fromhex(value))
            except ValueError as exc:
                raise ValueError("BLOB primary-key value is invalid") from exc
        else:
            raise ValueError(f"unknown primary-key SQLite type: {kind}")
    return tuple(values)


def _lookup_identity(
    conn: sqlite3.Connection,
    table: str,
    columns: list[str],
    plan: dict[str, Any],
    *,
    rowid: Optional[int],
    identity_json: Optional[str],
) -> Optional[tuple[Any, ...]]:
    if plan["kind"] == "rowid":
        if rowid is None:
            raise ValueError(f"rowid identity is absent for {table!r}")
        predicate = f"{_quote(plan['rowid_alias'])} IS ?"
        parameters: tuple[Any, ...] = (rowid,)
    else:
        if identity_json is None or plan["kind"] != "primary_key":
            raise ValueError(f"primary-key identity is absent for {table!r}")
        key_values = _decode_typed_key(identity_json, plan["primary_key"])
        terms = []
        parameters_list: list[Any] = []
        for column, value, item in zip(
            plan["primary_key"], key_values, json.loads(identity_json)
        ):
            terms.append(f"(typeof({_quote(column)})=? AND {_quote(column)} IS ?)")
            parameters_list.extend((item["sqlite_type"], value))
        predicate = " AND ".join(terms)
        parameters = tuple(parameters_list)
    cursor = conn.execute(
        f"SELECT * FROM {_quote(table)} WHERE {predicate} LIMIT 2", parameters
    )
    rows = cursor.fetchall()
    if len(rows) > 1:
        raise ValueError(f"change-feed identity resolves to multiple rows in {table!r}")
    if not rows:
        return None
    values = tuple(rows[0])
    if len(values) != len(columns):
        raise ValueError(f"source SELECT * columns changed while reading {table!r}")
    return values


def _readable_row_plan(
    conn: sqlite3.Connection,
    table: str,
    xinfo: list[dict[str, Any]],
    flags: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    return _identity_plan(conn, table, xinfo, flags, require_identity=False)


def _typed_identity_value(conn: sqlite3.Connection, value: Any) -> tuple[str, Any]:
    kind, encoded = conn.execute(
        "SELECT typeof(?), CASE typeof(?) "
        "WHEN 'integer' THEN CAST(? AS TEXT) "
        "WHEN 'real' THEN printf('%!.17g', ?) "
        "WHEN 'text' THEN ? "
        "WHEN 'blob' THEN hex(?) "
        "ELSE NULL END",
        (value, value, value, value, value, value),
    ).fetchone()
    return kind, encoded


class ChangeBatch(AbstractContextManager["ChangeBatch"]):
    """A pinned change window; all iterators are valid only inside its context.

    Metadata states the writer-side ``recursive_triggers`` requirement but does
    not verify it: this read-only connection cannot inspect past writer settings.
    """

    def __init__(self, conn: sqlite3.Connection, after_event_id: int):
        _check_source_connection(conn)
        if type(after_event_id) is not int or after_event_id < 0:
            raise ValueError("after_event_id must be a non-negative integer")
        self.conn = conn
        self.after_event_id = after_event_id
        self.through_event_id = after_event_id
        self.metadata: dict[str, Any] = {}
        self._owned_transaction = False
        self._active = False
        self._objects: list[dict[str, Any]] = []
        self._table_names: list[str] = []
        self._flags: dict[str, dict[str, Any]] = {}
        self._plans: dict[str, dict[str, Any]] = {}

    def __enter__(self) -> "ChangeBatch":
        if self._active:
            raise RuntimeError("change batch context is already active")
        self._owned_transaction = not self.conn.in_transaction
        if self._owned_transaction:
            self.conn.execute("BEGIN")
        try:
            _check_source_connection(self.conn)
            self._objects = _schema_objects(self.conn)
            self._table_names = _table_names(self._objects)
            self._flags = _check_supported_tables(self.conn, self._objects)
            feed = _object_by_name(self.conn, CHANGE_TABLE)
            if feed is None or feed[0] != "table" or feed[1] != CHANGE_TABLE:
                raise ValueError("archive_change_feed is missing or has the wrong schema object type")
            if _normalized_sql(feed[2]) != _normalized_sql(_CHANGE_TABLE_SQL):
                raise ValueError("archive_change_feed identity SQL mismatch")
            feed_cols = [
                (row[1], row[2], row[5])
                for row in self.conn.execute(f"PRAGMA table_xinfo({_quote(CHANGE_TABLE)})")
            ]
            if feed_cols != _CHANGE_TABLE_COLUMNS:
                raise ValueError("archive_change_feed column layout mismatch")
            feed_guard_status = _feed_integrity_guard_status(self.conn)
            writer_guard_status = inspect_writer_guards(self.conn)
            self.through_event_id = int(
                self.conn.execute(
                    f"SELECT COALESCE(MAX(event_id),0) FROM {_quote(CHANGE_TABLE)}"
                ).fetchone()[0]
            )
            if self.after_event_id > self.through_event_id:
                raise ValueError("after_event_id exceeds the pinned change-feed high-water mark")
            source_tables = [name for name in self._table_names if name.casefold() != CHANGE_TABLE.casefold()]
            self._plans = {
                table: _readable_row_plan(
                    self.conn,
                    table,
                    _table_xinfo(self.conn, table),
                    self._flags,
                )
                for table in source_tables
            }
            expected_triggers = _expected_feed_trigger_map(self.conn, self._objects, self._flags)
            actual_triggers = {
                row[0]: row[1]
                for row in self.conn.execute("SELECT name,sql FROM sqlite_master WHERE type='trigger'")
            }
            untracked_tables: set[str] = set()
            mismatched_triggers: list[str] = []
            for trigger_name, expected_sql in expected_triggers.items():
                actual_sql = actual_triggers.get(trigger_name)
                if actual_sql is None:
                    table = next(
                        table
                        for table in source_tables
                        if not _is_archive_internal(table)
                        and trigger_name in {
                            _trigger_name(table, operation)
                            for operation in ("INSERT", "UPDATE", "DELETE")
                        }
                    )
                    untracked_tables.add(table)
                elif _normalized_sql(actual_sql) != _normalized_sql(expected_sql):
                    mismatched_triggers.append(trigger_name)
                    table = next(
                        table
                        for table in source_tables
                        if not _is_archive_internal(table)
                        and trigger_name in {
                            _trigger_name(table, operation)
                            for operation in ("INSERT", "UPDATE", "DELETE")
                        }
                    )
                    untracked_tables.add(table)
            internal = [
                table
                for table in source_tables
                if _is_archive_internal(table)
            ]
            trigger_tables = [
                table
                for table in source_tables
                if not _is_archive_internal(table)
            ]
            current_counts = _source_row_counts(self.conn, self._table_names)
            window_tables = {
                row[0]
                for row in self.conn.execute(
                    f"SELECT DISTINCT table_name FROM {_quote(CHANGE_TABLE)} "
                    "WHERE event_id>? AND event_id<=?",
                    (self.after_event_id, self.through_event_id),
                )
            }
            missing_schema_tables = sorted(
                table for table in window_tables
                if table not in source_tables and table != CHANGE_TABLE
            )
            untracked_tables.update(missing_schema_tables)
            current_schema = _schema_sha256(self._objects)
            self.metadata = {
                "after_event_id": self.after_event_id,
                "through_event_id": self.through_event_id,
                "schemas": self._objects,
                "source_schema_sha256": current_schema,
                "source_row_counts": current_counts,
                "source_foreign_keys": _source_foreign_keys(self.conn, self._table_names),
                "source_table_columns": _source_table_columns(
                    self.conn, self._table_names
                ),
                "tracked_tables": sorted(trigger_tables),
                "replace_tables": sorted(internal),
                "untracked_tables": sorted(untracked_tables),
                "mismatched_triggers": sorted(mismatched_triggers),
                "event_tables_missing_from_schema": missing_schema_tables,
                "feed_integrity_guard_status": feed_guard_status,
                "writer_requirements": dict(_WRITER_REQUIREMENTS),
                "writer_requirements_verified": False,
                "writer_requirements_note": _WRITER_REQUIREMENTS_NOTE,
                "writer_guard_status": writer_guard_status,
                "all_writers_contract_enforced": writer_guard_status["all_writers_contract_enforced"],
                "requires_baseline_reconciliation": bool(untracked_tables)
                or feed_guard_status["status"] != "verified"
                or not writer_guard_status["all_writers_contract_enforced"],
                "schema_comparison_required": True,
            }
            self._active = True
            return self
        except BaseException:
            if self._owned_transaction and self.conn.in_transaction:
                self.conn.rollback()
            self._owned_transaction = False
            raise

    def __exit__(self, exc_type, exc, traceback) -> Optional[bool]:
        if not self._active:
            return None
        self._active = False
        if self._owned_transaction and self.conn.in_transaction:
            if exc_type is None:
                self.conn.commit()
            else:
                self.conn.rollback()
        self._owned_transaction = False
        return None

    def _require_active(self) -> None:
        if not self._active:
            raise RuntimeError("change-batch iterators are valid only inside the batch context")

    def events(self) -> Iterator[dict[str, Any]]:
        self._require_active()
        cursor = self.conn.execute(
            f"SELECT event_id,table_name,operation,old_rowid,new_rowid,old_key_json,new_key_json,changed_at "
            f"FROM {_quote(CHANGE_TABLE)} WHERE event_id>? AND event_id<=? ORDER BY event_id",
            (self.after_event_id, self.through_event_id),
        )
        for row in cursor:
            yield {
                "event_id": row[0],
                "table_name": row[1],
                "operation": row[2],
                "old_rowid": row[3],
                "new_rowid": row[4],
                "old_key_json": row[5],
                "new_key_json": row[6],
                "changed_at": row[7],
            }

    def _iter_replaced_table(self, table: str) -> Iterator[dict[str, Any]]:
        xinfo = _table_xinfo(self.conn, table)
        columns = _visible_columns(xinfo)
        plan = self._plans[table]
        yield {
            "table_name": table,
            "columns": columns,
            "identity_json": None,
            "source_rowid": None,
            "operation": "clear_table",
            "values": None,
        }
        select = "SELECT *"
        rowid_alias = plan["rowid_alias"]
        rowid_field = None
        if rowid_alias is not None:
            rowid_field = "__ikarchive_source_rowid"
            suffix = 0
            while rowid_field.casefold() in {column.casefold() for column in columns}:
                suffix += 1
                rowid_field = f"__ikarchive_source_rowid_{suffix}"
            select += f", {_quote(rowid_alias)} AS {_quote(rowid_field)}"
        order = [f"{_quote(rowid_alias)}"] if rowid_alias else []
        if plan["kind"] == "primary_key":
            order = [_quote(column) for column in plan["primary_key"]]
        query = f"{select} FROM {_quote(table)}"
        if order:
            query += " ORDER BY " + ", ".join(order)
        elif rowid_alias is None:
            query += " NOT INDEXED"
        for row in self.conn.execute(query):
            values = tuple(row[: len(columns)])
            source_rowid = row[-1] if rowid_field is not None else None
            identity_json = None
            if plan["kind"] == "primary_key":
                identity_parts = []
                for column in plan["primary_key"]:
                    value = values[columns.index(column)]
                    kind, encoded_value = _typed_identity_value(self.conn, value)
                    identity_parts.append(
                        {"column": column, "sqlite_type": kind, "value": encoded_value}
                    )
                identity_json = _canonical_json(identity_parts)
            yield {
                "table_name": table,
                "columns": columns,
                "identity_json": identity_json,
                "source_rowid": source_rowid,
                "operation": "upsert",
                "values": values,
            }

    def _changed_identities(self) -> Iterator[tuple[str, str, Any]]:
        query = (
            "SELECT table_name,identity_kind,identity_value FROM ("
            f"SELECT table_name,'rowid' AS identity_kind,CAST(old_rowid AS TEXT) AS identity_value FROM {_quote(CHANGE_TABLE)} "
            "WHERE event_id>? AND event_id<=? AND old_rowid IS NOT NULL "
            "UNION "
            f"SELECT table_name,'key',old_key_json FROM {_quote(CHANGE_TABLE)} "
            "WHERE event_id>? AND event_id<=? AND old_key_json IS NOT NULL "
            "UNION "
            f"SELECT table_name,'rowid',CAST(new_rowid AS TEXT) FROM {_quote(CHANGE_TABLE)} "
            "WHERE event_id>? AND event_id<=? AND new_rowid IS NOT NULL "
            "UNION "
            f"SELECT table_name,'key',new_key_json FROM {_quote(CHANGE_TABLE)} "
            "WHERE event_id>? AND event_id<=? AND new_key_json IS NOT NULL"
            ") ORDER BY table_name,identity_kind,identity_value"
        )
        parameters = (self.after_event_id, self.through_event_id) * 4
        for row in self.conn.execute(query, parameters):
            yield row[0], row[1], row[2]

    def iter_current_changes(self) -> Iterator[dict[str, Any]]:
        """Stream clear/upsert/delete records for the pinned window."""
        self._require_active()
        if self.metadata.get("event_tables_missing_from_schema"):
            raise ValueError(
                "change events refer to source tables absent from the pinned schema; "
                "baseline reconciliation is required"
            )
        for table in self.metadata["replace_tables"]:
            yield from self._iter_replaced_table(table)

        # Resolve changed identities as a SQL UNION so Python retains only one
        # identity and one source row at a time, even after long offline gaps.
        for table, kind, encoded_identity in self._changed_identities():
            if table == CHANGE_TABLE:
                # Journal rows themselves are emitted once below, in event-id order.
                continue
            plan = self._plans.get(table)
            if plan is None:
                raise ValueError(f"change-feed event table is not covered by the source schema: {table}")
            xinfo = _table_xinfo(self.conn, table)
            columns = _visible_columns(xinfo)
            if kind == "rowid":
                if plan["kind"] != "rowid":
                    raise ValueError(f"rowid event is incompatible with source identity: {table}")
                source_rowid = int(encoded_identity)
                identity_json = None
            elif kind == "key":
                if plan["kind"] != "primary_key":
                    raise ValueError(f"primary-key event is incompatible with source identity: {table}")
                source_rowid = None
                identity_json = str(encoded_identity)
            else:
                raise ValueError(f"unknown change-feed identity kind: {kind}")
            current = _lookup_identity(
                self.conn,
                table,
                columns,
                plan,
                rowid=source_rowid,
                identity_json=identity_json,
            )
            yield {
                "table_name": table,
                "columns": columns,
                "identity_json": identity_json,
                "source_rowid": source_rowid,
                "operation": "upsert" if current is not None else "delete",
                "values": current,
            }

        # The journal is itself part of the archive.  Emit every event row in
        # this window as a normal typed upsert so a consumer can append it.
        journal_columns = _visible_columns(_table_xinfo(self.conn, CHANGE_TABLE))
        for event in self.events():
            yield {
                "table_name": CHANGE_TABLE,
                "columns": journal_columns,
                "identity_json": None,
                "source_rowid": event["event_id"],
                "operation": "upsert",
                "values": tuple(event[key] for key in journal_columns),
            }


def read_change_batch(conn: sqlite3.Connection, after_event_id: int) -> ChangeBatch:
    """Return a context-managed, bounded-memory reader for one pinned window.

    Use ``with read_change_batch(conn, last_event_id) as batch``.  The context
    owns a read transaction only when the caller had no transaction; otherwise
    it leaves the caller's transaction open.  ``batch.events()`` and
    ``batch.iter_current_changes()`` must be consumed before leaving the block.
    The returned metadata reports that writer connections require
    ``PRAGMA recursive_triggers=ON`` for complete ``INSERT OR REPLACE``
    deletion tracking; this read-only connection cannot verify that setting.
    """
    return ChangeBatch(conn, after_event_id)


__all__ = ["CHANGE_TABLE", "ChangeBatch", "install_change_feed", "read_change_batch"]
