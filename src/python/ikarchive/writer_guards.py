"""Enforce the SQLite change-feed writer connection contract.

Every ordinary source table receives BEFORE INSERT, UPDATE, and DELETE
triggers. A connection with ``PRAGMA recursive_triggers=OFF`` is rejected
before a source mutation can commit. SQLite only runs the implicit DELETE
triggers for ``INSERT OR REPLACE`` when recursive triggers are enabled; this
guard makes an unsafe writer fail visibly instead of silently losing that
change-feed event.

The installer never changes the connection pragma, row values, or existing
triggers. The pragma reported by :func:`inspect_writer_guards` belongs only to
the connection passed to that call. It says nothing about other writer
connections; each writer is protected by the database triggers when it writes.
"""

from __future__ import annotations

import hashlib
import json
import re
import sqlite3
import uuid
from typing import Any


CHANGE_TABLE = "archive_change_feed"
_GUARD_PREFIX = "ia_writer_guard_"
_GUARD_MESSAGE = "ARCHIVE_CHANGE_FEED_RECURSIVE_TRIGGERS_REQUIRED"
_OPERATIONS = ("INSERT", "UPDATE", "DELETE")
_VIRTUAL_TABLE_SQL = re.compile(r"^\s*CREATE\s+VIRTUAL\s+TABLE\b", re.IGNORECASE)


def _quote(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _canonical_json(value: Any) -> bytes:
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")


def _schema_objects(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    return [
        {"type": row[0], "name": row[1], "tbl_name": row[2], "sql": row[3]}
        for row in conn.execute(
            "SELECT type,name,tbl_name,sql FROM sqlite_master ORDER BY type,name"
        )
    ]


def _schema_sha256(objects: list[dict[str, Any]]) -> str:
    return hashlib.sha256(_canonical_json(objects)).hexdigest()


def _validate_connection(conn: sqlite3.Connection) -> None:
    if not isinstance(conn, sqlite3.Connection):
        raise TypeError("conn must be a sqlite3.Connection")
    if conn.text_factory is not str:
        raise ValueError("connection must use sqlite3's standard str text_factory")
    attached = [
        row[1]
        for row in conn.execute("PRAGMA database_list")
        if row[1] not in {"main", "temp"}
    ]
    if attached:
        raise ValueError("attached databases are unsupported: " + ", ".join(attached))
    if conn.execute("SELECT 1 FROM sqlite_temp_master LIMIT 1").fetchone() is not None:
        raise ValueError("temporary schema objects are unsupported")


def _ordinary_tables(conn: sqlite3.Connection, objects: list[dict[str, Any]]) -> list[str]:
    table_kinds: dict[str, str] = {}
    for row in conn.execute("PRAGMA table_list"):
        if len(row) >= 3:
            table_kinds[str(row[1]).casefold()] = str(row[2]).casefold()

    result: list[str] = []
    for item in objects:
        if item["type"] != "table":
            continue
        name = item["name"]
        folded = name.casefold()
        if folded == CHANGE_TABLE.casefold() or folded.startswith("sqlite_"):
            continue
        kind = table_kinds.get(folded)
        if kind in {"virtual", "shadow"} or _VIRTUAL_TABLE_SQL.match(item.get("sql") or ""):
            raise ValueError(f"virtual/shadow source tables are unsupported: {name}")
        if kind is not None and kind != "table":
            raise ValueError(f"unsupported source table kind {kind!r}: {name}")
        result.append(name)
    return sorted(result)


def _trigger_name(table: str, operation: str) -> str:
    digest = hashlib.sha256(
        (table + "\0" + operation).encode("utf-8", "surrogatepass")
    ).hexdigest()
    return _GUARD_PREFIX + digest


def _trigger_sql(table: str, operation: str) -> str:
    name = _quote(_trigger_name(table, operation))
    target = _quote(table)
    return (
        f"CREATE TRIGGER {name} BEFORE {operation} ON {target} "
        "WHEN (SELECT recursive_triggers FROM pragma_recursive_triggers)=0 "
        f"BEGIN SELECT RAISE(ABORT,{_literal(_GUARD_MESSAGE)}); END"
    )


def _expected_triggers(tables: list[str]) -> list[dict[str, str]]:
    expected = [
        {
            "name": _trigger_name(table, operation),
            "table": table,
            "operation": operation,
            "sql": _trigger_sql(table, operation),
        }
        for table in tables
        for operation in _OPERATIONS
    ]
    expected.sort(key=lambda item: item["name"])
    return expected


def _connection_recursive_triggers(conn: sqlite3.Connection) -> bool:
    row = conn.execute("PRAGMA recursive_triggers").fetchone()
    if row is None or type(row[0]) is not int or row[0] not in (0, 1):
        raise ValueError("could not read this connection's recursive_triggers setting")
    return bool(row[0])


def _inspect_after_validation(conn: sqlite3.Connection) -> dict[str, Any]:
    objects = _schema_objects(conn)
    tables = _ordinary_tables(conn, objects)
    expected = _expected_triggers(tables)
    actual_by_name: dict[str, list[dict[str, Any]]] = {}
    for item in objects:
        if item["type"] in {"table", "index", "view", "trigger"}:
            actual_by_name.setdefault(item["name"].casefold(), []).append(item)

    missing: list[str] = []
    mismatched: list[str] = []
    for item in expected:
        matches = actual_by_name.get(item["name"].casefold(), [])
        if not matches:
            missing.append(item["name"])
        elif (
            len(matches) != 1
            or matches[0]["type"] != "trigger"
            or matches[0]["name"] != item["name"]
            or matches[0]["sql"] != item["sql"]
        ):
            mismatched.append(item["name"])

    if not missing and not mismatched:
        status = "verified"
    elif missing and mismatched:
        status = "missing_and_mismatched"
    elif missing:
        status = "missing"
    else:
        status = "mismatched"

    return {
        "version": 1,
        "status": status,
        "expected": expected,
        "missing": sorted(missing),
        "mismatched": sorted(mismatched),
        "all_writers_contract_enforced": status == "verified",
        "connection_recursive_triggers": _connection_recursive_triggers(conn),
        "connection_recursive_triggers_required": True,
        "source_schema_sha256": _schema_sha256(objects),
        "guarded_tables": tables,
    }


def inspect_writer_guards(conn: sqlite3.Connection) -> dict[str, Any]:
    """Inspect the guard contract without changing schema, data, or pragmas.

    ``status`` is ``verified`` only when every currently expected guard exists
    with the exact canonical SQL. The connection pragma is reported separately
    because it cannot describe other connections that may write this database.
    """
    _validate_connection(conn)
    return _inspect_after_validation(conn)


def install_writer_guards(conn: sqlite3.Connection) -> dict[str, Any]:
    """Install canonical writer guards atomically and return a verification receipt.

    Existing source triggers are preserved. A guard using one of the reserved
    deterministic names with different SQL is treated as a conflict and causes
    the entire savepoint to roll back. An outer transaction remains owned by
    the caller and is neither committed nor rolled back by this function.
    """
    _validate_connection(conn)
    savepoint = "ia_install_writer_guards_" + uuid.uuid4().hex
    conn.execute(f"SAVEPOINT {_quote(savepoint)}")
    try:
        before_objects = _schema_objects(conn)
        before_sha = _schema_sha256(before_objects)
        before = _inspect_after_validation(conn)
        if before["mismatched"]:
            raise ValueError(
                "existing writer guard has different identity SQL: "
                + ", ".join(before["mismatched"])
            )

        expected = before["expected"]
        installed: list[str] = []
        missing = set(before["missing"])
        for item in expected:
            if item["name"] not in missing:
                continue
            # A differently typed object or a case-folded name collision is a
            # mismatch, never an invitation to overwrite a user's object.
            collision = conn.execute(
                "SELECT type,name,sql FROM sqlite_master WHERE name=? COLLATE NOCASE",
                (item["name"],),
            ).fetchone()
            if collision is not None:
                raise ValueError(
                    f"writer guard name is already used by another schema object: {item['name']}"
                )
            conn.execute(item["sql"])
            created = conn.execute(
                "SELECT type,name,sql FROM sqlite_master WHERE name=? COLLATE NOCASE",
                (item["name"],),
            ).fetchone()
            if (
                created is None
                or created[0] != "trigger"
                or created[1] != item["name"]
                or created[2] != item["sql"]
            ):
                raise ValueError(f"created writer guard identity SQL mismatch: {item['name']}")
            installed.append(item["name"])

        after = _inspect_after_validation(conn)
        if after["status"] != "verified":
            raise ValueError("writer guards are not fully verified after installation")
        after_objects = _schema_objects(conn)
        after_sha = _schema_sha256(after_objects)
        conn.execute(f"RELEASE SAVEPOINT {_quote(savepoint)}")
        return {
            "version": 1,
            "status": after["status"],
            "all_writers_contract_enforced": after["all_writers_contract_enforced"],
            "guarded_tables": after["guarded_tables"],
            "expected": after["expected"],
            "trigger_names": sorted(item["name"] for item in after["expected"]),
            "installed_trigger_names": sorted(installed),
            "install_sql": [item["sql"] for item in after["expected"]],
            "source_schema_sha256_before_installation": before_sha,
            "source_schema_sha256_after_installation": after_sha,
            "source_schema_changed_by_installation": before_sha != after_sha,
            "connection_recursive_triggers": after["connection_recursive_triggers"],
            "connection_recursive_triggers_required": True,
        }
    except BaseException:
        conn.execute(f"ROLLBACK TO SAVEPOINT {_quote(savepoint)}")
        conn.execute(f"RELEASE SAVEPOINT {_quote(savepoint)}")
        raise


__all__ = ["CHANGE_TABLE", "install_writer_guards", "inspect_writer_guards"]
