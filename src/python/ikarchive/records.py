"""承認済み統合GUIの共通読み取り層。

対戦/バイトの一覧と単件取得を提供する。
ネットワークやUI方式を選ばず、DB作成・移行・書込みは一切行わない。
未知項目および数値表記の正典は source['body_bytes'] であり、
detail_json は派生 JSON 表現である。
"""

import base64
import hashlib
import math
import re
import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Optional, Union
from .display import GENRE_ORDER, genre_sort_index, rule_sort_index
from .slices import MODE_SLICES, UNCLASSIFIED

TAG_SUMMARY_SETS = tuple(
    name for name in MODE_SLICES
    if name == "bankara_open" or name.startswith("private_")
)
_JUDGEMENT_EXCLUDED = {"private", "salmon_regular", "big_run", "team_contest", "hold"}
JUDGEMENT_SETS = tuple(
    name for name in GENRE_ORDER
    if name in MODE_SLICES and name not in _JUDGEMENT_EXCLUDED
)
_VIEW_NAME = re.compile(r"analysis_[a-z0-9_]+_by_rule\Z")
from .store import Store

_ANALYSIS_SET_RE = re.compile(r'(?:unclassified|[a-z0-9_]{1,64})\Z')
_RULE_RE = re.compile(r'[A-Za-z0-9_]{1,64}\Z')
_PLAYED_RE = re.compile(r'[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z\Z')
_SQLITE_IDENTIFIER_TRANSLATION = str.maketrans(
    "ABCDEFGHIJKLMNOPQRSTUVWXYZ", "abcdefghijklmnopqrstuvwxyz"
)


def _sqlite_identifier_fold(value: str) -> str:
    """SQLite identifiers fold ASCII A-Z only; Unicode casefold would merge legal names."""
    return value.translate(_SQLITE_IDENTIFIER_TRANSLATION)


def _quote_sqlite_identifier(value: str) -> str:
    if type(value) is not str or not value:
        raise RuntimeError("Corrupt record: source column name is invalid")
    return '"' + value.replace('"', '""') + '"'


def _original_table_columns(db: sqlite3.Connection, table: str) -> List[str]:
    rows = db.execute(f"PRAGMA table_xinfo({_quote_sqlite_identifier(table)})").fetchall()
    if not rows:
        raise RuntimeError("Corrupt record: original source table schema is missing")
    ordered = sorted(rows, key=lambda row: row["cid"])
    columns = [row["name"] for row in ordered]
    if (
        any(type(name) is not str or not name for name in columns)
        or len({_sqlite_identifier_fold(name) for name in columns}) != len(columns)
    ):
        raise RuntimeError("Corrupt record: original source table columns are invalid")
    return columns


def _observable_source_rowid(
    db: sqlite3.Connection, table: str, columns: List[str]
) -> Optional[str]:
    shadowed = {_sqlite_identifier_fold(name) for name in columns}
    candidate = next(
        (name for name in ("_rowid_", "rowid", "oid") if _sqlite_identifier_fold(name) not in shadowed),
        None,
    )
    if candidate is None:
        return None

    table_info = None
    try:
        table_rows = db.execute("PRAGMA table_list").fetchall()
        table_info = next(
            (
                row for row in table_rows
                if len(row) >= 5 and row[0] == "main" and type(row[1]) is str
                and _sqlite_identifier_fold(row[1]) == _sqlite_identifier_fold(table)
            ),
            None,
        )
    except sqlite3.DatabaseError:
        pass
    if table_info is not None and type(table_info[4]) is int:
        if table_info[4] != 0:
            return None
        return candidate

    # Older SQLite builds may accept PRAGMA table_list but return no rows. Check
    # the qualified pseudo-column directly; an unqualified quoted name can be
    # interpreted as a string literal by SQLite's legacy DQS behavior.
    try:
        db.execute(
            f"SELECT source.{_quote_sqlite_identifier(candidate)} "
            f"FROM {_quote_sqlite_identifier(table)} AS source LIMIT 0"
        ).fetchall()
    except sqlite3.OperationalError as exc:
        if "no such column" in str(exc).casefold():
            return None
        raise
    return candidate


def _read_original_table_row(
    db: sqlite3.Connection, table: str, where_column: str, where_value: Any
) -> tuple[List[str], tuple[Any, ...], Any]:
    columns = _original_table_columns(db, table)
    lookup = {_sqlite_identifier_fold(name): name for name in columns}
    resolved_where = lookup.get(_sqlite_identifier_fold(where_column))
    if resolved_where is None:
        raise RuntimeError("Corrupt record: original source lookup column is missing")
    rowid_column = _observable_source_rowid(db, table, columns)
    rowid_select = (
        f"source.{_quote_sqlite_identifier(rowid_column)}"
        if rowid_column is not None else "NULL"
    )
    selected_columns = ", ".join(
        f"source.{_quote_sqlite_identifier(name)}" for name in columns
    )
    query = (
        f"SELECT {rowid_select}, {selected_columns} "
        f"FROM {_quote_sqlite_identifier(table)} AS source "
        f"WHERE source.{_quote_sqlite_identifier(resolved_where)} = ?"
    )
    row = db.execute(query, (where_value,)).fetchone()
    if row is None:
        raise RuntimeError("Corrupt record: original source row is missing")
    return columns, tuple(row[1:]), row[0]


def _sqlite_typed_cell(value: Any) -> Dict[str, Any]:
    if value is None:
        return {"type": "NULL", "value": None}
    if type(value) is int:
        return {"type": "INTEGER", "value": str(value)}
    if type(value) is float:
        return {"type": "REAL", "value": value.hex()}
    if type(value) is str:
        return {"type": "TEXT", "value": value}
    if type(value) is bytes:
        return {"type": "BLOB", "value": base64.b64encode(value).decode("ascii")}
    raise RuntimeError("Corrupt record: original source cell has an unsupported SQLite value")


def _original_column_value(columns: List[str], values: tuple[Any, ...], wanted: str) -> Any:
    folded = _sqlite_identifier_fold(wanted)
    for name, value in zip(columns, values):
        if _sqlite_identifier_fold(name) == folded:
            return value
    return None


def _source_rowid_cell(value: Any) -> Optional[Dict[str, Any]]:
    if value is None:
        return None
    if type(value) is not int:
        raise RuntimeError("Corrupt record: original source rowid is not an integer")
    return {"type": "INTEGER", "value": str(value)}


def _original_row_envelope(
    table: str, columns: List[str], values: tuple[Any, ...], source_rowid: Any,
    *, body_bytes: Optional[bytes] = None, body_sha256: Optional[str] = None,
) -> Dict[str, Any]:
    if len(columns) != len(values):
        raise RuntimeError("Corrupt record: original source row width is invalid")
    encoded_values: List[Dict[str, Any]] = []
    for name, value in zip(columns, values):
        if _sqlite_identifier_fold(table) == "bodies" and _sqlite_identifier_fold(name) == "body":
            if type(value) is not bytes or body_bytes is None or value != body_bytes:
                raise RuntimeError("Corrupt record: body BLOB storage type is invalid")
            if type(body_sha256) is not str:
                raise RuntimeError("Corrupt record: body SHA-256 is invalid")
            encoded_values.append({
                "type": "BLOB",
                "reference": "source_body",
                "byte_length": len(value),
                "sha256": body_sha256,
            })
        else:
            encoded_values.append(_sqlite_typed_cell(value))
    return {
        "columns": list(columns),
        "values": encoded_values,
        "source_rowid": _source_rowid_cell(source_rowid),
    }


def _whole_count(value: Any) -> int:
    if isinstance(value, bool):
        raise ValueError("INVALID_COUNT")
    if isinstance(value, int):
        number = value
    elif isinstance(value, float) and math.isfinite(value) and value.is_integer():
        number = int(value)
    else:
        raise ValueError("INVALID_COUNT")
    if number < 0:
        raise ValueError("INVALID_COUNT")
    return number


def _metric_key(series_id: str) -> str:
    if not series_id:
        return ""
    return series_id.split("|")[-1].split(".")[-1]


def _rate_hidden(series_id: str, label: str, source: str) -> bool:
    """既定のレート画面と同じく、勝敗由来とウデマエポイント増減は出さない。"""
    if source == "derived_judgement":
        return True
    metric = _metric_key(series_id)
    return metric == "earnedUdemaePoint" or label == "ウデマエポイント増減"


def _rate_unit(series_id: str, label: str) -> str:
    metric = _metric_key(series_id)
    if metric == "dangerRate" or (label and "キケン度" in label):
        return "ratio"
    return "number"


def _finite_number(value: Any) -> Optional[float]:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    if not math.isfinite(value):
        return None
    return float(value)


class RecordReader:
    """イカリングアーカイブの読み取り専用リーダー。

    contextmanager として使用し、__enter__ で既存 Store(path, readonly=True) を開き BEGIN を実行、
    __exit__ で必ず close する。
    context 外のメソッド利用や多重 enter は明確な RuntimeError を送出する。
    DB作成・移行・書込みは一切行わない。
    """

    def __init__(self, path: Union[str, Path]):
        self.path = Path(path)
        self._store: Optional[Store] = None
        self._entered: bool = False

    def __enter__(self) -> "RecordReader":
        if self._entered:
            raise RuntimeError("RecordReader cannot be re-entered (already active)")
        self._store = Store(self.path, readonly=True)
        try:
            self._store.db.execute("BEGIN")
        except BaseException:
            store = self._store
            self._store = None
            self._entered = False
            try:
                store.close()
            except BaseException:
                pass
            raise
        self._entered = True
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        store = self._store
        self._entered = False
        self._store = None
        if store is None:
            return
        if exc_type is not None:
            try:
                store.close()
            except BaseException:
                pass
            return
        store.close()

    def _check_active(self) -> None:
        if not self._entered or self._store is None:
            raise RuntimeError("RecordReader method called outside of active context")

    def _bounded_text(self, value: Optional[str], name: str, limit: int = 80) -> Optional[str]:
        if value is None:
            return None
        if not isinstance(value, str):
            raise ValueError(f"{name} must be a string")
        if any(ord(ch) < 32 or ord(ch) == 127 for ch in value):
            raise ValueError(f"{name} contains a control character")
        if not value or len(value) > limit:
            raise ValueError(f"{name} must be 1..{limit} characters")
        return value

    def _list_constraints(
        self,
        account: str,
        kind: Optional[str],
        analysis_set: Optional[str],
        rule_raw: Optional[str],
        played_from: Optional[str],
        played_to: Optional[str],
        weapon: Optional[str],
        tag: Optional[str],
        query: Optional[str],
        limit: int,
        offset: int,
    ):
        if not isinstance(account, str) or not account:
            raise ValueError("account must be a non-empty string")
        if kind not in (None, "vs", "coop"):
            raise ValueError("kind must be None, 'vs', or 'coop'")
        if isinstance(limit, bool) or not isinstance(limit, int) or limit < 1 or limit > 200:
            raise ValueError("limit must be an integer between 1 and 200 (bool not allowed)")
        if isinstance(offset, bool) or not isinstance(offset, int) or offset < 0:
            raise ValueError("offset must be an integer >= 0 (bool not allowed)")
        if analysis_set is not None and (
            not isinstance(analysis_set, str) or not _ANALYSIS_SET_RE.fullmatch(analysis_set)
        ):
            raise ValueError("analysis_set must be unclassified or a mode name")
        if rule_raw is not None and (not isinstance(rule_raw, str) or not _RULE_RE.fullmatch(rule_raw)):
            raise ValueError("rule_raw must be a rule token")
        for name, value in (("played_from", played_from), ("played_to", played_to)):
            if value is not None and (not isinstance(value, str) or not _PLAYED_RE.fullmatch(value)):
                raise ValueError(f"{name} must be YYYY-MM-DDTHH:MM:SSZ")
        if played_from is not None and played_to is not None and played_from > played_to:
            raise ValueError("played_from must be at or before played_to")
        weapon = self._bounded_text(weapon, "weapon")
        tag = self._bounded_text(tag, "tag")
        query = self._bounded_text(query, "query")
        clauses = ["m.account = ?"]
        params: List[Any] = [account]
        if kind is not None:
            clauses.append("m.kind = ?")
            params.append(kind)
        if analysis_set == UNCLASSIFIED:
            clauses.append("c.account IS NULL")
        elif analysis_set is not None:
            clauses.append("c.analysis_set = ?")
            params.append(analysis_set)
        if rule_raw is not None:
            clauses.append("c.rule_raw = ?")
            params.append(rule_raw)
        if played_from is not None or played_to is not None:
            clauses.append("json_extract(d.json_text, '$.playedTime') IS NOT NULL")
        if played_from is not None:
            clauses.append("json_extract(d.json_text, '$.playedTime') >= ?")
            params.append(played_from)
        if played_to is not None:
            clauses.append("json_extract(d.json_text, '$.playedTime') <= ?")
            params.append(played_to)
        if weapon is not None:
            clauses.append(
                """EXISTS (
                    SELECT 1 FROM battle_players bp
                    WHERE bp.account = m.account AND bp.match_key = m.match_key
                      AND bp.is_myself = 1 AND bp.weapon = ?
                )"""
            )
            params.append(weapon)
        if tag is not None:
            clauses.append(
                """EXISTS (
                    SELECT 1 FROM match_tags tg
                    WHERE tg.account = m.account AND tg.match_key = m.match_key AND tg.tag = ?
                )"""
            )
            params.append(tag)
        if query is not None:
            clauses.append(
                """(
                    instr(ifnull(m.match_key, ''), ?) > 0
                    OR instr(ifnull(c.rule_raw, ''), ?) > 0
                    OR instr(ifnull(c.rule_name, ''), ?) > 0
                    OR instr(ifnull(c.analysis_set, ''), ?) > 0
                    OR instr(ifnull(c.genre, ''), ?) > 0
                    OR instr(ifnull(json_extract(d.json_text, '$.vsStage.name'), ''), ?) > 0
                    OR instr(ifnull(json_extract(d.json_text, '$.coopStage.name'), ''), ?) > 0
                    OR instr(ifnull(json_extract(d.json_text, '$.vsRule.name'), ''), ?) > 0
                    OR EXISTS (
                        SELECT 1 FROM battle_players bp
                        WHERE bp.account = m.account AND bp.match_key = m.match_key
                          AND bp.is_myself = 1 AND instr(ifnull(bp.weapon, ''), ?) > 0
                    )
                    OR EXISTS (
                        SELECT 1 FROM match_tags tg
                        WHERE tg.account = m.account AND tg.match_key = m.match_key
                          AND instr(tg.tag, ?) > 0
                    )
                )"""
            )
            params.extend([query] * 10)
        return " AND ".join(clauses), params

    def _detail_state_sql(self) -> str:
        """保存schemaの状態viewが揃うときだけ pending/unavailable を判定する。"""
        names = {
            row[0]
            for row in self._store.db.execute(
                """SELECT name FROM sqlite_master
                   WHERE type='view' AND name IN ('pending_details', 'unavailable_details')"""
            )
        }
        if names == {"pending_details", "unavailable_details"}:
            return """CASE
                WHEN d.account IS NOT NULL THEN 'available'
                WHEN EXISTS (
                    SELECT 1 FROM pending_details p
                    WHERE p.account = m.account AND p.kind = m.kind AND p.match_key = m.match_key
                ) THEN 'pending'
                WHEN EXISTS (
                    SELECT 1 FROM unavailable_details u
                    WHERE u.account = m.account AND u.kind = m.kind AND u.match_key = m.match_key
                ) THEN 'unavailable'
                ELSE 'unresolved'
            END"""
        return """CASE
                WHEN d.account IS NOT NULL THEN 'available'
                ELSE 'unresolved'
            END"""

    def list_matches(
        self,
        account: str,
        *,
        kind: Optional[str] = None,
        analysis_set: Optional[str] = None,
        rule_raw: Optional[str] = None,
        played_from: Optional[str] = None,
        played_to: Optional[str] = None,
        weapon: Optional[str] = None,
        tag: Optional[str] = None,
        query: Optional[str] = None,
        limit: int = 50,
        offset: int = 0,
    ) -> Dict[str, Any]:
        """対戦・バイトの一覧を取得する。

        母集合は matches であり、documents と match_classification を
        LEFT JOIN する（documents は response_id/account/kind/match_key、
        match_classification は account/kind/match_key 完全一致）。
        絞り込みが無いときは詳細なし/未分類も残す。
        期間は詳細の playedTime だけで、first_seen では代用しない。
        ブキは対戦詳細の自分の weapon.name だけを見る。
        順序は kind ASC, match_key ASC（同じ read snapshot 内で安定）。
        件数は絞り込み後の matches 数であり成績集計ではない。
        list では body を読み込まない。

        Args:
            account: 空でない str 必須。
            kind: None, 'vs', 'coop' のみ。
            analysis_set: None、モード名、または分類行が無いことを示す unclassified。
            rule_raw: None またはルール記号。
            played_from, played_to: None または YYYY-MM-DDTHH:MM:SSZ。
            weapon, tag, query: None または 1..80 文字。制御文字は拒否する。
            limit: bool 以外の int 1..200。
            offset: bool 以外の int >= 0。

        Returns:
            {'total': 件数, 'limit': limit, 'offset': offset, 'items': [...]}
        """
        self._check_active()
        where_sql, params = self._list_constraints(
            account, kind, analysis_set, rule_raw, played_from, played_to,
            weapon, tag, query, limit, offset,
        )
        db = self._store.db
        joined = """
        FROM matches m
        LEFT JOIN documents d
            ON d.response_id = m.detail_response_id AND d.account = m.account AND d.kind = m.kind AND d.match_key = m.match_key
        LEFT JOIN match_classification c
            ON c.account = m.account AND c.kind = m.kind AND c.match_key = m.match_key
        """
        total_row = db.execute(f"SELECT count(*) {joined} WHERE {where_sql}", params).fetchone()
        total = total_row[0] if total_row else 0

        # 一覧の取得
        # detail_state の判定:
        # - canonical 詳細あり時のみ available
        # - jobs 由来のビューがあるときだけ pending / unavailable
        # - 派生ファイルにはそのビューが無いので、詳細行が無ければ unresolved
        # played_time は詳細 playedTime のみ、first_seen による代用禁止
        # rule_raw / classification は LEFT JOIN 結果、未分類を unknown/hold に推論変換しない
        query = f"""
        SELECT
            m.account,
            m.kind,
            m.match_key,
            m.first_seen,
            m.last_seen,
            m.detail_response_id,
            {self._detail_state_sql()} AS detail_state,
            c.genre,
            c.analysis_set,
            json_extract(d.json_text, '$.playedTime') AS played_time,
            c.rule_raw
        {joined}
        WHERE {where_sql}
        ORDER BY m.kind ASC, m.match_key ASC
        LIMIT ? OFFSET ?
        """
        list_params = params + [limit, offset]
        rows = db.execute(query, list_params).fetchall()
        tags = self._tags_by_match(account, [r["match_key"] for r in rows])

        items = []
        for r in rows:
            items.append({
                "account": r["account"],
                "kind": r["kind"],
                "match_key": r["match_key"],
                "first_seen": r["first_seen"],
                "last_seen": r["last_seen"],
                "detail_response_id": r["detail_response_id"],
                "detail_state": r["detail_state"],
                "genre": r["genre"],
                "analysis_set": r["analysis_set"],
                "played_time": r["played_time"],
                "rule_raw": r["rule_raw"],
                "tags": tags.get(r["match_key"], []),
            })

        return {
            "total": total,
            "limit": limit,
            "offset": offset,
            "items": items,
        }

    def get_match(self, account: str, kind: str, match_key: str) -> Optional[Dict[str, Any]]:
        """対戦・バイトの単件を取得する。

        他 account の同じ key は漏らさない。
        detail_json は派生 JSON 表現であり raw ではない。
        未知項目/数値表記の正典は source['body_bytes'] である。
        original_records は元responses/bodies行の全table_xinfo列とsource_rowidをtyped cellで返す。
        canonical 以外の新しい pager/部分応答を採用しない。
        元 response/BLOB 不在なら破損を隠さず RuntimeError を送出する（詳細なしは通常 None）。

        Args:
            account: 空でない str 必須。
            kind: 'vs' または 'coop' のみ。
            match_key: 空でない str 必須。

        Returns:
            item + {'detail_json': ..., 'source': ...}。
            見つからなければ None。

        Raises:
            ValueError: 引数の型や値が不正な場合。
            RuntimeError: detail_response_id があるにもかかわらず元 response や BLOB が不在な場合（破損）。
        """
        self._check_active()

        if not isinstance(account, str) or not account:
            raise ValueError("account must be a non-empty string")
        if kind not in ("vs", "coop"):
            raise ValueError("kind must be 'vs' or 'coop'")
        if not isinstance(match_key, str) or not match_key:
            raise ValueError("match_key must be a non-empty string")

        db = self._store.db

        query = f"""
        SELECT
            m.account,
            m.kind,
            m.match_key,
            m.first_seen,
            m.last_seen,
            m.detail_response_id,
            {self._detail_state_sql()} AS detail_state,
            c.genre,
            c.analysis_set,
            json_extract(d.json_text, '$.playedTime') AS played_time,
            c.rule_raw,
            d.json_text AS canonical_doc_json
        FROM matches m
        LEFT JOIN documents d
            ON d.response_id = m.detail_response_id AND d.account = m.account AND d.kind = m.kind AND d.match_key = m.match_key
        LEFT JOIN match_classification c
            ON c.account = m.account AND c.kind = m.kind AND c.match_key = m.match_key
        WHERE m.account = ? AND m.kind = ? AND m.match_key = ?
        """
        row = db.execute(query, (account, kind, match_key)).fetchone()
        if not row:
            return None

        detail_response_id = row["detail_response_id"]
        detail_json: Optional[str] = None
        source: Optional[Dict[str, Any]] = None

        if detail_response_id is None:
            # 詳細なしは通常 None
            detail_json = None
            source = None
        else:
            # canonical 詳細ありのはず
            canonical_doc = row["canonical_doc_json"]
            if canonical_doc is None:
                raise RuntimeError(
                    f"Corrupt record: detail_response_id={detail_response_id} is present, "
                    f"but canonical document is missing for match ({account}, {kind}, {match_key})"
                )
            detail_json = canonical_doc

            # 応答原文は全情報archiveから返す。旧partial sliceはStore入口で拒否する。
            # responsesがあるschemaで応答またはBLOBが欠ければ破損として扱う。
            has_responses = db.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='responses'"
            ).fetchone()
            if not has_responses:
                source = None
            else:
                try:
                    response_columns, response_values, response_rowid = _read_original_table_row(
                        db, "responses", "id", detail_response_id
                    )
                except RuntimeError as exc:
                    if str(exc) == "Corrupt record: original source row is missing":
                        raise RuntimeError(
                            f"Corrupt record: response {detail_response_id} not found for match "
                            f"({account}, {kind}, {match_key})"
                        ) from None
                    raise
                response_account = _original_column_value(response_columns, response_values, "account")
                if response_account != account:
                    raise RuntimeError(
                        "Corrupt record: canonical response account does not match the selected match"
                    )
                response_body_sha = _original_column_value(
                    response_columns, response_values, "body_sha256"
                )
                if type(response_body_sha) is not str:
                    raise RuntimeError(
                        "Corrupt record: canonical response body SHA-256 is invalid"
                    )
                try:
                    body_columns, body_values, body_rowid = _read_original_table_row(
                        db, "bodies", "sha256", response_body_sha
                    )
                except RuntimeError as exc:
                    if str(exc) == "Corrupt record: original source row is missing":
                        raise RuntimeError(
                            f"Corrupt record: body BLOB missing for response {detail_response_id} "
                            f"(body_sha256={response_body_sha})"
                        ) from None
                    raise
                body_bytes = _original_column_value(body_columns, body_values, "body")
                body_sha = _original_column_value(body_columns, body_values, "sha256")
                body_length = _original_column_value(body_columns, body_values, "byte_length")
                if body_bytes is None:
                    raise RuntimeError(
                        f"Corrupt record: body BLOB missing for response {detail_response_id} "
                        f"(body_sha256={response_body_sha})"
                    )
                if type(body_bytes) is not bytes:
                    raise RuntimeError("Corrupt record: body BLOB storage type is invalid")
                actual_body_sha = hashlib.sha256(body_bytes).hexdigest()
                if (
                    type(body_sha) is not str
                    or body_sha != response_body_sha
                    or actual_body_sha != response_body_sha
                    or type(body_length) is not int
                    or body_length != len(body_bytes)
                ):
                    raise RuntimeError("Corrupt record: body BLOB metadata does not match its bytes")

                source = {
                    "response_id": _original_column_value(response_columns, response_values, "id"),
                    "operation": _original_column_value(response_columns, response_values, "operation"),
                    "fetched_at": _original_column_value(response_columns, response_values, "fetched_at"),
                    "query_id": _original_column_value(response_columns, response_values, "query_id"),
                    "app_version": _original_column_value(response_columns, response_values, "app_version"),
                    "http_status": _original_column_value(response_columns, response_values, "http_status"),
                    "body_sha256": response_body_sha,
                    "body_bytes": body_bytes,
                    "original_records": {
                        "version": 1,
                        "responses": _original_row_envelope(
                            "responses", response_columns, response_values, response_rowid
                        ),
                        "bodies": _original_row_envelope(
                            "bodies", body_columns, body_values, body_rowid,
                            body_bytes=body_bytes, body_sha256=actual_body_sha,
                        ),
                    },
                }

        return {
            "account": row["account"],
            "kind": row["kind"],
            "match_key": row["match_key"],
            "first_seen": row["first_seen"],
            "last_seen": row["last_seen"],
            "detail_response_id": row["detail_response_id"],
            "detail_state": row["detail_state"],
            "genre": row["genre"],
            "analysis_set": row["analysis_set"],
            "played_time": row["played_time"],
            "rule_raw": row["rule_raw"],
            "detail_json": detail_json,
            "source": source,
            "tags": self._tags_by_match(account, [match_key]).get(match_key, []),
        }

    def tag_target(self, account: str, match_key: str) -> Optional[Dict[str, Any]]:
        """タグを付けてよいか判定するための分類と、現在のタグだけを返す。

        本文BLOBは読まない。試合が無ければ None。
        """
        self._check_active()
        if not isinstance(account, str) or not account:
            raise ValueError("account must be a non-empty string")
        if not isinstance(match_key, str) or not match_key:
            raise ValueError("match_key must be a non-empty string")
        rows = self._store.db.execute(
            '''SELECT m.kind, c.genre, c.analysis_set
               FROM matches m
               LEFT JOIN match_classification c
                 ON c.account=m.account AND c.kind=m.kind AND c.match_key=m.match_key
               WHERE m.account=? AND m.match_key=?
               ORDER BY m.kind''',
            (account, match_key)).fetchall()
        if not rows:
            return None
        return {
            "items": [
                {"kind": row["kind"], "genre": row["genre"], "analysis_set": row["analysis_set"]}
                for row in rows
            ],
            "tags": self._tags_by_match(account, [match_key]).get(match_key, []),
        }

    def list_facets(self, account: str) -> Dict[str, Any]:
        """一覧の絞り込みに使う、そのアカウントの観測値を返す。

        ジャンルは既知のモードと unclassified を固定順で出し、観測だけにある名前を後ろに足す。
        ルール、自分のブキ名、タグは観測されたものだけを返す。他アカウントは混ぜない。
        本文BLOBは読まない。
        """
        self._check_active()
        if not isinstance(account, str) or not account:
            raise ValueError("account must be a non-empty string")
        db = self._store.db
        observed = {
            row[0] for row in db.execute(
                """SELECT DISTINCT analysis_set FROM match_classification
                   WHERE account=? AND analysis_set IS NOT NULL""",
                (account,),
            )
        }
        ordered = list(dict.fromkeys([*MODE_SLICES, UNCLASSIFIED]))
        analysis_sets = [*ordered, *sorted(observed - set(ordered))]
        rules = [
            row[0] for row in db.execute(
                """SELECT DISTINCT rule_raw FROM match_classification
                   WHERE account=? AND rule_raw IS NOT NULL AND rule_raw != ''
                   ORDER BY rule_raw""",
                (account,),
            )
        ]
        weapons = [
            row[0] for row in db.execute(
                """SELECT DISTINCT weapon FROM battle_players
                   WHERE account=? AND is_myself=1 AND weapon IS NOT NULL AND weapon != ''
                   ORDER BY weapon""",
                (account,),
            )
        ]
        tags = [
            row[0] for row in db.execute(
                """SELECT DISTINCT tag FROM match_tags
                   WHERE account=? ORDER BY tag""",
                (account,),
            )
        ]
        return {
            "analysis_sets": analysis_sets,
            "rules": rules,
            "weapons": weapons,
            "tags": tags,
        }

    def tag_summary(self, account: str) -> Dict[str, Any]:
        """オープンとプラベの人数区分ごとに、タグの付いた試合数を返す。

        区分をまたいだ合計は作らない。タグが複数ある試合はタグごとに数える。
        タグの有無で analysis_set は動かさない。本文BLOBは読まない。
        """
        self._check_active()
        if not isinstance(account, str) or not account:
            raise ValueError("account must be a non-empty string")
        db = self._store.db
        placeholders = ",".join("?" * len(TAG_SUMMARY_SETS))
        params = (account, *TAG_SUMMARY_SETS)
        match_counts = {
            row[0]: row[1]
            for row in db.execute(
                f"""SELECT analysis_set, count(*)
                    FROM match_classification
                    WHERE account=? AND analysis_set IN ({placeholders})
                    GROUP BY analysis_set""",
                params,
            )
        }
        untagged_counts = {
            row[0]: row[1]
            for row in db.execute(
                f"""SELECT c.analysis_set, count(*)
                    FROM match_classification c
                    WHERE c.account=? AND c.analysis_set IN ({placeholders})
                    AND NOT EXISTS (
                        SELECT 1 FROM match_tags t
                        WHERE t.account=c.account AND t.match_key=c.match_key
                    )
                    GROUP BY c.analysis_set""",
                params,
            )
        }
        grouped = {name: [] for name in TAG_SUMMARY_SETS}
        for analysis_set, tag, count in db.execute(
            f"""SELECT c.analysis_set, t.tag, count(*)
                FROM match_classification c
                JOIN match_tags t
                  ON t.account=c.account AND t.match_key=c.match_key
                WHERE c.account=? AND c.analysis_set IN ({placeholders})
                GROUP BY c.analysis_set, t.tag
                ORDER BY c.analysis_set, t.tag""",
            params,
        ):
            grouped[analysis_set].append({"tag": tag, "matches": count})
        return {
            "sets": [
                {
                    "analysis_set": name,
                    "matches": match_counts.get(name, 0),
                    "untagged": untagged_counts.get(name, 0),
                    "tags": grouped[name],
                }
                for name in TAG_SUMMARY_SETS
            ]
        }

    def rule_results(self, account: str) -> Dict[str, Any]:
        """対戦の区分ごとに、ルール別の勝敗を返す。

        件数は analysis_*_by_rule のまま使う。区分とルールをまたいだ合計は作らない。
        バイトの納品数は勝敗に入れない。本文BLOBは読まない。
        """
        self._check_active()
        if not isinstance(account, str) or not account:
            raise ValueError("account must be a non-empty string")
        db = self._store.db
        present = {
            row[0]
            for row in db.execute("SELECT name FROM sqlite_master WHERE type='view'")
        }
        sets = []
        for name in JUDGEMENT_SETS:
            view = f"analysis_{name}_by_rule"
            if not _VIEW_NAME.fullmatch(view) or view not in present:
                raise ValueError("MISSING_ANALYSIS_VIEW")
            rules = []
            for row in db.execute(
                f"""SELECT rule_raw, rule_name, matches, wins, losses, draws, other_judgements
                    FROM {view} WHERE account=?""",
                (account,),
            ):
                rules.append({
                    "rule_raw": row[0],
                    "rule_name": row[1],
                    "matches": _whole_count(row[2]),
                    "wins": _whole_count(row[3]),
                    "losses": _whole_count(row[4]),
                    "draws": _whole_count(row[5]),
                    "other": _whole_count(row[6]),
                })
            rules.sort(key=lambda item: (
                rule_sort_index(item["rule_raw"] or ""),
                item["rule_raw"] or "",
                item["rule_name"] or "",
            ))
            sets.append({"analysis_set": name, "rules": rules})
        return {"sets": sets}

    def rate_summary(self, account: str) -> Dict[str, Any]:
        """rate_points の系列ごとに、最新・増減・最低・最高・点数と、時系列の点を返す。

        アカウント、区分、ルール、系列を混ぜない。勝敗から値を作らない。
        正本の値は変えず、キケン度も比率のまま返す。点の時刻が文字列でも空でもないときは空にする。
        本文BLOBは読まない。
        """
        self._check_active()
        if not isinstance(account, str) or not account:
            raise ValueError("account must be a non-empty string")
        grouped: Dict[tuple, List[Dict[str, Any]]] = {}
        meta: Dict[tuple, Dict[str, Any]] = {}
        rows = self._store.db.execute(
            """SELECT series_id, label, genre, rule_raw, source, priority, played_time, value
               FROM rate_points
               WHERE account=?
               ORDER BY series_id, genre, rule_raw, label, source, priority, played_time, match_key""",
            (account,),
        )
        for series_id, label, genre, rule_raw, source, priority, played_time, value in rows:
            if _rate_hidden(series_id, label, source):
                continue
            number = _finite_number(value)
            if number is None:
                continue
            if isinstance(played_time, str) or played_time is None:
                played = played_time
            else:
                played = None
            key = (series_id, label, genre, rule_raw, source, priority)
            grouped.setdefault(key, []).append({"played_time": played, "value": number})
            meta.setdefault(key, {
                "series_id": series_id,
                "label": label,
                "genre": genre,
                "rule_raw": rule_raw,
                "source": source,
                "priority": priority,
            })
        series = []
        for key, points in grouped.items():
            item = dict(meta[key])
            values = [point["value"] for point in points]
            latest = values[-1]
            previous = values[-2] if len(values) >= 2 else None
            item.update({
                "unit": _rate_unit(item["series_id"], item["label"]),
                "count": len(values),
                "latest": latest,
                "previous": previous,
                "delta": None if previous is None else latest - previous,
                "minimum": min(values),
                "maximum": max(values),
                "points": points,
            })
            series.append(item)
        series.sort(key=lambda item: (
            genre_sort_index(item["genre"] or ""),
            rule_sort_index(item["rule_raw"] or ""),
            item["label"] or "",
            item["series_id"] or "",
        ))
        return {"series": series}

    def _tags_by_match(self, account: str, match_keys: List[str]) -> Dict[str, List[Dict[str, Any]]]:
        grouped: Dict[str, List[Dict[str, Any]]] = {key: [] for key in match_keys}
        if not match_keys:
            return grouped
        placeholders = ",".join("?" * len(match_keys))
        rows = self._store.db.execute(
            f'''SELECT match_key, tag, note, created_at, updated_at
                FROM match_tags WHERE account=? AND match_key IN ({placeholders})
                ORDER BY match_key, tag''',
            [account, *match_keys]).fetchall()
        for row in rows:
            grouped.setdefault(row["match_key"], []).append({
                "tag": row["tag"],
                "note": row["note"],
                "created_at": row["created_at"],
                "updated_at": row["updated_at"],
            })
        return grouped
