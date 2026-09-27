"""承認済み統合GUIの共通読み取り層。

対戦/バイトの一覧と単件取得を提供する。
ネットワークやUI方式を選ばず、DB作成・移行・書込みは一切行わない。
未知項目および数値表記の正典は source['body_bytes'] であり、
detail_json は派生 JSON 表現である。
"""

from pathlib import Path
from typing import Any, Dict, List, Optional, Union
from .store import Store


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
        except Exception:
            self._store.close()
            self._store = None
            raise
        self._entered = True
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self._entered = False
        if self._store is not None:
            try:
                self._store.close()
            finally:
                self._store = None

    def _check_active(self) -> None:
        if not self._entered or self._store is None:
            raise RuntimeError("RecordReader method called outside of active context")

    def list_matches(
        self,
        account: str,
        *,
        kind: Optional[str] = None,
        limit: int = 50,
        offset: int = 0,
    ) -> Dict[str, Any]:
        """対戦・バイトの一覧を取得する。

        母集合は matches であり、documents と match_classification を
        LEFT JOIN する（documents は response_id/account/kind/match_key、
        match_classification は account/kind/match_key 完全一致）。
        詳細なし/未分類も残す。
        順序は kind ASC, match_key ASC（同じ read snapshot 内で安定）。
        件数は総 matches 数であり成績集計ではない。
        list では body を読み込まない。
        タグ/文字列検索/期間/ルール/ブキ filter はこの工程で追加しない。

        Args:
            account: 空でない str 必須。
            kind: None, 'vs', 'coop' のみ。
            limit: bool 以外の int 1..200。
            offset: bool 以外の int >= 0。

        Returns:
            {'total': 件数, 'limit': limit, 'offset': offset, 'items': [...]}
        """
        self._check_active()

        # パラメータ検証
        if not isinstance(account, str) or not account:
            raise ValueError("account must be a non-empty string")
        if kind not in (None, "vs", "coop"):
            raise ValueError("kind must be None, 'vs', or 'coop'")
        if isinstance(limit, bool) or not isinstance(limit, int) or limit < 1 or limit > 200:
            raise ValueError("limit must be an integer between 1 and 200 (bool not allowed)")
        if isinstance(offset, bool) or not isinstance(offset, int) or offset < 0:
            raise ValueError("offset must be an integer >= 0 (bool not allowed)")

        db = self._store.db

        # 総件数の取得（母集合 matches の件数、成績集計ではない）
        where_clauses = ["m.account = ?"]
        params: List[Any] = [account]
        if kind is not None:
            where_clauses.append("m.kind = ?")
            params.append(kind)

        where_sql = " AND ".join(where_clauses)
        total_row = db.execute(f"SELECT count(*) FROM matches m WHERE {where_sql}", params).fetchone()
        total = total_row[0] if total_row else 0

        # 一覧の取得
        # detail_state の判定:
        # - canonical 詳細あり時のみ available
        # - なければ既存 pending_details / unavailable_details ビューへ account/kind/match_key 完全一致で照合して pending / unavailable
        # - それ以外 unresolved
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
            CASE
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
            END AS detail_state,
            c.genre,
            c.analysis_set,
            json_extract(d.json_text, '$.playedTime') AS played_time,
            c.rule_raw
        FROM matches m
        LEFT JOIN documents d
            ON d.response_id = m.detail_response_id AND d.account = m.account AND d.kind = m.kind AND d.match_key = m.match_key
        LEFT JOIN match_classification c
            ON c.account = m.account AND c.kind = m.kind AND c.match_key = m.match_key
        WHERE {where_sql}
        ORDER BY m.kind ASC, m.match_key ASC
        LIMIT ? OFFSET ?
        """
        list_params = params + [limit, offset]
        rows = db.execute(query, list_params).fetchall()

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
        ヘッダー/認証データは返さない。
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

        query = """
        SELECT
            m.account,
            m.kind,
            m.match_key,
            m.first_seen,
            m.last_seen,
            m.detail_response_id,
            CASE
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
            END AS detail_state,
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

            # source の取得: canonical detail_response_id の responses JOIN bodies から取得
            # body_bytes は bytes で一字も再 serialize しない。
            # ヘッダー/認証データは返さない。
            # 未知項目/数値表記の正典は body_bytes。
            source_query = """
            SELECT
                r.id AS response_id,
                r.operation,
                r.fetched_at,
                r.query_id,
                r.app_version,
                r.http_status,
                r.body_sha256,
                b.body AS body_bytes
            FROM responses r
            LEFT JOIN bodies b ON b.sha256 = r.body_sha256
            WHERE r.id = ?
            """
            source_row = db.execute(source_query, (detail_response_id,)).fetchone()
            if not source_row:
                raise RuntimeError(
                    f"Corrupt record: response {detail_response_id} not found for match "
                    f"({account}, {kind}, {match_key})"
                )
            if source_row["body_bytes"] is None:
                raise RuntimeError(
                    f"Corrupt record: body BLOB missing for response {detail_response_id} "
                    f"(body_sha256={source_row['body_sha256']})"
                )

            source = {
                "response_id": source_row["response_id"],
                "operation": source_row["operation"],
                "fetched_at": source_row["fetched_at"],
                "query_id": source_row["query_id"],
                "app_version": source_row["app_version"],
                "http_status": source_row["http_status"],
                "body_sha256": source_row["body_sha256"],
                "body_bytes": source_row["body_bytes"],
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
        }
