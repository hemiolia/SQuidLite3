"""SQuidLite3 データ配置（設計 0.3）の部品作成と監査。

正本の SQLite を読み取り専用で開き、同じ静止点から「本籍」ごとの小さな SQLite 部品
（正本と同じ全表・全索引・全ビュー）と目録 catalog.sqlite3 を作る。
正本へは書き込まない。純粋なライブラリで、ネットワークにも Drive にも触れない。

設計書: docs/design/SQuidLite3_データ配置.md
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import sqlite3
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Optional

HOME_RULE_VERSION = 2
CHANGE_TABLE = "archive_change_feed"
PART_SUFFIX = ".sqlite3"
CATALOG_NAME = "catalog.sqlite3"
TMP_DIR_NAME = ".tmp-parts"

_JST = timezone(timedelta(hours=9))

# 1 応答 1 部品にする operation の固定一覧（設計書「ランキング系の operation」。ここ一か所だけで定義する）。
# この一覧を変えたら HOME_RULE_VERSION を上げ、全部品を作り直す。
RANKING_OPERATIONS = (
    "EventMatchRankingPeriodQuery",
    "EventMatchRankingSeasonPaginationQuery",
    "EventMatchRankingQuery",
    "RankingHoldersFestTeamRankingHoldersPaginationQuery",
    "WeaponRankingDetail_Ranking_RefetchQuery",
    "WeaponRankingDetailQuery",
    "XRankingDetailQuery",
    "XRankingRefetchQuery",
    "DetailRankingQuery",
    "DetailTabViewWeaponTopsArRefetchQuery",
    "DetailTabViewWeaponTopsClRefetchQuery",
    "DetailTabViewWeaponTopsGlRefetchQuery",
    "DetailTabViewWeaponTopsLfRefetchQuery",
    "DetailTabViewXRankingArRefetchQuery",
    "DetailTabViewXRankingClRefetchQuery",
    "DetailTabViewXRankingGlRefetchQuery",
    "DetailTabViewXRankingLfRefetchQuery",
)
UNKNOWN_DATE = "unknown-date"

SYSTEM_TABLES = (
    "runs",
    "control",
    "manifests",
    "endpoint_heads",
    "page_fingerprints",
    "schema_version",
    "analysis_genre",
)
_BY_MATCH_TABLES = ("match_classification", "match_refs", "sightings", "documents")
_BY_RESPONSE_TABLES = ("response_fetches", "asset_refs", "entities")

# 目録 table_homes の内容（設計書「本籍規則」の表。規則の版は HOME_RULE_VERSION）
TABLE_HOMES = (
    ("matches", "その試合の本籍", "matches/<analysis_set>/<rule_raw>/<YYYY-MM>/<YYYY-MM-DD>.sqlite3"),
    ("match_classification", "その試合の本籍", "matches/<analysis_set>/<rule_raw>/<YYYY-MM>/<YYYY-MM-DD>.sqlite3"),
    ("match_refs", "(account, kind, match_key) の試合の本籍。試合行が無ければ unplaced",
     "matches/<analysis_set>/<rule_raw>/<YYYY-MM>/<YYYY-MM-DD>.sqlite3"),
    ("sightings", "(account, kind, match_key) の試合の本籍。試合行が無ければ unplaced",
     "matches/<analysis_set>/<rule_raw>/<YYYY-MM>/<YYYY-MM-DD>.sqlite3"),
    ("documents", "(account, kind, match_key) の試合の本籍。試合行が無ければ unplaced",
     "matches/<analysis_set>/<rule_raw>/<YYYY-MM>/<YYYY-MM-DD>.sqlite3"),
    ("match_tags", "(account, match_key) が一致する試合の本籍（vs 優先）。無ければ unplaced",
     "matches/<analysis_set>/<rule_raw>/<YYYY-MM>/<YYYY-MM-DD>.sqlite3"),
    ("rate_points", "match_key が fetch: で始まらなければその試合の本籍、fetch:<event_id> なら response_fetches.event_id の応答の本籍",
     "matches/<analysis_set>/<rule_raw>/<YYYY-MM>/<YYYY-MM-DD>.sqlite3 または responses/<operation>/<YYYY-MM>/<YYYY-MM-DD>.sqlite3（ランキング系は .../<YYYY-MM-DD>/<response_id>.sqlite3）"),
    ("jobs", "kind と match_key があればその試合の本籍。試合行が無い・無ければ system/jobs",
     "matches/<analysis_set>/<rule_raw>/<YYYY-MM>/<YYYY-MM-DD>.sqlite3 または system/jobs.sqlite3"),
    ("responses", "応答の本籍（documents のある試合の本籍、無ければ operation と fetched_at の日付。ランキング系の operation は 1 応答 1 部品）",
     "matches/... または responses/<operation>/<YYYY-MM>/<YYYY-MM-DD>.sqlite3（ランキング系は .../<YYYY-MM-DD>/<response_id>.sqlite3）"),
    ("response_fetches", "response_id の応答の本籍", "応答の本籍と同じ"),
    ("asset_refs", "response_id の応答の本籍", "応答の本籍と同じ"),
    ("entities", "response_id の応答の本籍", "応答の本籍と同じ"),
    ("issues", "response_id があればその応答の本籍、無ければ system/issues",
     "応答の本籍 または system/issues.sqlite3"),
    ("bodies", "応答の本文は参照する各応答の本籍（写しを含む）。画像の本文と assets は images",
     "応答の本籍と同じ（写しあり） または images/<body_sha256 の先頭2桁>.sqlite3"),
    ("assets", "body_sha256 の先頭2桁。本文が無ければ no-body",
     "images/<body_sha256 の先頭2桁>.sqlite3 または images/no-body.sqlite3"),
    ("runs", "system/<表名>", "system/runs.sqlite3"),
    ("control", "system/<表名>", "system/control.sqlite3"),
    ("manifests", "system/<表名>", "system/manifests.sqlite3"),
    ("endpoint_heads", "system/<表名>", "system/endpoint_heads.sqlite3"),
    ("page_fingerprints", "system/<表名>", "system/page_fingerprints.sqlite3"),
    ("schema_version", "system/<表名>", "system/schema_version.sqlite3"),
    ("analysis_genre", "system/<表名>", "system/analysis_genre.sqlite3"),
    ("archive_change_feed", "changed_at の JST 日付と時",
     "system/archive_change_feed/<YYYY-MM-DD>/<HH>.sqlite3"),
    ("(上のどれにも当たらない表・行)", "unplaced/<表名>", "unplaced/<表名>.sqlite3"),
)


class PartsQuestion(Exception):
    """設計書で決まっておらず、実装側で決めてはならない事態。メッセージは QUESTION: で始める。"""


# ---------------------------------------------------------------------------
# 本籍の純粋関数
# ---------------------------------------------------------------------------

def encode_segment(value: str) -> str:
    """パスの一区間を符号化する。[A-Za-z0-9_-] はそのまま、他は UTF-8 の各バイトを ~ と大文字2桁16進にする。

    空文字は受け付けない。呼び出し側で unclassified / no-rule / unknown-month などに置き換えてから呼ぶ。
    """
    if isinstance(value, (bytes, bytearray)):
        raw = bytes(value)
    else:
        raw = value.encode("utf-8", "surrogatepass")
    if not raw:
        raise ValueError("encode_segment: 空文字は呼び出し側で置き換えること")
    out = []
    for b in raw:
        ch = chr(b)
        if b < 0x80 and (ch.isalnum() or ch in "_-"):
            out.append(ch)
        else:
            out.append("~%02X" % b)
    return "".join(out)


def decode_segment(value: str) -> str:
    """encode_segment の逆。目録の files 表へ元の値を書くために使う。"""
    raw = bytearray()
    i = 0
    while i < len(value):
        if value[i] == "~":
            raw.append(int(value[i + 1:i + 3], 16))
            i += 3
        else:
            raw.extend(value[i].encode("utf-8"))
            i += 1
    return raw.decode("utf-8", "replace")


def _parse_iso_jst(value: Any) -> Optional[datetime]:
    if isinstance(value, (bytes, bytearray)):
        value = bytes(value).decode("utf-8", "replace")
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value.strip())
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return None
    return parsed.astimezone(_JST)


def jst_month(played_time: Any) -> str:
    """ISO 8601（Z またはオフセット付き）を Asia/Tokyo の YYYY-MM にする。解析できなければ unknown-month。"""
    parsed = _parse_iso_jst(played_time)
    if parsed is None:
        return "unknown-month"
    return f"{parsed.year:04d}-{parsed.month:02d}"


def jst_day_path(value: Any) -> str:
    """ISO 8601 を Asia/Tokyo の `YYYY-MM/YYYY-MM-DD` にする。解析できなければ unknown-date（観測日時などで埋めない）。"""
    parsed = _parse_iso_jst(value)
    if parsed is None:
        return UNKNOWN_DATE
    return f"{parsed.year:04d}-{parsed.month:02d}/{parsed.year:04d}-{parsed.month:02d}-{parsed.day:02d}"


def response_period(operation: str, fetched_at: str) -> str:
    """応答の日付区間。fetched_at の JST の `YYYY-MM/YYYY-MM-DD`、解析できなければ unknown-date。"""
    return jst_day_path(fetched_at)


def _feed_part_path(changed_at: Any) -> str:
    parsed = _parse_iso_jst(changed_at)
    if parsed is None:
        return f"system/{CHANGE_TABLE}/unknown-date/unknown-hour{PART_SUFFIX}"
    return (
        f"system/{CHANGE_TABLE}/{parsed.year:04d}-{parsed.month:02d}-{parsed.day:02d}/"
        f"{parsed.hour:02d}{PART_SUFFIX}"
    )


def _sql_enc(value: Any) -> str:
    if value is None:
        raise ValueError("enc: NULL")
    return encode_segment(value)


def _sql_jst_day(value: Any) -> str:
    return jst_day_path(value)


def _sql_response_period(operation: Any, fetched_at: Any) -> str:
    if isinstance(operation, (bytes, bytearray)):
        operation = bytes(operation).decode("utf-8", "replace")
    if isinstance(fetched_at, (bytes, bytearray)):
        fetched_at = bytes(fetched_at).decode("utf-8", "replace")
    return response_period(operation or "", fetched_at or "")


def _sql_feed_part(changed_at: Any) -> str:
    return _feed_part_path(changed_at)


# ---------------------------------------------------------------------------
# 小道具
# ---------------------------------------------------------------------------

def _q(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _lit(text: str) -> str:
    return "'" + text.replace("'", "''") + "'"


def _seg(expr: str, default: str) -> str:
    """列の値を本籍の区間にする SQL。NULL と空文字は default に置き換える。"""
    return f"enc(CAST(COALESCE(NULLIF({expr},''),{_lit(default)}) AS BLOB))"


def _played_expr(alias: str) -> str:
    """documents.json_text の $.playedTime（文字列のときだけ）。壊れた JSON は NULL。"""
    col = f"{alias}.json_text"
    return (
        f"CASE WHEN json_valid({col}) THEN "
        f"CASE WHEN json_type({col},'$.playedTime')='text' THEN json_extract({col},'$.playedTime') END END"
    )


def _json_text_expr(alias: str, path: str) -> str:
    col = f"{alias}.json_text"
    return f"CASE WHEN json_valid({col}) THEN json_extract({col},{_lit(path)}) END"


def _sha256_file(path: Path) -> tuple[str, int]:
    digest = hashlib.sha256()
    size = 0
    with open(path, "rb") as fh:
        while True:
            chunk = fh.read(1 << 20)
            if not chunk:
                break
            digest.update(chunk)
            size += len(chunk)
    return digest.hexdigest(), size


def _fsync_file(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _fsync_dir(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _ro_uri(path: Path) -> str:
    return f"{Path(path).resolve().as_uri()}?mode=ro"


def _register_functions(conn: sqlite3.Connection) -> None:
    conn.create_function("enc", 1, _sql_enc, deterministic=True)
    conn.create_function("jst_day", 1, _sql_jst_day, deterministic=True)
    conn.create_function("resp_period", 2, _sql_response_period, deterministic=True)
    conn.create_function("feed_part", 1, _sql_feed_part, deterministic=True)


def _part_rank(kind: str) -> int:
    return {"table": 0, "index": 1, "view": 2}[kind]


class _Schema:
    """正本のスキーマ（トランザクション内で読む）。"""

    def __init__(self, conn: sqlite3.Connection):
        rows = conn.execute(
            "SELECT type,name,tbl_name,sql FROM src.sqlite_master ORDER BY rowid"
        ).fetchall()
        self.all_objects = [tuple(r) for r in rows]
        self.part_objects = [
            r for r in self.all_objects
            if r[0] in ("table", "index", "view") and r[3] is not None and not r[1].startswith("sqlite_")
        ]
        self.part_objects.sort(key=lambda r: _part_rank(r[0]))  # 安定ソート。表→索引→ビュー
        self.sha256 = hashlib.sha256(
            json.dumps(sorted(self.part_objects), ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        ).hexdigest()
        self.internal_tables = [
            r[1] for r in self.all_objects if r[0] == "table" and r[1].startswith("sqlite_")
            and r[1] != "sqlite_master"
        ]
        listing = conn.execute("PRAGMA src.table_list").fetchall()
        flags = {r[1]: (r[2], r[4]) for r in listing}
        self.tables: dict[str, dict[str, Any]] = {}
        for typ, name, _tbl, sql in self.all_objects:
            if typ != "table" or name.startswith("sqlite_"):
                continue
            kind, without_rowid = flags.get(name, ("table", 0))
            if kind != "table":
                raise PartsQuestion(f"QUESTION: 仮想表または shadow 表 {name!r} の扱いが設計書にない")
            if without_rowid:
                raise PartsQuestion(f"QUESTION: WITHOUT ROWID 表 {name!r} は rowid を持たず、行の写し方が設計書にない")
            cols = []
            pk_cols = []
            for cid, cname, ctype, _nn, _dflt, pk, hidden in conn.execute(f"PRAGMA src.table_xinfo({_q(name)})"):
                if hidden != 0:
                    raise PartsQuestion(
                        f"QUESTION: 表 {name!r} の列 {cname!r} は生成列または hidden 列で、扱いが設計書にない"
                    )
                cols.append(cname)
                if pk:
                    pk_cols.append((pk, cname, (ctype or "").upper()))
            alias = None
            if len(pk_cols) == 1 and pk_cols[0][2] == "INTEGER":
                alias = pk_cols[0][1]
            self.tables[name] = {"cols": cols, "alias": alias}


# ---------------------------------------------------------------------------
# 本籍の SQL
# ---------------------------------------------------------------------------

def _base(table: str, alias: str, restrict: bool) -> str:
    """行の取り出し元。restrict なら cand_<表>（候補 rowid）から rowid で引く（全走査しない）。"""
    qt = f"src.{_q(table)}"
    if restrict:
        return f"{_q('cand_' + table)} c CROSS JOIN {qt} {alias} ON {alias}.rowid=c.rid"
    return f"{qt} {alias}"


def _hm_select(table: str, restrict: bool = False) -> str:
    """表の行について (rid, part, role) を返す SELECT。match_home 等は作成済みであること。

    restrict=True のときは cand_<表> に入れた rowid の行だけを、rowid の検索で引く。
    """
    unplaced = _lit(f"unplaced/{encode_segment(table)}{PART_SUFFIX}")
    system = _lit(f"system/{encode_segment(table)}{PART_SUFFIX}")
    key_join = "mh.account=x.account AND mh.kind=x.kind AND mh.match_key=x.match_key"
    base = _base(table, "x", restrict)
    if table == "matches":
        return f"SELECT x.rowid, mh.part, 'home' FROM {base} JOIN match_home mh ON {key_join}"
    if table in _BY_MATCH_TABLES:
        return (
            f"SELECT x.rowid, COALESCE(mh.part,{unplaced}), 'home' FROM {base} "
            f"LEFT JOIN match_home mh ON {key_join}"
        )
    if table == "match_tags":
        return (
            f"SELECT x.rowid, COALESCE((SELECT k.part FROM mk_home k WHERE k.account=x.account "
            f"AND k.match_key=x.match_key),{unplaced}), 'home' FROM {base}"
        )
    if table == "rate_points":
        return (
            "SELECT x.rowid, COALESCE(CASE WHEN substr(x.match_key,1,6)='fetch:' THEN "
            "(SELECT rh.part FROM src.response_fetches f JOIN response_home rh ON rh.response_id=f.response_id "
            "WHERE f.event_id=substr(x.match_key,7)) "
            "ELSE (SELECT k.part FROM mk_home k WHERE k.account=x.account AND k.match_key=x.match_key) END,"
            f"{unplaced}), 'home' FROM {base}"
        )
    if table == "jobs":
        return (
            "SELECT x.rowid, COALESCE(CASE WHEN x.kind IS NOT NULL AND x.match_key IS NOT NULL THEN "
            "(SELECT mh.part FROM match_home mh WHERE mh.account=x.account AND mh.kind=x.kind "
            f"AND mh.match_key=x.match_key) END,{system}), 'home' FROM {base}"
        )
    if table == "responses":
        return f"SELECT x.rowid, rh.part, 'home' FROM {base} JOIN response_home rh ON rh.response_id=x.id"
    if table in _BY_RESPONSE_TABLES:
        return (
            f"SELECT x.rowid, COALESCE((SELECT rh.part FROM response_home rh WHERE rh.response_id=x.response_id),"
            f"{unplaced}), 'home' FROM {base}"
        )
    if table == "issues":
        return (
            f"SELECT x.rowid, CASE WHEN x.response_id IS NULL THEN {system} ELSE "
            f"COALESCE((SELECT rh.part FROM response_home rh WHERE rh.response_id=x.response_id),{unplaced}) END, "
            f"'home' FROM {base}"
        )
    if table == "assets":
        return (
            "SELECT x.rowid, CASE WHEN x.body_sha256 IS NULL OR x.body_sha256='' THEN "
            f"{_lit('images/no-body' + PART_SUFFIX)} ELSE "
            "'images/'||enc(CAST(substr(x.body_sha256,1,2) AS BLOB))||'.sqlite3' END, 'home' "
            f"FROM {base}"
        )
    if table == "bodies":
        base_b = _base(table, "b", restrict)
        return f"SELECT b.rowid, h.part, h.role FROM {base_b} JOIN body_homes h ON h.sha=b.sha256"
    if table in SYSTEM_TABLES:
        return f"SELECT x.rowid, {system}, 'home' FROM {base}"
    if table == CHANGE_TABLE:
        return f"SELECT x.rowid, feed_part(CAST(x.changed_at AS BLOB)), 'home' FROM {base}"
    return f"SELECT x.rowid, {unplaced}, 'home' FROM {base}"


# 同じ (account, match_key) に vs と他の kind があるとき vs を優先する mk_home の作り方
_MK_SQL = (
    "SELECT account,match_key,part FROM (SELECT account,match_key,part,"
    "ROW_NUMBER() OVER (PARTITION BY account,match_key ORDER BY (kind<>'vs'),kind) rn FROM {src}) "
    "WHERE rn=1"
)


def _create_home_tables(conn: sqlite3.Connection, tables: dict[str, Any]) -> None:
    """試合の本籍・応答の本籍・(account, match_key) の本籍を全件計算する（毎周期。試合は約千行、応答は約八千行）。"""
    conn.execute("CREATE TABLE match_home(account,kind,match_key,part,PRIMARY KEY(account,kind,match_key))")
    conn.execute("CREATE TABLE response_home(response_id INTEGER PRIMARY KEY,part)")
    conn.execute("CREATE TABLE mk_home(account,match_key,part,PRIMARY KEY(account,match_key))")
    if "matches" in tables:
        played = _played_expr("d")
        has_class = "match_classification" in tables
        has_docs = "documents" in tables
        class_join = (
            "LEFT JOIN src.match_classification c ON c.account=m.account AND c.kind=m.kind "
            "AND c.match_key=m.match_key" if has_class else ""
        )
        doc_join = (
            "LEFT JOIN src.documents d ON d.response_id=m.detail_response_id AND d.account=m.account "
            "AND d.kind=m.kind AND d.match_key=m.match_key" if has_docs else ""
        )
        set_expr = _seg("c.analysis_set", "unclassified") if has_class else enc_default("unclassified")
        rule_expr = _seg("c.rule_raw", "no-rule") if has_class else enc_default("no-rule")
        day_expr = f"jst_day(CAST({played} AS BLOB))" if has_docs else _lit(UNKNOWN_DATE)
        conn.execute(
            "INSERT INTO match_home(account,kind,match_key,part) "
            f"SELECT m.account,m.kind,m.match_key,'matches/'||{set_expr}||'/'||{rule_expr}||'/'||{day_expr}||'.sqlite3' "
            f"FROM src.matches m {class_join} {doc_join}"
        )
        conn.execute("INSERT INTO mk_home(account,match_key,part) " + _MK_SQL.format(src="match_home"))
    if "responses" in tables:
        if "documents" in tables and "matches" in tables:
            doc_part = (
                "(SELECT mh.part FROM src.documents d JOIN match_home mh ON mh.account=d.account "
                "AND mh.kind=d.kind AND mh.match_key=d.match_key WHERE d.response_id=r.id "
                "ORDER BY d.account,d.kind,d.match_key LIMIT 1)"
            )
        else:
            doc_part = "NULL"
        op_seg = _seg("r.operation", "no-operation")
        period = "resp_period(CAST(r.operation AS BLOB),CAST(r.fetched_at AS BLOB))"
        ranking = ",".join(_lit(op) for op in RANKING_OPERATIONS)
        own_part = (
            f"CASE WHEN r.operation IN ({ranking}) THEN 'responses/'||{op_seg}||'/'||{period}||'/'||r.id||'.sqlite3' "
            f"ELSE 'responses/'||{op_seg}||'/'||{period}||'.sqlite3' END"
        )
        conn.execute(
            "INSERT INTO response_home(response_id,part) "
            f"SELECT r.id, COALESCE({doc_part}, {own_part}) FROM src.responses r"
        )


def _create_body_homes(conn: sqlite3.Connection, tables: dict[str, Any], restrict: bool) -> None:
    """bodies の本籍（画像は images、応答の本文は参照する各応答の本籍へ写し）。

    restrict=True のときは cand_sha に入れた本文だけを作る（差分周期）。
    """
    has_responses = "responses" in tables
    has_assets = "assets" in tables
    conn.execute("CREATE TABLE body_homes(sha,part,role)")
    conn.execute("CREATE TEMP TABLE bh_img(sha PRIMARY KEY,part)")
    only_a = "AND body_sha256 IN (SELECT sha FROM cand_sha)" if restrict else ""
    only_r = "WHERE r.body_sha256 IN (SELECT sha FROM cand_sha)" if restrict else ""
    only_b = "AND b.sha256 IN (SELECT sha FROM cand_sha)" if restrict else ""
    if has_assets:
        conn.execute(
            "INSERT OR IGNORE INTO bh_img SELECT body_sha256,"
            "'images/'||enc(CAST(substr(body_sha256,1,2) AS BLOB))||'.sqlite3' FROM src.assets "
            f"WHERE body_sha256 IS NOT NULL AND body_sha256<>'' {only_a}"
        )
    conn.execute("CREATE TEMP TABLE bh_resp(sha,part,min_id,overall_min)")
    if has_responses:
        conn.execute(
            "INSERT INTO bh_resp SELECT sha,part,min_id,MIN(min_id) OVER (PARTITION BY sha) FROM ("
            "SELECT r.body_sha256 sha, rh.part part, MIN(r.id) min_id FROM src.responses r "
            f"JOIN response_home rh ON rh.response_id=r.id {only_r} GROUP BY r.body_sha256, rh.part)"
        )
    conn.execute("INSERT INTO body_homes SELECT sha,part,'home' FROM bh_img")
    conn.execute(
        "INSERT INTO body_homes SELECT sha,part,"
        "CASE WHEN min_id=overall_min AND sha NOT IN (SELECT sha FROM bh_img) THEN 'home' ELSE 'copy' END "
        "FROM bh_resp"
    )
    conn.execute(
        "INSERT INTO body_homes SELECT b.sha256,"
        f"{_lit('unplaced/' + encode_segment('bodies') + PART_SUFFIX)},'home' FROM src.bodies b "
        f"WHERE b.sha256 NOT IN (SELECT sha FROM bh_img) AND b.sha256 NOT IN (SELECT sha FROM bh_resp) {only_b}"
    )
    conn.execute("CREATE INDEX body_homes_sha ON body_homes(sha)")


def enc_default(text: str) -> str:
    return _lit(encode_segment(text))


# ---------------------------------------------------------------------------
# 部品ファイルの作成
# ---------------------------------------------------------------------------

def _write_part(final_path: Path, tmp_dir: Path, schema: _Schema, work_uri: str,
                meta: dict[str, Any]) -> dict[str, Any]:
    tmp_dir.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix="part-", suffix=".tmp", dir=tmp_dir)
    os.close(fd)
    tmp = Path(tmp_name)
    tmp.unlink()
    conn = sqlite3.connect(str(tmp), isolation_level=None, uri=True)
    try:
        conn.execute("PRAGMA foreign_keys=OFF")
        conn.execute("PRAGMA journal_mode=DELETE")
        conn.execute("ATTACH ? AS w", (work_uri,))
        conn.execute("BEGIN")
        for _typ, _name, _tbl, sql in schema.part_objects:
            conn.execute(sql)
        conn.execute("CREATE TABLE _part(key TEXT PRIMARY KEY, value TEXT)")
        conn.execute("CREATE TABLE _copies(table_name TEXT, rowid INTEGER, PRIMARY KEY(table_name,rowid))")
        part = meta["path"]
        for table, info in schema.tables.items():
            cols = ",".join(_q(c) for c in info["cols"])
            stg = f"w.{_q('stg_' + table)}"
            if info["alias"] is None:
                conn.execute(
                    f"INSERT INTO main.{_q(table)}(rowid,{cols}) SELECT _rowid,{cols} FROM {stg} WHERE _part=?",
                    (part,),
                )
            else:
                conn.execute(
                    f"INSERT INTO main.{_q(table)}({cols}) SELECT {cols} FROM {stg} WHERE _part=?",
                    (part,),
                )
            conn.execute(
                f"INSERT INTO _copies SELECT {_lit(table)},_rowid FROM {stg} WHERE _part=? AND _role='copy'",
                (part,),
            )
        # AUTOINCREMENT 表への挿入で部品の sqlite_sequence に行ができる。内部表は部品に入れない
        if conn.execute("SELECT 1 FROM sqlite_master WHERE name='sqlite_sequence'").fetchone():
            conn.execute("DELETE FROM sqlite_sequence")
        for key in ("path", "rule_version", "through_event_id", "built_at", "source_schema_sha256"):
            value = meta[key]
            conn.execute("INSERT INTO _part VALUES(?,?)", (key, None if value is None else str(value)))
        conn.execute("COMMIT")
        conn.execute("DETACH w")
    except BaseException:
        conn.close()
        for suffix in ("", "-journal"):
            try:
                os.unlink(str(tmp) + suffix)
            except FileNotFoundError:
                pass
        raise
    conn.close()
    _fsync_file(tmp)
    final_path.parent.mkdir(parents=True, exist_ok=True)
    os.replace(tmp, final_path)
    _fsync_dir(final_path.parent)
    sha, size = _sha256_file(final_path)
    return {"path": part, "sha256": sha, "bytes": size}


_STATE_DDL = (
    "CREATE TABLE IF NOT EXISTS state.row_homes(table_name TEXT, rowid INTEGER, part TEXT, role TEXT, "
    "PRIMARY KEY(table_name,rowid,part))",
    "CREATE INDEX IF NOT EXISTS state.row_homes_part ON row_homes(part)",
    "CREATE TABLE IF NOT EXISTS state.match_home_prev(account,kind,match_key,part,PRIMARY KEY(account,kind,match_key))",
    "CREATE TABLE IF NOT EXISTS state.response_home_prev(response_id INTEGER PRIMARY KEY,part)",
    "CREATE TABLE IF NOT EXISTS state.meta(key TEXT PRIMARY KEY,value TEXT)",
    "CREATE TABLE IF NOT EXISTS state.part_files(path TEXT PRIMARY KEY,bytes INTEGER,sha256 TEXT,"
    "built_at TEXT,through_event_id INTEGER)",
    # worker が Drive への送信と照合を確認した部品（部品の SHA-256 が part_files と同じなら送信済み）
    "CREATE TABLE IF NOT EXISTS state.published(path TEXT PRIMARY KEY,sha256 TEXT,bytes INTEGER,published_at TEXT)",
)


def _meta(conn: sqlite3.Connection, key: str) -> Optional[str]:
    row = conn.execute("SELECT value FROM state.meta WHERE key=?", (key,)).fetchone()
    return None if row is None else row[0]


class _PlanConn(sqlite3.Connection):
    """scan_log を設定すると、実行する各 SQL のバイトコード（EXPLAIN）を調べ、正本（src）の表・索引を
    条件なしで頭から読む（Rewind/Last）ものを scan_log に (表名, SQL の先頭) で溜める（検査用）。

    EXPLAIN QUERY PLAN の SCAN は別名の表を別名でしか示さないので、バイトコードで表を特定する。
    """

    scan_log: Optional[list] = None
    _VERBS = {"SELECT", "INSERT", "DELETE", "UPDATE", "WITH", "REPLACE"}
    _src_info: Optional[tuple] = None

    def _src(self) -> tuple:
        if self._src_info is None:
            index = next(r[0] for r in super().execute("PRAGMA database_list") if r[1] == "src")
            roots = {r[0]: r[1] for r in super().execute("SELECT rootpage,tbl_name FROM src.sqlite_master")}
            self._src_info = (index, roots)
        return self._src_info

    def execute(self, sql, parameters=()):  # type: ignore[override]
        log = self.scan_log
        if log is not None:
            words = sql.lstrip().split(None, 1)
            if words and words[0].upper() in self._VERBS and self._attached():
                index, roots = self._src()
                cursors: dict[int, tuple] = {}
                for _addr, opcode, p1, p2, p3, *_rest in super().execute("EXPLAIN " + sql, parameters):
                    if opcode in ("OpenRead", "OpenWrite"):
                        cursors[p1] = (p3, p2)
                    elif opcode in ("Rewind", "Last") and cursors.get(p1, (None, None))[0] == index:
                        table = roots.get(cursors[p1][1])
                        if table is not None:
                            log.append((table, " ".join(sql.split())[:120]))
        return super().execute(sql, parameters)

    def _attached(self) -> bool:
        return any(r[1] == "src" for r in super().execute("PRAGMA database_list"))


def _plan_delta(conn: sqlite3.Connection, tables: dict[str, Any], prev: int) -> None:
    """差分周期の作り直す部品と、行ごとの本籍の再計算（設計書「毎周期の計算量」）。

    全表を走査しない。行の本籍を計算し直すのは次の行だけ（cand_<表>）。
      (b) 変更追跡（event_id > 前回 through）に現れた行（new_rowid の現在の行と old_rowid）。
      (a) 本籍が変わった試合・応答に属する行。索引のある表は索引で引く（matches・match_classification・
          match_tags は主キー、response_fetches・asset_refs・entities は response_id の索引）。
          索引の無い表（documents・sightings・match_refs・jobs・rate_points・issues）は、状態 DB の
          row_homes で「前回その試合・応答の旧本籍にあった行」と「unplaced・system/jobs・system/issues にある行」を
          rowid で引いて再計算する。どれも作り直す部品に入る行なので、部品の大きさ以上の読みは生じない。
    旧本籍と新本籍の差（row_homes との差）から作り直す部品を決める。
    """
    names = list(tables)
    conn.execute("CREATE TABLE feed_ev(table_name TEXT, old_rid INTEGER, new_rid INTEGER)")
    conn.execute(
        "INSERT INTO feed_ev SELECT table_name,old_rowid,new_rowid "
        f"FROM src.{_q(CHANGE_TABLE)} WHERE event_id>?", (prev,)
    )
    conn.execute("CREATE INDEX feed_ev_t ON feed_ev(table_name)")

    # 本籍が変わった試合・応答・(account, match_key)（新規・削除・移動）
    conn.execute("CREATE TABLE chg_match(account,kind,match_key,old_part,new_part)")
    conn.execute(
        "INSERT INTO chg_match SELECT h.account,h.kind,h.match_key,p.part,h.part FROM main.match_home h "
        "LEFT JOIN state.match_home_prev p ON p.account=h.account AND p.kind=h.kind AND p.match_key=h.match_key "
        "WHERE p.part IS NOT h.part"
    )
    conn.execute(
        "INSERT INTO chg_match SELECT p.account,p.kind,p.match_key,p.part,NULL FROM state.match_home_prev p "
        "LEFT JOIN main.match_home h ON h.account=p.account AND h.kind=p.kind AND h.match_key=p.match_key "
        "WHERE h.part IS NULL"
    )
    conn.execute("CREATE TABLE mk_home_prev(account,match_key,part,PRIMARY KEY(account,match_key))")
    conn.execute("INSERT INTO mk_home_prev " + _MK_SQL.format(src="state.match_home_prev"))
    conn.execute("CREATE TABLE chg_mk(account,match_key,old_part,new_part)")
    conn.execute(
        "INSERT INTO chg_mk SELECT h.account,h.match_key,p.part,h.part FROM main.mk_home h "
        "LEFT JOIN mk_home_prev p ON p.account=h.account AND p.match_key=h.match_key WHERE p.part IS NOT h.part"
    )
    conn.execute(
        "INSERT INTO chg_mk SELECT p.account,p.match_key,p.part,NULL FROM mk_home_prev p "
        "LEFT JOIN main.mk_home h ON h.account=p.account AND h.match_key=p.match_key WHERE h.part IS NULL"
    )
    conn.execute("CREATE TABLE chg_resp(response_id INTEGER,old_part,new_part)")
    conn.execute(
        "INSERT INTO chg_resp SELECT h.response_id,p.part,h.part FROM main.response_home h "
        "LEFT JOIN state.response_home_prev p ON p.response_id=h.response_id WHERE p.part IS NOT h.part"
    )
    conn.execute(
        "INSERT INTO chg_resp SELECT p.response_id,p.part,NULL FROM state.response_home_prev p "
        "LEFT JOIN main.response_home h ON h.response_id=p.response_id WHERE h.part IS NULL"
    )
    entity_changed = any(
        conn.execute(f"SELECT 1 FROM {t} LIMIT 1").fetchone() for t in ("chg_match", "chg_mk", "chg_resp")
    )

    # 変更追跡の old_rowid が前回あった部品（必ず作り直す）
    conn.execute("CREATE TABLE feed_old_parts(part TEXT PRIMARY KEY)")
    conn.execute(
        "INSERT OR IGNORE INTO feed_old_parts SELECT h.part FROM feed_ev e "
        "JOIN state.row_homes h ON h.table_name=e.table_name AND h.rowid=e.old_rid WHERE e.old_rid IS NOT NULL"
    )
    # 行を再計算する旧本籍の部品
    conn.execute("CREATE TABLE tp(part TEXT PRIMARY KEY)")
    conn.execute("INSERT OR IGNORE INTO tp SELECT part FROM feed_old_parts")
    for table, column in (("chg_match", "old_part"), ("chg_mk", "old_part"), ("chg_resp", "old_part")):
        conn.execute(f"INSERT OR IGNORE INTO tp SELECT {column} FROM {table} WHERE {column} IS NOT NULL")
    if entity_changed:
        conn.execute(
            "INSERT OR IGNORE INTO tp SELECT path FROM state.part_files "
            "WHERE path LIKE 'unplaced/%' OR path IN (?,?)",
            (f"system/{encode_segment('jobs')}{PART_SUFFIX}", f"system/{encode_segment('issues')}{PART_SUFFIX}"),
        )

    # 候補 rowid
    for table in names:
        cand = _q("cand_" + table)
        conn.execute(f"CREATE TABLE {cand}(rid INTEGER PRIMARY KEY)")
        if table == CHANGE_TABLE:
            # 追記だけの表。前回の through より後の行がすべて新規
            conn.execute(f"INSERT INTO {cand} SELECT event_id FROM src.{_q(CHANGE_TABLE)} WHERE event_id>?", (prev,))
            continue
        conn.execute(
            f"INSERT OR IGNORE INTO {cand} SELECT old_rid FROM feed_ev WHERE table_name=? AND old_rid IS NOT NULL",
            (table,),
        )
        conn.execute(
            f"INSERT OR IGNORE INTO {cand} SELECT new_rid FROM feed_ev WHERE table_name=? AND new_rid IS NOT NULL",
            (table,),
        )
        conn.execute(
            f"INSERT OR IGNORE INTO {cand} SELECT h.rowid FROM tp CROSS JOIN state.row_homes h "
            "INDEXED BY row_homes_part ON h.part=tp.part WHERE h.table_name=?",
            (table,),
        )
    # 索引で引ける行（応答に属する行・試合に属する行）
    if "responses" in tables:
        conn.execute("INSERT OR IGNORE INTO cand_responses SELECT response_id FROM chg_resp")
    for table in ("response_fetches", "asset_refs", "entities"):
        if table in tables:
            conn.execute(
                f"INSERT OR IGNORE INTO {_q('cand_' + table)} SELECT x.rowid FROM chg_resp c "
                f"CROSS JOIN src.{_q(table)} x ON x.response_id=c.response_id"
            )
    for table in ("matches", "match_classification"):
        if table in tables:
            conn.execute(
                f"INSERT OR IGNORE INTO {_q('cand_' + table)} SELECT x.rowid FROM chg_match c "
                f"CROSS JOIN src.{_q(table)} x ON x.account=c.account AND x.kind=c.kind AND x.match_key=c.match_key"
            )
    if "match_tags" in tables:
        for table in ("chg_match", "chg_mk"):
            conn.execute(
                f"INSERT OR IGNORE INTO cand_match_tags SELECT x.rowid FROM {table} c "
                "CROSS JOIN src.match_tags x ON x.account=c.account AND x.match_key=c.match_key"
            )
    # 本文: 候補の応答・資産・本文が指す本文を、sha で引いて候補にする
    if "bodies" in tables:
        conn.execute("CREATE TABLE cand_sha(sha TEXT PRIMARY KEY)")
        conn.execute(
            "INSERT OR IGNORE INTO cand_sha SELECT b.sha256 FROM cand_bodies c "
            "CROSS JOIN src.bodies b ON b.rowid=c.rid"
        )
        if "responses" in tables:
            conn.execute(
                "INSERT OR IGNORE INTO cand_sha SELECT r.body_sha256 FROM cand_responses c "
                "CROSS JOIN src.responses r ON r.rowid=c.rid WHERE r.body_sha256 IS NOT NULL"
            )
        if "assets" in tables:
            conn.execute(
                "INSERT OR IGNORE INTO cand_sha SELECT x.body_sha256 FROM cand_assets c "
                "CROSS JOIN src.assets x ON x.rowid=c.rid WHERE x.body_sha256 IS NOT NULL AND x.body_sha256<>''"
            )
        conn.execute(
            "INSERT OR IGNORE INTO cand_bodies SELECT b.rowid FROM cand_sha s "
            "CROSS JOIN src.bodies b ON b.sha256=s.sha"
        )
        _create_body_homes(conn, tables, restrict=True)

    # 候補の行の新しい本籍
    for table in names:
        hm = _q("hm_" + table)
        conn.execute(f"CREATE TABLE {hm}(rid INTEGER,part TEXT,role TEXT)")
        conn.execute(f"INSERT INTO {hm} {_hm_select(table, True)}")

    # 作り直す部品: 旧本籍との差、変更追跡の行の旧・新の部品
    for table in names:
        hm = _q("hm_" + table)
        cand = _q("cand_" + table)
        conn.execute(
            f"INSERT OR IGNORE INTO rebuild SELECT part FROM (SELECT rid,part,role FROM {hm} "
            f"EXCEPT SELECT h.rowid,h.part,h.role FROM {cand} c JOIN state.row_homes h "
            "ON h.table_name=? AND h.rowid=c.rid)",
            (table,),
        )
        conn.execute(
            f"INSERT OR IGNORE INTO rebuild SELECT part FROM (SELECT h.rowid,h.part,h.role FROM {cand} c "
            f"JOIN state.row_homes h ON h.table_name=? AND h.rowid=c.rid EXCEPT SELECT rid,part,role FROM {hm})",
            (table,),
        )
        conn.execute(
            f"INSERT OR IGNORE INTO rebuild SELECT h.part FROM feed_ev e JOIN {hm} h ON h.rid=e.new_rid "
            "WHERE e.table_name=? AND e.new_rid IS NOT NULL",
            (table,),
        )
    conn.execute("INSERT OR IGNORE INTO rebuild SELECT part FROM feed_old_parts")


def build_parts(
    source_path: str | os.PathLike,
    out_dir: str | os.PathLike,
    state_path: str | os.PathLike,
    work_dir: str | os.PathLike | None = None,
    *,
    after_snapshot: Optional[Callable[[], None]] = None,
    scan_log: Optional[list] = None,
) -> dict[str, Any]:
    """一回分の作成。初回（または規則版・スキーマ変更・変更追跡なし）は全部品、以後は差分。

    after_snapshot はテスト用: 作業 DB への写しが済み、正本から離れた直後に呼ぶ。
    scan_log はテスト用: リストを渡すと、正本の表を索引なしで全走査した SQL が (表名, SQL) で入る。
    """
    source = Path(source_path)
    out = Path(out_dir)
    state = Path(state_path)
    if not source.is_file():
        raise FileNotFoundError(source)
    out.mkdir(parents=True, exist_ok=True)
    state.parent.mkdir(parents=True, exist_ok=True)
    if work_dir is not None:
        Path(work_dir).mkdir(parents=True, exist_ok=True)
    work_root = Path(tempfile.mkdtemp(prefix="parts-work-", dir=None if work_dir is None else str(work_dir)))
    work_file = work_root / "work.sqlite3"
    built_at = datetime.now(timezone.utc).isoformat()
    conn = sqlite3.connect(str(work_file), isolation_level=None, uri=True, factory=_PlanConn)
    conn.scan_log = scan_log
    try:
        conn.execute("PRAGMA journal_mode=OFF")
        conn.execute("PRAGMA synchronous=OFF")
        _register_functions(conn)
        conn.execute("ATTACH ? AS src", (_ro_uri(source),))
        conn.execute("ATTACH ? AS state", (str(state),))
        for ddl in _STATE_DDL:
            conn.execute(ddl)

        # ---- 同じ静止点（ここから COMMIT まで正本の読み取り一つ） ----
        conn.execute("BEGIN")
        try:
            has_feed = conn.execute(
                "SELECT 1 FROM src.sqlite_master WHERE type='table' AND name=?", (CHANGE_TABLE,)
            ).fetchone() is not None
            through: Optional[int] = None
            if has_feed:
                value = conn.execute(f"SELECT MAX(event_id) FROM src.{_q(CHANGE_TABLE)}").fetchone()[0]
                through = 0 if value is None else int(value)
            schema = _Schema(conn)
            tables = schema.tables
            names = list(tables)

            # ---- 全行の本籍を計算するか、差分だけにするか ----
            prev_through = _meta(conn, "through_event_id")
            full = (
                through is None
                or prev_through is None
                or _meta(conn, "rule_version") != str(HOME_RULE_VERSION)
                or _meta(conn, "source_schema_sha256") != schema.sha256
            )
            if not full:
                prev = int(prev_through)
                if through < prev:
                    full = True
                elif conn.execute(
                    f"SELECT 1 FROM src.{_q(CHANGE_TABLE)} WHERE event_id>? AND old_rowid IS NULL AND new_rowid IS NULL LIMIT 1",
                    (prev,),
                ).fetchone():
                    full = True
                else:
                    feed_tables = [r[0] for r in conn.execute(
                        f"SELECT DISTINCT table_name FROM src.{_q(CHANGE_TABLE)} WHERE event_id>?", (prev,))]
                    if any(t not in tables for t in feed_tables if t != CHANGE_TABLE):
                        full = True

            _create_home_tables(conn, tables)
            conn.execute("CREATE TABLE rebuild(path TEXT PRIMARY KEY)")
            if full:
                if "bodies" in tables:
                    _create_body_homes(conn, tables, restrict=False)
                for table in names:
                    hm = _q("hm_" + table)
                    conn.execute(f"CREATE TABLE {hm}(rid INTEGER,part TEXT,role TEXT)")
                    conn.execute(f"INSERT INTO {hm} {_hm_select(table, False)}")
                    conn.execute(f"CREATE INDEX {_q('hm_' + table + '_part')} ON {hm}(part)")
                    conn.execute(f"CREATE INDEX {_q('hm_' + table + '_rid')} ON {hm}(rid)")
                    conn.execute(f"INSERT OR IGNORE INTO rebuild SELECT DISTINCT part FROM {hm}")
                conn.execute("INSERT OR IGNORE INTO rebuild SELECT DISTINCT part FROM state.row_homes")
                conn.execute("INSERT OR IGNORE INTO rebuild SELECT path FROM state.part_files")
            else:
                _plan_delta(conn, tables, int(prev_through))
                # 部品ファイルが無くなっていたら作り直す
                for (path,) in conn.execute("SELECT path FROM state.part_files").fetchall():
                    if not (out / path).is_file():
                        conn.execute("INSERT OR IGNORE INTO rebuild VALUES(?)", (path,))

            # ---- 作業表へ写す ----
            for table, info in tables.items():
                cols = ",".join(_q(c) for c in info["cols"])
                xcols = ",".join("x." + _q(c) for c in info["cols"])
                stg = _q("stg_" + table)
                conn.execute(f"CREATE TABLE {stg}(_part,_rowid,_role,{cols})")
                conn.execute(
                    f"INSERT INTO {stg} SELECT h.part,h.rid,h.role,{xcols} "
                    f"FROM {_q('hm_' + table)} h JOIN src.{_q(table)} x ON x.rowid=h.rid "
                    "WHERE h.part IN (SELECT path FROM rebuild)"
                )
                if not full:
                    # 作り直す部品にある、再計算しなかった行（前回の本籍のまま）
                    conn.execute(
                        f"INSERT INTO {stg} SELECT r.part,r.rowid,r.role,{xcols} "
                        "FROM state.row_homes r INDEXED BY row_homes_part "
                        "CROSS JOIN src." + _q(table) + " x ON x.rowid=r.rowid "
                        "WHERE r.table_name=? AND r.part IN (SELECT path FROM rebuild) "
                        f"AND r.rowid NOT IN (SELECT rid FROM {_q('cand_' + table)})",
                        (table,),
                    )

            # ---- 目録の材料 ----
            _stage_catalog_material(conn, schema, tables)
            rebuilt = [r[0] for r in conn.execute("SELECT path FROM rebuild ORDER BY path")]
            conn.execute("COMMIT")
        except BaseException:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise

        # ---- ここから正本には触れない ----
        for table in tables:
            conn.execute(
                f"CREATE INDEX {_q('stg_' + table + '_part')} ON {_q('stg_' + table)}(_part)"
            )
        if after_snapshot is not None:
            after_snapshot()
        work_uri = _ro_uri(work_file)
        results = []
        for path in rebuilt:
            results.append(
                _write_part(
                    out / path, out / TMP_DIR_NAME, schema, work_uri,
                    {
                        "path": path,
                        "rule_version": HOME_RULE_VERSION,
                        "through_event_id": through,
                        "built_at": built_at,
                        "source_schema_sha256": schema.sha256,
                    },
                )
            )

        # ---- 状態 DB の更新（一つのトランザクション） ----
        conn.execute("BEGIN IMMEDIATE")
        try:
            if full:
                conn.execute("DELETE FROM state.row_homes")
            for table in names:
                if not full:
                    conn.execute(
                        "DELETE FROM state.row_homes WHERE table_name=? "
                        f"AND rowid IN (SELECT rid FROM main.{_q('cand_' + table)})",
                        (table,),
                    )
                conn.execute(
                    "INSERT OR REPLACE INTO state.row_homes(table_name,rowid,part,role) "
                    f"SELECT {_lit(table)},rid,part,role FROM main.{_q('hm_' + table)}"
                )
            conn.execute("DELETE FROM state.match_home_prev")
            conn.execute("INSERT INTO state.match_home_prev SELECT account,kind,match_key,part FROM main.match_home")
            conn.execute("DELETE FROM state.response_home_prev")
            conn.execute("INSERT INTO state.response_home_prev SELECT response_id,part FROM main.response_home")
            for res in results:
                conn.execute(
                    "INSERT OR REPLACE INTO state.part_files VALUES(?,?,?,?,?)",
                    (res["path"], res["bytes"], res["sha256"], built_at, through),
                )
            if through is None:
                conn.execute("DELETE FROM state.meta WHERE key='through_event_id'")
            else:
                conn.execute("INSERT OR REPLACE INTO state.meta VALUES('through_event_id',?)", (str(through),))
            conn.execute("INSERT OR REPLACE INTO state.meta VALUES('rule_version',?)", (str(HOME_RULE_VERSION),))
            conn.execute("INSERT OR REPLACE INTO state.meta VALUES('source_schema_sha256',?)", (schema.sha256,))
            conn.execute("COMMIT")
        except BaseException:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise

        catalog = _write_catalog(out, state, work_uri, through, built_at)
        return {
            "through_event_id": through,
            "full_rebuild": full,
            "rebuilt_parts": results,
            "catalog": catalog,
            "built_at": built_at,
        }
    finally:
        try:
            conn.close()
        finally:
            shutil.rmtree(work_root, ignore_errors=True)
            shutil.rmtree(out / TMP_DIR_NAME, ignore_errors=True)


# ---------------------------------------------------------------------------
# 目録
# ---------------------------------------------------------------------------

def _stage_catalog_material(conn: sqlite3.Connection, schema: _Schema, tables: dict[str, Any]) -> None:
    """目録のうち、正本の静止点から取らねばならない表を作業 DB に作る（BEGIN の中で呼ぶ）。"""
    conn.execute(
        "CREATE TABLE cat_match_index(account,kind,match_key,analysis_set,rule_raw,rule_name,played_time,"
        "stage,judgement,my_weapon,tags,detail_available,part_path)"
    )
    if "matches" in tables:
        has_class = "match_classification" in tables
        has_docs = "documents" in tables
        has_tags = "match_tags" in tables
        class_join = (
            "LEFT JOIN src.match_classification c ON c.account=m.account AND c.kind=m.kind "
            "AND c.match_key=m.match_key" if has_class else ""
        )
        doc_join = (
            "LEFT JOIN src.documents d ON d.response_id=m.detail_response_id AND d.account=m.account "
            "AND d.kind=m.kind AND d.match_key=m.match_key" if has_docs else ""
        )
        null = "NULL"
        stage = (
            f"CASE m.kind WHEN 'vs' THEN {_json_text_expr('d', '$.vsStage.name')} "
            f"WHEN 'coop' THEN {_json_text_expr('d', '$.coopStage.name')} END" if has_docs else null
        )
        judgement = (
            f"CASE m.kind WHEN 'vs' THEN {_json_text_expr('d', '$.judgement')} END" if has_docs else null
        )
        weapon = (
            f"CASE m.kind WHEN 'vs' THEN {_json_text_expr('d', '$.player.weapon.name')} END" if has_docs else null
        )
        tags = (
            "(SELECT group_concat(tag,'、') FROM (SELECT t.tag FROM src.match_tags t "
            "WHERE t.account=m.account AND t.match_key=m.match_key ORDER BY t.tag))" if has_tags else null
        )
        set_expr = "COALESCE(NULLIF(c.analysis_set,''),'unclassified')" if has_class else "'unclassified'"
        rule_expr = "COALESCE(NULLIF(c.rule_raw,''),'no-rule')" if has_class else "'no-rule'"
        name_expr = "c.rule_name" if has_class else null
        played = _played_expr("d") if has_docs else null
        avail = "CASE WHEN d.response_id IS NOT NULL THEN 1 ELSE 0 END" if has_docs else "0"
        conn.execute(
            "INSERT INTO cat_match_index "
            f"SELECT m.account,m.kind,m.match_key,{set_expr},{rule_expr},{name_expr},"
            f"{played},{stage},{judgement},{weapon},{tags},{avail},mh.part "
            "FROM src.matches m JOIN match_home mh ON mh.account=m.account AND mh.kind=m.kind "
            f"AND mh.match_key=m.match_key {class_join} {doc_join}"
        )
    conn.execute(
        "CREATE TABLE cat_response_index(response_id,account,operation,fetched_at,http_status,part_path)"
    )
    if "responses" in tables:
        conn.execute(
            "INSERT INTO cat_response_index SELECT r.id,r.account,r.operation,r.fetched_at,r.http_status,rh.part "
            "FROM src.responses r JOIN response_home rh ON rh.response_id=r.id"
        )
    conn.execute("CREATE TABLE cat_asset_index(url,state,body_sha256,content_type,part_path)")
    if "assets" in tables:
        conn.execute(
            "INSERT INTO cat_asset_index SELECT x.url,x.state,x.body_sha256,x.content_type,"
            "CASE WHEN x.body_sha256 IS NULL OR x.body_sha256='' THEN "
            f"{_lit('images/no-body' + PART_SUFFIX)} ELSE "
            "'images/'||enc(CAST(substr(x.body_sha256,1,2) AS BLOB))||'.sqlite3' END "
            "FROM src.assets x"
        )
    conn.execute("CREATE TABLE cat_labels(kind,code,ja)")
    if "analysis_genre" in tables:
        conn.execute("INSERT INTO cat_labels SELECT 'analysis_set',genre,label FROM src.analysis_genre")
    if "match_classification" in tables:
        conn.execute(
            "INSERT INTO cat_labels SELECT DISTINCT 'rule_raw',rule_raw,rule_name FROM src.match_classification "
            "WHERE rule_raw IS NOT NULL AND rule_name IS NOT NULL"
        )
    conn.execute("CREATE TABLE cat_source_schema(type,name,tbl_name,sql)")
    for row in schema.all_objects:
        conn.execute("INSERT INTO cat_source_schema VALUES(?,?,?,?)", row)
    conn.execute(
        "CREATE TABLE cat_internal(table_name TEXT,rowid INTEGER,column_index INTEGER,column_name TEXT,value)"
    )
    for name in schema.internal_tables:
        cols = [r[1] for r in conn.execute(f"PRAGMA src.table_xinfo({_q(name)})")]
        for index, col in enumerate(cols):
            conn.execute(
                f"INSERT INTO cat_internal SELECT {_lit(name)},x.rowid,{index},{_lit(col)},x.{_q(col)} "
                f"FROM src.{_q(name)} x"
            )


_CATALOG_DDL = (
    "CREATE TABLE files(path TEXT PRIMARY KEY,domain TEXT,analysis_set TEXT,rule_raw TEXT,month TEXT,day TEXT,"
    "operation TEXT,period TEXT,response_id INTEGER,bytes INTEGER,sha256 TEXT,rows_json TEXT,"
    "built_through_event_id INTEGER,built_at TEXT)",
    "CREATE TABLE match_index(account TEXT,kind TEXT,match_key TEXT,analysis_set TEXT,rule_raw TEXT,rule_name TEXT,"
    "played_time TEXT,stage TEXT,judgement TEXT,my_weapon TEXT,tags TEXT,detail_available INTEGER,part_path TEXT)",
    "CREATE TABLE response_index(response_id INTEGER,account TEXT,operation TEXT,fetched_at TEXT,"
    "http_status INTEGER,part_path TEXT)",
    "CREATE TABLE asset_index(url TEXT,state TEXT,body_sha256 TEXT,content_type TEXT,part_path TEXT)",
    "CREATE TABLE table_homes(table_name TEXT,rule_ja TEXT,path_pattern TEXT)",
    "CREATE TABLE labels(kind TEXT,code TEXT,ja TEXT)",
    "CREATE TABLE recipes(question_ja TEXT,steps_ja TEXT,sql TEXT)",
    "CREATE TABLE status(through_event_id INTEGER,built_at TEXT,published_at TEXT,last_audit_at TEXT,"
    "last_audit_result TEXT,rule_version INTEGER)",
    "CREATE TABLE source_schema(type TEXT,name TEXT,tbl_name TEXT,sql TEXT)",
    "CREATE TABLE source_sqlite_internal(table_name TEXT,rowid INTEGER,column_index INTEGER,"
    "column_name TEXT,value)",
)


def _path_fields(path: str) -> dict[str, Any]:
    """部品の住所から files 表の列を作る（住所の規則は設計書「置き場所」）。"""
    segs = path[: -len(PART_SUFFIX)].split("/")
    fields: dict[str, Any] = {
        "domain": segs[0], "analysis_set": None, "rule_raw": None, "month": None, "day": None,
        "operation": None, "period": None, "response_id": None,
    }
    if segs[0] == "matches" and len(segs) in (4, 5):
        fields.update(analysis_set=decode_segment(segs[1]), rule_raw=decode_segment(segs[2]))
        if len(segs) == 5:  # matches/<set>/<rule>/<YYYY-MM>/<YYYY-MM-DD>
            fields.update(month=segs[3], day=segs[4])
        else:  # matches/<set>/<rule>/unknown-date
            fields.update(month=segs[3], day=segs[3])
    elif segs[0] == "responses" and len(segs) >= 3:
        fields.update(operation=decode_segment(segs[1]))
        rest = segs[2:]
        if len(rest) >= 2 and rest[-1].isdigit():  # ランキング系: .../<日付区間>/<response_id>
            fields["response_id"] = int(rest[-1])
            rest = rest[:-1]
        if len(rest) == 2:  # <YYYY-MM>/<YYYY-MM-DD>
            fields.update(month=rest[0], day=rest[1], period=rest[1])
        else:  # unknown-date
            fields.update(month=rest[0], day=rest[0], period=rest[0])
    return fields


def _write_catalog(out: Path, state: Path, work_uri: str, through: Optional[int], built_at: str) -> dict[str, Any]:
    tmp_dir = out / TMP_DIR_NAME
    tmp_dir.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix="catalog-", suffix=".tmp", dir=tmp_dir)
    os.close(fd)
    tmp = Path(tmp_name)
    tmp.unlink()
    conn = sqlite3.connect(str(tmp), isolation_level=None, uri=True)
    try:
        conn.execute("PRAGMA journal_mode=DELETE")
        conn.execute("ATTACH ? AS w", (work_uri,))
        conn.execute("ATTACH ? AS s", (_ro_uri(state),))
        conn.execute("BEGIN")
        for ddl in _CATALOG_DDL:
            conn.execute(ddl)
        counts: dict[str, dict[str, dict[str, int]]] = {}
        for part, table, role, n in conn.execute(
            "SELECT part,table_name,role,count(*) FROM s.row_homes GROUP BY part,table_name,role"
        ):
            counts.setdefault(part, {}).setdefault(table, {"home": 0, "copy": 0})[role] = n
        for path, nbytes, sha, part_built, part_through in conn.execute(
            "SELECT path,bytes,sha256,built_at,through_event_id FROM s.part_files ORDER BY path"
        ).fetchall():
            fields = _path_fields(path)
            conn.execute(
                "INSERT INTO files VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (path, fields["domain"], fields["analysis_set"], fields["rule_raw"], fields["month"],
                 fields["day"], fields["operation"], fields["period"], fields["response_id"], nbytes, sha,
                 json.dumps(counts.get(path, {}), ensure_ascii=False, sort_keys=True), part_through, part_built),
            )
        conn.execute("INSERT INTO match_index SELECT * FROM w.cat_match_index")
        conn.execute("INSERT INTO response_index SELECT * FROM w.cat_response_index")
        conn.execute("INSERT INTO asset_index SELECT * FROM w.cat_asset_index")
        conn.execute("INSERT INTO labels SELECT * FROM w.cat_labels")
        conn.execute("INSERT INTO source_schema SELECT * FROM w.cat_source_schema")
        conn.execute("INSERT INTO source_sqlite_internal SELECT * FROM w.cat_internal")
        conn.executemany("INSERT INTO table_homes VALUES(?,?,?)", TABLE_HOMES)
        conn.execute(
            "INSERT INTO status(through_event_id,built_at,rule_version) VALUES(?,?,?)",
            (through, built_at, HOME_RULE_VERSION),
        )
        conn.execute("COMMIT")
        conn.execute("DETACH w")
        conn.execute("DETACH s")
    except BaseException:
        conn.close()
        for suffix in ("", "-journal"):
            try:
                os.unlink(str(tmp) + suffix)
            except FileNotFoundError:
                pass
        raise
    conn.close()
    _fsync_file(tmp)
    final = out / CATALOG_NAME
    os.replace(tmp, final)
    _fsync_dir(out)
    sha, size = _sha256_file(final)
    return {"path": CATALOG_NAME, "sha256": sha, "bytes": size}


# ---------------------------------------------------------------------------
# 監査
# ---------------------------------------------------------------------------

def _value_repr(column: str) -> str:
    c = _q(column)
    return (
        f"typeof({c})||':'||CASE typeof({c}) WHEN 'integer' THEN CAST({c} AS TEXT) "
        f"WHEN 'real' THEN printf('%!.17g',{c}) WHEN 'null' THEN '' ELSE hex({c}) END"
    )


def _digest_query(table: str, columns: list[str], where: str = "") -> str:
    reps = "||'|'||".join(_value_repr(c) for c in columns)
    return f"SELECT rowid,{reps} FROM {_q(table)} {where}"


def _digest(rowid: int, rep: str) -> bytes:
    return hashlib.sha256(f"{rowid}\x1f{rep}".encode("ascii")).digest()


def _visible_columns(conn: sqlite3.Connection, table: str) -> list[str]:
    cols = []
    for _cid, name, _t, _nn, _d, _pk, hidden in conn.execute(f"PRAGMA table_xinfo({_q(table)})"):
        if hidden != 0:
            raise PartsQuestion(f"QUESTION: 表 {table!r} の列 {name!r} は生成列または hidden 列で、扱いが設計書にない")
        cols.append(name)
    return cols


def audit(source_path: str | os.PathLike, parts_dir: str | os.PathLike) -> dict[str, Any]:
    """正本と全部品の本籍行の和を、表ごとに (rowid, 各列の typeof と値) の SHA-256 で照合する。"""
    parts_root = Path(parts_dir)
    src = sqlite3.connect(_ro_uri(Path(source_path)), uri=True, isolation_level=None)
    src_digests: dict[str, dict[int, bytes]] = {}
    columns: dict[str, list[str]] = {}
    try:
        src.execute("BEGIN")
        names = [
            r[0] for r in src.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite\\_%' ESCAPE '\\' "
                "ORDER BY name"
            )
        ]
        for table in names:
            columns[table] = _visible_columns(src, table)
            digests: dict[int, bytes] = {}
            for rowid, rep in src.execute(_digest_query(table, columns[table])):
                digests[rowid] = _digest(rowid, rep)
            src_digests[table] = digests
        src.execute("COMMIT")
    finally:
        src.close()

    part_paths = sorted(
        p for p in parts_root.rglob("*" + PART_SUFFIX)
        if TMP_DIR_NAME not in p.relative_to(parts_root).parts and p.relative_to(parts_root).as_posix() != CATALOG_NAME
    )
    part_digests: dict[str, dict[int, list[bytes]]] = {t: {} for t in names}
    copies: list[tuple[str, int, bytes, str]] = []
    errors: list[str] = []
    for path in part_paths:
        rel = path.relative_to(parts_root).as_posix()
        conn = sqlite3.connect(_ro_uri(path), uri=True)
        try:
            have = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            if "_copies" not in have or "_part" not in have:
                errors.append(f"{rel}: _part/_copies が無い")
                continue
            copy_rows = {(t, rid) for t, rid in conn.execute("SELECT table_name,rowid FROM _copies")}
            for table in names:
                if table not in have:
                    errors.append(f"{rel}: 表 {table} が無い")
                    continue
                for rowid, rep in conn.execute(_digest_query(table, columns[table])):
                    digest = _digest(rowid, rep)
                    if (table, rowid) in copy_rows:
                        copies.append((table, rowid, digest, rel))
                    else:
                        part_digests[table].setdefault(rowid, []).append(digest)
        finally:
            conn.close()

    tables_result: dict[str, Any] = {}
    all_ok = not errors
    for table in names:
        source = src_digests[table]
        parts = part_digests[table]
        bad = []
        for rowid, digest in source.items():
            got = parts.get(rowid)
            if got is None or len(got) != 1 or got[0] != digest:
                bad.append(rowid)
        bad.extend(rowid for rowid in parts if rowid not in source)
        bad.sort()
        tables_result[table] = {
            "source_rows": len(source),
            "part_rows": sum(len(v) for v in parts.values()),
            "mismatched": len(bad),
            "first_mismatch_rowids": bad[:10],
        }
        if bad or len(source) != sum(len(v) for v in parts.values()):
            all_ok = False
    copy_bad = []
    for table, rowid, digest, rel in copies:
        homes = part_digests.get(table, {}).get(rowid)
        if not homes or homes[0] != digest:
            copy_bad.append([table, rowid, rel])
    if copy_bad:
        all_ok = False
    return {
        "ok": all_ok,
        "tables": tables_result,
        "copies": {"checked": len(copies), "mismatched": len(copy_bad), "first_mismatch": copy_bad[:10]},
        "part_files": len(part_paths),
        "errors": errors,
    }
