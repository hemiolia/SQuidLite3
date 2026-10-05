"""部品作成と監査（src/python/ikarchive/parts.py）の試験。人工 DB のみを使う。

本籍規則 版 3（設計書「版 3 の改定」）:
  試合は 1 試合 1 部品、試合詳細でない応答は時単位（ランキング系は 1 応答 1 部品）、
  sightings は response_id の応答の本籍、response_fetches は自身の fetched_at の日付（fetches/）。
"""

import hashlib
import json
import shutil
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src/python"))
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))

import ikarchive.parts as parts_module
from ikarchive.change_feed import install_change_feed
from ikarchive.parts import (
    HOME_RULE_VERSION,
    PartsQuestion,
    audit,
    build_parts,
    decode_segment,
    encode_segment,
    fetch_part_path,
    jst_day_path,
    jst_hour_path,
    jst_month,
    response_period,
    RANKING_OPERATIONS,
)
from ikarchive.store import Store
from ikarchive.writer_guards import install_writer_guards

import parts_build


def match_path(key, analysis_set="xmatch", rule="AREA", day="2026-10/2026-10-05"):
    """版 3 の試合の部品の住所（1 試合 1 ファイル）。key は encode_segment 済みの値。"""
    return f"matches/{analysis_set}/{rule}/{day}/{key}.sqlite3"


M1_PART = match_path("m1")
M2_PART = match_path("m2")
M3_PART = match_path("m3", rule="GOAL", day="2026-10/2026-10-01")  # 2026-09-30T20:00Z は JST で 10-01
M4_PART = match_path("m4", analysis_set="nawabari", rule="TURF", day="2026-09/2026-09-10")
M5_PART = "matches/unclassified/no-rule/unknown-date/m5.sqlite3"
MATCH_PART = M1_PART
# seed の ConfigQuery の応答は fetched_at が UTC 02:00 = JST 11 時
CONFIG_OCT = "responses/ConfigQuery/2026-10/2026-10-05/11.sqlite3"
CONFIG_SEP = "responses/ConfigQuery/2026-09/2026-09-05/11.sqlite3"
FETCH_OCT = "fetches/2026-10/2026-10-05.sqlite3"
FETCH_SEP = "fetches/2026-09/2026-09-05.sqlite3"
FEED_PREFIX = "system/archive_change_feed/"


def sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


class Source:
    """人工の正本。Store で通常のスキーマを作り、変更追跡と書き手の守りを入れる。"""

    def __init__(self, path: Path, feed: bool = True):
        self.path = path
        path.parent.mkdir(parents=True, exist_ok=True)
        Store(path).close()
        self.next_id = 1
        self.next_event = 1
        self.detail_id: dict[str, int] = {}  # match_key -> 詳細の応答の id
        self.config_ids: tuple[int, int] = (0, 0)  # seed の ConfigQuery の応答
        if feed:
            conn = self.connect()
            install_change_feed(conn)
            install_writer_guards(conn)
            conn.commit()
            conn.close()

    def connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path, timeout=30)
        conn.execute("PRAGMA recursive_triggers=ON")
        conn.execute("PRAGMA foreign_keys=ON")
        return conn

    def response(self, conn, operation, fetched_at, body: bytes, status=200):
        rid = self.next_id
        self.next_id += 1
        conn.execute("INSERT OR IGNORE INTO bodies VALUES(?,?,?)", (sha(body), body, len(body)))
        conn.execute(
            "INSERT INTO responses(id,event_id,account,fetched_at,operation,variables_json,http_status,"
            "headers_json,body_sha256) VALUES(?,?,?,?,?,?,?,?,?)",
            (rid, f"ev{rid}", "acc", fetched_at, operation, "{}", status, "{}", sha(body)),
        )
        conn.execute(
            "INSERT INTO response_fetches(event_id,response_id,fetched_at,headers_json) VALUES(?,?,?,?)",
            (f"ev{rid}", rid, fetched_at, "{}"),
        )
        return rid

    def refetch(self, conn, response_id, fetched_at):
        """同じ応答の取り直し。response_fetches の行が一つ増えるだけ（応答・本文・試合は変わらない）。"""
        event_id = f"refetch{self.next_event}"
        self.next_event += 1
        conn.execute(
            "INSERT INTO response_fetches(event_id,response_id,fetched_at,headers_json) VALUES(?,?,?,?)",
            (event_id, response_id, fetched_at, "{}"),
        )
        return event_id

    def list_response(self, conn, operation, fetched_at, seen):
        """試合の詳細ではない一覧の応答。seen は [(kind, match_key)]。目撃記録（sightings）はその応答に付く。"""
        n = self.next_event
        self.next_event += 1
        rid = self.response(conn, operation, fetched_at, json.dumps({"list": n}).encode())
        for index, (kind, key) in enumerate(seen):
            conn.execute(
                "INSERT INTO sightings(response_id,account,kind,match_key,path,summary_json) VALUES(?,?,?,?,?,?)",
                (rid, "acc", kind, key, f"$.nodes[{index}]", "{}"),
            )
        return rid

    @staticmethod
    def detail_body(key, played):
        return json.dumps({"id": key, "playedTime": played, "judgement": "WIN",
                           "vsStage": {"name": "ステージ"}, "player": {"weapon": {"name": "ブキ"}}}).encode()

    @staticmethod
    def classify(conn, kind, key, analysis_set, rule, rid):
        conn.execute(
            "INSERT INTO match_classification(account,kind,match_key,genre,mode_raw,bankara_mode,rule_raw,"
            "rule_name,roster_class,analysis_set,team_count,my_player_count,opponent_counts,"
            "detail_response_id,classified_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            ("acc", kind, key, analysis_set, "X_MATCH", None, rule, rule, "four_vs_four", analysis_set,
             2, 4, "[4]", rid, "t"),
        )

    def match(self, conn, key, played="2026-10-05T01:00:00Z", analysis_set="xmatch", rule="AREA",
              kind="vs", classify=True, fetched_at="2026-10-05T02:00:00+00:00"):
        body = self.detail_body(key, played)
        rid = self.response(conn, "VsHistoryDetailQuery", fetched_at, body)
        self.detail_id[key] = rid
        conn.execute(
            "INSERT INTO matches(account,kind,match_key,first_seen,last_seen,detail_response_id) VALUES(?,?,?,?,?,?)",
            ("acc", kind, key, "t", "t", rid),
        )
        conn.execute(
            "INSERT INTO documents(response_id,account,kind,match_key,json_text) VALUES(?,?,?,?,?)",
            (rid, "acc", kind, key, body.decode()),
        )
        conn.execute(
            "INSERT INTO match_refs(account,kind,remote_id,match_key) VALUES(?,?,?,?)",
            ("acc", kind, "remote-" + key, key),
        )
        conn.execute(
            "INSERT INTO sightings(response_id,account,kind,match_key,path,summary_json) VALUES(?,?,?,?,?,?)",
            (rid, "acc", kind, key, "$.x", "{}"),
        )
        if classify:
            self.classify(conn, kind, key, analysis_set, rule, rid)
        return rid

    def pending_match(self, conn, key):
        """一覧にだけ現れた試合（詳細が未取得）。分類も日時も無いので unclassified/no-rule/unknown-date に置かれる。"""
        conn.execute(
            "INSERT INTO matches(account,kind,match_key,first_seen,last_seen,detail_response_id) VALUES(?,?,?,?,?,NULL)",
            ("acc", "vs", key, "t", "t"),
        )

    def orphan_rows(self, conn, key):
        """親の試合がまだ無い documents・rate_points・jobs（試合があとから現れると unplaced / system から試合へ移る）。"""
        body = self.detail_body(key, "2026-09-12T00:00:00Z")
        rid = self.response(conn, "VsHistoryDetailQuery", "2026-10-05T02:00:00+00:00", body)
        self.detail_id[key] = rid
        conn.execute(
            "INSERT INTO documents(response_id,account,kind,match_key,json_text) VALUES(?,?,?,?,?)",
            (rid, "acc", "vs", key, body.decode()),
        )
        conn.execute("INSERT INTO rate_points VALUES('acc','so','L','xmatch','AREA',?,NULL,3.5,'src','p')", (key,))
        conn.execute(
            "INSERT INTO jobs(account,operation,variables_json,kind,match_key) VALUES('acc','VsHistoryDetailQuery',?,'vs',?)",
            ('{"k":"%s"}' % key, key),
        )
        return rid

    def adopt_orphans(self, conn, key, analysis_set="xmatch", rule="AREA"):
        """orphan_rows の親の試合が現れる。"""
        rid = self.detail_id[key]
        conn.execute(
            "INSERT INTO matches(account,kind,match_key,first_seen,last_seen,detail_response_id) VALUES(?,?,?,?,?,?)",
            ("acc", "vs", key, "t", "t", rid),
        )
        conn.execute("INSERT INTO match_refs(account,kind,remote_id,match_key) VALUES(?,?,?,?)",
                     ("acc", "vs", "remote-" + key, key))
        self.classify(conn, "vs", key, analysis_set, rule, rid)

    def attach_detail(self, conn, key, played="2026-10-05T01:00:00Z", analysis_set="xmatch", rule="AREA",
                      fetched_at="2026-10-05T02:00:00+00:00"):
        """pending_match の試合に詳細が届く。matches の行は残り、詳細の応答・documents・目撃記録・分類が付く。"""
        body = self.detail_body(key, played)
        rid = self.response(conn, "VsHistoryDetailQuery", fetched_at, body)
        self.detail_id[key] = rid
        conn.execute(
            "INSERT INTO documents(response_id,account,kind,match_key,json_text) VALUES(?,?,?,?,?)",
            (rid, "acc", "vs", key, body.decode()),
        )
        conn.execute(
            "INSERT INTO sightings(response_id,account,kind,match_key,path,summary_json) VALUES(?,?,?,?,?,?)",
            (rid, "acc", "vs", key, "$.x", "{}"),
        )
        self.classify(conn, "vs", key, analysis_set, rule, rid)
        conn.execute("UPDATE matches SET detail_response_id=? WHERE account='acc' AND kind='vs' AND match_key=?",
                     (rid, key))
        return rid

    def seed(self):
        conn = self.connect()
        with conn:
            self.match(conn, "m1")
            self.match(conn, "m2")
            self.match(conn, "m3", played="2026-09-30T20:00:00Z", rule="GOAL")  # JST では 10 月
            self.match(conn, "m4", played="2026-09-10T00:00:00Z", analysis_set="nawabari", rule="TURF")
            self.match(conn, "m5", played="broken", classify=False)
            conn.execute("INSERT INTO match_tags VALUES('acc','m1','タグ甲',NULL,'t','t')")
            conn.execute("INSERT INTO match_tags VALUES('acc','m1','タグ乙','n','t','t')")
            conn.execute("INSERT INTO match_tags VALUES('acc','nomatch','孤児',NULL,'t','t')")
            conn.execute("INSERT INTO rate_points VALUES('acc','s','l','xmatch','AREA','m1',NULL,1.5,'src','p')")
            conn.execute("INSERT INTO runs(id,started_at,status) VALUES(1,'t','ok')")
            conn.execute("INSERT INTO jobs(account,operation,variables_json,kind,match_key) VALUES('acc','op','{}','vs','m1')")
            conn.execute("INSERT INTO jobs(account,operation,variables_json) VALUES('acc','op2','{}')")
            # 試合に属さない応答。同じ本文を別の月の応答が共有する（本文の写し）
            shared = b'{"shared":true}'
            r1 = self.response(conn, "ConfigQuery", "2026-10-05T02:00:00+00:00", shared)
            r2 = self.response(conn, "ConfigQuery", "2026-09-05T02:00:00+00:00", shared)
            self.config_ids = (r1, r2)
            conn.execute("INSERT INTO entities VALUES('acc','T','e1',?,'{}')", (r1,))
            conn.execute("INSERT INTO issues(response_id,code,context,created_at) VALUES(?,?,?,?)", (r2, "c", "{}", "t"))
            conn.execute("INSERT INTO issues(code,context,created_at) VALUES('c2','{}','t')")
            # 取得時点の値（match_key が fetch:<event_id>）は、その取得の応答の本籍に置かれる
            conn.execute("INSERT INTO rate_points VALUES('acc','sw','ブキ','weapon',NULL,?,NULL,2.5,'api_snapshot','primary')",
                         (f"fetch:ev{r1}",))
            # 画像
            img = b"\x89PNG\r\n\x1a\n\x00\x01"
            conn.execute("INSERT INTO bodies VALUES(?,?,?)", (sha(img), img, len(img)))
            conn.execute("INSERT INTO assets(url,state,body_sha256,content_type) VALUES('u1','done',?, 'image/png')", (sha(img),))
            conn.execute("INSERT INTO assets(url,state) VALUES('u2','pending')")
            conn.execute("INSERT INTO asset_refs VALUES(?,?,?)", (r1, "u1", "$.img"))
        conn.close()

    def execute(self, sql, params=()):
        conn = self.connect()
        with conn:
            conn.execute(sql, params)
        conn.close()


def part_sha(out: Path) -> dict:
    return {
        p.relative_to(out).as_posix(): hashlib.sha256(p.read_bytes()).hexdigest()
        for p in out.rglob("*.sqlite3")
        if p.name != "catalog.sqlite3"
    }


def rows(path: Path, sql, params=()):
    conn = sqlite3.connect(f"{path.resolve().as_uri()}?mode=ro", uri=True)
    try:
        return conn.execute(sql, params).fetchall()
    finally:
        conn.close()


def assert_audit_clean(test, report):
    """作成時点基準の監査: 全表で unexplained が 0、写しも一致、読めない部品なし。"""
    test.assertTrue(report["ok"], report)
    test.assertEqual(report["errors"], [])
    for table, info in report["tables"].items():
        test.assertEqual(info["unexplained"], 0, (table, info))
    test.assertEqual(report["copies"]["mismatched"], 0, report["copies"])


class PartsTestBase(unittest.TestCase):
    feed = True

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        root = Path(self.tmp.name)
        self.src = Source(root / "database" / "a.sqlite3", feed=self.feed)
        self.out = root / "parts"
        self.state = root / "state.sqlite3"
        self.src.seed()

    def tearDown(self):
        self.tmp.cleanup()

    def build(self, **kw):
        return build_parts(self.src.path, self.out, self.state, **kw)

    def audit(self):
        return audit(self.src.path, self.out)

    def audit_clean(self):
        """監査を回し、全表で unexplained が 0 であることを確かめる。報告を返す。"""
        report = self.audit()
        assert_audit_clean(self, report)
        return report

    def changed_parts(self, before):
        after = part_sha(self.out)
        return {p for p in after if before.get(p) != after[p]}

    @staticmethod
    def paths(result):
        return {r["path"] for r in result["rebuilt_parts"]}

    @staticmethod
    def non_feed(paths):
        return {p for p in paths if not p.startswith(FEED_PREFIX)}


class PureFunctionTests(unittest.TestCase):
    def test_encode_segment(self):
        self.assertEqual(encode_segment("AREA_x-1"), "AREA_x-1")
        self.assertEqual(encode_segment("あ/."), "~E3~81~82~2F~2E")
        self.assertEqual(decode_segment(encode_segment("あ/.a")), "あ/.a")
        with self.assertRaises(ValueError):
            encode_segment("")

    def test_jst_month(self):
        self.assertEqual(jst_month("2026-09-30T20:00:00Z"), "2026-10")
        self.assertEqual(jst_month("2026-09-30T14:59:59Z"), "2026-09")
        self.assertEqual(jst_month("2026-10-01T00:00:00+09:00"), "2026-10")
        self.assertEqual(jst_month("2026-09-30T23:00:00-05:00"), "2026-10")
        self.assertEqual(jst_month("2026-10-01"), "unknown-month")
        self.assertEqual(jst_month("zzz"), "unknown-month")
        self.assertEqual(jst_month(None), "unknown-month")
        self.assertEqual(jst_day_path("2026-09-30T14:59:59Z"), "2026-09/2026-09-30")
        self.assertEqual(jst_day_path("2026-10-01"), "unknown-date")
        self.assertEqual(jst_day_path(None), "unknown-date")

    def test_hour_period_and_fetch_paths(self):
        # 時は日本時間の 2 桁
        self.assertEqual(jst_hour_path("2026-09-30T20:00:00Z"), "2026-10/2026-10-01/05")
        self.assertEqual(jst_hour_path("2026-09-30T14:59:59Z"), "2026-09/2026-09-30/23")
        self.assertEqual(jst_hour_path("2026-10-01T00:00:00+09:00"), "2026-10/2026-10-01/00")
        self.assertEqual(jst_hour_path("2026-09-30T23:00:00-05:00"), "2026-10/2026-10-01/13")
        for bad in ("2026-10-01", "zzz", "", None):
            self.assertEqual(jst_hour_path(bad), "unknown-date", bad)
        # 通常の operation は時まで、ランキング系は日まで（1 応答 1 部品なので response_id が続く）
        self.assertEqual(response_period("ConfigQuery", "2026-09-30T20:00:00+00:00"), "2026-10/2026-10-01/05")
        self.assertEqual(response_period(RANKING_OPERATIONS[0], "2026-09-30T20:00:00+00:00"), "2026-10/2026-10-01")
        self.assertEqual(response_period("ConfigQuery", "zzz"), "unknown-date")
        self.assertEqual(response_period(RANKING_OPERATIONS[0], "zzz"), "unknown-date")
        # response_fetches の行は自身の fetched_at の日付
        self.assertEqual(fetch_part_path("2026-09-30T20:00:00+00:00"), "fetches/2026-10/2026-10-01.sqlite3")
        self.assertEqual(fetch_part_path("2026-09-30T14:59:59Z"), "fetches/2026-09/2026-09-30.sqlite3")
        self.assertEqual(fetch_part_path("zzz"), "fetches/unknown-date.sqlite3")
        self.assertEqual(fetch_part_path(None), "fetches/unknown-date.sqlite3")


class FullBuildTests(PartsTestBase):
    def test_full_build_audit_schema_views(self):
        result = self.build()
        self.assertTrue(result["full_rebuild"])
        self.assertEqual(result["match_index"], {"mode": "full", "recomputed": 5, "reused": 0})
        self.assertIsNotNone(result["through_event_id"])
        report = self.audit_clean()
        for table, info in report["tables"].items():
            self.assertEqual(info["mismatched"], 0, table)
            self.assertEqual(info["source_rows"], info["part_rows"], table)
        self.assertGreater(report["copies"]["checked"], 0)
        self.assertEqual(report["copies"]["mismatched"], 0)

        # 住所（版 3）: 試合は 1 試合 1 部品、応答は時単位、取得の記録は日単位
        paths = set(part_sha(self.out))
        for expected in (M1_PART, M2_PART, M3_PART, M4_PART, M5_PART, CONFIG_OCT, CONFIG_SEP, FETCH_OCT, FETCH_SEP):
            self.assertIn(expected, paths)
        self.assertIn("system/runs.sqlite3", paths)
        self.assertIn("system/jobs.sqlite3", paths)
        self.assertIn("system/issues.sqlite3", paths)
        self.assertIn("images/no-body.sqlite3", paths)
        self.assertIn("unplaced/match_tags.sqlite3", paths)
        self.assertTrue(any(p.startswith(FEED_PREFIX) for p in paths))
        img = hashlib.sha256(b"\x89PNG\r\n\x1a\n\x00\x01").hexdigest()[:2]
        self.assertIn(f"images/{img}.sqlite3", paths)
        # 版 2 の住所（日単位の試合・日単位の応答）は作られない
        self.assertNotIn("matches/xmatch/AREA/2026-10/2026-10-05.sqlite3", paths)
        self.assertNotIn("responses/ConfigQuery/2026-10/2026-10-05.sqlite3", paths)
        self.assertNotIn("matches/unclassified/no-rule/unknown-date.sqlite3", paths)
        for match_part in (M1_PART, M2_PART, M3_PART, M4_PART, M5_PART):
            self.assertEqual(rows(self.out / match_part, "SELECT count(*) FROM matches")[0][0], 1, match_part)
        # 試合に紐づく jobs は試合の本籍、紐づかないものは system/jobs
        self.assertEqual(rows(self.out / M1_PART, "SELECT count(*) FROM jobs")[0][0], 1)
        self.assertEqual(rows(self.out / "system/jobs.sqlite3", "SELECT count(*) FROM jobs")[0][0], 1)
        # 詳細の目撃記録は試合の部品（詳細の応答の本籍 = 試合の本籍）
        self.assertEqual(rows(self.out / M1_PART, "SELECT path FROM sightings"), [("$.x",)])
        self.assertEqual(rows(self.out / M1_PART, "SELECT count(*) FROM responses")[0][0], 1)
        # response_fetches は応答の本籍とは無関係に、自身の fetched_at の日付の部品にだけある
        self.assertEqual(rows(self.out / FETCH_OCT, "SELECT count(*) FROM response_fetches")[0][0], 6)  # m1〜m5 の詳細と r1
        self.assertEqual(rows(self.out / FETCH_SEP, "SELECT count(*) FROM response_fetches")[0][0], 1)  # r2
        for other in (M1_PART, M2_PART, M3_PART, M4_PART, M5_PART, CONFIG_OCT, CONFIG_SEP):
            self.assertEqual(rows(self.out / other, "SELECT count(*) FROM response_fetches")[0][0], 0, other)
        # fetch:<event_id> の rate_points は、その取得の応答（r1）の本籍に置かれる（取得の日付の部品ではない）
        self.assertEqual(rows(self.out / CONFIG_OCT, "SELECT series_id FROM rate_points"), [("sw",)])
        self.assertEqual(rows(self.out / M1_PART, "SELECT series_id FROM rate_points"), [("s",)])
        # 本文の写し: 共有本文は両方の応答部品にあり、写しとして記録される
        for p in (CONFIG_OCT, CONFIG_SEP):
            self.assertEqual(rows(self.out / p, "SELECT count(*) FROM bodies")[0][0], 1)
        copies = [rows(self.out / p, "SELECT count(*) FROM _copies WHERE table_name='bodies'")[0][0]
                  for p in (CONFIG_OCT, CONFIG_SEP)]
        self.assertEqual(sorted(copies), [0, 1])

        # 表定義・索引・ビューは正本と同じ。解析ビューが実行できる
        want = rows(self.src.path,
                    "SELECT type,name,tbl_name,sql FROM sqlite_master WHERE type IN('table','index','view') "
                    "AND sql IS NOT NULL AND name NOT LIKE 'sqlite\\_%' ESCAPE '\\' ORDER BY type,name")
        for p in self.out.rglob("*.sqlite3"):
            if p.name == "catalog.sqlite3":
                continue
            got = rows(p, "SELECT type,name,tbl_name,sql FROM sqlite_master WHERE type IN('table','index','view') "
                          "AND sql IS NOT NULL AND name NOT IN('_part','_copies') ORDER BY type,name")
            self.assertEqual(got, want, p)
            part = rows(p, "SELECT value FROM _part WHERE key='rule_version'")
            self.assertEqual(part, [(str(HOME_RULE_VERSION),)])
        self.assertEqual(rows(self.out / M1_PART, "SELECT count(*) FROM analysis_xmatch")[0][0], 1)
        self.assertEqual(rows(self.out / M1_PART, "SELECT count(*) FROM battle_players")[0][0], 0)

        # 目録
        cat = self.out / "catalog.sqlite3"
        self.assertEqual(rows(cat, "SELECT count(*) FROM match_index")[0][0], 5)
        m1 = rows(cat, "SELECT stage,judgement,my_weapon,tags,detail_available,part_path,played_time "
                       "FROM match_index WHERE match_key='m1'")[0]
        self.assertEqual(m1[:6], ("ステージ", "WIN", "ブキ", "タグ乙、タグ甲", 1, M1_PART))
        self.assertEqual(rows(cat, "SELECT part_path FROM match_index WHERE match_key='m2'"), [(M2_PART,)])
        self.assertEqual(rows(cat, "SELECT analysis_set,rule_raw,part_path FROM match_index WHERE match_key='m5'"),
                         [("unclassified", "no-rule", M5_PART)])
        self.assertEqual(rows(cat, "SELECT count(*) FROM response_index")[0][0], 7)
        self.assertEqual(rows(cat, "SELECT part_path FROM response_index WHERE response_id=?", (self.src.detail_id["m1"],)),
                         [(M1_PART,)])
        self.assertEqual(rows(cat, "SELECT part_path FROM response_index WHERE response_id=?", (self.src.config_ids[0],)),
                         [(CONFIG_OCT,)])
        self.assertEqual(rows(cat, "SELECT count(*) FROM asset_index")[0][0], 2)
        self.assertEqual(rows(cat, "SELECT through_event_id,rule_version FROM status"),
                         [(result["through_event_id"], HOME_RULE_VERSION)])
        self.assertGreater(rows(cat, "SELECT count(*) FROM table_homes")[0][0], 10)
        self.assertEqual(rows(cat, "SELECT count(*) FROM recipes")[0][0], 0)
        self.assertGreater(rows(cat, "SELECT count(*) FROM source_schema WHERE type='trigger'")[0][0], 0)
        self.assertEqual(rows(cat, "SELECT ja FROM labels WHERE kind='analysis_set' AND code='xmatch'"), [("Xマッチ",)])
        self.assertEqual(rows(cat, "SELECT count(*) FROM files")[0][0], len(part_sha(self.out)))
        for path, nbytes, digest in rows(cat, "SELECT path,bytes,sha256 FROM files"):
            data = (self.out / path).read_bytes()
            self.assertEqual((len(data), hashlib.sha256(data).hexdigest()), (nbytes, digest))

    def test_catalog_files_columns_for_each_kind_of_address(self):
        self.build()
        self.audit_clean()
        cat = self.out / "catalog.sqlite3"
        columns = [r[1] for r in rows(cat, "PRAGMA table_info(files)")]
        self.assertIn("match_key", columns)
        self.assertIn("hour", columns)
        sql = ("SELECT domain,analysis_set,rule_raw,month,day,operation,period,response_id,match_key,hour "
               "FROM files WHERE path=?")
        self.assertEqual(rows(cat, sql, (M1_PART,)),
                         [("matches", "xmatch", "AREA", "2026-10", "2026-10-05", None, None, None, "m1", None)])
        self.assertEqual(rows(cat, sql, (M3_PART,)),
                         [("matches", "xmatch", "GOAL", "2026-10", "2026-10-01", None, None, None, "m3", None)])
        self.assertEqual(rows(cat, sql, (M5_PART,)),
                         [("matches", "unclassified", "no-rule", "unknown-date", "unknown-date", None, None, None,
                           "m5", None)])
        self.assertEqual(rows(cat, sql, (CONFIG_OCT,)),
                         [("responses", None, None, "2026-10", "2026-10-05", "ConfigQuery", "2026-10-05", None, None,
                           "11")])
        self.assertEqual(rows(cat, sql, (FETCH_OCT,)),
                         [("fetches", None, None, "2026-10", "2026-10-05", None, None, None, None, None)])
        self.assertEqual(rows(cat, sql, ("system/runs.sqlite3",)),
                         [("system", None, None, None, None, None, None, None, None, None)])
        # 試合のファイルを files 表で列挙できる（設計書: 複数試合の分析は files 表で列挙して ATTACH）
        self.assertEqual(
            rows(cat, "SELECT match_key FROM files WHERE domain='matches' AND analysis_set='xmatch' AND rule_raw='AREA' "
                      "ORDER BY match_key"),
            [("m1",), ("m2",)])

    def test_match_key_with_colon_is_encoded_in_path_and_decoded_in_catalog(self):
        key = "u-abc123:20261005T010000_6f1e2d3c-0000-4000-8000-000000000000"
        conn = self.src.connect()
        with conn:
            self.src.next_id = 900
            self.src.match(conn, key)
        conn.close()
        self.build()
        want = match_path(encode_segment(key))
        self.assertEqual(want, "matches/xmatch/AREA/2026-10/2026-10-05/"
                               "u-abc123~3A20261005T010000_6f1e2d3c-0000-4000-8000-000000000000.sqlite3")
        self.assertIn(want, part_sha(self.out))
        cat = self.out / "catalog.sqlite3"
        self.assertEqual(rows(cat, "SELECT match_key FROM files WHERE path=?", (want,)), [(key,)])
        self.assertEqual(rows(cat, "SELECT part_path FROM match_index WHERE match_key=?", (key,)), [(want,)])
        self.audit_clean()

    def test_audit_detects_tampering(self):
        self.build()
        part = self.out / M1_PART
        conn = sqlite3.connect(part)
        conn.execute("UPDATE matches SET last_seen='改ざん' WHERE match_key='m1'")
        conn.commit()
        conn.close()
        report = self.audit()
        self.assertFalse(report["ok"])
        self.assertEqual(report["tables"]["matches"]["mismatched"], 1)
        # 本文の写しの改ざん
        self.build()
        copy_part = None
        for p in (CONFIG_OCT, CONFIG_SEP):
            if rows(self.out / p, "SELECT count(*) FROM _copies")[0][0]:
                copy_part = self.out / p
        conn = sqlite3.connect(copy_part)
        conn.execute("UPDATE bodies SET byte_length=byte_length+1")
        conn.commit()
        conn.close()
        report = self.audit()
        self.assertFalse(report["ok"])
        self.assertEqual(report["copies"]["mismatched"], 1)

    def test_readme_and_recipes_describe_rule_version_3(self):
        from ikarchive.parts_guide import RECIPES, render_readme

        self.build()
        self.audit_clean()
        cat = sqlite3.connect(f"{(self.out / 'catalog.sqlite3').resolve().as_uri()}?mode=ro", uri=True)
        try:
            text = render_readme(cat)
            homes = {r[0]: r for r in cat.execute("SELECT table_name,rule_ja,path_pattern FROM table_homes")}
            # recipes の SQL のうち目録で動くものは、仮の引数で実行できる
            listing = next(sql for question, _steps, sql in RECIPES if "試合のファイルの一覧" in question)
            got = cat.execute(listing, {"analysis_set": "xmatch", "rule_raw": "AREA"}).fetchall()
            self.assertEqual([r[0] for r in got], [M1_PART, M2_PART])
            index_sql = next(sql for question, _steps, sql in RECIPES if "試合の一覧" in question)
            self.assertEqual(len(cat.execute(index_sql).fetchall()), 5)
        finally:
            cat.close()
        # 版 3 の住所と、1 試合 1 ファイル・part_path・files で列挙して ATTACH・統合版の案内
        for needle in (
            "matches/<analysis_set>/<rule_raw>/<YYYY-MM>/<YYYY-MM-DD>/<match_key>.sqlite3",
            "responses/<operation>/<YYYY-MM>/<YYYY-MM-DD>/<HH>.sqlite3",
            "fetches/<YYYY-MM>/<YYYY-MM-DD>.sqlite3",
            "1 試合が 1 ファイル",
            "part_path",
            "ATTACH",
            "統合版",
            "unknown-date",
        ):
            self.assertIn(needle, text, needle)
        for stale in ("<YYYY-MM-DD>.sqlite3`: 試合ごと", "matches/<analysis_set>/<rule_raw>/unknown-date.sqlite3"):
            self.assertNotIn(stale, text, stale)
        # table_homes の説明が版 3
        self.assertIn("<match_key>", homes["matches"][2])
        self.assertIn("<HH>", homes["responses"][2])
        self.assertIn("fetches/", homes["response_fetches"][2])
        self.assertIn("fetched_at", homes["response_fetches"][1])
        self.assertIn("response_id", homes["sightings"][1])
        self.assertNotIn("(account, kind, match_key)", homes["sightings"][1])
        # 取り直しの回数の recipe は fetches の部品でそのまま動く
        refetch_sql = next(sql for question, _steps, sql in RECIPES if "取り直した" in question)
        self.assertEqual(len(rows(self.out / FETCH_OCT, refetch_sql)), 6)


class GuideTests(PartsTestBase):
    def test_every_recipe_sql_runs_where_the_recipe_says(self):
        """recipes の sql は「記載の部品を開いた状態でそのまま動く文」。どの手引きも、その場所で実行できる。"""
        from ikarchive.parts_guide import RECIPES

        self.build()
        self.audit_clean()
        img = hashlib.sha256(b"\x89PNG\r\n\x1a\n\x00\x01").hexdigest()[:2]
        where = {  # 質問の一部 -> (開く部品, 引数)
            "ルール別の勝敗数": ("catalog.sqlite3", {}),
            "試合の一覧（日時": ("catalog.sqlite3", {}),
            "ある試合の全データ": (M1_PART, {"match_key": "m1"}),
            "全プレイヤー": (M1_PART, {}),
            "試合のファイルの一覧": ("catalog.sqlite3", {"analysis_set": "xmatch", "rule_raw": "AREA"}),
            "WAVE": (M1_PART, {}),
            "パワー・ポイント": (M1_PART, {}),
            "タグ（": ("catalog.sqlite3", {"tag": "タグ"}),
            "画像（": (f"images/{img}.sqlite3", {"url": "u1"}),
            "試合以外の取得記録": ("catalog.sqlite3", {}),
            "取り直した": (FETCH_OCT, {}),
            "取得がうまくいったか": ("system/jobs.sqlite3", {}),
            "一つの SQLite": (M1_PART, {}),
        }
        self.assertEqual(len(RECIPES), len(where))  # 手引きを足したら、ここにも足す
        for question, steps, sql in RECIPES:
            key = next(k for k in where if k in question)
            path, params = where[key]
            conn = sqlite3.connect(f"{(self.out / path).resolve().as_uri()}?mode=ro", uri=True)
            try:
                if key == "一つの SQLite":
                    conn.execute("ATTACH ? AS other", (f"{(self.out / M2_PART).resolve().as_uri()}?mode=ro",))
                result = conn.execute(sql, params).fetchall()
            finally:
                conn.close()
            self.assertIsInstance(result, list, question)
            self.assertTrue(steps and sql, question)
        # 目録の手引きのうち、中身のあるものは期待どおりの行を返す
        conn = sqlite3.connect(f"{(self.out / 'catalog.sqlite3').resolve().as_uri()}?mode=ro", uri=True)
        try:
            wins = dict(((r[0], r[1]), r[3]) for r in conn.execute(RECIPES[0][2]))
            self.assertEqual(wins[("xmatch", "AREA")], 2)
            tagged = conn.execute(next(sql for q, _s, sql in RECIPES if q.startswith("タグ（")), {"tag": "タグ甲"}).fetchall()
            self.assertEqual([r[2] for r in tagged], ["m1"])
        finally:
            conn.close()
        self.assertEqual(rows(self.out / M1_PART, next(sql for q, _s, sql in RECIPES if "ある試合の全データ" in q),
                              {"match_key": "m1"})[0][-1].count("playedTime"), 1)


class HomeRuleTests(PartsTestBase):
    """版 3 の本籍規則: sightings は応答の本籍、response_fetches は自身の日付、応答は時単位。"""

    def test_list_response_sightings_stay_with_the_list_response(self):
        conn = self.src.connect()
        with conn:
            self.src.next_id = 100
            lst = self.src.list_response(conn, "VsHistoryQuery", "2026-10-05T03:20:00+00:00",
                                         [("vs", "m1"), ("vs", "m2"), ("vs", "ghost")])
        conn.close()
        self.build()
        self.audit_clean()
        list_part = "responses/VsHistoryQuery/2026-10/2026-10-05/12.sqlite3"  # UTC 03:20 = JST 12:20
        self.assertIn(list_part, part_sha(self.out))
        self.assertEqual(rows(self.out / list_part, "SELECT match_key FROM sightings ORDER BY path"),
                         [("m1",), ("m2",), ("ghost",)])  # 試合が無い目撃記録も、一覧の応答の部品へ
        # 載っている試合の部品には、詳細の目撃記録だけが残る
        self.assertEqual(rows(self.out / M1_PART, "SELECT response_id FROM sightings"), [(self.src.detail_id["m1"],)])
        self.assertEqual(rows(self.out / M2_PART, "SELECT response_id FROM sightings"), [(self.src.detail_id["m2"],)])
        self.assertFalse(any(p.startswith("unplaced/sightings") for p in part_sha(self.out)))
        # 応答の住所録も一覧の応答の部品を指す
        self.assertEqual(rows(self.out / "catalog.sqlite3", "SELECT part_path FROM response_index WHERE response_id=?",
                              (lst,)), [(list_part,)])

    def test_sighting_of_unknown_response_is_unplaced(self):
        conn = self.src.connect()
        with conn:
            conn.execute("PRAGMA foreign_keys=OFF")
            conn.execute("INSERT INTO sightings(response_id,account,kind,match_key,path,summary_json) "
                         "VALUES(99999,'acc','vs','m1','$.orphan','{}')")
        conn.close()
        self.build()
        self.audit_clean()
        self.assertEqual(rows(self.out / "unplaced/sightings.sqlite3", "SELECT path FROM sightings"), [("$.orphan",)])
        self.assertEqual(rows(self.out / M1_PART, "SELECT path FROM sightings"), [("$.x",)])

    def test_response_fetches_follow_their_own_date_not_the_response(self):
        conn = self.src.connect()
        with conn:
            r1 = self.src.config_ids[0]
            self.src.refetch(conn, r1, "2026-10-07T20:00:00+00:00")  # JST 10-08 05:00
            self.src.refetch(conn, r1, "2026-10-08T00:30:00+00:00")  # JST 10-08 09:30（同じ日）
            self.src.refetch(conn, r1, "zzz")  # 解析できない
            self.src.refetch(conn, self.src.detail_id["m1"], "2026-10-09T01:00:00+00:00")  # 試合の詳細の取り直し
        conn.close()
        self.build()
        self.audit_clean()
        paths = set(part_sha(self.out))
        for expected in ("fetches/2026-10/2026-10-08.sqlite3", "fetches/2026-10/2026-10-09.sqlite3",
                         "fetches/unknown-date.sqlite3", FETCH_OCT, FETCH_SEP):
            self.assertIn(expected, paths)
        self.assertEqual(rows(self.out / "fetches/2026-10/2026-10-08.sqlite3", "SELECT response_id FROM response_fetches"),
                         [(r1,), (r1,)])
        self.assertEqual(rows(self.out / "fetches/unknown-date.sqlite3", "SELECT fetched_at FROM response_fetches"),
                         [("zzz",)])
        self.assertEqual(rows(self.out / "fetches/2026-10/2026-10-09.sqlite3", "SELECT response_id FROM response_fetches"),
                         [(self.src.detail_id["m1"],)])
        # 取り直しがあっても、応答の部品・試合の部品には response_fetches が入らない
        self.assertEqual(rows(self.out / CONFIG_OCT, "SELECT count(*) FROM response_fetches")[0][0], 0)
        self.assertEqual(rows(self.out / M1_PART, "SELECT count(*) FROM response_fetches")[0][0], 0)
        cat = self.out / "catalog.sqlite3"
        self.assertEqual(rows(cat, "SELECT month,day,hour FROM files WHERE path='fetches/unknown-date.sqlite3'"),
                         [("unknown-date", "unknown-date", None)])
        self.assertEqual(rows(cat, "SELECT month,day FROM files WHERE path='fetches/2026-10/2026-10-08.sqlite3'"),
                         [("2026-10", "2026-10-08")])


class AuditThroughTests(PartsTestBase):
    def test_changes_after_build_are_explained(self):
        self.build()
        base = self.audit_clean()
        through = base["through_event_id"]
        self.assertIsNotNone(through)
        for info in base["tables"].values():
            self.assertEqual((info["changed_after_build"], info["unexplained"]), (0, 0))
        # 部品作成後に、変更追跡つきで追加・更新・削除する
        conn = self.src.connect()
        with conn:
            self.src.match(conn, "late")
            self.src.refetch(conn, self.src.config_ids[0], "2026-10-09T01:00:00+00:00")
            self.src.list_response(conn, "VsHistoryQuery", "2026-10-09T02:00:00+00:00", [("vs", "m1")])
            conn.execute("UPDATE match_tags SET note='後' WHERE tag='タグ甲'")
            conn.execute("DELETE FROM jobs WHERE operation='op2'")
        conn.close()
        report = self.audit_clean()
        self.assertEqual(report["through_event_id"], through)
        for table in ("matches", "match_tags", "jobs", "archive_change_feed", "response_fetches", "sightings"):
            info = report["tables"][table]
            self.assertGreater(info["changed_after_build"], 0, table)
            self.assertEqual(info["unexplained"], 0, table)
        self.assertGreater(report["tables"]["matches"]["mismatched"], 0)

    def test_untracked_tampering_is_unexplained(self):
        self.build()
        conn = self.src.connect()
        with conn:
            self.src.match(conn, "late")  # 追跡つきの正当な変化が併存しても区別できる
        conn.close()
        conn = sqlite3.connect(self.out / M1_PART)
        conn.execute("UPDATE matches SET last_seen='改ざん' WHERE match_key='m1'")
        conn.commit()
        conn.close()
        report = self.audit()
        self.assertFalse(report["ok"])
        info = report["tables"]["matches"]
        self.assertEqual(info["unexplained"], 1)
        self.assertEqual(len(info["first_unexplained_rowids"]), 1)
        self.assertGreater(info["changed_after_build"], 0)

    def test_no_catalog_behaves_as_before(self):
        self.build()
        (self.out / "catalog.sqlite3").unlink()
        report = self.audit()
        self.assertIsNone(report["through_event_id"])
        self.assertTrue(report["ok"], report)
        self.src.execute("INSERT INTO issues(code,context,created_at) VALUES('n','{}','t')")
        report = self.audit()
        self.assertFalse(report["ok"])
        self.assertEqual(report["tables"]["issues"]["changed_after_build"], 0)


class WeirdValueTests(PartsTestBase):
    def test_type_and_value_fidelity(self):
        conn = self.src.connect()
        with conn:
            conn.execute("INSERT INTO issues(code,context,created_at) VALUES('bad',CAST(X'fffe4180' AS TEXT),'t')")
            conn.execute("INSERT INTO bodies VALUES('blobsha',X'00ff0100',4)")
            conn.execute("INSERT INTO jobs(account,operation,variables_json,attempts) VALUES('a','big','{}',9223372036854775807)")
            conn.execute("INSERT INTO jobs(account,operation,variables_json,attempts) VALUES('a','txt','{}','abc')")
            conn.execute("INSERT INTO jobs(account,operation,variables_json,attempts) VALUES('a','neg','{}',-9223372036854775808)")
            conn.execute("INSERT INTO rate_points VALUES('acc','s2','l','g',NULL,'m1',NULL,0.1+0.2,'s','p')")
            conn.execute("INSERT INTO rate_points VALUES('acc','s3','l','g',NULL,'m1',NULL,1e300,'s','p')")
            conn.execute("INSERT INTO rate_points VALUES('acc','s4','l','g',NULL,'m1',NULL,NULL,'s','p')")
        conn.close()
        self.build()
        self.audit_clean()
        sel = {
            "issues": "SELECT typeof(context),hex(context),typeof(response_id) FROM issues WHERE code='bad'",
            "bodies": "SELECT typeof(body),hex(body),typeof(byte_length) FROM bodies WHERE sha256='blobsha'",
            "jobs": "SELECT operation,typeof(attempts),attempts FROM jobs WHERE account='a' ORDER BY operation",
            "rate_points": "SELECT series_id,typeof(value),printf('%!.17g',value) FROM rate_points ORDER BY series_id",
        }
        for table, sql in sel.items():
            want = rows(self.src.path, sql)
            got = []
            for p in self.out.rglob("*.sqlite3"):
                if p.name != "catalog.sqlite3":
                    got.extend(rows(p, sql))
            self.assertEqual(sorted(set(got), key=repr), sorted(set(want), key=repr), table)
            self.assertTrue(want)
        # 型が INTEGER 親和性の列に TEXT で入った行
        got = []
        for p in self.out.rglob("*.sqlite3"):
            if p.name != "catalog.sqlite3":
                got.extend(rows(p, "SELECT typeof(attempts) FROM jobs WHERE operation='txt'"))
        self.assertEqual(got, [("text",)])


def dump_parts(out: Path) -> dict:
    """部品ごとの全表の全行（_part は作成時刻が入るので除く）。空の表と空の部品は含めない。"""
    result = {}
    for p in sorted(out.rglob("*.sqlite3")):
        rel = p.relative_to(out).as_posix()
        if rel == "catalog.sqlite3":
            continue
        conn = sqlite3.connect(f"{p.resolve().as_uri()}?mode=ro", uri=True)
        try:
            tables = {}
            for (name,) in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name<>'_part' AND name NOT LIKE 'sqlite\\_%' ESCAPE '\\'"
            ).fetchall():
                found = conn.execute(f'SELECT rowid,* FROM "{name}" ORDER BY rowid').fetchall()
                if found:
                    tables[name] = found
        finally:
            conn.close()
        if tables:
            result[rel] = tables
    return result


def dump_catalog(out: Path) -> dict:
    """目録の中身（作成時刻・SHA-256 など作るたびに変わる列と、空の部品の行は除く）。"""
    cat = out / "catalog.sqlite3"
    result = {}
    for table in ("match_index", "response_index", "asset_index", "labels", "table_homes", "recipes",
                  "source_schema", "source_sqlite_internal"):
        result[table] = sorted(rows(cat, f"SELECT * FROM {table}"), key=repr)
    result["files"] = sorted(
        rows(cat, "SELECT path,domain,analysis_set,rule_raw,month,day,operation,period,response_id,match_key,hour,"
                  "rows_json FROM files WHERE rows_json<>'{}'"),
        key=repr,
    )
    result["status"] = rows(cat, "SELECT through_event_id,rule_version FROM status")
    return result


class IncrementalTests(PartsTestBase):
    def test_add_match_rebuilds_only_related_parts(self):
        self.build()
        before = part_sha(self.out)
        conn = self.src.connect()
        with conn:
            self.src.next_id = 100
            self.src.match(conn, "m6")
            self.src.response(conn, "ConfigQuery", "2026-10-06T02:00:00+00:00", b'{"other":1}')
        conn.close()
        result = self.build()
        self.assertFalse(result["full_rebuild"])
        rebuilt = self.paths(result)
        self.assertEqual(self.non_feed(rebuilt), {
            match_path("m6"),  # 新しい試合の 1 ファイルだけ（m1・m2 は触れない）
            "responses/ConfigQuery/2026-10/2026-10-06/11.sqlite3",
            FETCH_OCT,  # 新しい試合の詳細の取得（fetched_at は 10-05）
            "fetches/2026-10/2026-10-06.sqlite3",
        })
        self.assertEqual(self.changed_parts(before), rebuilt)  # 作り直していない部品は SHA-256 が不変
        self.audit_clean()
        self.assertEqual(rows(self.out / "catalog.sqlite3", "SELECT count(*) FROM match_index")[0][0], 6)
        self.assertEqual(result["match_index"], {"mode": "delta", "recomputed": 1, "reused": 5})
        # 何も変えなければ、部品は一つも作り直されない
        again = self.build()
        self.assertEqual(again["rebuilt_parts"], [])
        self.assertEqual(again["match_index"], {"mode": "delta", "recomputed": 0, "reused": 6})
        self.audit_clean()

    def test_refetch_rebuilds_only_the_fetch_day_and_system_parts(self):
        # 同じ応答の取り直し（response_fetches の行が一つ増えるだけ）: 試合の部品も古い応答の部品も作り直さない
        self.build()
        before = part_sha(self.out)
        conn = self.src.connect()
        with conn:
            self.src.refetch(conn, self.src.detail_id["m1"], "2026-10-07T01:00:00+00:00")  # 試合の詳細の取り直し（JST 10-07）
            self.src.refetch(conn, self.src.config_ids[0], "2026-10-07T02:00:00+00:00")  # 試合に属さない応答の取り直し
            # 収集が取り直しのたびに書く運用記録（system 系）
            conn.execute("UPDATE runs SET status='ok2' WHERE id=1")
            conn.execute("INSERT INTO control VALUES('last_cycle','t')")
            conn.execute("UPDATE jobs SET attempts=attempts+1 WHERE operation='op2'")
        conn.close()
        result = self.build()
        rebuilt = self.paths(result)
        self.assertEqual({p for p in rebuilt if not p.startswith("system/")}, {"fetches/2026-10/2026-10-07.sqlite3"})
        self.assertTrue({"system/runs.sqlite3", "system/control.sqlite3", "system/jobs.sqlite3"} <= rebuilt)
        self.assertEqual(self.changed_parts(before), rebuilt)  # 作り直していない部品は SHA-256 が不変
        after = part_sha(self.out)
        for untouched in (M1_PART, M2_PART, CONFIG_OCT, CONFIG_SEP, FETCH_OCT, FETCH_SEP):
            self.assertEqual(after[untouched], before[untouched], untouched)
        self.assertEqual(rows(self.out / "fetches/2026-10/2026-10-07.sqlite3",
                              "SELECT response_id FROM response_fetches ORDER BY response_id"),
                         [(self.src.detail_id["m1"],), (self.src.config_ids[0],)])
        self.assertEqual(result["match_index"], {"mode": "delta", "recomputed": 0, "reused": 5})
        self.audit_clean()

    def test_new_list_response_adds_only_its_response_part(self):
        # 新しい一覧の応答が届くと、その時の応答部品だけが増え、載っている過去の試合の部品は作り直さない
        self.build()
        before = part_sha(self.out)
        conn = self.src.connect()
        with conn:
            self.src.next_id = 100
            lst = self.src.list_response(conn, "VsHistoryQuery", "2026-10-05T04:20:00+00:00",
                                         [("vs", "m1"), ("vs", "m2"), ("vs", "m3"), ("vs", "m4")])
        conn.close()
        result = self.build()
        rebuilt = self.paths(result)
        list_part = "responses/VsHistoryQuery/2026-10/2026-10-05/13.sqlite3"  # UTC 04:20 = JST 13:20
        self.assertEqual(self.non_feed(rebuilt), {list_part, FETCH_OCT})  # 応答の部品と、その取得の日の記録だけ
        after = part_sha(self.out)
        self.assertEqual({p for p in after if p not in before and not p.startswith(FEED_PREFIX)}, {list_part})
        for match_part in (M1_PART, M2_PART, M3_PART, M4_PART, M5_PART):
            self.assertEqual(after[match_part], before[match_part], match_part)
        self.assertEqual(rows(self.out / list_part, "SELECT response_id,match_key FROM sightings ORDER BY path"),
                         [(lst, "m1"), (lst, "m2"), (lst, "m3"), (lst, "m4")])
        self.assertEqual(rows(self.out / list_part, "SELECT count(*) FROM bodies")[0][0], 1)
        self.assertEqual(result["match_index"], {"mode": "delta", "recomputed": 0, "reused": 5})
        self.audit_clean()
        # matches の行が変わった試合だけは、その試合の 1 部品が作り直される（ほかの試合・応答は作り直さない）
        before = part_sha(self.out)
        self.src.execute("UPDATE matches SET last_seen='later' WHERE match_key='m1'")
        result = self.build()
        self.assertEqual(self.non_feed(self.paths(result)), {M1_PART})
        self.assertEqual(self.non_feed(self.changed_parts(before)), {M1_PART})
        self.assertEqual(result["match_index"], {"mode": "delta", "recomputed": 1, "reused": 4})
        self.audit_clean()

    def test_classification_change_moves_match(self):
        self.build()
        before = part_sha(self.out)
        self.src.execute("UPDATE match_classification SET rule_raw='GOAL',rule_name='GOAL' WHERE match_key='m2'")
        result = self.build()
        rebuilt = self.paths(result)
        goal = match_path("m2", rule="GOAL")
        self.assertEqual(self.non_feed(rebuilt), {M2_PART, goal})  # m1 の部品も、m3 の部品も触れない
        self.assertEqual(self.changed_parts(before), rebuilt)
        self.assertEqual(rows(self.out / goal, "SELECT match_key FROM matches ORDER BY 1"), [("m2",)])
        # 動いた試合の詳細の応答・目撃記録・本文もいっしょに動く
        self.assertEqual(rows(self.out / goal, "SELECT id FROM responses"), [(self.src.detail_id["m2"],)])
        self.assertEqual(rows(self.out / goal, "SELECT path FROM sightings"), [("$.x",)])
        self.assertEqual(rows(self.out / goal, "SELECT count(*) FROM bodies")[0][0], 1)
        # 元の部品は削除されず、空の部品になる
        old = self.out / M2_PART
        self.assertTrue(old.is_file())
        for table in ("matches", "documents", "match_classification", "responses", "sightings", "bodies"):
            self.assertEqual(rows(old, f"SELECT count(*) FROM {table}")[0][0], 0, table)
        self.assertEqual(rows(self.out / "catalog.sqlite3", "SELECT part_path FROM match_index WHERE match_key='m2'"),
                         [(goal,)])
        self.assertEqual(result["match_index"], {"mode": "delta", "recomputed": 1, "reused": 4})
        self.audit_clean()
        # 部品に残る最後の試合が動いたら、旧部品は削除されず空の部品になる
        self.src.execute("UPDATE match_classification SET rule_raw='X',rule_name='X' WHERE match_key='m4'")
        self.build()
        old = self.out / M4_PART
        self.assertTrue(old.is_file())
        for table in ("matches", "documents", "match_classification", "responses", "bodies"):
            self.assertEqual(rows(old, f"SELECT count(*) FROM {table}")[0][0], 0, table)
        self.assertEqual(rows(old, "SELECT count(*) FROM _part")[0][0], 5)
        self.assertEqual(rows(old, "SELECT count(*) FROM sqlite_master WHERE type='view'")[0][0] > 0, True)
        self.audit_clean()

    def test_delete_is_reflected(self):
        self.build()
        self.src.execute("DELETE FROM match_tags WHERE tag='タグ甲'")
        result = self.build()
        self.assertEqual(self.non_feed(self.paths(result)), {M1_PART})
        self.assertEqual(rows(self.out / M1_PART, "SELECT tag FROM match_tags"), [("タグ乙",)])
        self.audit_clean()
        # 応答ごとの削除（子から親の順）
        conn = self.src.connect()
        with conn:
            conn.execute("DELETE FROM jobs WHERE operation='op2'")
        conn.close()
        result = self.build()
        self.assertIn("system/jobs.sqlite3", self.paths(result))
        self.assertEqual(rows(self.out / "system/jobs.sqlite3", "SELECT count(*) FROM jobs")[0][0], 0)
        self.audit_clean()

    def test_deleted_match_leaves_an_empty_part_and_no_index_row(self):
        self.build()
        conn = self.src.connect()
        with conn:
            rid = self.src.detail_id["m4"]
            for sql in ("DELETE FROM sightings WHERE response_id=?", "DELETE FROM match_classification WHERE match_key='m4'",
                        "DELETE FROM match_refs WHERE match_key='m4'", "DELETE FROM documents WHERE response_id=?",
                        "DELETE FROM matches WHERE match_key='m4'", "DELETE FROM response_fetches WHERE response_id=?",
                        "DELETE FROM responses WHERE id=?"):
                conn.execute(sql, (rid,) if "?" in sql else ())
        conn.close()
        result = self.build()
        self.assertEqual(result["match_index"]["recomputed"], 0)
        self.assertEqual(rows(self.out / "catalog.sqlite3", "SELECT count(*) FROM match_index WHERE match_key='m4'")[0][0], 0)
        self.assertEqual(rows(self.out / "catalog.sqlite3", "SELECT count(*) FROM match_index")[0][0], 4)
        self.assertEqual(rows(self.out / M4_PART, "SELECT count(*) FROM matches")[0][0], 0)
        self.audit_clean()
        # 消えた試合の前回の行は状態 DB からも捨てられる（次の周期も差分のまま、使い回しは 4 件）
        again = self.build()
        self.assertEqual(again["match_index"], {"mode": "delta", "recomputed": 0, "reused": 4})
        self.assertEqual(rows(self.state, "SELECT count(*) FROM match_index_prev")[0][0], 4)

    def test_unplaced_and_later_adoption(self):
        self.build()
        self.assertEqual(rows(self.out / "unplaced/match_tags.sqlite3", "SELECT match_key FROM match_tags"),
                         [("nomatch",)])
        self.audit_clean()
        # 親の試合が後から現れたら、タグは unplaced から試合の本籍へ移る
        conn = self.src.connect()
        with conn:
            self.src.next_id = 200
            self.src.match(conn, "nomatch", played="2026-08-01T00:00:00Z", analysis_set="nawabari", rule="TURF")
        conn.close()
        result = self.build()
        rebuilt = self.paths(result)
        self.assertIn("unplaced/match_tags.sqlite3", rebuilt)
        self.assertEqual(rows(self.out / "unplaced/match_tags.sqlite3", "SELECT count(*) FROM match_tags")[0][0], 0)
        adopted = match_path("nomatch", analysis_set="nawabari", rule="TURF", day="2026-08/2026-08-01")
        self.assertEqual(rows(self.out / adopted, "SELECT tag FROM match_tags"), [("孤児",)])
        self.assertEqual(rows(self.out / "catalog.sqlite3", "SELECT tags FROM match_index WHERE match_key='nomatch'"),
                         [("孤児",)])
        self.audit_clean()

    def test_orphan_rows_are_adopted_when_their_match_appears(self):
        # 親の試合が無い間、索引の無い表の行は unplaced / system に置かれ、試合が現れると試合の部品へ移る
        conn = self.src.connect()
        with conn:
            self.src.next_id = 400
            rid = self.src.orphan_rows(conn, "adopt")
        conn.close()
        self.build()
        self.audit_clean()
        hourly = "responses/VsHistoryDetailQuery/2026-10/2026-10-05/11.sqlite3"
        self.assertEqual(rows(self.out / "unplaced/documents.sqlite3", "SELECT match_key FROM documents"), [("adopt",)])
        self.assertEqual(rows(self.out / "unplaced/rate_points.sqlite3", "SELECT match_key FROM rate_points"), [("adopt",)])
        self.assertEqual(rows(self.out / "system/jobs.sqlite3", "SELECT count(*) FROM jobs WHERE match_key='adopt'")[0][0], 1)
        self.assertEqual(rows(self.out / hourly, "SELECT id FROM responses"), [(rid,)])  # 詳細のはずの応答も、試合が無い間は時の部品
        conn = self.src.connect()
        with conn:
            self.src.adopt_orphans(conn, "adopt")
        conn.close()
        result = self.build()
        adopted = match_path("adopt", day="2026-09/2026-09-12")  # orphan_rows の詳細の playedTime
        self.assertIn(adopted, self.paths(result))
        self.assertEqual(rows(self.out / adopted, "SELECT match_key FROM documents"), [("adopt",)])
        self.assertEqual(rows(self.out / adopted, "SELECT match_key FROM rate_points"), [("adopt",)])
        self.assertEqual(rows(self.out / adopted, "SELECT match_key FROM jobs"), [("adopt",)])
        self.assertEqual(rows(self.out / adopted, "SELECT id FROM responses"), [(rid,)])
        for table, part in (("documents", "unplaced/documents"), ("rate_points", "unplaced/rate_points")):
            self.assertEqual(rows(self.out / (part + ".sqlite3"), f"SELECT count(*) FROM {table}")[0][0], 0, table)
        self.assertEqual(rows(self.out / "system/jobs.sqlite3", "SELECT count(*) FROM jobs WHERE match_key='adopt'")[0][0], 0)
        self.assertEqual(rows(self.out / hourly, "SELECT count(*) FROM responses")[0][0], 0)
        self.assertEqual(rows(self.out / "catalog.sqlite3", "SELECT part_path,detail_available FROM match_index "
                                                            "WHERE match_key='adopt'"), [(adopted, 1)])
        self.audit_clean()

    def test_pending_match_moves_when_its_detail_arrives(self):
        conn = self.src.connect()
        with conn:
            self.src.pending_match(conn, "pend")
        conn.close()
        self.build()
        pending = "matches/unclassified/no-rule/unknown-date/pend.sqlite3"
        self.assertIn(pending, part_sha(self.out))
        self.assertEqual(rows(self.out / "catalog.sqlite3", "SELECT part_path,detail_available FROM match_index "
                                                            "WHERE match_key='pend'"), [(pending, 0)])
        self.audit_clean()
        # 詳細が届くと、分類と日時が決まり、試合は新しい部品へ移る（元の部品は空のまま残る）
        conn = self.src.connect()
        with conn:
            self.src.next_id = 300
            self.src.attach_detail(conn, "pend")
        conn.close()
        result = self.build()
        arrived = match_path("pend")
        self.assertIn(arrived, self.paths(result))
        self.assertEqual(rows(self.out / arrived, "SELECT count(*) FROM matches")[0][0], 1)
        self.assertEqual(rows(self.out / pending, "SELECT count(*) FROM matches")[0][0], 0)
        self.assertEqual(rows(self.out / "catalog.sqlite3", "SELECT part_path,detail_available,stage FROM match_index "
                                                            "WHERE match_key='pend'"), [(arrived, 1, "ステージ")])
        self.audit_clean()

    def test_deleted_part_file_is_recreated(self):
        self.build()
        before = part_sha(self.out)
        (self.out / M1_PART).unlink()
        result = self.build()
        self.assertIn(M1_PART, self.paths(result))
        self.assertEqual(rows(self.out / M1_PART, "SELECT match_key FROM matches"), [("m1",)])
        self.assertEqual(rows(self.out / M1_PART, "SELECT count(*) FROM match_tags")[0][0], 2)
        self.assertEqual(self.non_feed(self.changed_parts(before)), {M1_PART})
        self.audit_clean()

    def test_snapshot_ignores_writes_after_copy(self):
        first = self.build()
        through0 = first["through_event_id"]

        def write_after_copy():
            conn = self.src.connect()
            with conn:
                self.src.next_id = 300
                self.src.match(conn, "late")
            conn.close()

        before_count = rows(self.src.path, "SELECT count(*) FROM matches")[0][0]
        result = self.build(after_snapshot=write_after_copy)
        self.assertEqual(result["through_event_id"], through0)  # 書き込み前の静止点
        self.assertEqual(rows(self.src.path, "SELECT count(*) FROM matches")[0][0], before_count + 1)
        for part in self.out.rglob("*.sqlite3"):
            if part.name == "catalog.sqlite3":
                continue
            self.assertEqual(rows(part, "SELECT count(*) FROM matches WHERE match_key='late'")[0][0], 0)
        self.assertEqual(rows(self.out / "catalog.sqlite3", "SELECT count(*) FROM match_index WHERE match_key='late'")[0][0], 0)
        self.assertEqual(rows(self.out / "catalog.sqlite3", "SELECT through_event_id FROM status"), [(through0,)])
        # 次の回で追いつき、監査が一致する
        self.build()
        self.audit_clean()
        self.assertEqual(rows(self.out / match_path("late"), "SELECT count(*) FROM matches WHERE match_key='late'")[0][0], 1)
        self.assertEqual(rows(self.out / "catalog.sqlite3", "SELECT count(*) FROM match_index WHERE match_key='late'")[0][0], 1)


RANK = "EventMatchRankingPeriodQuery"


class GranularityTests(PartsTestBase):
    def test_ranking_one_response_one_part_and_unknown_date(self):
        conn = self.src.connect()
        with conn:
            self.src.next_id = 500
            a = self.src.response(conn, RANK, "2026-10-05T02:00:00+00:00", b'{"a":1}')
            b = self.src.response(conn, RANK, "2026-10-05T03:00:00+00:00", b'{"b":1}')
            c = self.src.response(conn, RANK, "zzz", b'{"c":1}')
            d = self.src.response(conn, "ConfigQuery", "zzz", b'{"d":1}')
            e = self.src.response(conn, "XRankingDetailQuery", "2026-09-30T20:00:00+00:00", b'{"e":1}')
            f = self.src.response(conn, "ConfigQuery", "2026-10-05T02:30:00+00:00", b'{"f":1}')
            conn.execute("INSERT INTO entities VALUES('acc','T','ea',?,'{}')", (a,))
            conn.execute("INSERT INTO entities VALUES('acc','T','eb',?,'{}')", (b,))
            conn.execute("INSERT INTO asset_refs VALUES(?,?,?)", (a, "u1", "$.i"))
        conn.close()
        self.build()
        self.audit_clean()
        paths = set(part_sha(self.out))
        self.assertIn(f"responses/{RANK}/2026-10/2026-10-05/{a}.sqlite3", paths)
        self.assertIn(f"responses/{RANK}/2026-10/2026-10-05/{b}.sqlite3", paths)  # 同じ日でも別部品
        self.assertIn(f"responses/{RANK}/unknown-date/{c}.sqlite3", paths)
        self.assertIn("responses/ConfigQuery/unknown-date.sqlite3", paths)
        self.assertIn(f"responses/XRankingDetailQuery/2026-10/2026-10-01/{e}.sqlite3", paths)  # JST で 10-01
        self.assertNotIn(f"responses/{RANK}/2026-10/2026-10-05.sqlite3", paths)
        self.assertFalse(any(p.startswith(f"responses/{RANK}/") and p.count("/") == 3 and p.endswith("/05.sqlite3")
                             for p in paths))  # ランキング系は時単位にしない
        # 通常の operation は同じ時の応答が一つの部品に入る（f と、seed の ConfigQuery r1 は JST 11 時）
        hour = self.out / CONFIG_OCT
        self.assertEqual(sorted(r[0] for r in rows(hour, "SELECT id FROM responses")), sorted([self.src.config_ids[0], f]))
        # ランキング系の部品にはその応答の行だけが入る
        part_a = self.out / f"responses/{RANK}/2026-10/2026-10-05/{a}.sqlite3"
        self.assertEqual(rows(part_a, "SELECT id FROM responses"), [(a,)])
        self.assertEqual(rows(part_a, "SELECT entity_id FROM entities"), [("ea",)])
        self.assertEqual(rows(part_a, "SELECT count(*) FROM asset_refs")[0][0], 1)
        self.assertEqual(rows(part_a, "SELECT count(*) FROM bodies")[0][0], 1)
        self.assertEqual(rows(part_a, "SELECT count(*) FROM response_fetches")[0][0], 0)  # 取得の記録は fetches/ へ
        # 解析できない fetched_at の取得の記録は fetches/unknown-date に集まる
        self.assertEqual(sorted(r[0] for r in rows(self.out / "fetches/unknown-date.sqlite3",
                                                   "SELECT response_id FROM response_fetches")), sorted([c, d]))
        # 目録の files 表と response_index
        cat = self.out / "catalog.sqlite3"
        self.assertEqual(
            rows(cat, "SELECT domain,operation,month,day,period,response_id,hour FROM files WHERE path=?",
                 (f"responses/{RANK}/2026-10/2026-10-05/{a}.sqlite3",)),
            [("responses", RANK, "2026-10", "2026-10-05", "2026-10-05", a, None)])
        self.assertEqual(
            rows(cat, "SELECT month,day,response_id,hour FROM files WHERE path=?",
                 (f"responses/{RANK}/unknown-date/{c}.sqlite3",)),
            [("unknown-date", "unknown-date", c, None)])
        self.assertEqual(
            rows(cat, "SELECT operation,month,day,period,response_id,hour FROM files WHERE path=?", (CONFIG_OCT,)),
            [("ConfigQuery", "2026-10", "2026-10-05", "2026-10-05", None, "11")])
        self.assertEqual(
            rows(cat, "SELECT operation,month,day,period,response_id,hour FROM files WHERE path=?",
                 ("responses/ConfigQuery/unknown-date.sqlite3",)),
            [("ConfigQuery", "unknown-date", "unknown-date", "unknown-date", None, None)])
        self.assertEqual(
            rows(cat, "SELECT analysis_set,rule_raw,month,day,match_key FROM files WHERE path=?", (M1_PART,)),
            [("xmatch", "AREA", "2026-10", "2026-10-05", "m1")])
        self.assertEqual(
            rows(cat, "SELECT month,day,match_key FROM files WHERE path=?", (M5_PART,)),
            [("unknown-date", "unknown-date", "m5")])
        self.assertEqual(
            rows(cat, "SELECT part_path FROM response_index WHERE response_id=?", (b,)),
            [(f"responses/{RANK}/2026-10/2026-10-05/{b}.sqlite3",)])

    def test_responses_are_split_by_hour_in_japan_time(self):
        conn = self.src.connect()
        with conn:
            self.src.next_id = 400
            same_a = self.src.response(conn, "ConfigQuery", "2026-10-05T02:10:00+00:00", b'{"h":1}')  # JST 11:10
            same_b = self.src.response(conn, "ConfigQuery", "2026-10-05T02:59:59+00:00", b'{"h":2}')  # JST 11:59
            next_hour = self.src.response(conn, "ConfigQuery", "2026-10-05T03:00:00+00:00", b'{"h":3}')  # JST 12:00
            before_midnight = self.src.response(conn, "ConfigQuery", "2026-10-05T14:59:59+00:00", b'{"h":4}')  # JST 23:59
            after_midnight = self.src.response(conn, "ConfigQuery", "2026-10-05T15:00:00+00:00", b'{"h":5}')  # JST 10-06 00:00
            offset = self.src.response(conn, "ConfigQuery", "2026-10-05T05:30:00+09:00", b'{"h":6}')  # JST 05:30
            other_op = self.src.response(conn, "OtherQuery", "2026-10-05T02:10:00+00:00", b'{"h":7}')
        conn.close()
        self.build()
        self.audit_clean()

        def ids(path):
            return sorted(r[0] for r in rows(self.out / path, "SELECT id FROM responses"))

        r1 = self.src.config_ids[0]
        self.assertEqual(ids(CONFIG_OCT), sorted([r1, same_a, same_b]))
        self.assertEqual(ids("responses/ConfigQuery/2026-10/2026-10-05/12.sqlite3"), [next_hour])
        self.assertEqual(ids("responses/ConfigQuery/2026-10/2026-10-05/23.sqlite3"), [before_midnight])
        self.assertEqual(ids("responses/ConfigQuery/2026-10/2026-10-06/00.sqlite3"), [after_midnight])  # 時は 2 桁
        self.assertEqual(ids("responses/ConfigQuery/2026-10/2026-10-05/05.sqlite3"), [offset])
        self.assertEqual(ids("responses/OtherQuery/2026-10/2026-10-05/11.sqlite3"), [other_op])  # operation ごとに別
        cat = self.out / "catalog.sqlite3"
        self.assertEqual(
            rows(cat, "SELECT operation,month,day,period,hour FROM files WHERE path=?",
                 ("responses/ConfigQuery/2026-10/2026-10-06/00.sqlite3",)),
            [("ConfigQuery", "2026-10", "2026-10-06", "2026-10-06", "00")])
        self.assertEqual(
            rows(cat, "SELECT part_path FROM response_index WHERE response_id=?", (same_b,)), [(CONFIG_OCT,)])
        self.assertEqual(
            rows(cat, "SELECT count(*) FROM files WHERE domain='responses' AND operation='ConfigQuery' AND day='2026-10-05'")[0][0],
            4)  # 11 時・12 時・23 時・05 時（seed の r1 を含む）
        # 取得の記録は応答の時ではなく日で分かれる（10-05 は seed の 6 行 + 6 行、10-06 は 1 行）
        self.assertEqual(rows(self.out / FETCH_OCT, "SELECT count(*) FROM response_fetches")[0][0], 6 + 6)
        self.assertEqual(rows(self.out / "fetches/2026-10/2026-10-06.sqlite3",
                              "SELECT count(*) FROM response_fetches")[0][0], 1)

    def test_ranking_list_is_defined_once(self):
        self.assertEqual(len(RANKING_OPERATIONS), 17)
        self.assertEqual(len(set(RANKING_OPERATIONS)), 17)
        self.assertIn(RANK, RANKING_OPERATIONS)
        self.assertEqual(HOME_RULE_VERSION, 3)

    def test_match_with_ranking_name_response_stays_with_match(self):
        # 試合の詳細に紐づく応答は operation がランキング系でも試合の本籍に入る
        conn = self.src.connect()
        with conn:
            self.src.next_id = 600
            rid = self.src.match(conn, "mr")
            conn.execute("UPDATE responses SET operation=? WHERE id=?", (RANK, rid))
        conn.close()
        self.build()
        self.audit_clean()
        self.assertEqual(rows(self.out / match_path("mr"), "SELECT count(*) FROM responses WHERE id=?", (rid,))[0][0], 1)
        self.assertFalse(any(p.startswith(f"responses/{RANK}/") for p in part_sha(self.out)))


class RuleVersionGuardTests(PartsTestBase):
    """規則の版が変わったとき、古い版の部品を同じ場所に混ぜない（設計書「本籍規則 版 3」）。"""

    @staticmethod
    def state_dump(path: Path):
        out = {}
        for table in ("row_homes", "match_home_prev", "response_home_prev", "match_index_prev", "meta", "part_files",
                      "published"):
            out[table] = sorted(rows(path, f"SELECT * FROM {table}"), key=repr)
        return out

    def patched_version(self, version):
        return mock.patch.object(parts_module, "HOME_RULE_VERSION", version)

    def test_other_rule_version_with_nonempty_out_stops_and_writes_nothing(self):
        self.build()
        before_parts = {p: (self.out / p).read_bytes() for p in list(part_sha(self.out)) + ["catalog.sqlite3"]}
        before_state = self.state_dump(self.state)
        self.assertEqual(dict(before_state["meta"]).get("rule_version"), str(HOME_RULE_VERSION))
        with self.patched_version(HOME_RULE_VERSION + 1):
            with self.assertRaises(PartsQuestion) as ctx:
                self.build()
        message = str(ctx.exception)
        self.assertTrue(message.startswith("QUESTION:"), message)
        self.assertIn(f"版 {HOME_RULE_VERSION}", message)
        self.assertIn(f"版 {HOME_RULE_VERSION + 1}", message)
        after_parts = {p: (self.out / p).read_bytes() for p in list(part_sha(self.out)) + ["catalog.sqlite3"]}
        self.assertEqual(after_parts, before_parts)  # 出力先は 1 バイトも変わらない（混ぜない）
        self.assertEqual(self.state_dump(self.state), before_state)  # 状態 DB も変わらない
        self.assertEqual({p.name for p in self.out.iterdir()}, {"catalog.sqlite3", "fetches", "images", "matches",
                                                                "responses", "system", "unplaced"})
        # 元の版に戻せば、そのまま差分周期が続く
        again = self.build()
        self.assertFalse(again["full_rebuild"])
        self.assertEqual(again["rebuilt_parts"], [])
        self.audit_clean()

    def test_older_rule_version_in_state_with_nonempty_out_stops(self):
        # 版 2 の状態 DB と出力先を模す（状態の記録を 2 に書き換える）
        self.build()
        conn = sqlite3.connect(self.state)
        conn.execute("UPDATE meta SET value='2' WHERE key='rule_version'")
        conn.commit()
        conn.close()
        before = part_sha(self.out)
        with self.assertRaises(PartsQuestion) as ctx:
            self.build()
        self.assertIn("版 2", str(ctx.exception))
        self.assertEqual(part_sha(self.out), before)

    def test_missing_state_with_existing_parts_stops(self):
        self.build()
        before = part_sha(self.out)
        self.state.unlink()
        with self.assertRaises(PartsQuestion) as ctx:
            self.build()
        self.assertTrue(str(ctx.exception).startswith("QUESTION:"))
        self.assertIn("記録なし", str(ctx.exception))
        self.assertEqual(part_sha(self.out), before)

    def test_other_rule_version_with_empty_out_rebuilds_everything_and_resets_state(self):
        self.build()
        # 前の出力先を空にし（別の場所に移した体）、状態 DB には前の出力に属する古い記録を残す
        shutil.rmtree(self.out)
        self.out.mkdir()
        conn = sqlite3.connect(self.state)
        conn.execute("INSERT INTO part_files VALUES('matches/old/format.sqlite3',1,'x','t',1)")
        conn.execute("INSERT INTO published VALUES('matches/old/format.sqlite3','x',1,'t')")
        conn.execute("INSERT INTO meta VALUES('catalog_published_through','1')")
        conn.commit()
        conn.close()
        with self.patched_version(HOME_RULE_VERSION + 1):
            result = self.build()
            self.assertTrue(result["full_rebuild"])
            self.assertEqual(result["match_index"]["mode"], "full")
            paths = set(part_sha(self.out))
            self.assertIn(M1_PART, paths)
            self.assertNotIn("matches/old/format.sqlite3", paths)  # 古い記録の部品を空で作らない
            for p in paths:
                self.assertEqual(rows(self.out / p, "SELECT value FROM _part WHERE key='rule_version'"),
                                 [(str(HOME_RULE_VERSION + 1),)], p)
            state = self.state_dump(self.state)
            self.assertEqual(dict(state["meta"]).get("rule_version"), str(HOME_RULE_VERSION + 1))
            self.assertNotIn("catalog_published_through", dict(state["meta"]))
            self.assertEqual({r[0] for r in state["part_files"]}, paths)
            self.assertEqual(state["published"], [])  # 新しい出力は送信済みの記録を引き継がない
            self.assertEqual(rows(self.out / "catalog.sqlite3", "SELECT rule_version FROM status"),
                             [(HOME_RULE_VERSION + 1,)])
            assert_audit_clean(self, self.audit())

    def test_temp_dir_leftover_is_not_a_part(self):
        # 作成中の一時ディレクトリだけが残っている出力先は空として扱う（初回の失敗後の再実行）
        (self.out / ".tmp-parts").mkdir(parents=True)
        (self.out / ".tmp-parts" / "part-x.tmp").write_bytes(b"leftover")
        result = self.build()
        self.assertTrue(result["full_rebuild"])
        self.audit_clean()
        self.assertFalse((self.out / ".tmp-parts").exists())

    def test_cli_exits_2_with_question_on_stderr(self):
        import contextlib
        import io

        self.build()
        err = io.StringIO()
        with self.patched_version(HOME_RULE_VERSION + 1), contextlib.redirect_stderr(err):
            code = parts_build.main(["build", "--source", str(self.src.path), "--out", str(self.out),
                                     "--state", str(self.state)])
        self.assertEqual(code, 2)
        self.assertTrue(err.getvalue().startswith("QUESTION:"))


BIG_TABLES = {"asset_refs", "entities", "sightings", "response_fetches"}
# 差分周期で走査してよいのは小さな表だけ（試合・応答・資産の数千行以下）。それ以外の全走査は出ない
SMALL_TABLES = {"matches", "match_classification", "responses", "assets", "analysis_genre", "archive_change_feed"}


class DeltaCycleCostTests(PartsTestBase):
    def change_everything(self):
        conn = self.src.connect()
        with conn:
            self.src.next_id = 700
            self.src.match(conn, "m6")
            r = self.src.response(conn, RANK, "2026-10-06T02:00:00+00:00", b'{"rank":1}')
            conn.execute("INSERT INTO entities VALUES('acc','T','e9',?,'{}')", (r,))
            conn.execute("INSERT INTO asset_refs VALUES(?,?,?)", (r, "u1", "$.z"))
            self.src.list_response(conn, "VsHistoryQuery", "2026-10-06T03:00:00+00:00",
                                   [("vs", "m1"), ("vs", "m2"), ("vs", "m6")])
            self.src.refetch(conn, self.src.detail_id["m1"], "2026-10-06T04:00:00+00:00")
            self.src.refetch(conn, self.src.config_ids[0], "2026-10-06T04:01:00+00:00")
            conn.execute("UPDATE match_classification SET rule_raw='GOAL',rule_name='GOAL' WHERE match_key='m2'")
            conn.execute("DELETE FROM match_tags WHERE tag='タグ甲'")
            conn.execute("UPDATE jobs SET attempts=attempts+1")
        conn.close()

    def test_full_build_scans_big_tables_but_delta_does_not(self):
        control: list = []
        self.build(scan_log=control)
        self.assertTrue(BIG_TABLES <= {t for t, _ in control}, control)  # 負の対照: 初回は全行を読む
        self.change_everything()
        log: list = []
        result = self.build(scan_log=log)
        self.assertFalse(result["full_rebuild"])
        self.assertEqual(result["match_index"]["mode"], "delta")
        scanned = {t for t, _ in log}
        self.assertFalse(scanned & BIG_TABLES, [x for x in log if x[0] in BIG_TABLES])
        self.assertTrue(scanned <= SMALL_TABLES, sorted(scanned - SMALL_TABLES))
        self.audit_clean()

    def test_refetch_and_new_list_response_cycles_scan_nothing_big(self):
        self.build()
        conn = self.src.connect()
        with conn:
            self.src.next_id = 710
            self.src.refetch(conn, self.src.detail_id["m1"], "2026-10-06T04:00:00+00:00")
        conn.close()
        log: list = []
        self.build(scan_log=log)
        self.assertFalse({t for t, _ in log} & BIG_TABLES, log)
        self.assertTrue({t for t, _ in log} <= SMALL_TABLES, log)
        conn = self.src.connect()
        with conn:
            self.src.list_response(conn, "VsHistoryQuery", "2026-10-06T05:00:00+00:00", [("vs", "m1"), ("vs", "m2")])
        conn.close()
        log = []
        self.build(scan_log=log)
        self.assertFalse({t for t, _ in log} & BIG_TABLES, log)
        self.assertTrue({t for t, _ in log} <= SMALL_TABLES, log)
        self.audit_clean()

    def test_idle_cycle_reads_nothing_big(self):
        self.build()
        log: list = []
        result = self.build(scan_log=log)
        self.assertEqual(result["rebuilt_parts"], [])
        self.assertFalse({t for t, _ in log} & BIG_TABLES)

    def test_delta_equals_fresh_full_build(self):
        self.build()
        shared_sha = sha(b'{"shared":true}')
        r1, r2 = self.src.config_ids

        def add_everything():
            self.change_everything()

        def asset_takes_shared_body():
            self.src.execute("UPDATE assets SET state='done',body_sha256=?,content_type='image/png' WHERE url='u2'",
                             (shared_sha,))

        def adopt_orphan_tag():
            conn = self.src.connect()
            with conn:
                self.src.next_id = 800
                self.src.match(conn, "nomatch", played="2026-08-01T00:00:00Z", analysis_set="nawabari", rule="TURF")
            conn.close()

        def asset_loses_body():
            self.src.execute("UPDATE assets SET state='pending',body_sha256=NULL WHERE url='u2'")

        def delete_response():
            conn = self.src.connect()
            with conn:
                conn.execute("DELETE FROM issues WHERE response_id IS NOT NULL")
                conn.execute("DELETE FROM response_fetches WHERE response_id IN "
                             "(SELECT id FROM responses WHERE fetched_at LIKE '2026-09-05%')")
                conn.execute("DELETE FROM responses WHERE fetched_at LIKE '2026-09-05%'")
            conn.close()

        def move_match_day():
            conn = self.src.connect()
            with conn:
                conn.execute("UPDATE documents SET json_text=replace(json_text,'2026-10-05T01:00:00Z','2026-11-20T01:00:00Z') "
                             "WHERE match_key='m1'")
            conn.close()

        def new_list_response_with_sightings():
            conn = self.src.connect()
            with conn:
                self.src.next_id = 900
                self.src.list_response(conn, "VsHistoryQuery", "2026-10-07T03:10:00+00:00",
                                       [("vs", "m1"), ("vs", "m3"), ("vs", "m4")])
                self.src.list_response(conn, "VsHistoryQuery", "2026-10-07T03:40:00+00:00", [("vs", "m3")])
                self.src.list_response(conn, "VsHistoryQuery", "2026-10-07T04:40:00+00:00", [("vs", "m3")])
            conn.close()

        def refetch_on_later_days():
            conn = self.src.connect()
            with conn:
                self.src.refetch(conn, self.src.detail_id["m3"], "2026-10-08T01:00:00+00:00")
                self.src.refetch(conn, r1, "2026-10-09T01:00:00+00:00")
                self.src.refetch(conn, r1, "zzz")
            conn.close()

        def response_moves_to_another_hour():
            # 応答の fetched_at が変わると、応答・本文・資産参照・entities が別の時の部品へ移る。取得の記録は動かない
            self.src.execute("UPDATE responses SET fetched_at='2026-10-05T10:00:00+00:00' WHERE id=?", (r1,))

        def fetch_row_moves_to_another_day():
            self.src.execute("UPDATE response_fetches SET fetched_at='2026-10-12T01:00:00+00:00' WHERE event_id LIKE 'refetch%' "
                             "AND fetched_at='2026-10-09T01:00:00+00:00'")

        def sighting_changes_and_goes():
            conn = self.src.connect()
            with conn:
                conn.execute("UPDATE sightings SET summary_json='{\"changed\":1}' WHERE path='$.nodes[0]'")
                conn.execute("DELETE FROM sightings WHERE path='$.nodes[1]'")
            conn.close()

        def list_response_moves_hour():
            self.src.execute("UPDATE responses SET fetched_at='2026-10-07T07:00:00+00:00' "
                             "WHERE operation='VsHistoryQuery' AND fetched_at='2026-10-07T03:40:00+00:00'")

        def vs_and_coop_share_a_match_key():
            conn = self.src.connect()
            with conn:
                self.src.next_id = 1000
                self.src.match(conn, "m2", kind="coop", analysis_set="salmon_regular", rule="REGULAR",
                               played="2026-10-05T08:00:00Z")

        def tag_on_a_key_shared_by_two_kinds():
            self.src.execute("INSERT INTO match_tags VALUES('acc','m2','共有キー',NULL,'t','t')")

        def orphan_rows_appear():
            conn = self.src.connect()
            with conn:
                self.src.next_id = 1300
                self.src.orphan_rows(conn, "adopt")
            conn.close()

        def orphan_rows_are_adopted():
            conn = self.src.connect()
            with conn:
                self.src.adopt_orphans(conn, "adopt")
            conn.close()

        def pending_match_appears():
            conn = self.src.connect()
            with conn:
                self.src.pending_match(conn, "pend")
            conn.close()

        def pending_match_gets_a_tag():
            conn = self.src.connect()
            with conn:
                conn.execute("INSERT INTO match_tags VALUES('acc','pend','待機中',NULL,'t','t')")
            conn.close()

        def detail_replaced_by_a_newer_response():
            # 同じ試合に新しい詳細の応答が付く。古い応答も documents の行を持つので、どちらも試合の部品に入る
            body = json.dumps({"id": "m3", "playedTime": "2026-09-30T20:00:00Z", "judgement": "LOSE",
                               "vsStage": {"name": "別のステージ"}, "player": {"weapon": {"name": "別のブキ"}}}).encode()
            conn = self.src.connect()
            with conn:
                self.src.next_id = 1200
                rid = self.src.response(conn, "VsHistoryDetailQuery", "2026-10-06T02:00:00+00:00", body)
                conn.execute("INSERT INTO documents(response_id,account,kind,match_key,json_text) VALUES(?,?,?,?,?)",
                             (rid, "acc", "vs", "m3", body.decode()))
                conn.execute("UPDATE matches SET detail_response_id=? WHERE match_key='m3'", (rid,))
            conn.close()

        def a_list_response_is_deleted():
            conn = self.src.connect()
            with conn:
                rid = conn.execute("SELECT min(id) FROM responses WHERE operation='VsHistoryQuery'").fetchone()[0]
                conn.execute("DELETE FROM sightings WHERE response_id=?", (rid,))
                conn.execute("DELETE FROM response_fetches WHERE response_id=?", (rid,))
                conn.execute("DELETE FROM responses WHERE id=?", (rid,))
            conn.close()

        def refetch_with_a_weapon_snapshot_row():
            # 取り直しのたびに増える fetch:<event_id> の rate_points は、その取得の応答の本籍に置かれる
            conn = self.src.connect()
            with conn:
                event = self.src.refetch(conn, r1, "2026-10-13T01:00:00+00:00")
                conn.execute("INSERT INTO rate_points VALUES('acc','sw','ブキ','weapon',NULL,?,NULL,2.75,"
                             "'api_snapshot','primary')", (f"fetch:{event}",))
            conn.close()

        def pending_match_gets_its_detail():
            conn = self.src.connect()
            with conn:
                self.src.next_id = 1100
                self.src.attach_detail(conn, "pend", played="2026-10-03T05:00:00Z", analysis_set="bankara_open", rule="AREA")
            conn.close()

        steps = (add_everything, asset_takes_shared_body, adopt_orphan_tag, asset_loses_body,
                 delete_response, move_match_day, new_list_response_with_sightings, refetch_on_later_days,
                 response_moves_to_another_hour, fetch_row_moves_to_another_day, sighting_changes_and_goes,
                 list_response_moves_hour, vs_and_coop_share_a_match_key, tag_on_a_key_shared_by_two_kinds,
                 detail_replaced_by_a_newer_response, a_list_response_is_deleted, refetch_with_a_weapon_snapshot_row,
                 orphan_rows_appear, orphan_rows_are_adopted,
                 pending_match_appears, pending_match_gets_a_tag, pending_match_gets_its_detail)
        for step in steps:
            step()
            result = self.build()
            self.assertFalse(result["full_rebuild"], step.__name__)
            self.assertEqual(result["match_index"]["mode"], "delta", step.__name__)
            assert_audit_clean(self, self.audit())
            fresh_out = Path(self.tmp.name) / ("fresh-" + step.__name__)
            fresh = build_parts(self.src.path, fresh_out, Path(self.tmp.name) / ("fresh-" + step.__name__ + ".state"))
            self.assertEqual(fresh["match_index"]["mode"], "full", step.__name__)
            self.assertEqual(dump_parts(self.out), dump_parts(fresh_out), step.__name__)
            # 目録の match_index（差分更新）は全件計算と同じ
            self.assertEqual(dump_catalog(self.out), dump_catalog(fresh_out), step.__name__)
            self.assertEqual(rows(self.out / "catalog.sqlite3", "SELECT * FROM match_index ORDER BY account,kind,match_key"),
                             rows(fresh_out / "catalog.sqlite3", "SELECT * FROM match_index ORDER BY account,kind,match_key"),
                             step.__name__)
        self.assertIn(match_path("m1", day="2026-11/2026-11-20"), part_sha(self.out))
        self.assertGreater(len(rows(self.out / "catalog.sqlite3", "SELECT * FROM match_index")), 6)

    def test_match_index_incremental_recomputes_only_what_changed(self):
        self.build()

        def cycle():
            result = self.build()
            self.assertEqual(result["match_index"]["mode"], "delta")
            self.audit_clean()
            return result["match_index"]

        # 何も変わらない周期は 0 件
        self.assertEqual(cycle(), {"mode": "delta", "recomputed": 0, "reused": 5})
        # 取り直しだけ（試合の部品も古い応答の部品も変わらない）も 0 件
        conn = self.src.connect()
        with conn:
            self.src.refetch(conn, self.src.detail_id["m1"], "2026-10-06T04:00:00+00:00")
        conn.close()
        self.assertEqual(cycle(), {"mode": "delta", "recomputed": 0, "reused": 5})
        # 新しい一覧の応答が届いて、過去の試合を載せても、その試合の目録の行は計算し直さない
        conn = self.src.connect()
        with conn:
            self.src.next_id = 50
            self.src.list_response(conn, "VsHistoryQuery", "2026-10-06T05:00:00+00:00", [("vs", "m1"), ("vs", "m2")])
        conn.close()
        self.assertEqual(cycle(), {"mode": "delta", "recomputed": 0, "reused": 5})
        # タグを付けた試合だけ
        self.src.execute("INSERT INTO match_tags VALUES('acc','m2','新タグ',NULL,'t','t')")
        self.assertEqual(cycle(), {"mode": "delta", "recomputed": 1, "reused": 4})
        self.assertEqual(rows(self.out / "catalog.sqlite3", "SELECT tags FROM match_index WHERE match_key='m2'"),
                         [("新タグ",)])
        # 詳細の JSON が変わった試合だけ（勝敗が変わる）
        self.src.execute("UPDATE documents SET json_text=replace(json_text,'WIN','LOSE') WHERE match_key='m3'")
        self.assertEqual(cycle(), {"mode": "delta", "recomputed": 1, "reused": 4})
        self.assertEqual(rows(self.out / "catalog.sqlite3", "SELECT judgement FROM match_index WHERE match_key='m3'"),
                         [("LOSE",)])
        # 新しい試合は 1 件、ほかは使い回す
        conn = self.src.connect()
        with conn:
            self.src.next_id = 60
            self.src.match(conn, "m7")
        conn.close()
        self.assertEqual(cycle(), {"mode": "delta", "recomputed": 1, "reused": 5})

    def test_stale_previous_row_is_recomputed(self):
        # 前回の行の part_path が今の本籍と食い違っていたら、その試合だけ計算し直す（安全網）
        self.build()
        conn = sqlite3.connect(self.state)
        conn.execute("UPDATE match_index_prev SET part_path='stale',judgement='STALE' WHERE match_key='m1'")
        conn.commit()
        conn.close()
        result = self.build()
        self.assertEqual(result["match_index"], {"mode": "delta", "recomputed": 1, "reused": 4})
        self.assertEqual(rows(self.out / "catalog.sqlite3", "SELECT part_path,judgement FROM match_index "
                                                            "WHERE match_key='m1'"), [(M1_PART, "WIN")])
        self.assertEqual(rows(self.state, "SELECT part_path,judgement FROM match_index_prev WHERE match_key='m1'"),
                         [(M1_PART, "WIN")])
        self.audit_clean()

    def test_match_index_rows_fall_back_to_full_when_previous_rows_are_missing(self):
        self.build()
        conn = sqlite3.connect(self.state)
        conn.execute("DELETE FROM match_index_prev")  # 前回の行が揃っていない状態
        conn.commit()
        conn.close()
        self.src.execute("INSERT INTO match_tags VALUES('acc','m2','新タグ',NULL,'t','t')")
        result = self.build()
        self.assertFalse(result["full_rebuild"])  # 部品は差分のまま
        self.assertEqual(result["match_index"], {"mode": "full", "recomputed": 5, "reused": 0})
        self.audit_clean()
        again = self.build()
        self.assertEqual(again["match_index"], {"mode": "delta", "recomputed": 0, "reused": 5})


class RandomMutations:
    """正本への無作為な変更（固定の乱数の種で再現できる）。差分周期と、新しい出力先への全件作成が同じ結果になることの試験用。"""

    PLAYED = ["2026-09-%02dT%02d:00:00Z" % (day, hour) for day in (1, 15, 30) for hour in (0, 14)]
    FETCHED = ["2026-10-%02dT%02d:%02d:00+00:00" % (day, hour, minute)
               for day in (3, 5) for hour in (0, 15) for minute in (0, 30)] + ["zzz"]
    SETS = (("xmatch", "AREA"), ("xmatch", "GOAL"), ("bankara_open", "LOFT"), ("nawabari", "TURF"))
    OPERATIONS = ("ConfigQuery", "VsHistoryQuery", "WeaponQuery", RANKING_OPERATIONS[0], "OtherQuery")

    def __init__(self, seed, src):
        import random

        self.rnd = random.Random(seed)
        self.src = src
        self.keys = ["m1", "m2", "m3", "m4", "m5"]
        self.counter = 0
        src.next_id = 5000
        self.names = [n for n in dir(self) if n.startswith("op_")]

    def run_once(self):
        conn = self.src.connect()
        try:
            with conn:
                name = self.rnd.choice(self.names)
                getattr(self, name)(conn)
        finally:
            conn.close()
        return name

    def column(self, conn, sql, params=()):
        return [r[0] for r in conn.execute(sql, params)]

    def new_key(self):
        self.counter += 1
        return f"k{self.counter}"

    def pick_rowid(self, conn, table):
        """乱数の種で決まる行を選ぶ（SQL の random() は種で再現できないので使わない）。"""
        ids = self.column(conn, f"SELECT rowid FROM {table} ORDER BY rowid")
        return self.rnd.choice(ids) if ids else None

    def op_add_match(self, conn):
        key = self.new_key()
        analysis_set, rule = self.rnd.choice(self.SETS)
        self.src.match(conn, key, played=self.rnd.choice(self.PLAYED + ["broken"]), analysis_set=analysis_set,
                       rule=rule, classify=self.rnd.random() > 0.2, fetched_at=self.rnd.choice(self.FETCHED))
        self.keys.append(key)

    def op_pending_then_detail(self, conn):
        pending = self.column(conn, "SELECT match_key FROM matches WHERE detail_response_id IS NULL AND kind='vs'")
        if pending and self.rnd.random() < 0.6:
            analysis_set, rule = self.rnd.choice(self.SETS)
            self.src.attach_detail(conn, self.rnd.choice(pending), played=self.rnd.choice(self.PLAYED),
                                   analysis_set=analysis_set, rule=rule, fetched_at=self.rnd.choice(self.FETCHED))
        else:
            key = self.new_key()
            self.src.pending_match(conn, key)
            self.keys.append(key)

    def op_list_response(self, conn):
        seen = [("vs", k) for k in self.rnd.sample(self.keys, self.rnd.randint(0, 3))]
        self.src.list_response(conn, self.rnd.choice(["VsHistoryQuery", "CoopHistoryQuery"]),
                               self.rnd.choice(self.FETCHED), seen)

    def op_refetch(self, conn):
        ids = self.column(conn, "SELECT id FROM responses")
        self.src.refetch(conn, self.rnd.choice(ids), self.rnd.choice(self.FETCHED))

    def op_reclassify(self, conn):
        analysis_set, rule = self.rnd.choice(self.SETS)
        conn.execute("UPDATE match_classification SET analysis_set=?,rule_raw=?,rule_name=? WHERE match_key=?",
                     (analysis_set, rule, rule + "x", self.rnd.choice(self.keys)))

    def op_edit_document(self, conn):
        key = self.rnd.choice(self.keys)
        row = conn.execute("SELECT response_id,json_text FROM documents WHERE match_key=? LIMIT 1", (key,)).fetchone()
        if row:
            body = json.loads(row[1])
            body["playedTime"] = self.rnd.choice(self.PLAYED)
            body["judgement"] = self.rnd.choice(["WIN", "LOSE"])
            conn.execute("UPDATE documents SET json_text=? WHERE response_id=? AND match_key=?",
                         (json.dumps(body), row[0], key))

    def op_tag(self, conn):
        if self.rnd.random() < 0.7:
            conn.execute("INSERT OR IGNORE INTO match_tags VALUES('acc',?,?,NULL,'t','t')",
                         (self.rnd.choice(self.keys + ["orphan"]), "tag%d" % self.rnd.randint(1, 3)))
        else:
            conn.execute("DELETE FROM match_tags WHERE rowid=?", (self.pick_rowid(conn, "match_tags"),))

    def drop_response(self, conn, response_id):
        for table in ("sightings", "asset_refs", "entities", "issues", "response_fetches", "documents"):
            conn.execute(f"DELETE FROM {table} WHERE response_id=?", (response_id,))
        conn.execute("DELETE FROM endpoint_heads WHERE response_id=?", (response_id,))
        conn.execute("UPDATE matches SET detail_response_id=NULL WHERE detail_response_id=?", (response_id,))
        conn.execute("UPDATE jobs SET last_response_id=NULL WHERE last_response_id=?", (response_id,))
        conn.execute("DELETE FROM responses WHERE id=?", (response_id,))

    def op_delete_match(self, conn):
        key = self.rnd.choice(self.keys)
        response_ids = self.column(conn, "SELECT response_id FROM documents WHERE match_key=?", (key,))
        for table in ("jobs", "rate_points", "match_classification", "match_refs"):
            conn.execute(f"DELETE FROM {table} WHERE match_key=?", (key,))
        conn.execute("DELETE FROM sightings WHERE match_key=? AND response_id IN "
                     "(SELECT response_id FROM documents WHERE match_key=?)", (key, key))
        conn.execute("DELETE FROM documents WHERE match_key=?", (key,))
        conn.execute("DELETE FROM matches WHERE match_key=?", (key,))
        if self.rnd.random() < 0.5:  # 応答まで消すか、応答だけ残す（応答の本籍が試合から応答の時へ変わる）
            for response_id in response_ids:
                self.drop_response(conn, response_id)

    def op_delete_list_response(self, conn):
        ids = self.column(conn, "SELECT id FROM responses WHERE operation IN ('VsHistoryQuery','CoopHistoryQuery','ConfigQuery')")
        if ids:
            self.drop_response(conn, self.rnd.choice(ids))

    def op_move_response(self, conn):
        ids = self.column(conn, "SELECT id FROM responses")
        conn.execute("UPDATE responses SET fetched_at=? WHERE id=?", (self.rnd.choice(self.FETCHED), self.rnd.choice(ids)))

    def op_move_fetch(self, conn):
        conn.execute("UPDATE response_fetches SET fetched_at=? WHERE rowid=?",
                     (self.rnd.choice(self.FETCHED), self.pick_rowid(conn, "response_fetches")))

    def op_entity(self, conn):
        ids = self.column(conn, "SELECT id FROM responses")
        conn.execute("INSERT INTO entities VALUES('acc','T',?,?,?) ON CONFLICT(account,typename,entity_id) DO UPDATE "
                     "SET response_id=excluded.response_id,json_text=excluded.json_text",
                     ("e%d" % self.rnd.randint(1, 4), self.rnd.choice(ids), json.dumps({"v": self.rnd.randint(1, 3)})))

    def op_asset(self, conn):
        ids = self.column(conn, "SELECT id FROM responses")
        conn.execute("INSERT OR IGNORE INTO asset_refs VALUES(?,?,?)",
                     (self.rnd.choice(ids), self.rnd.choice(["u1", "u2"]), "$.p%d" % self.rnd.randint(1, 4)))
        if self.rnd.random() < 0.5:
            shas = self.column(conn, "SELECT sha256 FROM bodies")
            conn.execute("UPDATE assets SET state='done',body_sha256=?,content_type='x' WHERE url=?",
                         (self.rnd.choice(shas), self.rnd.choice(["u1", "u2"])))
        else:
            conn.execute("UPDATE assets SET state='pending',body_sha256=NULL WHERE url=?", (self.rnd.choice(["u1", "u2"]),))

    def op_system(self, conn):
        conn.execute("UPDATE jobs SET attempts=attempts+1 WHERE rowid=?", (self.pick_rowid(conn, "jobs"),))
        conn.execute("INSERT OR REPLACE INTO control VALUES('k',?)", (str(self.rnd.random()),))
        conn.execute("INSERT INTO issues(response_id,code,context,created_at) VALUES(?,?,?,?)",
                     (self.rnd.choice(self.column(conn, "SELECT id FROM responses") + [None]), "c", "{}", "t"))

    def op_shared_body_response(self, conn):
        self.src.response(conn, self.rnd.choice(self.OPERATIONS), self.rnd.choice(self.FETCHED),
                          self.rnd.choice([b'{"shared":true}', b'{"shared":2}']))

    def op_coop_sibling(self, conn):
        key = self.rnd.choice(self.keys)
        if not conn.execute("SELECT 1 FROM matches WHERE match_key=? AND kind='coop'", (key,)).fetchone():
            self.src.match(conn, key, kind="coop", analysis_set="salmon_regular", rule="REGULAR",
                           played=self.rnd.choice(self.PLAYED), fetched_at=self.rnd.choice(self.FETCHED))

    def op_replace_detail(self, conn):
        key = self.rnd.choice(self.keys)
        row = conn.execute("SELECT detail_response_id FROM matches WHERE match_key=? AND kind='vs'", (key,)).fetchone()
        if row and row[0]:
            body = self.src.detail_body(key, self.rnd.choice(self.PLAYED))
            response_id = self.src.response(conn, "VsHistoryDetailQuery", self.rnd.choice(self.FETCHED), body)
            conn.execute("INSERT INTO documents(response_id,account,kind,match_key,json_text) VALUES(?,?,?,?,?)",
                         (response_id, "acc", "vs", key, body.decode()))
            conn.execute("UPDATE matches SET detail_response_id=? WHERE match_key=? AND kind='vs'", (response_id, key))

    def op_ranking_response(self, conn):
        self.src.response(conn, RANKING_OPERATIONS[0], self.rnd.choice(self.FETCHED),
                          json.dumps({"r": self.rnd.random()}).encode())

    def op_weapon_snapshot(self, conn):
        events = self.column(conn, "SELECT event_id FROM response_fetches")
        conn.execute("INSERT OR IGNORE INTO rate_points VALUES('acc',?,'L','weapon',NULL,?,NULL,1.5,'api_snapshot','primary')",
                     ("sw%d" % self.rnd.randint(1, 3), "fetch:" + self.rnd.choice(events)))


class RandomMutationTests(PartsTestBase):
    def test_random_mutations_delta_equals_fresh_full_build(self):
        """どの周期でも、差分更新の部品・目録（match_index を含む）は、新しい出力先への全件作成と同じで、監査も一致する。"""
        for seed in (1, 2, 3):
            with self.subTest(seed=seed):
                tmp = Path(self.tmp.name) / f"random{seed}"
                src = Source(tmp / "database" / "a.sqlite3")
                src.seed()
                out, state = tmp / "parts", tmp / "state.sqlite3"
                build_parts(src.path, out, state)
                mutations = RandomMutations(seed, src)
                done = []
                for cycle in range(7):
                    for _ in range(mutations.rnd.randint(1, 4)):
                        done.append(mutations.run_once())
                    result = build_parts(src.path, out, state)
                    self.assertFalse(result["full_rebuild"], (seed, cycle))
                    self.assertEqual(result["match_index"]["mode"], "delta", (seed, cycle))
                    assert_audit_clean(self, audit(src.path, out))
                    fresh = tmp / f"fresh{cycle}"
                    build_parts(src.path, fresh, tmp / f"fresh{cycle}.state")
                    context = (seed, cycle, done[-8:])
                    self.assertEqual(dump_parts(out), dump_parts(fresh), context)
                    self.assertEqual(dump_catalog(out), dump_catalog(fresh), context)
                    self.assertEqual(
                        rows(out / "catalog.sqlite3", "SELECT * FROM match_index ORDER BY account,kind,match_key"),
                        rows(fresh / "catalog.sqlite3", "SELECT * FROM match_index ORDER BY account,kind,match_key"),
                        context)
                    shutil.rmtree(fresh)


class NoFeedTests(PartsTestBase):
    feed = False

    def test_without_change_feed_every_run_is_full(self):
        first = self.build()
        self.assertIsNone(first["through_event_id"])
        self.assertTrue(first["full_rebuild"])
        self.assertEqual(first["match_index"]["mode"], "full")
        self.src.execute("UPDATE matches SET last_seen='z' WHERE match_key='m1'")
        second = self.build()
        self.assertTrue(second["full_rebuild"])
        self.assertEqual(second["match_index"]["mode"], "full")
        self.audit_clean()
        self.assertEqual(rows(self.out / M1_PART, "SELECT last_seen FROM matches WHERE match_key='m1'"), [("z",)])


class InternalAndQuestionTests(PartsTestBase):
    def test_sqlite_internal_tables_go_to_catalog(self):
        conn = self.src.connect()
        conn.execute("CREATE TABLE seqt(id INTEGER PRIMARY KEY AUTOINCREMENT, v)")
        conn.execute("INSERT INTO seqt(v) VALUES('a')")
        conn.execute("ANALYZE")
        conn.commit()
        conn.close()
        self.build()
        cat = self.out / "catalog.sqlite3"
        self.assertEqual(rows(cat, "SELECT column_name,value FROM source_sqlite_internal WHERE table_name='sqlite_sequence'"),
                         [("name", "seqt"), ("seq", 1)])
        self.assertGreater(rows(cat, "SELECT count(*) FROM source_sqlite_internal WHERE table_name='sqlite_stat1'")[0][0], 0)
        for p in self.out.rglob("*.sqlite3"):
            if p.name != "catalog.sqlite3":
                names = {r[0] for r in rows(p, "SELECT name FROM sqlite_master")}
                self.assertNotIn("sqlite_stat1", names)
                if "sqlite_sequence" in names:  # AUTOINCREMENT 表が自動で作る空の表。行は入れない
                    self.assertEqual(rows(p, "SELECT count(*) FROM sqlite_sequence")[0][0], 0)
        self.audit_clean()
        # 表が増えた（スキーマが変わった）ら全部品を作り直す。未知の表は unplaced
        self.assertIn("unplaced/seqt.sqlite3", part_sha(self.out))

    def test_schema_change_rebuilds_all_parts_and_recomputes_match_index(self):
        self.build()
        conn = self.src.connect()
        conn.execute("CREATE TABLE extra(v)")
        conn.commit()
        conn.close()
        result = self.build()
        self.assertTrue(result["full_rebuild"])
        self.assertEqual(result["match_index"], {"mode": "full", "recomputed": 5, "reused": 0})
        self.audit_clean()

    def test_generated_column_is_a_question(self):
        conn = self.src.connect()
        conn.execute("CREATE TABLE gen(a INTEGER, b INTEGER GENERATED ALWAYS AS (a+1) VIRTUAL)")
        conn.commit()
        conn.close()
        with self.assertRaises(PartsQuestion) as ctx:
            self.build()
        self.assertTrue(str(ctx.exception).startswith("QUESTION:"))

    def test_without_rowid_is_a_question(self):
        conn = self.src.connect()
        conn.execute("CREATE TABLE wr(a TEXT PRIMARY KEY) WITHOUT ROWID")
        conn.commit()
        conn.close()
        with self.assertRaises(PartsQuestion):
            self.build()


class CliTests(PartsTestBase):
    def test_cli_build_and_audit(self):
        import contextlib
        import io

        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            code = parts_build.main(["build", "--source", str(self.src.path), "--out", str(self.out),
                                     "--state", str(self.state), "--work-dir", str(Path(self.tmp.name) / "work")])
        self.assertEqual(code, 0)
        result = json.loads(buf.getvalue())
        self.assertTrue(result["rebuilt_parts"])
        for item in result["rebuilt_parts"]:
            self.assertEqual(set(item), {"path", "sha256", "bytes"})
        self.assertEqual(result["match_index"], {"mode": "full", "recomputed": 5, "reused": 0})
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            code = parts_build.main(["audit", "--source", str(self.src.path), "--out", str(self.out)])
        self.assertEqual(code, 0)
        self.assertTrue(json.loads(buf.getvalue())["ok"])
        self.assertEqual(list((Path(self.tmp.name) / "work").iterdir()), [])  # 作業 DB は消える


if __name__ == "__main__":
    unittest.main()
