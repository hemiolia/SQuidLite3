"""部品作成と監査（src/python/ikarchive/parts.py）の試験。人工 DB のみを使う。"""

import hashlib
import json
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src/python"))
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))

from ikarchive.change_feed import install_change_feed
from ikarchive.parts import (
    HOME_RULE_VERSION,
    PartsQuestion,
    audit,
    build_parts,
    decode_segment,
    encode_segment,
    jst_month,
    response_period,
)
from ikarchive.store import Store
from ikarchive.writer_guards import install_writer_guards

import parts_build

MATCH_PART = "matches/xmatch/AREA/2026-10.sqlite3"


def sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


class Source:
    """人工の正本。Store で通常のスキーマを作り、変更追跡と書き手の守りを入れる。"""

    def __init__(self, path: Path, feed: bool = True):
        self.path = path
        path.parent.mkdir(parents=True, exist_ok=True)
        Store(path).close()
        self.next_id = 1
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

    def match(self, conn, key, played="2026-10-05T01:00:00Z", analysis_set="xmatch", rule="AREA",
              kind="vs", classify=True):
        body = json.dumps({"id": key, "playedTime": played, "judgement": "WIN",
                           "vsStage": {"name": "ステージ"}, "player": {"weapon": {"name": "ブキ"}}}).encode()
        rid = self.response(conn, "VsHistoryDetailQuery", "2026-10-05T02:00:00+00:00", body)
        conn.execute("INSERT INTO matches VALUES(?,?,?,?,?,?)", ("acc", kind, key, "t", "t", rid))
        conn.execute("INSERT INTO documents VALUES(?,?,?,?,?)", (rid, "acc", kind, key, body.decode()))
        conn.execute("INSERT INTO match_refs VALUES(?,?,?,?)", ("acc", kind, "remote-" + key, key))
        conn.execute("INSERT INTO sightings VALUES(?,?,?,?,?,?)", (rid, "acc", kind, key, "$.x", "{}"))
        if classify:
            conn.execute(
                "INSERT INTO match_classification VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                ("acc", kind, key, analysis_set, "X_MATCH", None, rule, rule, "four_vs_four", analysis_set,
                 2, 4, "[4]", rid, "t"),
            )
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
            conn.execute("INSERT INTO entities VALUES('acc','T','e1',?,'{}')", (r1,))
            conn.execute("INSERT INTO issues(response_id,code,context,created_at) VALUES(?,?,?,?)", (r2, "c", "{}", "t"))
            conn.execute("INSERT INTO issues(code,context,created_at) VALUES('c2','{}','t')")
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
        self.assertEqual(response_period("Op", "2026-09-30T20:00:00+00:00"), "2026-10")


class FullBuildTests(PartsTestBase):
    def test_full_build_audit_schema_views(self):
        result = self.build()
        self.assertTrue(result["full_rebuild"])
        self.assertIsNotNone(result["through_event_id"])
        report = self.audit()
        self.assertTrue(report["ok"], report)
        for table, info in report["tables"].items():
            self.assertEqual(info["mismatched"], 0, table)
            self.assertEqual(info["source_rows"], info["part_rows"], table)
        self.assertGreater(report["copies"]["checked"], 0)
        self.assertEqual(report["copies"]["mismatched"], 0)

        # 住所
        paths = set(part_sha(self.out))
        self.assertIn(MATCH_PART, paths)
        self.assertIn("matches/xmatch/GOAL/2026-10.sqlite3", paths)  # m3 は JST で 10 月
        self.assertIn("matches/nawabari/TURF/2026-09.sqlite3", paths)
        self.assertIn("matches/unclassified/no-rule/unknown-month.sqlite3", paths)
        self.assertIn("responses/ConfigQuery/2026-10.sqlite3", paths)
        self.assertIn("responses/ConfigQuery/2026-09.sqlite3", paths)
        self.assertIn("system/runs.sqlite3", paths)
        self.assertIn("system/jobs.sqlite3", paths)
        self.assertIn("system/issues.sqlite3", paths)
        self.assertIn("images/no-body.sqlite3", paths)
        self.assertIn("unplaced/match_tags.sqlite3", paths)
        self.assertTrue(any(p.startswith("system/archive_change_feed/") for p in paths))
        img = hashlib.sha256(b"\x89PNG\r\n\x1a\n\x00\x01").hexdigest()[:2]
        self.assertIn(f"images/{img}.sqlite3", paths)
        # 試合に紐づく jobs は試合の本籍、紐づかないものは system/jobs
        self.assertEqual(rows(self.out / MATCH_PART, "SELECT count(*) FROM jobs")[0][0], 1)
        self.assertEqual(rows(self.out / "system/jobs.sqlite3", "SELECT count(*) FROM jobs")[0][0], 1)
        # 本文の写し: 共有本文は両方の応答部品にあり、写しとして記録される
        for p in ("responses/ConfigQuery/2026-10.sqlite3", "responses/ConfigQuery/2026-09.sqlite3"):
            self.assertEqual(rows(self.out / p, "SELECT count(*) FROM bodies")[0][0], 1)
        copies = [rows(self.out / p, "SELECT count(*) FROM _copies WHERE table_name='bodies'")[0][0]
                  for p in ("responses/ConfigQuery/2026-10.sqlite3", "responses/ConfigQuery/2026-09.sqlite3")]
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
        self.assertEqual(rows(self.out / MATCH_PART, "SELECT count(*) FROM analysis_xmatch")[0][0], 2)
        self.assertEqual(rows(self.out / MATCH_PART, "SELECT count(*) FROM battle_players")[0][0], 0)

        # 目録
        cat = self.out / "catalog.sqlite3"
        self.assertEqual(rows(cat, "SELECT count(*) FROM match_index")[0][0], 5)
        m1 = rows(cat, "SELECT stage,judgement,my_weapon,tags,detail_available,part_path,played_time "
                       "FROM match_index WHERE match_key='m1'")[0]
        self.assertEqual(m1[:6], ("ステージ", "WIN", "ブキ", "タグ乙、タグ甲", 1, MATCH_PART))
        self.assertEqual(rows(cat, "SELECT analysis_set,rule_raw,part_path FROM match_index WHERE match_key='m5'"),
                         [("unclassified", "no-rule", "matches/unclassified/no-rule/unknown-month.sqlite3")])
        self.assertEqual(rows(cat, "SELECT count(*) FROM response_index")[0][0], 7)
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

    def test_audit_detects_tampering(self):
        self.build()
        part = self.out / MATCH_PART
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
        for p in ("responses/ConfigQuery/2026-10.sqlite3", "responses/ConfigQuery/2026-09.sqlite3"):
            if rows(self.out / p, "SELECT count(*) FROM _copies")[0][0]:
                copy_part = self.out / p
        conn = sqlite3.connect(copy_part)
        conn.execute("UPDATE bodies SET byte_length=byte_length+1")
        conn.commit()
        conn.close()
        report = self.audit()
        self.assertFalse(report["ok"])
        self.assertEqual(report["copies"]["mismatched"], 1)


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
        report = self.audit()
        self.assertTrue(report["ok"], report)
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


class IncrementalTests(PartsTestBase):
    def changed_parts(self, before):
        after = part_sha(self.out)
        return {p for p in after if before.get(p) != after[p]}

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
        rebuilt = {r["path"] for r in result["rebuilt_parts"]}
        non_feed = {p for p in rebuilt if not p.startswith("system/archive_change_feed/")}
        self.assertEqual(non_feed, {MATCH_PART, "responses/ConfigQuery/2026-10.sqlite3"})
        self.assertEqual(self.changed_parts(before), rebuilt)  # 作り直していない部品は SHA-256 が不変
        self.assertTrue(self.audit()["ok"])
        self.assertEqual(rows(self.out / "catalog.sqlite3", "SELECT count(*) FROM match_index")[0][0], 6)
        # 何も変えなければ、部品は一つも作り直されない
        again = self.build()
        self.assertEqual(again["rebuilt_parts"], [])
        self.assertTrue(self.audit()["ok"])

    def test_classification_change_moves_match(self):
        self.build()
        before = part_sha(self.out)
        self.src.execute("UPDATE match_classification SET rule_raw='GOAL',rule_name='GOAL' WHERE match_key='m2'")
        result = self.build()
        rebuilt = {r["path"] for r in result["rebuilt_parts"]}
        self.assertIn(MATCH_PART, rebuilt)
        self.assertIn("matches/xmatch/GOAL/2026-10.sqlite3", rebuilt)
        for p in (MATCH_PART,):
            self.assertEqual(rows(self.out / p, "SELECT match_key FROM matches ORDER BY 1"), [("m1",)])
        self.assertEqual(rows(self.out / "matches/xmatch/GOAL/2026-10.sqlite3",
                              "SELECT match_key FROM matches ORDER BY 1"), [("m2",), ("m3",)])
        self.assertTrue(self.audit()["ok"])
        # 部品に残る最後の試合が動いたら、旧部品は削除されず空の部品になる
        self.src.execute("UPDATE match_classification SET rule_raw='X',rule_name='X' WHERE match_key='m4'")
        self.build()
        old = self.out / "matches/nawabari/TURF/2026-09.sqlite3"
        self.assertTrue(old.is_file())
        for table in ("matches", "documents", "match_classification", "responses", "bodies"):
            self.assertEqual(rows(old, f"SELECT count(*) FROM {table}")[0][0], 0, table)
        self.assertEqual(rows(old, "SELECT count(*) FROM _part")[0][0], 5)
        self.assertEqual(rows(old, "SELECT count(*) FROM sqlite_master WHERE type='view'")[0][0] > 0, True)
        self.assertTrue(self.audit()["ok"])

    def test_delete_is_reflected(self):
        self.build()
        self.src.execute("DELETE FROM match_tags WHERE tag='タグ甲'")
        result = self.build()
        self.assertIn(MATCH_PART, {r["path"] for r in result["rebuilt_parts"]})
        self.assertEqual(rows(self.out / MATCH_PART, "SELECT tag FROM match_tags"), [("タグ乙",)])
        self.assertTrue(self.audit()["ok"])
        # 応答ごとの削除（子から親の順）
        conn = self.src.connect()
        with conn:
            conn.execute("DELETE FROM jobs WHERE operation='op2'")
        conn.close()
        result = self.build()
        self.assertIn("system/jobs.sqlite3", {r["path"] for r in result["rebuilt_parts"]})
        self.assertEqual(rows(self.out / "system/jobs.sqlite3", "SELECT count(*) FROM jobs")[0][0], 0)
        self.assertTrue(self.audit()["ok"])

    def test_unplaced_and_later_adoption(self):
        self.build()
        self.assertEqual(rows(self.out / "unplaced/match_tags.sqlite3", "SELECT match_key FROM match_tags"),
                         [("nomatch",)])
        self.assertTrue(self.audit()["ok"])
        # 親の試合が後から現れたら、タグは unplaced から試合の本籍へ移る
        conn = self.src.connect()
        with conn:
            self.src.next_id = 200
            self.src.match(conn, "nomatch", played="2026-08-01T00:00:00Z", analysis_set="nawabari", rule="TURF")
        conn.close()
        result = self.build()
        rebuilt = {r["path"] for r in result["rebuilt_parts"]}
        self.assertIn("unplaced/match_tags.sqlite3", rebuilt)
        self.assertEqual(rows(self.out / "unplaced/match_tags.sqlite3", "SELECT count(*) FROM match_tags")[0][0], 0)
        self.assertEqual(rows(self.out / "matches/nawabari/TURF/2026-08.sqlite3", "SELECT tag FROM match_tags"),
                         [("孤児",)])
        self.assertTrue(self.audit()["ok"])

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
        self.assertTrue(self.audit()["ok"])
        self.assertEqual(rows(self.out / MATCH_PART, "SELECT count(*) FROM matches WHERE match_key='late'")[0][0], 1)


class NoFeedTests(PartsTestBase):
    feed = False

    def test_without_change_feed_every_run_is_full(self):
        first = self.build()
        self.assertIsNone(first["through_event_id"])
        self.assertTrue(first["full_rebuild"])
        self.src.execute("UPDATE matches SET last_seen='z' WHERE match_key='m1'")
        second = self.build()
        self.assertTrue(second["full_rebuild"])
        self.assertTrue(self.audit()["ok"])
        self.assertEqual(rows(self.out / MATCH_PART, "SELECT last_seen FROM matches WHERE match_key='m1'"), [("z",)])


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
        self.assertTrue(self.audit()["ok"])
        # 表が増えた（スキーマが変わった）ら全部品を作り直す。未知の表は unplaced
        self.assertIn("unplaced/seqt.sqlite3", part_sha(self.out))

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
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            code = parts_build.main(["audit", "--source", str(self.src.path), "--out", str(self.out)])
        self.assertEqual(code, 0)
        self.assertTrue(json.loads(buf.getvalue())["ok"])
        self.assertEqual(list((Path(self.tmp.name) / "work").iterdir()), [])  # 作業 DB は消える


if __name__ == "__main__":
    unittest.main()
