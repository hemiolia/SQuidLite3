"""RecordReader の単体テスト。

実DB・実アカウントを一切使わず、人工SQLite環境で以下を網羅的に検証する：
- VS/Coop/詳細なしpending/unavailable/unresolved/未分類を母集合から落とさない
- 複合キーaccountとkindの分離
- limit offset全ページが重複漏れなし、悪意文字列/invalidparams安全
- canonical部分応答が上書きしない
- raw body特殊数値表記/空配列/未知キーをbyte同一で返す（detail_jsonと区別）
- context終了close、存在しないDBを作らない、READONLYで書込不可
- 元response/BLOB不在時の破損検出（RuntimeError）
- WAL人工DBスナップショット分離（context開放中の別書き込みが次ページ/totalに影響せず、新contextのみ反映）
- context内例外発生時の実コネクションclose（ProgrammingError）
- set_authorizerによるbodiesテーブルREAD拒否時の負の対照（list_matches成功、get_match拒否）
- detail_state優先度（unavailable+retryはpending優先、availableならjobsよりcanonical優先）
"""

import hashlib, json, sqlite3, sys, tempfile, unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src/python"))

from ikarchive.store import Store, js
from ikarchive.records import RecordReader


class TestRecordReader(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tmp.name) / "database" / "archive.sqlite3"
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        # Store経由で空スキーマの人工DBを作成
        s = Store(self.db_path)
        s.close()

    def tearDown(self):
        self.tmp.cleanup()

    def _insert_match(
        self,
        account: str,
        kind: str,
        match_key: str,
        first_seen: str = "2026-09-01T00:00:00Z",
        last_seen: str = "2026-09-01T00:00:00Z",
        detail_response_id: int = None,
    ):
        conn = sqlite3.connect(self.db_path)
        conn.execute(
            "INSERT INTO matches(account, kind, match_key, first_seen, last_seen, detail_response_id) "
            "VALUES(?,?,?,?,?,?)",
            (account, kind, match_key, first_seen, last_seen, detail_response_id),
        )
        conn.commit()
        conn.close()

    def _insert_job(self, account: str, operation: str, match_key: str, state: str, kind: str):
        conn = sqlite3.connect(self.db_path)
        conn.execute(
            "INSERT INTO jobs(account, operation, variables_json, kind, match_key, state) "
            "VALUES(?,?,?,?,?,?)",
            (account, operation, js({"match_key": match_key}), kind, match_key, state),
        )
        conn.commit()
        conn.close()

    def _insert_canonical_detail(
        self,
        account: str,
        kind: str,
        match_key: str,
        response_id: int,
        raw_body_bytes: bytes,
        doc_json: str,
        genre: str = None,
        analysis_set: str = None,
        rule_raw: str = None,
        operation: str = "VsHistoryDetailQuery",
    ):
        body_sha = hashlib.sha256(raw_body_bytes).hexdigest()
        conn = sqlite3.connect(self.db_path)
        conn.execute("PRAGMA foreign_keys=OFF")  # 人工データ構築用
        conn.execute(
            "INSERT OR IGNORE INTO bodies(sha256, body, byte_length) VALUES(?,?,?)",
            (body_sha, raw_body_bytes, len(raw_body_bytes)),
        )
        conn.execute(
            "INSERT INTO responses(id, event_id, account, fetched_at, operation, variables_json, "
            "query_id, app_version, http_status, headers_json, body_sha256, json_text, projected) "
            "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,1)",
            (
                response_id,
                f"ev-{response_id}",
                account,
                "2026-09-01T01:00:00Z",
                operation,
                js({"id": match_key}),
                "test-query-id",
                "10.0.0",
                200,
                js({"x-secret-auth": "MUST_NOT_BE_RETURNED"}),
                body_sha,
                doc_json,
            ),
        )
        conn.execute(
            "INSERT INTO documents(response_id, account, kind, match_key, json_text) VALUES(?,?,?,?,?)",
            (response_id, account, kind, match_key, doc_json),
        )
        conn.execute(
            "UPDATE matches SET detail_response_id=? WHERE account=? AND kind=? AND match_key=?",
            (response_id, account, kind, match_key),
        )
        if genre is not None:
            conn.execute(
                "INSERT INTO match_classification(account, kind, match_key, genre, roster_class, "
                "analysis_set, classified_at, rule_raw) VALUES(?,?,?,?,?,?,?,?)",
                (
                    account,
                    kind,
                    match_key,
                    genre,
                    "four_vs_four",
                    analysis_set or genre,
                    "2026-09-01T01:00:00Z",
                    rule_raw,
                ),
            )
        conn.commit()
        conn.close()

    def test_list_matches_comprehensiveness_and_states(self):
        """VS/Coop/詳細なしpending/unavailable/unresolved/未分類を母集合から落とさない。"""
        acc = "test-account-1"

        # 1. VS available
        raw_vs = b'{"data":{"vsHistoryDetail":{"playedTime":"2026-09-01T00:10:00Z"}}}'
        self._insert_match(acc, "vs", "vs-key-1")
        self._insert_canonical_detail(
            acc, "vs", "vs-key-1", 101, raw_vs,
            '{"playedTime":"2026-09-01T00:10:00Z"}',
            genre="bankara_open", rule_raw="AREA"
        )

        # 2. Coop available
        raw_coop = b'{"data":{"coopHistoryDetail":{"playedTime":"2026-09-01T00:20:00Z"}}}'
        self._insert_match(acc, "coop", "coop-key-1")
        self._insert_canonical_detail(
            acc, "coop", "coop-key-1", 102, raw_coop,
            '{"playedTime":"2026-09-01T00:20:00Z"}',
            genre="salmon_regular", rule_raw="REGULAR",
            operation="CoopHistoryDetailQuery"
        )

        # 3. VS pending (詳細なし + jobs pending)
        self._insert_match(acc, "vs", "vs-key-pending")
        self._insert_job(acc, "VsHistoryDetailQuery", "vs-key-pending", "pending", "vs")

        # 4. Coop unavailable (詳細なし + jobs unavailable)
        self._insert_match(acc, "coop", "coop-key-unavailable")
        self._insert_job(acc, "CoopHistoryDetailQuery", "coop-key-unavailable", "unavailable", "coop")

        # 5. VS unresolved (詳細なし + jobs なし)
        self._insert_match(acc, "vs", "vs-key-unresolved")

        # 6. Coop 未分類 (詳細なし + jobs なし + classification なし)
        self._insert_match(acc, "coop", "coop-key-unclassified")

        with RecordReader(self.db_path) as reader:
            res = reader.list_matches(acc, limit=100)
            self.assertEqual(res["total"], 6)
            self.assertEqual(len(res["items"]), 6)

            items_by_key = {item["match_key"]: item for item in res["items"]}

            # 順序検証: kind ASC, match_key ASC ('coop' < 'vs')
            expected_keys = sorted(
                items_by_key.keys(),
                key=lambda k: (items_by_key[k]["kind"], items_by_key[k]["match_key"])
            )
            actual_keys = [item["match_key"] for item in res["items"]]
            self.assertEqual(actual_keys, expected_keys)

            # detail_state 検証
            self.assertEqual(items_by_key["vs-key-1"]["detail_state"], "available")
            self.assertEqual(items_by_key["vs-key-1"]["played_time"], "2026-09-01T00:10:00Z")
            self.assertEqual(items_by_key["vs-key-1"]["genre"], "bankara_open")
            self.assertEqual(items_by_key["vs-key-1"]["rule_raw"], "AREA")

            self.assertEqual(items_by_key["coop-key-1"]["detail_state"], "available")
            self.assertEqual(items_by_key["coop-key-1"]["played_time"], "2026-09-01T00:20:00Z")
            self.assertEqual(items_by_key["coop-key-1"]["genre"], "salmon_regular")

            self.assertEqual(items_by_key["vs-key-pending"]["detail_state"], "pending")
            self.assertIsNone(items_by_key["vs-key-pending"]["played_time"])
            self.assertIsNone(items_by_key["vs-key-pending"]["genre"])

            self.assertEqual(items_by_key["coop-key-unavailable"]["detail_state"], "unavailable")
            self.assertIsNone(items_by_key["coop-key-unavailable"]["played_time"])

            self.assertEqual(items_by_key["vs-key-unresolved"]["detail_state"], "unresolved")
            self.assertIsNone(items_by_key["vs-key-unresolved"]["played_time"])

            # 未分類を unknown/hold に推論変換しないこと
            self.assertEqual(items_by_key["coop-key-unclassified"]["detail_state"], "unresolved")
            self.assertIsNone(items_by_key["coop-key-unclassified"]["genre"])
            self.assertIsNone(items_by_key["coop-key-unclassified"]["analysis_set"])
            self.assertIsNone(items_by_key["coop-key-unclassified"]["rule_raw"])

            # played_time は first_seen で代用されていないこと
            self.assertIsNone(items_by_key["coop-key-unclassified"]["played_time"])
            self.assertIsNotNone(items_by_key["coop-key-unclassified"]["first_seen"])

            # kind 絞り込み
            vs_res = reader.list_matches(acc, kind="vs")
            self.assertEqual(vs_res["total"], 3)
            self.assertTrue(all(it["kind"] == "vs" for it in vs_res["items"]))

            coop_res = reader.list_matches(acc, kind="coop")
            self.assertEqual(coop_res["total"], 3)
            self.assertTrue(all(it["kind"] == "coop" for it in coop_res["items"]))

    def test_composite_key_account_and_kind_isolation(self):
        """複合キーaccountとkindの分離。他accountの同じkeyは漏らさない。"""
        acc_a = "user-alice"
        acc_b = "user-bob"
        shared_key = "match-shared-123"

        # Alice: vs match-shared-123, coop match-shared-123
        self._insert_match(acc_a, "vs", shared_key)
        self._insert_match(acc_a, "coop", shared_key)

        # Bob: vs match-shared-123
        self._insert_match(acc_b, "vs", shared_key)

        # Bob のみ詳細あり
        raw_bob = b'{"data":{"vsHistoryDetail":{"playedTime":"2026-09-01T10:00:00Z"}}}'
        self._insert_canonical_detail(
            acc_b, "vs", shared_key, 201, raw_bob,
            '{"playedTime":"2026-09-01T10:00:00Z"}',
            genre="nawabari"
        )

        with RecordReader(self.db_path) as reader:
            # Alice の list_matches に Bob のデータは出ない
            res_a = reader.list_matches(acc_a)
            self.assertEqual(res_a["total"], 2)
            for it in res_a["items"]:
                self.assertEqual(it["account"], acc_a)
                # Alice は詳細なしなので unresolved
                self.assertEqual(it["detail_state"], "unresolved")

            # Alice の get_match('vs', shared_key)
            match_a_vs = reader.get_match(acc_a, "vs", shared_key)
            self.assertIsNotNone(match_a_vs)
            self.assertEqual(match_a_vs["account"], acc_a)
            self.assertEqual(match_a_vs["kind"], "vs")
            self.assertIsNone(match_a_vs["detail_json"])
            self.assertIsNone(match_a_vs["source"])

            # Bob の get_match('vs', shared_key)
            match_b_vs = reader.get_match(acc_b, "vs", shared_key)
            self.assertIsNotNone(match_b_vs)
            self.assertEqual(match_b_vs["account"], acc_b)
            self.assertEqual(match_b_vs["detail_state"], "available")
            self.assertIsNotNone(match_b_vs["source"])

            # Alice に存在しない Bob の kind/key 組み合わせ
            self.assertIsNone(reader.get_match(acc_b, "coop", shared_key))

            # 存在しない key
            self.assertIsNone(reader.get_match(acc_a, "vs", "non-existent-key"))

    def test_pagination_and_invalid_params_safety(self):
        """limit offset全ページが重複漏れなし、悪意文字列/invalidparams安全。"""
        acc = "user-pager"
        total_items = 25
        for i in range(total_items):
            self._insert_match(acc, "vs", f"key-{i:03d}")

        with RecordReader(self.db_path) as reader:
            # ページング検証 (limit=7)
            seen_keys = []
            page_size = 7
            offset = 0
            while True:
                page = reader.list_matches(acc, limit=page_size, offset=offset)
                self.assertEqual(page["total"], total_items)
                self.assertEqual(page["limit"], page_size)
                self.assertEqual(page["offset"], offset)
                items = page["items"]
                if not items:
                    break
                seen_keys.extend([it["match_key"] for it in items])
                offset += page_size

            self.assertEqual(len(seen_keys), total_items)
            self.assertEqual(len(set(seen_keys)), total_items)  # 重複なし
            expected_keys = [f"key-{i:03d}" for i in range(total_items)]
            self.assertEqual(seen_keys, expected_keys)  # 漏れなし・順序一致

            # 不正パラメータの検証 (ValueError)
            # account
            with self.assertRaises(ValueError):
                reader.list_matches("")
            with self.assertRaises(ValueError):
                reader.list_matches(None)
            with self.assertRaises(ValueError):
                reader.list_matches(123)

            # kind
            with self.assertRaises(ValueError):
                reader.list_matches(acc, kind="salmon")
            with self.assertRaises(ValueError):
                reader.list_matches(acc, kind=123)

            # limit (bool は int のサブクラスだが拒否)
            with self.assertRaises(ValueError):
                reader.list_matches(acc, limit=0)
            with self.assertRaises(ValueError):
                reader.list_matches(acc, limit=201)
            with self.assertRaises(ValueError):
                reader.list_matches(acc, limit=-5)
            with self.assertRaises(ValueError):
                reader.list_matches(acc, limit="50")
            with self.assertRaises(ValueError):
                reader.list_matches(acc, limit=True)
            with self.assertRaises(ValueError):
                reader.list_matches(acc, limit=False)

            # offset (bool は int のサブクラスだが拒否)
            with self.assertRaises(ValueError):
                reader.list_matches(acc, offset=-1)
            with self.assertRaises(ValueError):
                reader.list_matches(acc, offset="0")
            with self.assertRaises(ValueError):
                reader.list_matches(acc, offset=True)
            with self.assertRaises(ValueError):
                reader.list_matches(acc, offset=False)

            # get_match 不正引数
            with self.assertRaises(ValueError):
                reader.get_match("", "vs", "k1")
            with self.assertRaises(ValueError):
                reader.get_match(acc, "invalid_kind", "k1")
            with self.assertRaises(ValueError):
                reader.get_match(acc, None, "k1")
            with self.assertRaises(ValueError):
                reader.get_match(acc, "vs", "")
            with self.assertRaises(ValueError):
                reader.get_match(acc, "vs", None)

            # 悪意ある文字列（SQLインジェクション安全）
            res_injection = reader.list_matches("'; DROP TABLE matches; --")
            self.assertEqual(res_injection["total"], 0)
            self.assertEqual(len(res_injection["items"]), 0)

            res_or = reader.list_matches("' OR 1=1 --")
            self.assertEqual(res_or["total"], 0)

            # テーブルがドロップされず健在であることを確認
            res_after = reader.list_matches(acc, limit=1)
            self.assertEqual(res_after["total"], total_items)

            match_inj = reader.get_match("acc' OR 1=1 --", "vs", "key' OR 1=1 --")
            self.assertIsNone(match_inj)

    def test_canonical_detail_not_overwritten_by_partial_responses(self):
        """canonical部分応答が上書きしない。"""
        acc = "user-canonical"
        key = "vs-match-1"
        self._insert_match(acc, "vs", key)

        # 1. 完全詳細を canonical として登録
        canonical_raw = b'{"data":{"vsHistoryDetail":{"id":"vs-match-1","playedTime":"2026-09-01T12:00:00Z","canonical":true}}}'
        self._insert_canonical_detail(
            acc, "vs", key, 301, canonical_raw,
            '{"id":"vs-match-1","playedTime":"2026-09-01T12:00:00Z","canonical":true}',
            genre="bankara_challenge"
        )

        # 2. pager や一覧などの部分応答（VsHistoryDetailPagerRefetchQuery など）が responses / sightings に追加される
        conn = sqlite3.connect(self.db_path)
        partial_raw = b'{"data":{"vsHistoryDetail":{"id":"vs-match-1","partialPager":true}}}'
        partial_sha = hashlib.sha256(partial_raw).hexdigest()
        conn.execute("INSERT OR IGNORE INTO bodies VALUES(?,?,?)", (partial_sha, partial_raw, len(partial_raw)))
        conn.execute(
            "INSERT INTO responses(id, event_id, account, fetched_at, operation, variables_json, "
            "headers_json, body_sha256, json_text, projected) "
            "VALUES(302, 'ev-302', ?, '2026-09-01T12:05:00Z', 'VsHistoryDetailPagerRefetchQuery', "
            "'{}', '{}', ?, ?, 1)",
            (acc, partial_sha, partial_raw.decode("utf8")),
        )
        conn.execute(
            "INSERT INTO sightings(response_id, account, kind, match_key, path, summary_json) "
            "VALUES(302, ?, 'vs', ?, '$.history', '{}')",
            (acc, key),
        )
        conn.commit()
        conn.close()

        with RecordReader(self.db_path) as reader:
            # list_matches の検証
            res = reader.list_matches(acc)
            self.assertEqual(res["total"], 1)
            item = res["items"][0]
            self.assertEqual(item["detail_response_id"], 301)  # 302に上書きされていない
            self.assertEqual(item["detail_state"], "available")
            self.assertEqual(item["played_time"], "2026-09-01T12:00:00Z")

            # get_match の検証
            match = reader.get_match(acc, "vs", key)
            self.assertIsNotNone(match)
            self.assertEqual(match["detail_response_id"], 301)
            self.assertIn('"canonical":true', match["detail_json"])
            self.assertNotIn("partialPager", match["detail_json"])
            self.assertEqual(match["source"]["response_id"], 301)
            self.assertEqual(match["source"]["operation"], "VsHistoryDetailQuery")
            self.assertEqual(match["source"]["body_bytes"], canonical_raw)

    def test_raw_body_bytes_identity_and_detail_json_distinction(self):
        """raw body特殊数値表記/空配列/未知キーをbyte同一で返す（detail_jsonと区別）。"""
        acc = "user-raw"
        key = "vs-match-special"
        self._insert_match(acc, "vs", key)

        # 特殊な数値表記（巨大整数、末尾ゼロ保持の浮動小数点表現）、空配列、未知キーを含む生バイト列
        raw_body = (
            b'{"data":{"vsHistoryDetail":{"id":"vs-match-special",'
            b'"huge_int":9999999999999999999999999999999999999999,'
            b'"special_float":1.200000000000000000,'
            b'"empty_arr":[],'
            b'"unknown_nested":{"custom_flag":true},'
            b'"playedTime":"2026-09-01T15:00:00Z"}}}'
        )
        # detail_json は documents に格納された派生JSON表現
        derived_doc = json.dumps({
            "id": "vs-match-special",
            "playedTime": "2026-09-01T15:00:00Z",
            "empty_arr": [],
        })

        self._insert_canonical_detail(
            acc, "vs", key, 401, raw_body, derived_doc,
            genre="xmatch"
        )

        with RecordReader(self.db_path) as reader:
            match = reader.get_match(acc, "vs", key)
            self.assertIsNotNone(match)

            # detail_json は派生JSON文字列
            self.assertEqual(match["detail_json"], derived_doc)

            # source['body_bytes'] はバイト列そのままで1字も再シリアライズされていない
            source = match["source"]
            self.assertIsNotNone(source)
            self.assertIsInstance(source["body_bytes"], bytes)
            self.assertEqual(source["body_bytes"], raw_body)

            # raw_body 内の特殊表記が byte 同一で存在することを確認
            self.assertIn(b"9999999999999999999999999999999999999999", source["body_bytes"])
            self.assertIn(b"1.200000000000000000", source["body_bytes"])
            self.assertIn(b'"empty_arr":[]', source["body_bytes"])
            self.assertIn(b'"unknown_nested":{"custom_flag":true}', source["body_bytes"])

            # 認証情報やヘッダーが source に含まれていないこと
            self.assertNotIn("headers_json", source)
            self.assertNotIn("x-secret-auth", str(source))
            self.assertNotIn("headers", source)

            # source の必須フィールド
            expected_fields = {
                "response_id", "operation", "fetched_at", "query_id",
                "app_version", "http_status", "body_sha256", "body_bytes"
            }
            self.assertEqual(set(source.keys()), expected_fields)

    def test_corrupt_record_raises_runtime_error(self):
        """元response/BLOB不在なら破損を隠さずRuntimeError（詳細なしは通常None）。"""
        acc = "user-corrupt"

        # Case 1: detail_response_id があるが documents レコードがない
        self._insert_match(acc, "vs", "key-missing-doc", detail_response_id=901)
        # Case 2: documents はあるが responses レコードがない
        self._insert_match(acc, "vs", "key-missing-resp", detail_response_id=902)
        # Case 3: responses はあるが bodies レコードがない
        self._insert_match(acc, "vs", "key-missing-body", detail_response_id=903)

        conn = sqlite3.connect(self.db_path)
        conn.execute("PRAGMA foreign_keys=OFF")
        conn.execute(
            "INSERT INTO documents(response_id, account, kind, match_key, json_text) VALUES(902, ?, 'vs', 'key-missing-resp', '{}')",
            (acc,),
        )
        conn.execute(
            "INSERT INTO documents(response_id, account, kind, match_key, json_text) VALUES(903, ?, 'vs', 'key-missing-body', '{}')",
            (acc,),
        )
        conn.execute(
            "INSERT INTO responses(id, event_id, account, fetched_at, operation, variables_json, headers_json, body_sha256, projected) "
            "VALUES(903, 'ev-903', ?, '2026-09-01T00:00:00Z', 'VsHistoryDetailQuery', '{}', '{}', 'missing-sha', 1)",
            (acc,),
        )
        conn.commit()
        conn.close()

        with RecordReader(self.db_path) as reader:
            with self.assertRaises(RuntimeError) as cm1:
                reader.get_match(acc, "vs", "key-missing-doc")
            self.assertIn("canonical document is missing", str(cm1.exception))

            with self.assertRaises(RuntimeError) as cm2:
                reader.get_match(acc, "vs", "key-missing-resp")
            self.assertIn("response 902 not found", str(cm2.exception))

            with self.assertRaises(RuntimeError) as cm3:
                reader.get_match(acc, "vs", "key-missing-body")
            self.assertIn("body BLOB missing", str(cm3.exception))

    def test_lifecycle_readonly_and_no_creation(self):
        """context終了close、存在しないDBを作らない、READONLYで書込不可。"""
        # 1. context 外でのメソッド利用 -> RuntimeError (引数不正でもcontextチェックが先行)
        reader = RecordReader(self.db_path)
        with self.assertRaises(RuntimeError):
            reader.list_matches("some-account")
        with self.assertRaises(RuntimeError):
            reader.list_matches("")
        with self.assertRaises(RuntimeError):
            reader.get_match("some-account", "vs", "some-key")

        # 2. 多重 enter の禁止 -> RuntimeError
        with reader:
            with self.assertRaises(RuntimeError):
                with reader:
                    pass

        # 3. context 終了後のメソッド利用 -> RuntimeError
        with self.assertRaises(RuntimeError):
            reader.list_matches("some-account")
        with self.assertRaises(RuntimeError):
            reader.get_match("some-account", "vs", "some-key")

        # 4. 存在しないDBパスを指定した場合、ファイルを作らず FileNotFoundError
        non_existent_path = Path(self.tmp.name) / "no_such_dir" / "never_created.sqlite3"
        self.assertFalse(non_existent_path.exists())
        bad_reader = RecordReader(non_existent_path)
        with self.assertRaises(FileNotFoundError):
            with bad_reader:
                pass
        self.assertFalse(non_existent_path.exists())  # DBが作成されていないこと

        # 5. READONLY で書込不可
        with RecordReader(self.db_path) as active_reader:
            with self.assertRaises(sqlite3.OperationalError):
                active_reader._store.db.execute("INSERT INTO matches VALUES('x','vs','k','t','t',NULL)")

    def test_wal_snapshot_isolation_during_active_context(self):
        """WAL人工DBをRecordReaderで最初にlistし、そのcontextを開いたまま別writerでmatchを追加。
        readerの次ページ/totalは元snapshotのまま、新contextだけ追加行を見られること。
        """
        acc = "user-wal"
        # 初期データとして2試合を登録
        self._insert_match(acc, "vs", "match-01")
        self._insert_match(acc, "vs", "match-02")

        with RecordReader(self.db_path) as reader1:
            # 最初に list を実行して deferred トランザクションを開始し、WAL read snapshot を固定
            page1 = reader1.list_matches(acc, limit=1, offset=0)
            self.assertEqual(page1["total"], 2)
            self.assertEqual(len(page1["items"]), 1)
            self.assertEqual(page1["items"][0]["match_key"], "match-01")

            # reader1 の context を開いたまま、別 writer コネクションで match を追加
            self._insert_match(acc, "vs", "match-03")

            # reader1 の次ページ取得: 元の snapshot のまま
            page2 = reader1.list_matches(acc, limit=1, offset=1)
            self.assertEqual(page2["total"], 2)  # 新規追加の match-03 は反映されず 2 のまま
            self.assertEqual(len(page2["items"]), 1)
            self.assertEqual(page2["items"][0]["match_key"], "match-02")

            # 範囲外の offset では空リスト、total も元のまま
            page3 = reader1.list_matches(acc, limit=1, offset=2)
            self.assertEqual(page3["total"], 2)
            self.assertEqual(len(page3["items"]), 0)

        # 新しい context を開くと、追加された match-03 を含む最新行が見られる
        with RecordReader(self.db_path) as reader2:
            all_page = reader2.list_matches(acc, limit=10, offset=0)
            self.assertEqual(all_page["total"], 3)
            self.assertEqual(len(all_page["items"]), 3)
            keys = [it["match_key"] for it in all_page["items"]]
            self.assertEqual(keys, ["match-01", "match-02", "match-03"])

    def test_context_exception_closes_connection(self):
        """context内例外でも実connectionがcloseされexecuteがProgrammingErrorとなること。"""
        reader = RecordReader(self.db_path)
        raw_db_conn = None

        class IntentionalError(Exception):
            pass

        with self.assertRaises(IntentionalError):
            with reader:
                # 実 connection を保持
                raw_db_conn = reader._store.db
                # context 内では正常に execute 可能
                self.assertIsNotNone(raw_db_conn.execute("SELECT 1").fetchone())
                raise IntentionalError("error inside context")

        # context 終了後、reader 内部状態がリセットされていること
        self.assertFalse(reader._entered)
        self.assertIsNone(reader._store)

        # 実 connection が close されており、execute すると ProgrammingError になること
        self.assertIsNotNone(raw_db_conn)
        with self.assertRaises(sqlite3.ProgrammingError) as cm:
            raw_db_conn.execute("SELECT 1")
        self.assertIn("closed", str(cm.exception).lower())

    def test_list_matches_does_not_read_bodies_negative_control(self):
        """SQLite set_authorizerでbodiesテーブルのREADを拒否してlist_matches成功、get_matchは拒否される負の対照。
        一覧が原文BLOBを読まない実証。
        """
        acc = "user-authorizer"
        key = "match-auth-1"
        raw_body = b'{"data":{"vsHistoryDetail":{"id":"match-auth-1","playedTime":"2026-09-01T08:00:00Z"}}}'
        self._insert_match(acc, "vs", key)
        self._insert_canonical_detail(
            acc, "vs", key, 501, raw_body,
            '{"id":"match-auth-1","playedTime":"2026-09-01T08:00:00Z"}',
            genre="nawabari"
        )

        with RecordReader(self.db_path) as reader:
            # bodies テーブルからの READ を明示的に拒否する authorizer を登録
            def authorizer(action, arg1, arg2, dbname, source):
                if action == sqlite3.SQLITE_READ and arg1 == "bodies":
                    return sqlite3.SQLITE_DENY
                return sqlite3.SQLITE_OK

            reader._store.db.set_authorizer(authorizer)

            # list_matches は bodies テーブルを読まないため成功する
            res = reader.list_matches(acc)
            self.assertEqual(res["total"], 1)
            self.assertEqual(len(res["items"]), 1)
            self.assertEqual(res["items"][0]["match_key"], key)
            self.assertEqual(res["items"][0]["detail_state"], "available")

            # get_match は canonical detail の source 取得で bodies テーブルを読むため、拒否される（負の対照）
            with self.assertRaises(sqlite3.DatabaseError):
                reader.get_match(acc, "vs", key)

    def test_detail_state_precedence_pending_and_canonical(self):
        """同じ試合にunavailable jobと別operationのretry jobがあればpending優先、availableならjobsよりcanonical優先。"""
        acc = "user-precedence"

        # 1. unavailable job と別 operation の retry job がある場合 -> pending 優先
        key_pending_prio = "match-pending-prio"
        self._insert_match(acc, "vs", key_pending_prio)
        self._insert_job(acc, "VsHistoryDetailQuery", key_pending_prio, "unavailable", "vs")
        self._insert_job(acc, "VsHistoryDetailPagerRefetchQuery", key_pending_prio, "retry", "vs")

        # 2. canonical detail がある場合 -> jobs (pending / unavailable) より canonical (available) 優先
        key_canon_prio = "match-canon-prio"
        raw_body = b'{"data":{"vsHistoryDetail":{"id":"match-canon-prio","playedTime":"2026-09-01T09:00:00Z"}}}'
        self._insert_match(acc, "vs", key_canon_prio)
        self._insert_canonical_detail(
            acc, "vs", key_canon_prio, 601, raw_body,
            '{"id":"match-canon-prio","playedTime":"2026-09-01T09:00:00Z"}',
            genre="bankara_open"
        )
        # 同じ試合に pending および unavailable のジョブを登録
        self._insert_job(acc, "VsHistoryDetailQuery", key_canon_prio, "pending", "vs")
        self._insert_job(acc, "VsHistoryDetailPagerRefetchQuery", key_canon_prio, "unavailable", "vs")

        with RecordReader(self.db_path) as reader:
            res = reader.list_matches(acc)
            self.assertEqual(res["total"], 2)
            items_by_key = {it["match_key"]: it for it in res["items"]}

            # 1 の検証: unavailable と retry が混在する場合、pending が優先される
            self.assertEqual(items_by_key[key_pending_prio]["detail_state"], "pending")
            match_pending = reader.get_match(acc, "vs", key_pending_prio)
            self.assertIsNotNone(match_pending)
            self.assertEqual(match_pending["detail_state"], "pending")
            self.assertIsNone(match_pending["detail_json"])

            # 2 の検証: jobs に pending/unavailable があっても、canonical detail があれば available が最優先
            self.assertEqual(items_by_key[key_canon_prio]["detail_state"], "available")
            match_canon = reader.get_match(acc, "vs", key_canon_prio)
            self.assertIsNotNone(match_canon)
            self.assertEqual(match_canon["detail_state"], "available")
            self.assertIsNotNone(match_canon["detail_json"])
            self.assertEqual(match_canon["source"]["response_id"], 601)
            self.assertEqual(match_canon["source"]["body_bytes"], raw_body)

    def test_list_filters_keep_unfiltered_population_and_use_played_time_only(self):
        """絞り込み無しは全件。期間は playedTime だけ。分類なしは分類行が無い試合。"""
        acc = "filter-account"
        other = "other-account"
        wakaba = (
            '{"playedTime":"2026-09-01T00:10:00Z","vsStage":{"name":"ユノハナ大渓谷"},'
            '"myTeam":{"players":[{"isMyself":true,"weapon":{"name":"わかばシューター"}}]},'
            '"otherTeams":[{"players":[{"isMyself":false,"weapon":{"name":"リッター4K"}}]}]}'
        )
        later = '{"playedTime":"2026-09-01T00:20:00Z","coopStage":{"name":"アラマキ砦"}}'
        self._insert_match(acc, "vs", "open-area", first_seen="2026-08-01T00:00:00Z")
        self._insert_canonical_detail(
            acc, "vs", "open-area", 701, wakaba.encode(), wakaba,
            genre="bankara_open", rule_raw="AREA",
        )
        self._insert_match(acc, "coop", "salmon-1", first_seen="2026-09-02T00:00:00Z")
        self._insert_canonical_detail(
            acc, "coop", "salmon-1", 702, later.encode(), later,
            genre="salmon_regular", rule_raw="REGULAR",
            operation="CoopHistoryDetailQuery",
        )
        self._insert_match(acc, "vs", "pending-old", first_seen="2026-07-01T00:00:00Z")
        self._insert_job(acc, "VsHistoryDetailQuery", "pending-old", "pending", "vs")
        self._insert_match(other, "vs", "open-area")
        self._insert_canonical_detail(
            other, "vs", "open-area", 703, wakaba.encode(), wakaba,
            genre="bankara_open", rule_raw="AREA",
        )
        conn = sqlite3.connect(self.db_path)
        conn.execute(
            "INSERT INTO match_tags(account, match_key, tag, note, created_at, updated_at) "
            "VALUES(?,?,?,?,?,?)",
            (acc, "open-area", "下げラン", None, "2026-09-01T02:00:00Z", "2026-09-01T02:00:00Z"),
        )
        conn.commit()
        conn.close()

        with RecordReader(self.db_path) as reader:
            everything = reader.list_matches(acc, limit=100)
            self.assertEqual(everything["total"], 3)
            self.assertEqual(
                [item["match_key"] for item in everything["items"]],
                ["salmon-1", "open-area", "pending-old"],
            )

            by_rule = reader.list_matches(acc, rule_raw="AREA", limit=100)
            self.assertEqual([item["match_key"] for item in by_rule["items"]], ["open-area"])

            by_mode = reader.list_matches(acc, analysis_set="salmon_regular", limit=100)
            self.assertEqual([item["match_key"] for item in by_mode["items"]], ["salmon-1"])

            unclassified = reader.list_matches(acc, analysis_set="unclassified", limit=100)
            self.assertEqual([item["match_key"] for item in unclassified["items"]], ["pending-old"])
            self.assertIsNone(unclassified["items"][0]["played_time"])
            self.assertEqual(unclassified["items"][0]["first_seen"], "2026-07-01T00:00:00Z")

            period = reader.list_matches(
                acc, played_from="2026-09-01T00:15:00Z", played_to="2026-09-01T00:30:00Z", limit=100,
            )
            self.assertEqual([item["match_key"] for item in period["items"]], ["salmon-1"])

            weapon = reader.list_matches(acc, weapon="わかばシューター", limit=100)
            self.assertEqual([item["match_key"] for item in weapon["items"]], ["open-area"])
            self.assertEqual(reader.list_matches(acc, weapon="リッター4K", limit=100)["total"], 0)

            tagged = reader.list_matches(acc, tag="下げラン", limit=100)
            self.assertEqual([item["match_key"] for item in tagged["items"]], ["open-area"])

            found = reader.list_matches(acc, query="ユノハナ", limit=100)
            self.assertEqual([item["match_key"] for item in found["items"]], ["open-area"])
            self.assertEqual(reader.list_matches(acc, query="2026-07-01", limit=100)["total"], 0)

            facets = reader.list_facets(acc)
            self.assertIn("bankara_open", facets["analysis_sets"])
            self.assertIn("unclassified", facets["analysis_sets"])
            self.assertEqual(facets["rules"], ["AREA", "REGULAR"])
            self.assertEqual(facets["weapons"], ["わかばシューター"])
            self.assertEqual(facets["tags"], ["下げラン"])
            self.assertEqual(reader.list_facets(other)["tags"], [])

            def authorizer(action, arg1, arg2, dbname, source):
                if action == sqlite3.SQLITE_READ and arg1 == "bodies":
                    return sqlite3.SQLITE_DENY
                return sqlite3.SQLITE_OK

            reader._store.db.set_authorizer(authorizer)
            filtered = reader.list_matches(acc, query="わかば", weapon="わかばシューター", limit=100)
            self.assertEqual(filtered["total"], 1)

            for kwargs in (
                {"analysis_set": "not a mode"},
                {"rule_raw": "ガチエリア"},
                {"played_from": "2026-09-01"},
                {"played_from": "2026-09-01T00:30:00Z", "played_to": "2026-09-01T00:10:00Z"},
                {"weapon": "わかば\nシューター"},
                {"query": ""},
            ):
                with self.assertRaises(ValueError):
                    reader.list_matches(acc, **kwargs)

    def test_tag_summary_keeps_open_and_private_populations_separate(self):
        """タグ件数はオープンとプラベの人数区分だけ。合計も他区分も混ぜない。"""
        from ikarchive.records import TAG_SUMMARY_SETS
        acc = "summary-account"
        other = "summary-other"
        doc = '{"playedTime":"2026-09-01T00:10:00Z"}'
        self._insert_match(acc, "vs", "open-tagged")
        self._insert_canonical_detail(
            acc, "vs", "open-tagged", 801, doc.encode(), doc,
            genre="bankara_open", analysis_set="bankara_open", rule_raw="AREA",
        )
        self._insert_match(acc, "vs", "open-plain")
        self._insert_canonical_detail(
            acc, "vs", "open-plain", 802, doc.encode(), doc,
            genre="bankara_open", analysis_set="bankara_open", rule_raw="AREA",
        )
        self._insert_match(acc, "vs", "private-two")
        self._insert_canonical_detail(
            acc, "vs", "private-two", 803, doc.encode(), doc,
            genre="private", analysis_set="private_two_vs_two", rule_raw="AREA",
        )
        self._insert_match(acc, "vs", "x-tagged")
        self._insert_canonical_detail(
            acc, "vs", "x-tagged", 804, doc.encode(), doc,
            genre="xmatch", analysis_set="xmatch", rule_raw="AREA",
        )
        self._insert_match(other, "vs", "open-other")
        self._insert_canonical_detail(
            other, "vs", "open-other", 805, doc.encode(), doc,
            genre="bankara_open", analysis_set="bankara_open", rule_raw="AREA",
        )
        conn = sqlite3.connect(self.db_path)
        conn.executemany(
            "INSERT INTO match_tags(account, match_key, tag, note, created_at, updated_at) "
            "VALUES(?,?,?,?,?,?)",
            [
                (acc, "open-tagged", "下げラン", None, "2026-09-01T02:00:00Z", "2026-09-01T02:00:00Z"),
                (acc, "open-tagged", "ガチ", None, "2026-09-01T02:00:00Z", "2026-09-01T02:00:00Z"),
                (acc, "private-two", "エンジョイ", None, "2026-09-01T02:00:00Z", "2026-09-01T02:00:00Z"),
                (acc, "x-tagged", "対象外", None, "2026-09-01T02:00:00Z", "2026-09-01T02:00:00Z"),
                (other, "open-other", "下げラン", None, "2026-09-01T02:00:00Z", "2026-09-01T02:00:00Z"),
            ],
        )
        conn.commit()
        conn.close()

        with RecordReader(self.db_path) as reader:
            summary = reader.tag_summary(acc)
            self.assertEqual(set(summary), {"sets"})
            self.assertEqual(
                [item["analysis_set"] for item in summary["sets"]],
                list(TAG_SUMMARY_SETS),
            )
            by_set = {item["analysis_set"]: item for item in summary["sets"]}
            self.assertEqual(by_set["bankara_open"]["matches"], 2)
            self.assertEqual(by_set["bankara_open"]["untagged"], 1)
            self.assertEqual(
                by_set["bankara_open"]["tags"],
                [{"tag": "ガチ", "matches": 1}, {"tag": "下げラン", "matches": 1}],
            )
            self.assertEqual(by_set["private_two_vs_two"]["matches"], 1)
            self.assertEqual(by_set["private_two_vs_two"]["untagged"], 0)
            self.assertEqual(by_set["private_two_vs_two"]["tags"], [{"tag": "エンジョイ", "matches": 1}])
            self.assertEqual(by_set["private_four_vs_four"]["matches"], 0)
            self.assertEqual(by_set["private_four_vs_four"]["untagged"], 0)
            self.assertEqual(by_set["private_four_vs_four"]["tags"], [])
            shown = [tag["tag"] for item in summary["sets"] for tag in item["tags"]]
            self.assertNotIn("対象外", shown)
            self.assertNotIn("イカップル", shown)
            other_summary = reader.tag_summary(other)
            other_open = next(item for item in other_summary["sets"] if item["analysis_set"] == "bankara_open")
            self.assertEqual(other_open["matches"], 1)
            self.assertEqual(other_open["tags"], [{"tag": "下げラン", "matches": 1}])

            def authorizer(action, arg1, arg2, dbname, source):
                if action == sqlite3.SQLITE_READ and arg1 == "bodies":
                    return sqlite3.SQLITE_DENY
                return sqlite3.SQLITE_OK

            reader._store.db.set_authorizer(authorizer)
            self.assertEqual(reader.tag_summary(acc)["sets"][0]["matches"], 2)
            with self.assertRaises(ValueError):
                reader.tag_summary("")

    def test_rule_results_and_rates_stay_inside_one_population(self):
        """勝敗とレートは区分とルールをまたいで足さない。"""
        from ikarchive.records import JUDGEMENT_SETS
        acc = "results-account"
        other = "results-other"
        rows = [
            (acc, "open-win", 901, "bankara_open", "bankara_open", "AREA", "WIN"),
            (acc, "open-lose", 902, "bankara_open", "bankara_open", "AREA", "LOSE"),
            (acc, "open-exempt", 903, "bankara_open", "bankara_open", "AREA", "EXEMPTED_LOSE"),
            (acc, "open-draw", 904, "bankara_open", "bankara_open", "LOFT", "DRAW"),
            (acc, "turf-win", 905, "nawabari", "nawabari", "TURF_WAR", "WIN"),
            (acc, "x-win", 906, "xmatch", "xmatch", "AREA", "WIN"),
            (acc, "pair-win", 907, "private", "private_two_vs_two", "AREA", "WIN"),
            (other, "open-other", 908, "bankara_open", "bankara_open", "AREA", "WIN"),
        ]
        for account, key, response_id, genre, analysis_set, rule_raw, judgement in rows:
            doc = '{"playedTime":"2026-09-01T00:10:00Z","judgement":"%s"}' % judgement
            self._insert_match(account, "vs", key)
            self._insert_canonical_detail(
                account, "vs", key, response_id, doc.encode(), doc,
                genre=genre, analysis_set=analysis_set, rule_raw=rule_raw,
            )
        salmon = '{"playedTime":"2026-09-01T00:10:00Z","dangerRate":0.2}'
        self._insert_match(acc, "coop", "salmon-one")
        self._insert_canonical_detail(
            acc, "coop", "salmon-one", 909, salmon.encode(), salmon,
            genre="salmon_regular", analysis_set="salmon_regular", rule_raw="REGULAR",
            operation="CoopHistoryDetailQuery",
        )
        conn = sqlite3.connect(self.db_path)
        conn.executemany(
            """INSERT INTO rate_points(
                   account, series_id, label, genre, rule_raw, match_key,
                   played_time, value, source, priority
               ) VALUES(?,?,?,?,?,?,?,?,?,?)""",
            [
                (acc, "bankara_open|AREA|bankaraPower", "バンカラパワー", "bankara_open", "AREA",
                 "open-late", "2026-09-02T00:00:00Z", 2100, "api", "primary"),
                (acc, "bankara_open|AREA|bankaraPower", "バンカラパワー", "bankara_open", "AREA",
                 "open-early", "2026-09-01T00:00:00Z", 2000, "api", "primary"),
                (acc, "bankara_open|LOFT|bankaraPower", "バンカラパワー", "bankara_open", "LOFT",
                 "open-loft", "2026-09-01T00:00:00Z", 1500, "api", "primary"),
                (acc, "salmon_regular|REGULAR|dangerRate", "キケン度", "salmon_regular", "REGULAR",
                 "salmon-early", "2026-09-01T00:00:00Z", 0.2, "api", "secondary"),
                (acc, "salmon_regular|REGULAR|dangerRate", "キケン度", "salmon_regular", "REGULAR",
                 "salmon-late", "2026-09-02T00:00:00Z", 0.25, "api", "secondary"),
                (acc, "nawabari|TURF_WAR|vibes", "チョーシ", "nawabari", "TURF_WAR",
                 "fetch:vibes", "2026-09-03T00:00:00Z", 4, "api_snapshot", "primary"),
                (acc, "nawabari|TURF_WAR|streak", "連勝", "nawabari", "TURF_WAR",
                 "streak-1", "2026-09-01T00:00:00Z", 3, "derived_judgement", "primary"),
                (acc, "bankara_open|AREA|earnedUdemaePoint", "ウデマエポイント", "bankara_open", "AREA",
                 "udemae-1", "2026-09-01T00:00:00Z", 8, "api", "primary"),
                (acc, "bankara_open|AREA|pointDelta", "ウデマエポイント増減", "bankara_open", "AREA",
                 "delta-1", "2026-09-01T00:00:00Z", 8, "api", "primary"),
                (acc, "bankara_open|AREA|bankaraPower", "バンカラパワー", "bankara_open", "AREA",
                 "open-blank", "2026-09-03T00:00:00Z", None, "api", "primary"),
                (other, "bankara_open|AREA|bankaraPower", "バンカラパワー", "bankara_open", "AREA",
                 "other-power", "2026-09-02T00:00:00Z", 9999, "api", "primary"),
            ],
        )
        conn.commit()
        conn.close()

        with RecordReader(self.db_path) as reader:
            results = reader.rule_results(acc)
            self.assertEqual(set(results), {"sets"})
            self.assertEqual(
                [item["analysis_set"] for item in results["sets"]],
                list(JUDGEMENT_SETS),
            )
            self.assertNotIn("salmon_regular", [item["analysis_set"] for item in results["sets"]])
            self.assertNotIn("hold", [item["analysis_set"] for item in results["sets"]])
            by_set = {item["analysis_set"]: item for item in results["sets"]}
            open_rules = {item["rule_raw"]: item for item in by_set["bankara_open"]["rules"]}
            self.assertEqual(list(open_rules), ["AREA", "LOFT"])
            self.assertEqual(open_rules["AREA"]["matches"], 3)
            self.assertEqual(open_rules["AREA"]["wins"], 1)
            self.assertEqual(open_rules["AREA"]["losses"], 1)
            self.assertEqual(open_rules["AREA"]["draws"], 0)
            self.assertEqual(open_rules["AREA"]["other"], 1)
            self.assertEqual(open_rules["LOFT"]["draws"], 1)
            self.assertEqual(open_rules["LOFT"]["matches"], 1)
            self.assertEqual(by_set["nawabari"]["rules"][0]["wins"], 1)
            self.assertEqual(by_set["xmatch"]["rules"][0]["wins"], 1)
            self.assertEqual(by_set["private_two_vs_two"]["rules"][0]["rule_raw"], "AREA")
            self.assertEqual(by_set["private_two_vs_two"]["rules"][0]["wins"], 1)
            self.assertEqual(by_set["private_four_vs_four"]["rules"], [])
            self.assertNotIn("イカップル", json.dumps(results, ensure_ascii=False))
            other_results = reader.rule_results(other)
            other_open = next(item for item in other_results["sets"] if item["analysis_set"] == "bankara_open")
            self.assertEqual(other_open["rules"][0]["wins"], 1)
            self.assertEqual(other_open["rules"][0]["matches"], 1)

            rates = reader.rate_summary(acc)
            self.assertEqual(set(rates), {"series"})
            shown = [(item["label"], item["rule_raw"]) for item in rates["series"]]
            self.assertEqual(shown, [
                ("チョーシ", "TURF_WAR"),
                ("バンカラパワー", "AREA"),
                ("バンカラパワー", "LOFT"),
                ("キケン度", "REGULAR"),
            ])
            by_label = {(item["label"], item["rule_raw"]): item for item in rates["series"]}
            power = by_label[("バンカラパワー", "AREA")]
            self.assertEqual(power["count"], 2)
            self.assertEqual(power["latest"], 2100.0)
            self.assertEqual(power["previous"], 2000.0)
            self.assertEqual(power["delta"], 100.0)
            self.assertEqual(power["minimum"], 2000.0)
            self.assertEqual(power["maximum"], 2100.0)
            self.assertEqual(power["unit"], "number")
            self.assertEqual(power["source"], "api")
            self.assertNotEqual(power["latest"], 9999.0)
            self.assertEqual(
                [(point["played_time"], point["value"]) for point in power["points"]],
                [
                    ("2026-09-01T00:00:00Z", 2000.0),
                    ("2026-09-02T00:00:00Z", 2100.0),
                ],
            )
            danger = by_label[("キケン度", "REGULAR")]
            self.assertEqual(danger["unit"], "ratio")
            self.assertEqual(danger["latest"], 0.25)
            self.assertEqual(danger["previous"], 0.2)
            self.assertAlmostEqual(danger["delta"], 0.05)
            self.assertEqual(danger["priority"], "secondary")
            self.assertEqual(
                [(point["played_time"], point["value"]) for point in danger["points"]],
                [
                    ("2026-09-01T00:00:00Z", 0.2),
                    ("2026-09-02T00:00:00Z", 0.25),
                ],
            )
            vibe = by_label[("チョーシ", "TURF_WAR")]
            self.assertEqual(vibe["source"], "api_snapshot")
            self.assertEqual(vibe["count"], 1)
            self.assertIsNone(vibe["previous"])
            self.assertIsNone(vibe["delta"])
            self.assertEqual(vibe["points"], [
                {"played_time": "2026-09-03T00:00:00Z", "value": 4.0},
            ])
            dumped = json.dumps(rates, ensure_ascii=False)
            self.assertNotIn("連勝", dumped)
            self.assertNotIn("ウデマエ", dumped)
            self.assertNotIn(other, dumped)
            other_rates = reader.rate_summary(other)
            self.assertEqual(len(other_rates["series"]), 1)
            self.assertEqual(other_rates["series"][0]["latest"], 9999.0)

            def authorizer(action, arg1, arg2, dbname, source):
                if action == sqlite3.SQLITE_READ and arg1 == "bodies":
                    return sqlite3.SQLITE_DENY
                return sqlite3.SQLITE_OK

            reader._store.db.set_authorizer(authorizer)
            self.assertEqual(reader.rule_results(acc)["sets"][3]["analysis_set"], "bankara_open")
            self.assertEqual(reader.rate_summary(acc)["series"][1]["latest"], 2100.0)
            with self.assertRaises(ValueError):
                reader.rule_results("")
            with self.assertRaises(ValueError):
                reader.rate_summary("")


if __name__ == "__main__":
    unittest.main()
