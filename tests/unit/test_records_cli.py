"""records CLI サブコマンドおよび NAS 中継の単体テスト。

実DB・実NAS・実アカウント・実APIを一切使わず、人工SQLite環境と
実 subprocess CLI を用いて以下を網羅的に検証する：
1. missingDB: 存在しないDBパスで nonzero かつ DB および親ディレクトリ未作成
2. list comprehensiveness: 詳細なし対戦・バイト（pending/unavailable/unresolved等）を母集合から落とさない
3. account / kind isolation: アカウント分離および対戦/バイト種別分離
4. limit / offset / invalidbounds: ページネーションおよび境界値不正（0, 201, -1, 空account等）で nonzero
5. get canonical: 原文 unknown key / 特殊 number 表記の base64 完全一致、headers 不在、detail_json 保持
6. get missing / corrupt: 不在 key で null / exit 0、欠損 canonical で nonzero (exit 1)
7. DB bytes / schema immutability: list / get 実行前後で DB の SHA256 が完全一致（無書込確認）
8. NAS marker / NAS forwarding: デフォルトパス拒否 (NAS_STORAGE_ACTIVE) および
   nas_archive.py による特殊文字の安全な quote 中継とローカル無書込
"""

import base64
import hashlib
import json
import os
import shlex
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src/python"))

from ikarchive.store import Store, js


class TestRecordsCLI(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.tmp_dir = Path(self.tmp.name)
        self.db_path = self.tmp_dir / "database" / "archive.sqlite3"
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        # Store経由で空スキーマの人工DBを作成
        s = Store(self.db_path)
        s.close()
        self.cli_path = ROOT / "archive.py"

    def tearDown(self):
        self.tmp.cleanup()

    def _run_cli(self, args, db_path=None, env=None):
        cmd = [sys.executable, str(self.cli_path)]
        if db_path is not False:
            target_db = str(db_path if db_path is not None else self.db_path)
            cmd += ["--db", target_db]
        cmd += args
        run_env = dict(os.environ)
        if env:
            run_env.update(env)
        return subprocess.run(cmd, capture_output=True, text=True, env=run_env)

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
        conn.execute("PRAGMA foreign_keys=OFF")
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
                js({"x-secret-auth": "MUST_NOT_BE_RETURNED", "authorization": "Bearer secret"}),
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

    def test_01_missing_db(self):
        """存在しないDBパスで nonzero かつ DB および親ディレクトリが作成されないこと。"""
        missing_db = self.tmp_dir / "nonexistent_dir" / "missing.sqlite3"
        proc = self._run_cli(["records", "list", "--account", "test-acc"], db_path=missing_db)
        self.assertNotEqual(proc.returncode, 0)
        self.assertFalse(missing_db.exists())
        self.assertFalse(missing_db.parent.exists())
        err = json.loads(proc.stderr)
        self.assertIn("error", err)

    def test_02_records_list_comprehensiveness(self):
        """詳細なし対戦・バイト（pending/unavailable/unresolved）を母集合から落とさないこと。"""
        acc = "dummy-account-1"

        # 1. VS available (詳細あり)
        raw_vs = b'{"data":{"vsHistoryDetail":{"playedTime":"2026-09-01T00:10:00Z"}}}'
        self._insert_match(acc, "vs", "vs-key-avail")
        self._insert_canonical_detail(
            acc, "vs", "vs-key-avail", 101, raw_vs,
            '{"playedTime":"2026-09-01T00:10:00Z"}',
            genre="bankara_open", rule_raw="AREA"
        )

        # 2. Coop available (詳細あり)
        raw_coop = b'{"data":{"coopHistoryDetail":{"playedTime":"2026-09-01T00:20:00Z"}}}'
        self._insert_match(acc, "coop", "coop-key-avail")
        self._insert_canonical_detail(
            acc, "coop", "coop-key-avail", 102, raw_coop,
            '{"playedTime":"2026-09-01T00:20:00Z"}',
            genre="salmon_regular", rule_raw="REGULAR",
            operation="CoopHistoryDetailQuery"
        )

        # 3. VS pending (jobsにpending)
        self._insert_match(acc, "vs", "vs-key-pending")
        self._insert_job(acc, "VsHistoryDetailQuery", "vs-key-pending", "pending", "vs")

        # 4. Coop unavailable (jobsにunavailable)
        self._insert_match(acc, "coop", "coop-key-unavail")
        self._insert_job(acc, "CoopHistoryDetailQuery", "coop-key-unavail", "unavailable", "coop")

        # 5. VS unresolved (jobsなし、詳細なし)
        self._insert_match(acc, "vs", "vs-key-unresolved")

        proc = self._run_cli(["records", "list", "--account", acc])
        self.assertEqual(proc.returncode, 0, f"CLI error: {proc.stderr}")
        data = json.loads(proc.stdout)
        self.assertEqual(data["total"], 5)
        self.assertEqual(len(data["items"]), 5)

        items_by_key = {item["match_key"]: item for item in data["items"]}
        self.assertEqual(items_by_key["vs-key-avail"]["detail_state"], "available")
        self.assertEqual(items_by_key["coop-key-avail"]["detail_state"], "available")
        self.assertEqual(items_by_key["vs-key-pending"]["detail_state"], "pending")
        self.assertEqual(items_by_key["coop-key-unavail"]["detail_state"], "unavailable")
        self.assertEqual(items_by_key["vs-key-unresolved"]["detail_state"], "unresolved")

    def test_03_records_list_account_and_kind_isolation(self):
        """アカウント分離および kind (vs/coop) フィルタの正確性を検証。"""
        acc_a = "account-alpha"
        acc_b = "account-beta"

        self._insert_match(acc_a, "vs", "vs-a-1")
        self._insert_match(acc_a, "vs", "vs-a-2")
        self._insert_match(acc_a, "coop", "coop-a-1")
        self._insert_match(acc_b, "vs", "vs-b-1")

        # acc_a 全件 (3件)
        proc_a = self._run_cli(["records", "list", "--account", acc_a])
        self.assertEqual(proc_a.returncode, 0)
        data_a = json.loads(proc_a.stdout)
        self.assertEqual(data_a["total"], 3)
        self.assertTrue(all(item["account"] == acc_a for item in data_a["items"]))

        # acc_a vs のみ (2件)
        proc_a_vs = self._run_cli(["records", "list", "--account", acc_a, "--kind", "vs"])
        self.assertEqual(proc_a_vs.returncode, 0)
        data_a_vs = json.loads(proc_a_vs.stdout)
        self.assertEqual(data_a_vs["total"], 2)
        self.assertTrue(all(item["kind"] == "vs" for item in data_a_vs["items"]))

        # acc_a coop のみ (1件)
        proc_a_coop = self._run_cli(["records", "list", "--account", acc_a, "--kind", "coop"])
        self.assertEqual(proc_a_coop.returncode, 0)
        data_a_coop = json.loads(proc_a_coop.stdout)
        self.assertEqual(data_a_coop["total"], 1)
        self.assertEqual(data_a_coop["items"][0]["match_key"], "coop-a-1")

        # acc_b 全件 (1件)
        proc_b = self._run_cli(["records", "list", "--account", acc_b])
        self.assertEqual(proc_b.returncode, 0)
        data_b = json.loads(proc_b.stdout)
        self.assertEqual(data_b["total"], 1)
        self.assertEqual(data_b["items"][0]["match_key"], "vs-b-1")

        # 未知アカウント (0件)
        proc_c = self._run_cli(["records", "list", "--account", "nonexistent-user"])
        self.assertEqual(proc_c.returncode, 0)
        data_c = json.loads(proc_c.stdout)
        self.assertEqual(data_c["total"], 0)
        self.assertEqual(data_c["items"], [])

    def test_04_records_list_pagination_and_bounds(self):
        """limit/offset ページネーションおよび境界値不正の検証。"""
        acc = "account-page"
        for i in range(5):
            self._insert_match(acc, "vs", f"vs-page-{i:02d}")

        # 正常ページネーション
        p1 = json.loads(self._run_cli(["records", "list", "--account", acc, "--limit", "2", "--offset", "0"]).stdout)
        p2 = json.loads(self._run_cli(["records", "list", "--account", acc, "--limit", "2", "--offset", "2"]).stdout)
        p3 = json.loads(self._run_cli(["records", "list", "--account", acc, "--limit", "2", "--offset", "4"]).stdout)

        self.assertEqual([it["match_key"] for it in p1["items"]], ["vs-page-00", "vs-page-01"])
        self.assertEqual([it["match_key"] for it in p2["items"]], ["vs-page-02", "vs-page-03"])
        self.assertEqual([it["match_key"] for it in p3["items"]], ["vs-page-04"])

        # 不正な limit (0, 201)
        proc_bad_limit0 = self._run_cli(["records", "list", "--account", acc, "--limit", "0"])
        self.assertNotEqual(proc_bad_limit0.returncode, 0)
        proc_bad_limit201 = self._run_cli(["records", "list", "--account", acc, "--limit", "201"])
        self.assertNotEqual(proc_bad_limit201.returncode, 0)

        # 不正な offset (-1)
        proc_bad_offset = self._run_cli(["records", "list", "--account", acc, "--offset", "-1"])
        self.assertNotEqual(proc_bad_offset.returncode, 0)

        # 空の account
        proc_empty_acc = self._run_cli(["records", "list", "--account", ""])
        self.assertNotEqual(proc_empty_acc.returncode, 0)

    def test_05_records_get_canonical_integrity_and_no_headers(self):
        """原文未知キー/特殊数値表記の base64 完全一致および headers 不在の検証。"""
        acc = "account-get"
        match_key = "vs-get-01"
        # 未知項目・巨大数値表現を含む raw bytes
        raw_body_bytes = (
            b'{"data":{"vsHistoryDetail":{"id":"vs-get-01",'
            b'"unknown_custom_key":[1,{"nested_flag":true}],'
            b'"special_float_num":12345678901234567890.123456,'
            b'"playedTime":"2026-09-01T15:30:00Z"}}}'
        )
        doc_json = '{"id":"vs-get-01","playedTime":"2026-09-01T15:30:00Z"}'

        self._insert_match(acc, "vs", match_key)
        self._insert_canonical_detail(
            acc, "vs", match_key, 201, raw_body_bytes, doc_json,
            genre="bankara_open", rule_raw="CLAM"
        )

        proc = self._run_cli(["records", "get", "--account", acc, "--kind", "vs", "--match-key", match_key])
        self.assertEqual(proc.returncode, 0, f"CLI error: {proc.stderr}")

        # 出力テキスト全体に秘密ヘッダーが含まれないことを直接文字列探索で確認
        self.assertNotIn("MUST_NOT_BE_RETURNED", proc.stdout)
        self.assertNotIn("Bearer secret", proc.stdout)
        self.assertNotIn("x-secret-auth", proc.stdout)

        data = json.loads(proc.stdout)
        self.assertEqual(data["account"], acc)
        self.assertEqual(data["kind"], "vs")
        self.assertEqual(data["match_key"], match_key)
        self.assertEqual(data["detail_json"], doc_json)

        # source.body_base64 を復号し、元の raw_body_bytes と完全一致することを検証
        self.assertIn("source", data)
        self.assertIn("body_base64", data["source"])
        self.assertNotIn("body_bytes", data["source"])
        self.assertNotIn("headers", data["source"])
        self.assertNotIn("headers_json", data["source"])

        decoded_bytes = base64.b64decode(data["source"]["body_base64"])
        self.assertEqual(decoded_bytes, raw_body_bytes)

    def test_06_records_get_missing_and_corrupt(self):
        """存在しないキーで null/exit 0、欠損 canonical で nonzero (exit 1)。"""
        acc = "account-missing-test"
        self._insert_match(acc, "vs", "vs-valid-01")

        # 存在しないキー -> JSON null, exit 0
        proc_missing = self._run_cli(["records", "get", "--account", acc, "--kind", "vs", "--match-key", "no-such-key"])
        self.assertEqual(proc_missing.returncode, 0)
        self.assertEqual(proc_missing.stdout.strip(), "null")

        # 欠損 canonical (detail_response_id があるのに documents に不在)
        self._insert_match(acc, "vs", "vs-corrupt-01", detail_response_id=99999)
        proc_corrupt = self._run_cli(["records", "get", "--account", acc, "--kind", "vs", "--match-key", "vs-corrupt-01"])
        self.assertNotEqual(proc_corrupt.returncode, 0)
        err = json.loads(proc_corrupt.stderr)
        self.assertIn("Corrupt record", err.get("error", ""))

    def test_07_db_bytes_and_schema_immutability(self):
        """records list / get の実行前後で DB bytes が完全に不変（無書込）であること。"""
        acc = "account-immutable"
        raw_body = b'{"data":{"vsHistoryDetail":{"playedTime":"2026-09-01T00:00:00Z"}}}'
        self._insert_match(acc, "vs", "vs-imm-01")
        self._insert_canonical_detail(
            acc, "vs", "vs-imm-01", 301, raw_body,
            '{"playedTime":"2026-09-01T00:00:00Z"}',
            genre="regular"
        )

        # 投入データをDB本体に完全反映して静止点を作成
        conn = sqlite3.connect(self.db_path)
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        initial_schema = [r[0] for r in conn.execute("SELECT sql FROM sqlite_master WHERE sql IS NOT NULL ORDER BY 1")]
        conn.close()

        initial_bytes = self.db_path.read_bytes()
        initial_hash = hashlib.sha256(initial_bytes).hexdigest()

        # list 実行
        proc_list = self._run_cli(["records", "list", "--account", acc])
        self.assertEqual(proc_list.returncode, 0)

        # get 実行
        proc_get = self._run_cli(["records", "get", "--account", acc, "--kind", "vs", "--match-key", "vs-imm-01"])
        self.assertEqual(proc_get.returncode, 0)

        # DB bytes の完全不変（ハッシュおよびバイト列比較による無書込確認）
        after_bytes = self.db_path.read_bytes()
        after_hash = hashlib.sha256(after_bytes).hexdigest()
        self.assertEqual(initial_hash, after_hash)
        self.assertEqual(initial_bytes, after_bytes)

        # スキーマの完全不変確認
        conn_check = sqlite3.connect(f"{self.db_path.resolve().as_uri()}?mode=ro", uri=True)
        after_schema = [r[0] for r in conn_check.execute("SELECT sql FROM sqlite_master WHERE sql IS NOT NULL ORDER BY 1")]
        conn_check.close()
        self.assertEqual(initial_schema, after_schema)

        # writer lock ファイルが作成されていないことを確認
        self.assertFalse(Path(str(self.db_path) + ".lock").exists())

    def test_08_nas_marker_and_nas_archive_forwarding(self):
        """NAS marker defaultpath 拒否および nas_archive.py による安全な引数 quote 中継。"""
        acc = "account-nas-test"

        # (A) NAS marker defaultpath 拒否
        fake_data_dir = self.tmp_dir / "fake_data_root"
        fake_data_dir.mkdir(parents=True, exist_ok=True)
        config_dir = fake_data_dir / "config"
        config_dir.mkdir(parents=True, exist_ok=True)
        marker_file = config_dir / "storage-location.json"
        marker_content = {
            "schema_version": 1,
            "backend": "nas",
            "ssh_host": "nas-test-host",
            "container": "ikaring-test-container",
            "database": "/data/database/archive.sqlite3",
        }
        marker_file.write_text(json.dumps(marker_content), encoding="utf-8")

        # IKARING_ARCHIVE_DATA_DIR を偽ディレクトリに向けて、--db なしで実行
        env = {"IKARING_ARCHIVE_DATA_DIR": str(fake_data_dir)}
        proc_default = self._run_cli(["records", "list", "--account", acc], db_path=False, env=env)
        self.assertNotEqual(proc_default.returncode, 0)
        self.assertIn("NAS_STORAGE_ACTIVE", proc_default.stderr)

        # (B) scripts/nas_archive.py による特殊文字の安全な quote 中継
        from scripts import nas_archive

        special_account = 'acc-with-$dollar;&"quote"'
        special_key = "key-with-'single'and`backtick`"

        with patch("scripts.nas_archive.read_marker", return_value=marker_content), \
             patch("scripts.nas_archive.subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0)

            # records list の中継
            exit_code_list = nas_archive.main([
                "records", "list",
                "--account", special_account,
                "--kind", "vs",
                "--limit", "10",
            ])
            self.assertEqual(exit_code_list, 0)
            self.assertTrue(mock_run.called)
            called_cmd = mock_run.call_args[0][0]

            # SSH コマンド形式の検証
            self.assertEqual(called_cmd[0], "ssh")
            self.assertEqual(called_cmd[1], "-o")
            self.assertEqual(called_cmd[2], "BatchMode=yes")
            self.assertEqual(called_cmd[3], "nas-test-host")

            remote_cmd = called_cmd[4]
            # shlex.split して引数が元の特殊文字列と完全一致すること（安全にクォートされていること）を検証
            tokens = shlex.split(remote_cmd)
            self.assertEqual(tokens[:4], ["docker", "exec", "-i", "ikaring-test-container"])
            self.assertEqual(tokens[4:8], ["python3", "/app/archive.py", "--db", "/data/database/archive.sqlite3"])
            self.assertEqual(tokens[8:10], ["records", "list"])
            self.assertIn("--account", tokens)
            acc_idx = tokens.index("--account")
            self.assertEqual(tokens[acc_idx + 1], special_account)

            # records get の中継
            mock_run.reset_mock()
            exit_code_get = nas_archive.main([
                "records", "get",
                "--account", special_account,
                "--kind", "coop",
                "--match-key", special_key,
            ])
            self.assertEqual(exit_code_get, 0)
            called_cmd_get = mock_run.call_args[0][0]
            remote_cmd_get = called_cmd_get[4]
            tokens_get = shlex.split(remote_cmd_get)
            self.assertIn("--match-key", tokens_get)
            key_idx = tokens_get.index("--match-key")
            self.assertEqual(tokens_get[key_idx + 1], special_key)

            # ローカルへのファイル書き込みがないこと（exports フォルダ等へのダウンロードが発生しないこと）
            local_exports = fake_data_dir / "exports"
            self.assertFalse(local_exports.exists())


if __name__ == "__main__":
    unittest.main()
