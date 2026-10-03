"""画像URL状態と本文参照の限定監査を検査する。"""

import json
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "src/python"))

from archive import audit
from ikarchive.asset_evidence import asset_acquisition_evidence
from ikarchive.store import Store


def make_db():
    db = sqlite3.connect(":memory:")
    db.executescript("""
        CREATE TABLE bodies(sha256 TEXT PRIMARY KEY, body BLOB NOT NULL, byte_length INTEGER NOT NULL);
        CREATE TABLE assets(
            url TEXT PRIMARY KEY, state TEXT NOT NULL DEFAULT 'pending',
            body_sha256 TEXT, last_error TEXT
        );
        CREATE TABLE asset_refs(response_id INTEGER NOT NULL, url TEXT NOT NULL, path TEXT NOT NULL);
    """)
    return db


def add_asset(db, url, state="pending", body_sha256=None, last_error=None):
    db.execute(
        "INSERT INTO assets(url,state,body_sha256,last_error) VALUES(?,?,?,?)",
        (url, state, body_sha256, last_error),
    )


class AssetEvidenceTests(unittest.TestCase):
    def setUp(self):
        self.db = make_db()

    def tearDown(self):
        self.db.close()

    def test_counts_assets_rows_and_same_address_without_claiming_byte_equivalence(self):
        # Same address despite URL query/fragment changes. The stored bytes and
        # URL hashes differ; the address match must not claim byte equivalence.
        self.db.executemany(
            "INSERT INTO bodies(sha256,body,byte_length) VALUES(?,?,?)",
            [
                ("body-key-saved", b"saved image bytes", 999),
                ("body-key-done", b"another image", 14),
                ("body-key-retry", b"retry payload", 13),
            ],
        )
        saved = "HTTPS://Example.COM:443/img%2Fraw?token=saved#one"
        add_asset(self.db, saved, "done", "body-key-saved")
        add_asset(self.db, "https://example.com/img%2Fraw?token=expired#two",
                  "retry", None, "AssetUrlExpired")
        add_asset(self.db, "https://example.com/img%2Fraw?different=1",
                  "retry", None, "AssetUrlExpired")
        add_asset(self.db, "https://example.com/img/raw", "retry", None, "AssetUrlExpired")
        add_asset(self.db, "http://example.com/img%2Fraw", "retry", None, "AssetUrlExpired")
        add_asset(self.db, "https://user@example.com/private", "retry", None, "AssetUrlExpired")
        add_asset(self.db, "https://example.com:99999/bad-port", "retry", None, "AssetUrlExpired")
        add_asset(self.db, "relative/path", "retry", None, "AssetUrlExpired")
        add_asset(self.db, "https://example.com/done-missing", "done", "missing-body-key")
        add_asset(self.db, "https://example.net/done-old-error", "done",
                  "body-key-done", "AssetUrlExpired")
        add_asset(self.db, "https://example.org/pending")
        add_asset(self.db, "https://example.org/retry", "retry", "body-key-retry", "TimeoutError")
        add_asset(self.db, "https://example.org/other", "future-state")

        result = asset_acquisition_evidence(self.db)
        self.assertEqual(result["asset_url_rows"], 13)
        self.assertEqual(result["state_url_rows"], {
            "pending": 1, "retry": 8, "done": 3, "other": 1,
        })
        self.assertEqual(result["done_body_reference_present_url_rows"], 2)
        self.assertEqual(result["done_body_reference_missing_url_rows"], 1)
        self.assertEqual(result["expired_url_rows"], 7)
        self.assertEqual(result["expired_urls_with_saved_same_address"], 2)
        self.assertEqual(result["expired_urls_without_saved_same_address"], 2)
        self.assertEqual(result["distinct_expired_addresses"], 3)
        self.assertEqual(result["unsafe_address_url_rows"], 3)
        self.assertEqual(result["expired_unsafe_address_url_rows"], 3)
        self.assertFalse(result["historical_image_bytes_equivalence_verified"])
        self.assertFalse(result["all_server_images_verified"])

        serialized = json.dumps(result, ensure_ascii=False)
        for private_value in (
            saved, "token=saved", "body-key-saved", "body-key-done", "missing-body-key",
            "saved image bytes", "another image", "retry payload", "user@example.com",
        ):
            self.assertNotIn(private_value, serialized)
        self.assertIn("asset_refs edges are not read or counted", result["limit"])
        self.assertIn("query and fragment are ignored", result["limit"])

    def test_authorizer_can_forbid_asset_refs_blob_reads_and_writes(self):
        add_asset(self.db, "https://saved.example/image", "done", "body-key")
        self.db.execute("INSERT INTO bodies VALUES('body-key',x'01',1)")
        # Several reference edges exist, but this per-URL-state audit neither
        # reads nor counts them.
        self.db.executemany(
            "INSERT INTO asset_refs VALUES(?,?,?)",
            [(1, "https://saved.example/image", "a"),
             (2, "https://saved.example/image", "b")],
        )
        self.db.commit()
        changes_before = self.db.total_changes
        self.db.execute("BEGIN")

        def authorizer(action, arg1, arg2, database, source):
            if action == sqlite3.SQLITE_READ and arg1 == "asset_refs":
                return sqlite3.SQLITE_DENY
            if action == sqlite3.SQLITE_READ and arg1 == "bodies" and arg2 == "body":
                return sqlite3.SQLITE_DENY
            if action in (sqlite3.SQLITE_INSERT, sqlite3.SQLITE_UPDATE, sqlite3.SQLITE_DELETE):
                return sqlite3.SQLITE_DENY
            return sqlite3.SQLITE_OK

        self.db.set_authorizer(authorizer)
        try:
            result = asset_acquisition_evidence(self.db)
        finally:
            self.db.set_authorizer(None)
        self.assertEqual(result["asset_url_rows"], 1)
        self.assertEqual(result["done_body_reference_present_url_rows"], 1)
        self.assertEqual(self.db.total_changes, changes_before)
        self.assertTrue(self.db.in_transaction)

    def test_malformed_urls_null_and_missing_body_keys_keep_counts_and_privacy(self):
        self.db.executemany(
            "INSERT INTO bodies(sha256,body,byte_length) VALUES(?,?,?)",
            [("private-body-key-present", b"private saved bytes", 19)],
        )
        private_done_url = "https://Example.COM:443/image?private=query#fragment"
        add_asset(self.db, private_done_url, "done", "private-body-key-present")
        add_asset(self.db, "https://example.com/image?different=secret", "retry",
                  None, "AssetUrlExpired")
        add_asset(self.db, "https://unmatched.example/image", "retry",
                  None, "AssetUrlExpired")
        add_asset(self.db, "https://unknown.example/image", "future-state",
                  None, "AssetUrlExpired")
        # SQLite TEXT affinity preserves NULL and BLOB values, allowing the
        # parser's non-string behavior to be exercised against real SQLite.
        add_asset(self.db, None, "retry", None, "AssetUrlExpired")
        add_asset(self.db, "", "retry", None, "AssetUrlExpired")
        add_asset(self.db, b"private-url-blob", "retry", None, "AssetUrlExpired")
        add_asset(self.db, "https://done-null.example/image", "done", None)
        add_asset(self.db, "https://done-empty-key.example/image", "done", "")
        add_asset(self.db, "https://retry-body.example/image", "retry",
                  "private-body-key-present", "TimeoutError")
        add_asset(self.db, "https://[2001:DB8::1]/image", "done",
                  "private-body-key-present")
        add_asset(self.db, "https://[2001:db8::1]:99999/image", "retry",
                  None, "AssetUrlExpired")
        add_asset(self.db, "https://control.example/a\nb", "retry",
                  None, "AssetUrlExpired")
        add_asset(self.db, "https://[broken/image", "retry",
                  None, "AssetUrlExpired")
        self.db.execute(
            "INSERT INTO asset_refs(response_id,url,path) VALUES(1,?,?)",
            (private_done_url, "private-path"),
        )
        self.db.commit()

        changes_before = self.db.total_changes
        self.db.execute("BEGIN")

        def authorizer(action, arg1, arg2, database, source):
            if action == sqlite3.SQLITE_READ and arg1 == "asset_refs":
                return sqlite3.SQLITE_DENY
            if action == sqlite3.SQLITE_READ and arg1 == "bodies" and arg2 == "body":
                return sqlite3.SQLITE_DENY
            if action in (sqlite3.SQLITE_INSERT, sqlite3.SQLITE_UPDATE, sqlite3.SQLITE_DELETE):
                return sqlite3.SQLITE_DENY
            return sqlite3.SQLITE_OK

        self.db.set_authorizer(authorizer)
        try:
            result = asset_acquisition_evidence(self.db)
        finally:
            self.db.set_authorizer(None)

        self.assertEqual(result["asset_url_rows"], 14)
        self.assertEqual(result["state_url_rows"], {
            "pending": 0, "retry": 9, "done": 4, "other": 1,
        })
        self.assertEqual(result["done_body_reference_present_url_rows"], 2)
        self.assertEqual(result["done_body_reference_missing_url_rows"], 2)
        self.assertEqual(result["expired_url_rows"], 9)
        self.assertEqual(result["expired_urls_with_saved_same_address"], 1)
        self.assertEqual(result["expired_urls_without_saved_same_address"], 2)
        self.assertEqual(result["expired_unsafe_address_url_rows"], 6)
        self.assertEqual(result["distinct_expired_addresses"], 3)
        self.assertEqual(result["unsafe_address_url_rows"], 6)
        self.assertEqual(
            result["expired_urls_with_saved_same_address"]
            + result["expired_urls_without_saved_same_address"]
            + result["expired_unsafe_address_url_rows"],
            result["expired_url_rows"],
        )
        self.assertEqual(
            result["done_body_reference_present_url_rows"]
            + result["done_body_reference_missing_url_rows"],
            result["state_url_rows"]["done"],
        )
        serialized = json.dumps(result, ensure_ascii=False)
        for private_value in (
            private_done_url, "private=query", "private-url-blob", "private-path",
            "private-body-key-present", "private saved bytes",
            "https://unmatched.example/image", "https://unknown.example/image",
        ):
            self.assertNotIn(private_value, serialized)
        self.assertEqual(self.db.total_changes, changes_before)
        self.assertTrue(self.db.in_transaction)

    def test_empty_database_has_fixed_private_safe_shape(self):
        result = asset_acquisition_evidence(self.db)
        self.assertEqual(result["asset_url_rows"], 0)
        self.assertEqual(result["state_url_rows"], {
            "pending": 0, "retry": 0, "done": 0, "other": 0,
        })
        self.assertEqual(result["done_body_reference_present_url_rows"], 0)
        self.assertEqual(result["done_body_reference_missing_url_rows"], 0)
        self.assertEqual(result["expired_url_rows"], 0)
        self.assertEqual(result["distinct_expired_addresses"], 0)

    def test_audit_includes_evidence_without_disclosing_urls_or_body_keys(self):
        with tempfile.TemporaryDirectory() as temporary:
            store = Store(Path(temporary) / "artificial.sqlite3")
            try:
                manifest = (ROOT / "config/query-catalog.snapshot.json").read_text(encoding="utf-8")
                store.db.execute(
                    "INSERT INTO manifests(sha256,fetched_at,json_text) VALUES('test','2026-10-03',?)",
                    (manifest,),
                )
                store.db.execute(
                    "INSERT INTO bodies(sha256,body,byte_length) VALUES('audit-body-key',x'4142',2)"
                )
                add_asset(store.db, "https://private.example/image?secret=query", "done", "audit-body-key")
                add_asset(store.db, "https://private.example/image?expired=secret",
                          "retry", None, "AssetUrlExpired")
                response_id = store.db.execute(
                    """INSERT INTO responses(
                        event_id,account,fetched_at,operation,variables_json,headers_json,
                        http_status,body_sha256,json_text,projected)
                       VALUES('asset-audit-event','private-account','2026-10-03',
                              'test','{}','{}',200,'audit-body-key','{}',1)"""
                ).lastrowid
                store.db.executemany(
                    "INSERT INTO asset_refs(response_id,url,path) VALUES(?,?,?)",
                    [(response_id, "https://private.example/image?secret=query", "path-a"),
                     (response_id, "https://private.example/image?secret=query", "path-b")],
                )
                result = audit(store)
                evidence = result["image_acquisition_evidence"]
                self.assertEqual(evidence["asset_url_rows"], 2)
                self.assertEqual(evidence["done_body_reference_present_url_rows"], 1)
                self.assertEqual(evidence["expired_urls_with_saved_same_address"], 1)
                self.assertFalse(evidence["historical_image_bytes_equivalence_verified"])
                self.assertFalse(evidence["all_server_images_verified"])
                serialized = json.dumps(result, ensure_ascii=False)
                for private_value in (
                    "private.example", "secret=query", "expired=secret", "private-account",
                    "audit-body-key", "asset-audit-event",
                ):
                    self.assertNotIn(private_value, serialized)
            finally:
                store.close()


if __name__ == "__main__":
    unittest.main()
