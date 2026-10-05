#!/usr/bin/env python3
"""同じ試合の詳細の重複（取り直し・別一覧の ID で取った分）を消す（2026-10-06 前田さんの指示）。

前田さんの指示（原文ママ・抜粋）: 「現在ある重複分は全て消すように。」

重複の定義（実測にもとづく。開発ログ 2026-10-06）:
  同じ試合 (account, kind, match_key) について、詳細の取得（VsHistoryDetailQuery / CoopHistoryDetailQuery）
  の応答が 2 件以上ある。違いは「次の試合・前の試合へのリンク」と、見つけた一覧ごとに異なる ID の書き方
  （…:PRIVATE:… / …:RECENT:…）だけで、試合の中身は同じ。

残すもの: 各試合で最初に取れた完全な詳細の応答 1 件（K）。
消すもの: それ以外の詳細の応答（D）と、D にぶら下がる行（documents・sightings・asset_refs・
  response_fetches・issues）、D からしか取り出されていない entities、どこからも参照されなくなった bodies。
付け替え: matches / match_classification の detail_response_id、jobs の last_response_id を K に。
  D が上書きしていた entities で K にも同じ ID があるものは、K の内容と response_id に戻す。
  同じ試合のほかの詳細の仕事（別一覧の ID）は superseded にする。

使い方:
  --dry-run   読み取り専用で計画と件数を出す（何も変えない）
  --execute   一つの書き込みトランザクションで実行し、検査が一つでも通らなければ取り消す
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from collections import defaultdict

DETAIL_OPS = {"vs": "VsHistoryDetailQuery", "coop": "CoopHistoryDetailQuery"}
ROOTS = {"VsHistoryDetailQuery": "vsHistoryDetail", "CoopHistoryDetailQuery": "coopHistoryDetail"}


def js(value):
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def complete(op: str, json_text: str | None) -> bool:
    if not json_text:
        return False
    try:
        obj = json.loads(json_text)
    except ValueError:
        return False
    if not isinstance(obj, dict) or obj.get("errors"):
        return False
    data = obj.get("data")
    return isinstance(data, dict) and isinstance(data.get(ROOTS[op]), dict)


def find_object(node, entity_id, typename):
    """K の原文から、id（と __typename があれば型）が一致する辞書を探す。"""
    stack = [node]
    while stack:
        cur = stack.pop()
        if isinstance(cur, dict):
            if cur.get("id") == entity_id and cur.get("__typename", typename) == typename:
                return cur
            stack.extend(cur.values())
        elif isinstance(cur, list):
            stack.extend(cur)
    return None


def plan(c: sqlite3.Connection):
    by_match = defaultdict(list)
    for rid, acc, kind, key, op, fa, status, jt in c.execute(
        """SELECT r.id, d.account, d.kind, d.match_key, r.operation, r.fetched_at, r.http_status, r.json_text
           FROM documents d JOIN responses r ON r.id=d.response_id
           WHERE r.operation IN ('VsHistoryDetailQuery','CoopHistoryDetailQuery')"""
    ):
        by_match[(acc, kind, key)].append((fa, rid, op, status, jt))
    keep, drop, no_complete = {}, {}, []
    for m, lst in by_match.items():
        lst.sort()
        good = [x for x in lst if x[3] == 200 and complete(x[2], x[4])]
        if not good:
            no_complete.append(m)
            continue
        k = good[0][1]
        keep[m] = k
        for x in lst:
            if x[1] != k:
                drop[x[1]] = m
    return by_match, keep, drop, no_complete


def counts(c, drop_ids):
    ids = list(drop_ids)
    c.execute("CREATE TEMP TABLE IF NOT EXISTS d_ids(id INTEGER PRIMARY KEY)")
    c.execute("DELETE FROM d_ids")
    c.executemany("INSERT INTO d_ids VALUES(?)", [(i,) for i in ids])
    out = {}
    for t, col in (("documents", "response_id"), ("sightings", "response_id"), ("asset_refs", "response_id"),
                   ("response_fetches", "response_id"), ("issues", "response_id"), ("entities", "response_id"),
                   ("matches", "detail_response_id"), ("match_classification", "detail_response_id"),
                   ("jobs", "last_response_id"), ("endpoint_heads", "response_id")):
        out[f"{t}.{col}"] = c.execute(f"SELECT count(*) FROM {t} WHERE {col} IN (SELECT id FROM d_ids)").fetchone()[0]
    out["responses"] = len(ids)
    out["response_bytes_json"] = c.execute(
        "SELECT coalesce(sum(length(json_text)),0) FROM responses WHERE id IN (SELECT id FROM d_ids)").fetchone()[0]
    return out


def execute(c, keep, drop):
    c.execute("CREATE TEMP TABLE IF NOT EXISTS d_ids(id INTEGER PRIMARY KEY)")
    c.execute("DELETE FROM d_ids")
    c.executemany("INSERT INTO d_ids VALUES(?)", [(i,) for i in drop])
    stats = {}
    # 1) entities: D を指す行を、K にも同じ ID があれば K の内容に戻し、無ければ消す
    kept_json = {}
    restored = removed = 0
    rows = c.execute("SELECT rowid, account, typename, entity_id, response_id FROM entities "
                     "WHERE response_id IN (SELECT id FROM d_ids)").fetchall()
    for rowid, acc, typename, eid, rid in rows:
        m = drop[rid]
        k = keep[m]
        if k not in kept_json:
            kept_json[k] = json.loads(c.execute("SELECT json_text FROM responses WHERE id=?", (k,)).fetchone()[0])
        obj = find_object(kept_json[k], eid, typename)
        if obj is not None:
            c.execute("UPDATE entities SET response_id=?, json_text=? WHERE rowid=?", (k, js(obj), rowid))
            restored += 1
        else:
            c.execute("DELETE FROM entities WHERE rowid=?", (rowid,))
            removed += 1
    stats["entities_restored_to_kept"] = restored
    stats["entities_deleted"] = removed
    # 2) 付け替え
    for m, k in keep.items():
        acc, kind, key = m
        stats.setdefault("matches_repointed", 0)
        stats["matches_repointed"] += c.execute(
            "UPDATE matches SET detail_response_id=? WHERE account=? AND kind=? AND match_key=? AND detail_response_id IS NOT ?",
            (k, acc, kind, key, k)).rowcount
        stats.setdefault("classification_repointed", 0)
        stats["classification_repointed"] += c.execute(
            "UPDATE match_classification SET detail_response_id=? WHERE account=? AND kind=? AND match_key=? AND detail_response_id IS NOT ?",
            (k, acc, kind, key, k)).rowcount
        kv = c.execute("SELECT variables_json, operation FROM responses WHERE id=?", (k,)).fetchone()
        stats.setdefault("jobs_kept_repointed", 0)
        stats["jobs_kept_repointed"] += c.execute(
            "UPDATE jobs SET last_response_id=? WHERE account=? AND operation=? AND variables_json=? AND last_response_id IN (SELECT id FROM d_ids)",
            (k, acc, kv[1], kv[0])).rowcount
        stats.setdefault("jobs_superseded", 0)
        stats["jobs_superseded"] += c.execute(
            "UPDATE jobs SET state='superseded' WHERE account=? AND operation=? AND match_key=? AND variables_json<>? AND state<>'superseded'",
            (acc, kv[1], key, kv[0])).rowcount
        stats.setdefault("jobs_other_repointed", 0)
        stats["jobs_other_repointed"] += c.execute(
            "UPDATE jobs SET last_response_id=? WHERE account=? AND operation=? AND match_key=? AND last_response_id IN (SELECT id FROM d_ids)",
            (k, acc, kv[1], key)).rowcount
    # 詳細が保存済みのすべての試合について、取り直しの設定で pending に戻されていた詳細の仕事を片付ける。
    # 保存済みの詳細と同じ ID の仕事は done に戻し、別の一覧の ID の仕事は superseded にする（2026-10-06 Opus 判断）。
    stats["jobs_reset_to_done"] = 0
    stats["jobs_other_context_superseded"] = 0
    for acc, kind, key, drid in c.execute(
            "SELECT account, kind, match_key, detail_response_id FROM matches WHERE detail_response_id IS NOT NULL").fetchall():
        op = DETAIL_OPS.get(kind)
        if op is None:
            continue
        v = c.execute("SELECT variables_json FROM responses WHERE id=?", (drid,)).fetchone()
        if v is None:
            continue
        stats["jobs_reset_to_done"] += c.execute(
            "UPDATE jobs SET state='done' WHERE account=? AND operation=? AND match_key=? AND variables_json=? AND state IN ('pending','retry')",
            (acc, op, key, v[0])).rowcount
        stats["jobs_other_context_superseded"] += c.execute(
            "UPDATE jobs SET state='superseded' WHERE account=? AND operation=? AND match_key=? AND variables_json<>? AND state IN ('pending','retry','done')",
            (acc, op, key, v[0])).rowcount
    # endpoint_heads（操作ごとの最新の応答）が D を指していれば、残る中で最新の同じ操作の応答へ付け替える
    stats["endpoint_heads_repointed"] = 0
    for acc, op, rid in c.execute("SELECT account, operation, response_id FROM endpoint_heads WHERE response_id IN (SELECT id FROM d_ids)").fetchall():
        latest = c.execute("SELECT id FROM responses WHERE account=? AND operation=? AND id NOT IN (SELECT id FROM d_ids) "
                           "ORDER BY julianday(fetched_at) DESC, id DESC LIMIT 1", (acc, op)).fetchone()
        if latest is None:
            raise RuntimeError(f"endpoint_heads の付け替え先が無い {op}")
        c.execute("UPDATE endpoint_heads SET response_id=? WHERE account=? AND operation=?", (latest[0], acc, op))
        stats["endpoint_heads_repointed"] += 1
    # 3) ぶら下がる行を消す
    for t in ("documents", "sightings", "asset_refs", "response_fetches", "issues"):
        stats[f"{t}_deleted"] = c.execute(f"DELETE FROM {t} WHERE response_id IN (SELECT id FROM d_ids)").rowcount
    stats["responses_deleted"] = c.execute("DELETE FROM responses WHERE id IN (SELECT id FROM d_ids)").rowcount
    stats["bodies_deleted"] = c.execute(
        "DELETE FROM bodies WHERE sha256 NOT IN (SELECT body_sha256 FROM responses) "
        "AND sha256 NOT IN (SELECT body_sha256 FROM assets WHERE body_sha256 IS NOT NULL)").rowcount
    return stats


def checks(c, keep):
    errs = []
    left = c.execute("""SELECT count(*) FROM (SELECT account,kind,match_key FROM documents d JOIN responses r ON r.id=d.response_id
        WHERE r.operation IN ('VsHistoryDetailQuery','CoopHistoryDetailQuery') GROUP BY 1,2,3 HAVING count(*)>1)""").fetchone()[0]
    if left:
        errs.append(f"詳細が2件以上残る試合 {left}")
    for (acc, kind, key), k in keep.items():
        row = c.execute("SELECT detail_response_id FROM matches WHERE account=? AND kind=? AND match_key=?", (acc, kind, key)).fetchone()
        if row is None or row[0] != k:
            errs.append(f"matches の詳細が残す応答を指していない {key}")
            break
        if c.execute("SELECT 1 FROM documents WHERE response_id=? AND match_key=?", (k, key)).fetchone() is None:
            errs.append(f"残す詳細の documents が無い {key}")
            break
    for t, col in (("documents", "response_id"), ("sightings", "response_id"), ("asset_refs", "response_id"),
                   ("response_fetches", "response_id"), ("issues", "response_id"), ("entities", "response_id"),
                   ("matches", "detail_response_id"), ("match_classification", "detail_response_id"),
                   ("jobs", "last_response_id"), ("endpoint_heads", "response_id")):
        n = c.execute(f"SELECT count(*) FROM {t} WHERE {col} IS NOT NULL AND {col} NOT IN (SELECT id FROM responses)").fetchone()[0]
        if n:
            errs.append(f"{t}.{col} が存在しない応答を指す行 {n}")
    n = c.execute("SELECT count(*) FROM responses WHERE body_sha256 NOT IN (SELECT sha256 FROM bodies)").fetchone()[0]
    if n:
        errs.append(f"本文の無い応答 {n}")
    fk = c.execute("PRAGMA foreign_key_check").fetchall()
    if fk:
        errs.append(f"foreign_key_check {len(fk)} 件")
    return errs


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True)
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--dry-run", action="store_true")
    g.add_argument("--execute", action="store_true")
    a = ap.parse_args()
    if a.dry_run:
        c = sqlite3.connect(f"file:{a.db}?mode=ro", uri=True)
        c.execute("BEGIN")
        by_match, keep, drop, no_complete = plan(c)
        out = {"matches_with_detail": len(by_match), "matches_with_duplicates": sum(1 for m in by_match if len(by_match[m]) > 1),
               "keep": len(keep), "drop_responses": len(drop), "matches_without_complete_detail": len(no_complete),
               "would_touch": counts(c, drop)}
        print(json.dumps(out, ensure_ascii=False, indent=1))
        return 0
    c = sqlite3.connect(a.db, timeout=60, isolation_level=None)
    c.execute("PRAGMA recursive_triggers=ON")
    c.execute("PRAGMA foreign_keys=ON")
    c.execute("BEGIN IMMEDIATE")
    try:
        by_match, keep, drop, no_complete = plan(c)
        before = counts(c, drop)
        stats = execute(c, keep, drop)
        errs = checks(c, keep)
        if errs:
            raise RuntimeError("検査に失敗したので取り消す: " + " / ".join(errs))
        c.execute("COMMIT")
    except BaseException:
        c.execute("ROLLBACK")
        raise
    print(json.dumps({"keep": len(keep), "drop": len(drop), "before": before, "stats": stats, "checks": "ok"}, ensure_ascii=False, indent=1))
    return 0


if __name__ == "__main__":
    sys.exit(main())
