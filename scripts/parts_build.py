#!/usr/bin/env python3
"""SQuidLite3 データ配置の部品作成と監査の CLI（設計 0.4・本籍規則 版 3）。

  build  --source 正本 --out 出力ディレクトリ --state 状態DB [--work-dir 作業ディレクトリ]
  audit  --source 正本 --out 出力ディレクトリ

正本へは書き込まない（読み取りは mode=ro）。結果は JSON を標準出力へ出す。
build は、作り直した結果、本籍の行が一つも無くなった部品を作らず、すでにある部品なら出力先から消して
状態 DB と目録から外す（結果の removed_parts）。消すのは状態 DB の part_files に記録がある `.sqlite3` だけ。
build は、規則の版が状態 DB の記録と違う（または記録が無い）のに出力先が空でないとき、何も書かずに
標準エラーへ QUESTION: で始まる説明を出して終了コード 2 で終わる（古い版の部品を同じ場所に混ぜない）。
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src/python"))

from ikarchive.parts import PartsQuestion, audit, build_parts  # noqa: E402


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)
    build = sub.add_parser("build", help="部品を作る（初回は全部、以後は差分）")
    build.add_argument("--source", required=True)
    build.add_argument("--out", required=True)
    build.add_argument("--state", required=True)
    build.add_argument("--work-dir")
    check = sub.add_parser("audit", help="正本と全部品の和を照合する")
    check.add_argument("--source", required=True)
    check.add_argument("--out", required=True)
    args = parser.parse_args(argv)
    try:
        if args.command == "build":
            result = build_parts(args.source, args.out, args.state, args.work_dir)
            code = 0
        else:
            result = audit(args.source, args.out)
            code = 0 if result["ok"] else 1
    except PartsQuestion as exc:
        print(str(exc), file=sys.stderr)
        return 2
    json.dump(result, sys.stdout, ensure_ascii=False, indent=2)
    sys.stdout.write("\n")
    return code


if __name__ == "__main__":
    raise SystemExit(main())
