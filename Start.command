#!/bin/sh
cd "$(dirname "$0")" || exit 1
if ! command -v python3 >/dev/null 2>&1; then
  echo 'Python 3.10以上をインストールしてください: https://www.python.org/downloads/'
  read -r answer
  exit 1
fi
python3 scripts/setup_wizard.py
result=$?
if [ "$result" -ne 0 ]; then
  echo '開始できませんでした。上のエラーを確認してください。Enterで閉じます。'
  read -r answer
fi
exit "$result"
