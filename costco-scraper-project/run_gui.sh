#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

if [[ -x "./venv/bin/python" ]]; then
  PYTHON_BIN="./venv/bin/python"
elif command -v python3 >/dev/null 2>&1; then
  PYTHON_BIN="python3"
else
  echo "Python 3 is required but was not found." >&2
  exit 1
fi

echo "Launching Costco GUI..."
"$PYTHON_BIN" costco_gui.py
