#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPORT_DT="${1:-${REPORT_DT:-}}"
PYTHON_BIN="${PROJECT_ROOT}/.venv/bin/python"

if [[ -z "${REPORT_DT}" ]]; then
    echo "Usage: $(basename "$0") <YYYY-MM|YYYY-MM-DD>" >&2
    exit 1
fi

if [[ ! -x "${PYTHON_BIN}" ]]; then
    echo "Python executable not found: ${PYTHON_BIN}" >&2
    exit 1
fi

export PYTHONPATH="${PROJECT_ROOT}${PYTHONPATH:+:${PYTHONPATH}}"
exec "${PYTHON_BIN}" -m src.app "${REPORT_DT}"