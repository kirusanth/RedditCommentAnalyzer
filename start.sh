#!/bin/bash
ROOT="$(dirname "${BASH_SOURCE[0]}")"
cd "$ROOT"
source "$ROOT/tools/pyinstall.sh"
[[ $# -gt 0 ]] && exec "$@"
