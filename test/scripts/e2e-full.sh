#!/usr/bin/env bash
# Public REST/SDK journey against an already deployed, isolated Cellbox server.
set -euo pipefail
root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)
exec uv run --no-project python "$root/test/e2e/full.py" "$@"
