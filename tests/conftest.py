from __future__ import annotations

import os
import sys


# When running via the `pytest` console script, Python sets `sys.path[0]` to the
# script location (e.g. `.venv/bin`) rather than the repo root. Ensure local
# top-level packages like `services/`, `core/`, `infra/` are importable.
_REPO_ROOT = os.path.dirname(os.path.dirname(__file__))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

