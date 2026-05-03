"""Root conftest — makes data_integration/src importable as `src.*` in all tests."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "data_integration"))
