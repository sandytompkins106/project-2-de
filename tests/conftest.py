"""Root conftest — makes data_integration importable in all tests."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "data_folders"))
