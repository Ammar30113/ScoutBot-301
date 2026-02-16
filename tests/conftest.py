"""Shared test fixtures and import shims."""

import sys
from unittest.mock import MagicMock

# If the 'ta' library is not installed (e.g., in CI without native deps),
# provide a mock so tests that don't directly use it can still run.
if "ta" not in sys.modules:
    ta_mock = MagicMock()
    sys.modules["ta"] = ta_mock
    sys.modules["ta.momentum"] = ta_mock.momentum
    sys.modules["ta.trend"] = ta_mock.trend
