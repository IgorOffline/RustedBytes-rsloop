"""Run the unittest suite with diagnostics for tests that stop making progress."""

from __future__ import annotations

import faulthandler
import os
import sys
import unittest
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]


def _traceback_interval() -> int:
    raw_value = os.environ.get("RSLOOP_TEST_TRACEBACK_INTERVAL_SECONDS", "60")
    try:
        interval = int(raw_value)
    except ValueError as exc:
        raise SystemExit(
            "RSLOOP_TEST_TRACEBACK_INTERVAL_SECONDS must be an integer"
        ) from exc
    if interval <= 0:
        raise SystemExit(
            "RSLOOP_TEST_TRACEBACK_INTERVAL_SECONDS must be greater than zero"
        )
    return interval


def main() -> int:
    os.chdir(ROOT_DIR)
    sys.path.insert(0, str(ROOT_DIR))
    faulthandler.enable()
    faulthandler.dump_traceback_later(_traceback_interval(), repeat=True)
    try:
        suite = unittest.defaultTestLoader.discover("tests")
        result = unittest.TextTestRunner(verbosity=2, buffer=False).run(suite)
    finally:
        faulthandler.cancel_dump_traceback_later()
    return 0 if result.wasSuccessful() else 1


if __name__ == "__main__":
    sys.exit(main())
