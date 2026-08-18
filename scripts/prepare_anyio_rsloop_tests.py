"""Add the local rsloop event loop to AnyIO's upstream pytest matrix."""

from __future__ import annotations

import argparse
from pathlib import Path

RSLOOP_PARAMETER = """\
# Injected by rsloop's downstream compatibility workflow.
import rsloop

asyncio_params.append(
    pytest.param(
        (
            "asyncio",
            {"debug": True, "loop_factory": rsloop.new_event_loop},
        ),
        id="asyncio+rsloop",
    )
)

"""
INSERT_BEFORE = "backend_params = asyncio_params.copy()\n"


def add_rsloop_parameter(conftest: Path) -> None:
    source = conftest.read_text(encoding="utf-8")
    if 'id="asyncio+rsloop"' in source:
        return

    if source.count(INSERT_BEFORE) != 1:
        raise RuntimeError(
            f"Could not locate AnyIO's backend matrix anchor in {conftest}"
        )

    conftest.write_text(
        source.replace(INSERT_BEFORE, RSLOOP_PARAMETER + INSERT_BEFORE),
        encoding="utf-8",
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "anyio_checkout",
        type=Path,
        help="path to an AnyIO source checkout",
    )
    args = parser.parse_args()
    add_rsloop_parameter(args.anyio_checkout / "tests" / "conftest.py")


if __name__ == "__main__":
    main()
