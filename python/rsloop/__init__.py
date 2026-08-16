from __future__ import annotations

# ruff: noqa: E402

from ._bootstrap import bootstrap as __bootstrap

__bootstrap()

from ._loop_compat import Loop
from ._loop_compat import __version__
from ._loop_compat import build_info
from ._loop_compat import reset_transport_stats
from ._loop_compat import transport_stats
from ._profile import profile
from ._profile import profiler_compiled
from ._profile import profiler_running
from ._profile import start_profiler
from ._profile import stop_profiler
from ._run import EventLoopPolicy
from ._run import install
from ._run import new_event_loop
from ._run import run
from ._run import uninstall

__all__: tuple[str, ...] = (
    "EventLoopPolicy",
    "Loop",
    "__version__",
    "build_info",
    "install",
    "new_event_loop",
    "profile",
    "profiler_compiled",
    "profiler_running",
    "run",
    "reset_transport_stats",
    "start_profiler",
    "stop_profiler",
    "transport_stats",
    "uninstall",
)
