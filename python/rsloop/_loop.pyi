from asyncio import (
    AbstractEventLoop,
    Server,
    StreamReader,
    StreamWriter,
)
from collections.abc import Awaitable, Callable, Sequence
from typing import Any

__version__: str

class PyLoop(AbstractEventLoop):
    ...

async def start_server(
    client_connected_cb:Callable[[StreamReader, StreamWriter], Awaitable[None]] | Callable[[StreamReader, StreamWriter], None],
    host: str | Sequence[str] | None = None,
    port: int | str | None = None,
    *,
    limit: int = 65536,
    ssl_handshake_timeout: float | None = None,
    **kwds: Any
) -> Server:
    ...

async def open_connection(
    host: str | None = None,
    port: int | str | None = None,
    *,
    limit: int = 65536,
    ssl_handshake_timeout: float | None = None,
    **kwds: Any
) -> tuple[StreamReader, StreamWriter]:...
