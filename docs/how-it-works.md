# How It Works

This page explains the architecture without going too deep into Rust details.

## The short version

`rsloop` is a hybrid project:

- Python gives the package interface that users import
- Rust implements the core event loop and transport machinery
- PyO3 connects both sides

## Request flow

A simple mental model is:

1. Your Python code calls `rsloop.run(...)` or uses `rsloop.Loop`.
2. The Python wrapper creates or manages a native loop object.
3. The Rust extension schedules timers, callbacks, I/O, and transport work.
4. Your Python callbacks and coroutines still run as Python code.

So the project is not "Python replaced by Rust". It is "Python application code on top of a Rust event loop".

## The Python layer

The Python package lives in `python/rsloop/`.

Important files:

- `__init__.py`: exports the public API
- `_run.py`: defines `run(...)`, `new_event_loop()`, and the installable event
  loop policy
- `_loop_compat.py`: compatibility helpers and monkeypatches
- `_bootstrap.py`: startup helpers, including Windows DLL and SSL-related setup
- `_profile.py`: small Python wrappers around the profiler API

This layer is a thin adapter. It keeps the user-facing API pleasant while the heavy lifting happens in Rust.

## The Rust layer

The Rust code lives in `src/`.

Important files:

- `lib.rs`: extension module entry point
- `bindings/loop_api.rs`: exposes Rust functionality as Python classes and functions
- `engine/loop_core.rs`: core loop state and loop-thread execution
- `engine/commands.rs`: commands shared by the loop and runtime dispatcher
- `engine/dispatcher.rs`: coordination-thread runtime work
- `engine/callbacks.rs`: callback handles and scheduling helpers
- `transport/stream/`: stream transports, servers, fast streams, and I/O workers
- `transport/process/`: subprocess and pipe transport support
- `transport/tls/`: TLS configuration and certificate material
- `platform/fd/`: lower-level cross-platform descriptor work
- `context.rs`: running-loop and context management helpers
- `errors.rs`: shared error types
- `profiler.rs`: Tracy profiler support
- `rust_async.rs`: public Rust/Python async interop helpers for downstream extensions
- `async_event.rs`, `blocking.rs`, `python_names.rs`: support code used by the public pieces
- `platform/windows_vibeio.rs`: Windows-specific runtime support

You do not need to understand every file before using the project. For a first
pass, `lib.rs`, `bindings/loop_api.rs`, and `engine/loop_core.rs` are the most
useful entry points.

## Runtime model

Each loop currently uses two related execution contexts:

- a per-loop `vibeio` runtime lives on the thread running the Python event loop;
  it drives direct I/O while the loop is parked
- a dedicated Rust coordination thread dispatches loop commands, timers, and
  compatibility paths through a separate `vibeio` runtime

- Python tasks and callbacks still execute on the Python side
- plain TCP / Unix reads and non-TLS accepts run directly on `vibeio`
- generic descriptor watches and some TLS, write, and older transport paths
  still use helper threads

The separate coordination thread is transitional infrastructure. This hybrid
model explains why some paths run directly through the loop-thread reactor while
other paths still cross threads or use helper workers.

## Compatibility goal

The project tries to feel close to standard `asyncio`.

That is why the repository contains:

- compatibility logic in `_loop_compat.py`
- many tests for behavior that should match normal `asyncio`
- examples that use standard Python async patterns instead of a custom API style

## Current limitations

Some important limitations are already known:

- TLS support is narrower than CPython's OpenSSL-based `ssl` support
- encrypted private keys are not supported yet
- some TLS and transport paths still rely on helper threads
- `preexec_fn` for subprocesses is unsupported
- Unix sockets and signal handlers are naturally Unix-only

These are good things to know before using the project in production.
