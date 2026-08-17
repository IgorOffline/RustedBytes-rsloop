set shell := ["bash", "-euo", "pipefail", "-c"]
set windows-shell := ["powershell.exe", "-NoLogo", "-NoProfile", "-Command"]

benchmark-backend := if os() == "windows" { "winloop" } else { "uvloop" }
python := if os() == "windows" { "python" } else { "python3" }

tls-test-certs outdir="tests/fixtures/tls":
    uv run --no-project python scripts/generate_test_tls_certs.py {{outdir}}

fmt:
    uv run ruff format .
    cargo fmt --all

clippy:
    cargo clippy --all-targets --all-features -- -D warnings

test-rust:
    {{python}} scripts/run_rust_tests.py

test: tls-test-certs test-rust
    uv run python -u scripts/run_python_tests.py

test-frameworks:
    uv run --with uvicorn python tests/packages/uvicorn_test.py
    uv run --with daphne python tests/packages/daphne_test.py
    uv run --with hypercorn python tests/packages/hypercorn_test.py
    uv run --with mangum python tests/packages/mangum_test.py
    uv run --with granian --with litestar python tests/packages/litestar_granian_test.py
    uv run --with fastapi --with uvicorn python tests/packages/fastapi_test.py
    uv run --with starlette --with uvicorn python tests/packages/starlette_test.py
    uv run --with aiohttp python tests/packages/aiohttp_test.py
    uv run --with sanic python tests/packages/sanic_test.py
    uv run --with litestar --with uvicorn python tests/packages/litestar_test.py
    uv run --with django --with uvicorn python tests/packages/django_asgi_test.py
    uv run --with falcon --with uvicorn python tests/packages/falcon_test.py
    uv run --with quart --with hypercorn python tests/packages/quart_test.py
    uv run --with 'faststream[nats]' python tests/packages/faststream_test.py

bench-real-world:
    uv run --with {{benchmark-backend}} python benches/workload_matrix.py
