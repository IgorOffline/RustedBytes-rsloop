# Kani verification

rsloop uses [Kani](https://model-checking.github.io/kani/) as a formal,
bounded verification layer alongside Rust unit tests, proptests, and Python
integration tests. Kani proves properties over every input in each harness's
declared domain; it does not replace tests outside that domain.

The project currently pins Kani 0.67.0 in CI.

## What is proved

The proof harnesses live beside the private implementation they exercise and
are compiled only under `cfg(kani)`.

| Area | Claim |
| --- | --- |
| Write limits | Every `Option<usize>` input either produces ordered low/high water marks with the documented defaults or is rejected; deriving `4 * low` cannot overflow. |
| Write buffers | For arbitrary data up to 8 bytes and valid advances, `remaining`, `len`, and `is_empty` agree. Append and pool-retention decisions are exhaustive over `usize`. |
| Read buffers | An arbitrary `usize` consume count saturates at the end without overflow and leaves a valid buffer offset. |
| Separator scanning | For input lengths through 8 and every `usize` limit, a fruitless two-byte separator scan reports the exact asyncio offset and over-limit boundary. A focused harness checks all offsets 0 through 6 for the multi-byte search path. |
| Writer queue | The deterministic closed-sender and closed-receiver states return the correct result. FIFO ordering and adjacent-data coalescing remain covered by Rust tests because model checking `VecDeque` allocation is not tractable within CI memory. |
| Buffer pools | Six arbitrary acquire/release choices prove slot accounting for both the bounded read pool and fallback-enabled write pool. These proofs exercise the same allocation and release decisions as production without claiming mutex behavior. |

Kani's default panic, overflow, memory-safety, undefined-function, and
unwinding checks remain enabled. The project does not use
`--ignore-global-asm`, unchecked stubs, or disabled unwinding assertions to
make a proof pass.

## Explicit exclusions

Kani does not model real concurrency or I/O. Therefore these proofs make no
claim about:

- thread interleavings, mutex implementation, atomics, or wake-up ordering;
- sockets, files, subprocesses, signals, or operating-system calls;
- Python/PyO3 and CPython FFI behavior;
- TLS handshakes, record I/O, or cryptographic implementations;
- end-to-end asyncio scheduling.

Those behaviors remain covered by Rust tests, proptests, Python integration
tests, and the supported-framework matrix. A Kani warning about an unsupported
construct is acceptable only when the construct is unreachable from the proof;
reaching one fails verification.

## Running verification

Install Kani using its
[installation guide](https://model-checking.github.io/kani/install-guide.html),
then run:

```bash
just kani-list
just kani-core
just kani
```

`kani-core` is the pull-request gate. `kani` runs every harness and is used by
the nightly workflow. To audit whether assumptions have accidentally removed
important paths, run:

```bash
just kani-coverage
```

Source coverage is an audit aid, not a proof by itself.

For a failing harness, print a concrete counterexample without editing source:

```bash
cargo kani --harness <name> --concrete-playback=print
```

Reproduce a genuine defect with a normal Rust regression test before applying
the smallest production fix.

## Adding a proof

1. Put the harness in an adjacent `#[cfg(kani)] mod verification` module so it
   can exercise private production logic without exposing new APIs.
2. Prefix fast merge-gating harnesses with `core_`; use `extended_` for bounded
   state sequences that belong in the nightly suite.
3. Generate nondeterministic scalar values with `kani::any`. Use helpers from
   `src/verification.rs` for bounded collections.
4. Use `kani::assume` only for real API preconditions. Add `kani::cover` while
   developing a constrained harness to detect vacuous paths.
5. Give every reachable loop an explicit `#[kani::unwind]` bound and document
   the input or operation bound in the claim above. The manifest default of 1
   intentionally makes missing bounds fail.
6. Prefer an independent, simple oracle. If heap-backed symbolic data causes
   solver or memory blow-up, prove the scalar transition decisions in Kani and
   retain broad content/order equivalence in proptests.
7. Run the normal Rust tests and Clippy as well as both Kani suites.

Kani's [bounded-proof documentation](https://model-checking.github.io/kani/reference/bounded_arbitrary.html)
explains why a successful bounded harness must not be presented as a claim
about larger collections.
