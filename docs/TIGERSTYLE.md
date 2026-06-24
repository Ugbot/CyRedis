# TigerStyle for CyRedis

← [README](../README.md)

CyRedis follows [TigerStyle](https://github.com/tigerbeetle/tigerbeetle/blob/main/docs/TIGER_STYLE.md)
— the safety-first engineering discipline from TigerBeetle — adapted for a
Python/Cython library that talks to Redis over a vendored C library. The goal
is software that is **correct, performant, and obvious**, in that order.

This is a living standard. New code must follow it; existing code is being
migrated module by module (the core client and C-interop paths first).

## Safety

**Assert relentlessly.** A function asserts what it requires and what it
guarantees. Aim for **two or more assertions per function** on average.
Assertions catch programmer error close to the source, turning silent
corruption into a loud, local failure.

- **Preconditions** — assert arguments at the top of the function.
- **Postconditions** — assert the result (and mutated invariants) before return.
- **Invariants** — assert relationships that must always hold (`self._in_use <=
  self._max_connections`, a reply length matching its declared size).
- **Assert the impossible.** If a branch "can't happen", assert it can't.
- **Pair assertions.** Check a fact at both the caller and the callee, and
  check both the positive and negative space (the thing that must be true *and*
  the thing that must be false).

**Assertions vs. error handling — a hard line:**

- `assert` is for **programmer error** (an impossible internal state). It is a
  bug detector, not control flow.
- **User/environment error is not an assertion.** Bad arguments from a caller,
  a refused TCP connection, a RESP error reply, a timeout — these `raise`
  explicit exceptions (`RedisError`, `ConnectionError`, `ValueError`). They are
  expected and must be handled in "negative space".

Note: Python strips `assert` under `-O`. We rely on asserts as an in-development
invariant net; anything that must hold in production is enforced with an
explicit `raise`, never with `assert` alone.

**Bound everything.**

- Every loop has an explicit, provable upper bound. No unbounded `while True`
  without a documented exit invariant and a fail-safe cap.
- Every buffer, queue, pool, and retry count has a fixed maximum. The
  connection pool is bounded by a semaphore; reply parsing is bounded by the
  declared element/byte counts; retries have a fixed ceiling.
- No unbounded recursion. RESP reply parsing recurses with depth proportional
  to nesting — assert a maximum nesting depth.

**Handle all errors.** Every C return code is checked. Every `NULL` reply is a
branch. Resources (replies, contexts, argv buffers) are freed on every path,
including error paths — use `try/finally`.

## Performance

- Think about the bottleneck first: network ≫ disk ≫ memory ≫ CPU. Batch round
  trips (pipelines), release the GIL around blocking C calls, and keep hot
  paths allocation-light.
- Do the napkin math before optimizing; measure, don't guess (project rule:
  benchmark, add to the suite — don't build ad-hoc tests).

## Developer experience

- **Functions ≤ 70 lines.** If it's longer, it's doing too much — split it.
- **Names are explicit.** No cryptic abbreviations. Encode units and
  qualifiers as suffixes: `timeout_seconds`, `wait_timeout`, `max_connections`,
  `reply_length`. A name should make the wrong usage look wrong.
- **Split compound conditions** into named intermediates when it aids reading.
- **Declare variables in the smallest scope**, close to first use.
- **No dead code, no TODOs, no placeholders.** Incomplete-but-working beats
  fake-complete (see the repo's core principles).
- Keep formatting consistent (`black`, `isort`); 88-column lines.

## Applying this to Cython

- Annotate C-level intent: `cdef`/`cpdef` types are part of the contract —
  assert the Python-level preconditions that the C signature can't express.
- Free hiredis `redisReply*` / `redisContext*` and `malloc`'d argv on every
  path. Assert non-NULL after a successful allocation; branch (raise) on the
  failure path.
- Release the GIL (`with nogil:`) around blocking socket calls, staging C
  buffers before and parsing the reply after.

## Checklist for a reviewed change

- [ ] Two or more assertions per non-trivial function (pre + post).
- [ ] `assert` only for impossible states; `raise` for caller/environment error.
- [ ] Every loop and buffer has an explicit bound.
- [ ] Every C resource freed on every path.
- [ ] Functions ≤ 70 lines; names carry units/qualifiers.
- [ ] No dead code; tests added to the suite (not ad-hoc), run with real data.
