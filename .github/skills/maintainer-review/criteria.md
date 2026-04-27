<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/licenses/LICENSE-2.0 -->

# Review criteria — quick reference

This file is a **summary checklist** of the project's review
criteria. It is not authoritative — the source files are:

- [`.github/instructions/code-review.instructions.md`](../../../.github/instructions/code-review.instructions.md) — the
  rule set every Apache Airflow PR is reviewed against.
- [`AGENTS.md`](../../../AGENTS.md) — repo-wide AI/agent
  instructions, coding standards, testing standards, commit /
  PR conventions, the security model summary.
- Per-area `AGENTS.md` files, picked up automatically when the
  PR touches code under their tree:
  - [`registry/AGENTS.md`](../../../registry/AGENTS.md)
  - [`dev/AGENTS.md`](../../../dev/AGENTS.md)
  - [`dev/ide_setup/AGENTS.md`](../../../dev/ide_setup/AGENTS.md)
  - [`providers/AGENTS.md`](../../../providers/AGENTS.md)
  - [`providers/elasticsearch/AGENTS.md`](../../../providers/elasticsearch/AGENTS.md)
  - [`providers/opensearch/AGENTS.md`](../../../providers/opensearch/AGENTS.md)
  - …and any other `AGENTS.md` discovered under a touched path
    (the per-PR review flow re-runs `git ls-files
    "<touched-tree>/**/AGENTS.md"`).
- Provider docs and shared-library READMEs when the PR touches
  one of them — the relevant boundaries are usually documented
  inline.

When you find a violation, **quote the rule from the source
file** in the review finding (with file:line), not from this
summary. Do not invent rules; do not soften documented ones. If
the rules conflict (rare — usually a per-area `AGENTS.md`
specialising a repo-wide rule), the more specific one wins,
**provided the maintainer explicitly accepts the override** in
the per-PR confirmation step. Surface the conflict.

---

## Categories at a glance

The headings below mirror the structure of the source files so
you can map findings 1:1.

### Architecture boundaries

- Scheduler must never run user code; it processes serialized
  Dags only.
- Workers must never access the metadata DB directly — they go
  through the Execution API.
- Dag Processor / Triggerer have direct DB access and bypass JWT
  via in-process Execution API transport. **This is intentional
  per the documented security model**, not a vulnerability —
  flag any code that violates the documented security model, not
  the documented design.
- Providers must not import core internals (`SUPERVISOR_COMMS`,
  task-runner plumbing). Providers interact through public SDK
  and Execution API only.

### Database / query correctness

- N+1 queries: SQLAlchemy relationship access inside a loop
  without `joinedload()` / `selectinload()`.
- Queries on `run_id` without `dag_id` (run_id is unique only
  per DAG).
- `session.commit()` inside a function in `airflow-core` that
  receives a `session` parameter (callers manage the
  transaction, not callees).
- `session` parameter that is not keyword-only (`*, session`)
  in `airflow-core`.
- Database-specific SQL (LATERAL joins, PostgreSQL-only or
  MySQL-only syntax) without cross-DB handling.

### Code quality

- `assert` in non-test code (stripped under `python -O`).
- `time.time()` for measuring durations (use `time.monotonic()`).
- Imports inside function/method bodies (top-of-file only,
  except: circular-import workaround, lazy load for worker
  isolation, `TYPE_CHECKING` blocks).
- `@lru_cache(maxsize=None)` (unbounded cache).
- Heavy imports (e.g. `kubernetes.client`) in multi-process code
  paths not behind `TYPE_CHECKING`.
- Files / connections / sessions opened without a context
  manager or `try/finally`.
- Raising the broad `AirflowException` directly — prefer
  dedicated exception classes or stdlib exceptions
  (`ValueError`, etc.).
- New files without the Apache License header (prek enforces
  this; flag if missing).

### Testing

- New public method or behavior without corresponding tests.
- `unittest.TestCase` subclass (use pytest patterns).
- `mock.Mock()` / `mock.MagicMock()` without `spec` / `autospec`.
- `time.sleep` / `datetime.now()` in tests (use `time_machine`).
- Issue numbers in test docstrings (test names should describe
  behavior, not track tickets).
- For bug-fix PRs: missing regression test (a test that fails
  without the fix, passes with it).
- Existing tests modified to accommodate new behavior — may
  indicate a regression rather than a fix.
- `caplog` in tests — prefer asserting on logic, not log output
  (per `AGENTS.md`).
- Newsfragments — only on major / breaking changes; flag if
  added unnecessarily.

### API correctness

- Queries on mapped task instances missing a `map_index` filter.
- Execution API change without a Cadwyn version migration
  (CalVer format).

### UI (React/TypeScript)

- `useState + useEffect` to sync derived state — use nullish
  coalescing or nullable override patterns.
- Logic copy-pasted across components — extract into a custom
  hook.

### Generated files

- Manual edits to `openapi-gen/` / Task SDK generated models —
  must be regenerated, not hand-edited.

### AI-generated code signals

- Fabricated diffs (changes to nonexistent files / paths).
- Unrelated files included.
- Description doesn't match the code.
- Claims of fixes without test evidence; "I cannot run the test
  suite" admissions.
- Over-engineered solutions (caching layers, complex locking,
  benchmark scripts where the problem is misunderstood).
- Narrating comments (`# Add the item to the list` before
  `list.append(item)`).
- Empty PR descriptions / boilerplate template only.

---

## Provider-specific signals

When the PR touches `providers/<name>/`, additionally check:

- **Provider boundary**: the provider does not import core
  internals (`from airflow.jobs.*`, `from
  airflow.dag_processing.*`, `from airflow.models.dagbag`, etc.
  outside `TYPE_CHECKING`). Providers depend on `airflow.sdk`
  and the public Execution API only.
- **Compat layer**: changes that need to work across multiple
  Airflow core versions go through
  `providers/common/compat/`, not direct version branches in
  the provider source.
- **`pyproject.toml`** changes: dependency bumps need an
  accompanying `provider.yaml` regen if the dep version is
  pinned there (look for `airflow.providers_manager`
  metadata).
- **Provider-specific `AGENTS.md`** if present (e.g.
  `providers/elasticsearch/AGENTS.md`,
  `providers/opensearch/AGENTS.md`) — quote any rule from there
  that the diff violates.

---

## Security model — calibration

When you spot something that looks security-flavoured, check
against the documented security model
([`airflow-core/docs/security/security_model.rst`](../../../airflow-core/docs/security/security_model.rst))
**before** flagging:

1. **Actual vulnerability** — code violates the documented
   model (e.g. a worker gaining DB access; an unauthenticated
   user reaching a protected endpoint). Flag as a blocking
   finding.
2. **Known limitation** — already-documented gap (DFP/Triggerer
   DB access, shared Execution API resources, multi-team
   gaps). **Do not flag as a new finding** — it's intentional.
3. **Deployment hardening** — measure the deployment manager
   could take (per-component config, asymmetric JWT keys,
   network policies). Belongs in deployment guidance, not as
   a code finding.

The `AGENTS.md` "Security Model" section calls this out
explicitly; quote it when downgrading what looked like a
finding.

---

## Backports and version-specific PRs

Branch `vX-Y-test` PRs are backports of already-merged `main`
work. The review bar is:

- **Diff parity**: does this match what was merged on `main`?
- **Cherry-pick conflicts**: any resolution introduced new
  changes that need scrutiny?
- **API/migration version markers**: backports should not
  introduce new Cadwyn version bumps; if they do, that's a
  finding.

For these PRs, prefer `COMMENT` over `REQUEST_CHANGES` unless
the cherry-pick has clearly drifted from the `main` change.

---

## When in doubt — defer

If after reading the diff you're not sure whether something is a
finding or just a style preference, **do not flag it**. Surface
the uncertainty to the maintainer (one line: *"Hmm — line N
does X, which I'm not sure violates the rules; flagging for
your eye."*) and let them decide. The cost of an over-zealous
auto-finding is a contributor who feels nitpicked; the cost of
a missed nit is one round of back-and-forth a maintainer can
catch easily on their own pass.
