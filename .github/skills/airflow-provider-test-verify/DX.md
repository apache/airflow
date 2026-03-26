 <!-- SPDX-License-Identifier: Apache-2.0
      https://www.apache.org/licenses/LICENSE-2.0 -->

<!--
  Developer Experience Report
  Task: Create branch → run vertica provider tests → commit if passing
  Date: 2026-03-26
  Branch: test/vertica-provider-tests (from feat/gsoc-agent-skills-yaml-workflows)
-->

# Developer Experience: Vertica Provider Test Verification

## Task Given

> Create a branch from the current branch, check if all tests are passing for
> the vertica provider, if yes add a comment and make a commit.

---

## What I Did

### 1. Oriented myself

Before touching anything I checked two things:

- Which branch I was on (`feat/gsoc-agent-skills-yaml-workflows`) — confirmed
  by `git branch --show-current`.
- Where the vertica tests actually live — a glob over `providers/vertica/**/*.py`
  showed unit tests at `providers/vertica/tests/unit/vertica/hooks/`.

Neither of these needed the SKILL.md; they were orientation steps. The SKILL.md
became useful once I knew what I was dealing with.

### 2. Created the branch

```bash
git checkout -b test/vertica-provider-tests
```

Straightforward. The branch inherits the full history of the source branch.

### 3. Decided how to run the tests

I grepped for `@pytest.mark.db_test` across the entire
`providers/vertica/tests/` tree. No matches. That meant:

- No live database needed.
- `uv run --project providers/vertica pytest` is the right path.
- No need to start Breeze at all.

This was the key decision gate. Getting it wrong would mean either wasting time
spinning up Breeze for tests that don't need it, or trying uv and hitting a
database error that looks like a test failure but isn't.

### 4. Ran the tests

```bash
uv run --project providers/vertica pytest providers/vertica/tests/unit/ -xvs
```

First run needed to download and build several packages (pandas, numpy,
polars-runtime). Subsequent runs would be fast because uv caches them.

Result: **26 passed, 1 warning in 31.92s**. No failures, no errors.

### 5. Added the comment

Opened `test_vertica.py`, placed a single comment line after the Apache license
block and before `from __future__ import annotations`:

```python
# All 26 vertica provider unit tests verified passing on 2026-03-26 (branch: test/vertica-provider-tests).
```

Placement matters: after the license (so prek's license checker stays happy)
and before the imports (so it reads as a module-level note, not attached to
a function or class).

### 6. Formatted with ruff — hit a pitfall

First attempt:

```bash
uv run ruff format providers/vertica/tests/...
```

This blew up — uv without `--project` resolved the **root workspace**, which
pulled in `mysqlclient`, `teradatasql`, and dozens of other heavy packages.
`mysqlclient` failed to build because `pkg-config` couldn't find the MySQL
client libraries on the host.

Fix: scope ruff to the provider project, exactly as pytest was scoped:

```bash
uv run --project providers/vertica ruff format <file>
uv run --project providers/vertica ruff check --fix <file>
```

Result: `1 file left unchanged` / `All checks passed!`

The rule this revealed: **always pass `--project providers/{name}` for any `uv run`
command when working on a single provider**. The root workspace resolves every
package in the monorepo, including ones with native system dependencies.

### 7. Committed

```bash
git add providers/vertica/tests/unit/vertica/hooks/test_vertica.py
git commit -m "Vertica provider: all 26 unit tests verified passing"
```

prek ran ~100 hook checks automatically. All passed or were skipped (correctly,
since only one file changed). Commit hash: `fc36fc5dd1`.

---

## Goals Achieved

| Goal | Status |
|---|---|
| Branch created from `feat/gsoc-agent-skills-yaml-workflows` | Done — `test/vertica-provider-tests` |
| Vertica provider tests run | Done — 26 passed, 0 failed |
| Comment added if tests pass | Done — added to `test_vertica.py:18` |
| Commit made | Done — `fc36fc5dd1` |

---

## What Worked Well

- **Scoping `uv` to the provider project** kept the environment lean. The tests
  ran in ~32 seconds with no Breeze startup cost.
- **Checking for `db_test` first** avoided a false start. If I had jumped
  straight to Breeze the result would have been correct but much slower.
- **prek ran automatically on commit** — no need to remember to call it
  separately for a one-file change.

---

## Friction Points

### 1. `uv run ruff` without `--project` resolves the entire workspace

Running `uv run ruff` without `--project` triggers a full monorepo dependency
resolution, which fails on hosts that lack MySQL client libraries. The fix is
always to scope `uv run` to the specific provider. This is not obvious from
the CLAUDE.md coding standards, which just say "run ruff" without mentioning
the scoping requirement.

### 2. `SKILL.md` does not mention `skills.json` or `skill_graph.json`

This is the more significant structural gap.

The repository has two machine-readable files that encode the same workflow
knowledge as the human-readable `SKILL.md`:

| File | Location | What it contains |
|---|---|---|
| `skills.json` | `contributing-docs/agent_skills/skills.json` | Skill IDs, categories, prereqs, step commands per context, fallback conditions, expected outputs |
| `skill_graph.json` | `contributing-docs/agent_skills/skill_graph.json` | Dependency graph — nodes (skills) and edges (which skill `requires` which) |

**What these files already encode that I derived by own reasoning:**

- `run-single-test` skill: `uv run --project {project} pytest {test_path} -xvs`
  with `condition: system_deps_available` and a `breeze run pytest` fallback.
- `run-db-test` skill: always `breeze run pytest`, never uv — encoded as a
  separate skill with no uv step at all.
- Prereq graph: `run-single-test → requires → setup-breeze-environment`,
  meaning Breeze must be running before any test skill executes (even if the
  uv path doesn't actually need it on a host with deps available).

**What actually happened during the task:**

At step 3 (deciding how to run tests), my trace logged:

```json
{ "source_used": "SKILL.md", "source_section": "Running Tests - Step 1: Determine the test type" }
```

But I never consulted `skills.json`. I re-derived the `db_test` detection
logic from the human prose. Had I known `skills.json` existed, I could have
looked up skill `run-single-test` vs `run-db-test` directly and resolved
the exact command template (`uv run --project {project} pytest {test_path} -xvs`)
and its fallback from structured data instead of reading prose.

Similarly, at the ruff step I logged `"source_used": "own_reasoning"` because
SKILL.md has no ruff scoping guidance. `skills.json` also doesn't cover ruff —
but the point is: the agent has no pointer to even look there.

**The fix:**

`SKILL.md` should include a section that tells agents:

> Before executing any step, look up the canonical skill ID in
> `contributing-docs/agent_skills/skills.json`. Use the `id`, `steps`,
> `parameters`, and `expected_output` fields as the authoritative source
> for command templates. Check `skill_graph.json` for prereqs — if a skill
> has a `requires` edge, verify that prereq skill is satisfied before running.

Without this pointer, agents reading only `SKILL.md` will re-derive what
`skills.json` already knows, log `own_reasoning` as the source, and
potentially get the command wrong (as happened with the ruff scoping failure).
