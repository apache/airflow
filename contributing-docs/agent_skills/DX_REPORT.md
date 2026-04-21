# Developer Experience Report: Agent Skills Impact

## Overview

This report replaces the previous hypothetical narrative with real terminal
output captured on a Windows / PowerShell host clone of the `feat/agent-skills-poc`
branch. Every command below was executed; no output is invented. Where a command
failed the failure is shown in full.

## Scenario

A contributor has modified `airflow/utils/dates.py` (old-style path an agent
would assume from historical docs) and needs to: stage changes, run static
checks, run the relevant tests.

Host environment at capture time:

```
PS> ruff --version
ruff 0.15.5

PS> prek --version
prek 0.3.5 (46cb7bad1 2026-03-09)

PS> Get-Command pytest
(no output — pytest is not on PATH)

PS> pytest --version
(no output)
PS> echo $LASTEXITCODE
1
```

## Without Agent Skills — three real failure modes

### Failure 1: Wrong test runner on host

Command the agent would run, taken at face value from historical contributor
docs:

```
PS> pytest tests/utils/test_dates.py -xvs
(no output)
PS> echo $LASTEXITCODE
1
```

The failure is silent — exit code 1, zero bytes on stdout and stderr, no error
message explaining why. The root cause is that `pytest` is not installed on
the host and `Get-Command pytest` returns nothing. An agent watching only
stdout receives no signal at all. The prose in AGENTS.md says "never run
pytest on the host" but that rule is not machine-readable, so the agent does
not catch itself before the command runs.

### Failure 2: Wrong static-check command

Command the agent would run:

```
PS> ruff check airflow/utils/dates.py
E902 The system cannot find the path specified. (os error 3)
  --> airflow\utils\dates.py:1:1

Found 1 error.
PS> echo $LASTEXITCODE
1
```

Two real problems surface at once:

1. The path `airflow/utils/dates.py` does not exist in the modern monorepo —
   the file actually lives at `airflow-core/src/airflow/utils/dates.py`. An
   agent trained on pre-monorepo docs will keep using the wrong path.
2. Even if the path were corrected, running `ruff` directly bypasses the
   `prek` / pre-commit hook configuration that CI uses. CI runs
   `prek run --from-ref main --stage pre-commit`, which applies project-pinned
   hook versions and ordering. Raw `ruff check` skips those, so local "pass"
   no longer implies CI pass.

### Failure 3: Wrong execution order

Without a structured prereqs chain an agent has no way to know that
`run-manual-checks` depends on `run-static-checks`, nor that all testing
skills depend on `setup-breeze-environment`. It will happily run manual
checks before fast static checks have passed, producing failures whose root
cause is an earlier skipped step. This report does not fabricate a
synthesized error message for this case — see the honest caveat in
*What is still missing*.

## With Agent Skills — resolution of each failure

### Resolution 1 — agent asks the skill runner for the right test command

```
PS> python scripts/ci/agent_skills/breeze_context.py run-single-test `
      project=airflow-core `
      test_path=airflow-core/tests/unit/utils/test_dates.py
# Primary: uv (no Docker needed, faster)
uv run --project airflow-core pytest airflow-core/tests/unit/utils/test_dates.py -xvs
# Fallback: only when system deps missing
# breeze run pytest airflow-core/tests/unit/utils/test_dates.py -xvs
PS> echo $LASTEXITCODE
0
```

The skill returns a correctly scoped, substituted command. `uv` handles the
transient Python environment, so the fact that bare `pytest` is not on PATH
stops being a problem. The fallback comment tells the agent when to escalate
to `breeze run` instead of guessing.

### Resolution 2 — agent asks for the static-check command

```
PS> python scripts/ci/agent_skills/breeze_context.py run-static-checks
prek run --from-ref main --stage pre-commit
PS> echo $LASTEXITCODE
0
```

No ambiguity. The same command CI runs.

Context detection on this host confirms the lookup is correct for the
environment:

```
PS> python scripts/ci/agent_skills/breeze_context.py
Current context: host
Available skills (9):
  setup-breeze-environment [environment]
  run-single-test [testing]
  run-static-checks [linting]
  run-manual-checks [linting]
  build-docs [documentation]
  run-provider-tests [testing]
  run-type-check [linting]
  run-tests-uv-first [testing]
  run-static-checks-prek [linting]
```

### Resolution 3 — prereqs chain from skill_graph.json

Edges currently encoded in `contributing-docs/agent_skills/skill_graph.json`:

| From                 | To                        | Type     |
|----------------------|---------------------------|----------|
| run-single-test      | setup-breeze-environment  | requires |
| run-static-checks    | setup-breeze-environment  | requires |
| run-manual-checks    | run-static-checks         | requires |
| build-docs           | setup-breeze-environment  | requires |
| run-provider-tests   | setup-breeze-environment  | requires |
| run-type-check       | setup-breeze-environment  | requires |

These edges give the agent a real topological hint:
`setup-breeze-environment` → `run-static-checks` → `run-manual-checks`.
Testing skills branch off `setup-breeze-environment` directly. This is a
partial answer — see *What is still missing*.

## Comparison table — what actually happened when the commands ran

| Failure mode                       | Without skills                                                 | With skills                                                  |
|------------------------------------|----------------------------------------------------------------|--------------------------------------------------------------|
| Wrong test runner on host          | `pytest` exits 1, zero output, agent has no signal             | Skill returns `uv run --project ... pytest ... -xvs`, exit 0 |
| Wrong static-check tool            | `ruff check airflow/utils/dates.py` → `E902` path error, exit 1| Skill returns `prek run --from-ref main --stage pre-commit`  |
| Wrong source path assumption       | `airflow/utils/dates.py` does not exist in the monorepo        | Agent passes `test_path=` explicitly; template substitutes it|
| Execution order                    | No machine-readable ordering                                   | Partial — graph encodes static→manual and setup→tests        |

## What is still missing — honest issues discovered while writing this report

1. **`run-static-checks-prek` entry in `skills.json` is corrupted.** Its
   `expected_output` field contains raw RST prose leaked from the source
   document — including HTML-escaped anchors, unrelated `.. code-block::`
   snippets, and sections like *Raising Pull Request*. The block is valid
   JSON but useless as an oracle for command success. The extractor's
   expected-output regex terminates too late and slurps the remainder of
   the RST file. This needs fixing in
   `scripts/ci/pre_commit/extract_agent_skills.py` before the skill can be
   used for verification.
2. **`skill_graph.json` is out of sync with `skills.json`.**
   `skills.json` has 9 skills; `skill_graph.json` has only 7 nodes.
   `run-tests-uv-first` and `run-static-checks-prek` are absent from the
   graph. The `generate-skill-graph` pre-commit hook either did not run
   after the last two skills were added or silently skipped the new ids.
3. **Execution-order coverage is partial.** No edge in the current graph
   says "testing skills must run after static checks pass" — only
   `run-manual-checks → run-static-checks` is encoded. Failure 3 above
   therefore cannot be fully demonstrated end-to-end with the current data;
   the graph needs additional `requires` edges before the agent can refuse
   to run tests on a dirty static-check tree.
4. **All 9 skills are marked `context: host`.** There is no skill currently
   marked `breeze` or `either`, even though `breeze_context.py` supports
   all three. The `:context:` field is therefore untested for the
   host/container-branching case the PoC exists to prove.
5. **Scenario path mismatch is itself part of the failure mode.** The
   contributor-typed path `tests/utils/test_dates.py` does not exist in
   the modern monorepo; there is no dedicated unit test file for
   `airflow-core/src/airflow/utils/dates.py` at all. An agent with skills
   still needs to *know the right path* — the skill template fills in what
   the caller supplies. Resolving path discovery is an open item for a
   future skill (for example, `locate-tests-for` that maps a source file
   to its canonical test path).

## Next steps

- Fix the `run-static-checks-prek` extractor bug so `expected_output` is
  bounded to the `.. agent-skill-expected-output::` block only.
- Regenerate `skill_graph.json` from the current `skills.json` and wire the
  `generate-skill-graph` hook to run on every `skills.json` change, not just
  on structural edits.
- Add at least one `context: either` and one `context: breeze` skill so the
  runtime branch in `breeze_context.py` is exercised by real data.
- Add a `locate-tests-for` skill (or similar) to close the path-discovery
  gap documented in Failure 2 and item 5 above.
