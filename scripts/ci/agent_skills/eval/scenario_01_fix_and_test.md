# Scenario 01: Fix a bug and verify the contribution

## Purpose
Verify that an AI agent using the Airflow agent skills can correctly
navigate the contribution workflow: detect context, run static checks,
run targeted tests.

## Setup
1. You are on the HOST machine (not inside Breeze).
2. You have made a trivial change to airflow/utils/dates.py
   (e.g. added a comment on line 1: # fixed).
3. Your task is to: stage the change, run static checks, run the
   relevant test, verify it passes.

## Step 1 — Detect context
Run:
  python scripts/ci/agent_skills/breeze_context.py

Expected output:
  Current context: host
  Available skills (N): [list of skills]

Pass criteria: output says "host", not "breeze".

## Step 2 — Get the static check command
Run:
  python scripts/ci/agent_skills/breeze_context.py run-static-checks

Expected: prints the prek command.
Pass criteria: command starts with "prek run", not "ruff" or "flake8".

## Step 3 — Get the test command
Run:
  python scripts/ci/agent_skills/breeze_context.py run-single-test \
    project=airflow test_path=tests/utils/test_dates.py

Expected: prints the uv run command.
Pass criteria: command starts with "uv run --project airflow",
not "pytest tests/" directly.

## Step 4 — Run the test
Run the command from Step 3.
Pass criteria: output ends with "passed" or "PASSED".
Fail criteria: "ModuleNotFoundError", "command not found",
"ImportError" → escalate to Breeze fallback (see run-single-test
fallback field in skills.json).

## Step 5 — Verify drift detection still works
Run:
  python scripts/ci/pre_commit/extract_agent_skills.py --check

Pass criteria: exits 0, prints "OK: skills.json is in sync".

## Scoring
| Step | Pass | Fail |
|------|------|------|
| 1 Context detection | correct env printed | wrong env or error |
| 2 Static check routing | prek command | ruff/flake8/wrong cmd |
| 3 Test routing | uv run --project | bare pytest |
| 4 Test execution | PASSED | error unrelated to code |
| 5 Drift check | exits 0 | exits 1 |

Score 5/5 = skills are working correctly.
Score < 4/5 = file a bug against the failing skill.

## Known gap
This scenario only covers host context. A companion scenario
(scenario_02_breeze_context.md) should cover the same steps
from inside a Breeze container. Not yet written.
