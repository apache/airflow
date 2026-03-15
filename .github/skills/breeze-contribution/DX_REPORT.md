 <!-- SPDX-License-Identifier: Apache-2.0
      https://www.apache.org/licenses/LICENSE-2.0 -->

# Developer Experience Report: With vs Without Breeze Agent Skill

## The Problem This Skill Solves

Without this skill, AI agents treat Airflow like a generic Python project.
This report documents the exact failure modes and how the skill fixes them.

## Failure Mode 1: Wrong test command on host

WITHOUT skill:
    Agent runs: pytest airflow-core/tests/unit/test_dag.py
    Result: ModuleNotFoundError: No module named 'airflow'
    Agent is stuck. No context for what to do next.

WITH skill:
    Agent calls: get_command('run-tests', test_path='airflow-core/tests/unit/test_dag.py')
    Context detected: HOST
    Agent runs: uv run --project airflow-core pytest airflow-core/tests/unit/test_dag.py -xvs
    Result: Tests run correctly

## Failure Mode 2: Running breeze inside breeze

WITHOUT skill:
    Agent inside container runs: breeze shell
    Result: Hangs or throws Docker-in-Docker error
    Agent cannot recover

WITH skill:
    Agent calls: get_command('enter-breeze')
    Context detected: BREEZE
    Agent receives: ERROR: already inside Breeze container
    Agent stops and runs pytest directly instead

## Failure Mode 3: Git operations inside container

WITHOUT skill:
    Agent inside container runs: git push origin main
    Result: SSH key not available, credential error
    Agent cannot push

WITH skill:
    Agent calls: get_command('git-operations')
    Context detected: BREEZE
    Agent receives: ERROR: git operations must run on host
    Agent exits container first, then pushes

## Failure Mode 4: Skill drift (the maintenance problem)

WITHOUT sync mechanism:
    Breeze adds new command -> SKILL.md not updated -> agents use wrong commands
    Detected only when agent fails in production

WITH extract_agent_skills.py --check as prek hook:
    Contributor changes SKILL.md without updating skills.json -> prek fails
    Drift detected at commit time, not runtime

## Verification

Run context detection on host:
    python3 scripts/ci/prek/breeze_context_detect.py
    Expected: Context: HOST

Run drift check:
    python3 scripts/ci/prek/extract_agent_skills.py --check
    Expected: OK: skills.json is in sync with SKILL.md

Run full test suite:
    python3 -m pytest scripts/ci/prek/test_breeze_agent_skills.py -v
    Expected: 20 passed
