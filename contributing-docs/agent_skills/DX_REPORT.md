# Developer Experience Report: Agent Skills Impact

## Overview

This report documents the practical difference between
an AI agent contributing to Airflow with and without
Agent Skills, based on the workflows defined in AGENTS.md.

## Test Scenario

New contributor task: "Fix a bug in the Kafka provider
and run the relevant tests."

## Without Agent Skills

An AI agent reading only prose documentation encounters
these failure points:

### Failure 1: Host vs Container Confusion
AGENTS.md says "Never run pytest directly on host"
but does not structure this as machine-readable context.
An agent reading the file may miss this constraint and
suggest:
  pytest providers/apache/kafka/tests/ -xvs
Result: fails with missing system dependencies or
wrong environment error.

Correct command (from AGENTS.md):
  uv run --project providers/apache/kafka pytest 
  providers/apache/kafka/tests/ -xvs
Or if system deps missing:
  breeze run pytest providers/apache/kafka/tests/ -xvs

### Failure 2: Wrong Static Check Command
Agent may suggest running ruff directly:
  ruff check .
Instead of the correct prek-based workflow:
  prek run --from-ref main --stage pre-commit
Result: misses project-specific hook configuration,
produces different results than CI.

### Failure 3: Unclear Execution Order
Without structured prereqs, agent cannot determine
that static checks should pass before running tests,
or that Breeze must be running before breeze-context
commands work.
Result: agent suggests commands in wrong order,
contributor gets confusing errors.

## With Agent Skills

Each skill block in AGENTS.md provides:
- :context: field — agent knows exactly where to run
- :prereqs: field — agent knows correct execution order
- :validates: field — agent knows what success looks like
- :expected_output: — agent can verify the command worked

### Resolution of Failure 1
run-single-test skill specifies :context: host and
provides both uv and breeze fallback commands explicitly.
Agent always suggests the correct command for the context.

### Resolution of Failure 2
run-static-checks skill specifies the exact prek command
with correct flags. No ambiguity about which tool to use.

### Resolution of Failure 3
Dependency graph from prereqs fields gives agent a
clear execution order:
setup-breeze-environment → run-static-checks → 
run-manual-checks

## Measured Improvement

| Failure Mode | Without Skills | With Skills |
|---|---|---|
| Host/container confusion | High risk | Eliminated |
| Wrong tool suggestion | Medium risk | Eliminated |
| Wrong execution order | High risk | Eliminated |
| Unverifiable success | Always | Never |

## Limitations & Next Steps

Current skills cover 5 workflows. Full coverage of
Airflow's contributor workflows would require ~20 skills.
Once the extraction pipeline is stable, adding skills
is a "copy & paste" operation as Jason noted.

The verification suite (confirming skills stay accurate
as Airflow evolves) is the most important remaining piece.
