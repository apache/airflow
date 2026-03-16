---
name: run-single-core-test
description: Run one airflow-core pytest target with host first then Breeze fallback
---

# Run Single Core Test

## When to use
Use this when validating Airflow changes with this workflow.

## Instructions

1. Detect execution context:
   - If running inside a Breeze container, use Breeze commands.
   - Otherwise, use local environment commands.

2. Execute:

### Local
uv run --project airflow-core pytest airflow-core/tests/cli/test_cli_parser.py -xvs

### Breeze
breeze run pytest airflow-core/tests/cli/test_cli_parser.py -xvs

3. If local execution fails due to missing dependencies, fallback to Breeze.

4. Return clear success/failure signals.
