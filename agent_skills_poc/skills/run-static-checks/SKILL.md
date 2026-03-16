---
name: run-static-checks
description: Run static checks first locally and fallback to Breeze
---

# Run Static Checks

## When to use
Use this when validating Airflow changes with this workflow.

## Instructions

1. Detect execution context:
   - If running inside a Breeze container, use Breeze commands.
   - Otherwise, use local environment commands.

2. Execute:

### Local
uv run --project airflow-core ruff check .

### Breeze
breeze run prek run --stage pre-commit

3. If local execution fails due to missing dependencies, fallback to Breeze.

4. Return clear success/failure signals.
