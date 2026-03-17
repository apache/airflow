 <!-- SPDX-License-Identifier: Apache-2.0
      https://www.apache.org/licenses/LICENSE-2.0 -->

# Breeze Contribution Agent Skills

This file defines reusable skills for AI agents working with Apache Airflow's Breeze environment.

## Skill: run-static-checks
```json
{
  "id": "run-static-checks",
  "name": "Run Static Checks",
  "description": "Run prek static checks (linting, formatting, type checks)",
  "commands": {
    "host": "prek run {--target module}",
    "breeze": "python -m pytest --doctest-modules {module}"
  },
  "preferred_context": "host",
  "parameters": {
    "module": {
      "type": "string",
      "required": false,
      "description": "Target module (e.g., airflow/api)"
    }
  },
  "prerequisites": [],
  "success_criteria": "exit_code == 0"
}
```

## Skill: run-unit-tests
```json
{
  "id": "run-unit-tests",
  "name": "Run Unit Tests",
  "description": "Run targeted unit tests with UV-first, Breeze-fallback strategy",
  "commands": {
    "host": "uv run --project airflow pytest {test_path}",
    "breeze": "breeze exec pytest {test_path}"
  },
  "preferred_context": "host",
  "parameters": {
    "test_path": {
      "type": "string",
      "required": true,
      "description": "Path to test file or directory"
    }
  },
  "prerequisites": ["run-static-checks"],
  "success_criteria": "exit_code == 0"
}
```

## Usage

These skills are consumed by:
1. AI agents (Claude Code, Gemini, etc.)
2. Human developers (for reference)
3. Generated skills.json (for machines)

To regenerate skills.json:
```bash
python scripts/ci/prek/validate_skills.py --fix
```

To check for drift:
```bash
python scripts/ci/prek/validate_skills.py --check
```
