<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/licenses/LICENSE-2.0 -->

Breeze Contribution & Verification (Prototype)
=============================================

This document is the structured source for a small set of contributor workflows that AI tools can execute reliably
while respecting host vs Breeze boundaries.

The machine-readable definition lives in the `agent-skill` block below and is exported to `skills.json` by
`scripts/ci/prek/extract_breeze_contribution_skills.py`.

```agent-skill
[
  {
    "id": "static-checks",
    "title": "Run static checks (pre-commit)",
    "context": "host",
    "recommended": [
      "prek run --from-ref <target_branch> --stage pre-commit"
    ],
    "notes": [
      "Prefer running static checks on the host; fall back to Breeze only when required by missing system dependencies."
    ]
  },
  {
    "id": "unit-tests",
    "title": "Run targeted unit tests",
    "context": "host",
    "inputs": {
      "PROJECT": {
        "description": "Folder containing the pyproject.toml for the package to test (e.g. airflow-core, providers/amazon)."
      },
      "TESTS": {
        "description": "Pytest node id or path (e.g. airflow-core/tests/unit/...::test_name)."
      }
    },
    "recommended": [
      "uv run --project <PROJECT> pytest <TESTS> -xvs"
    ],
    "fallback": [
      "breeze run pytest <TESTS> -xvs"
    ],
    "notes": [
      "Use Breeze when uv tests fail due to missing system dependencies or when reproducing CI-only failures."
    ]
  },
  {
    "id": "context-detect",
    "title": "Detect host vs Breeze environment",
    "context": "either",
    "recommended": [
      "python scripts/ci/prek/breeze_context.py"
    ],
    "notes": [
      "Returns a short string indicating the current execution context (host or breeze)."
    ]
  }
]
```
