Executable Documentation PoC
============================

This file is the source of truth for developer workflows consumed by AI agents.

Running tests
-------------

.. agent-skill::
   :id: run_tests
   :description: Run tests locally first and fallback to Breeze
   :local: uv run --project distribution_folder pytest
   :fallback: breeze exec pytest

Running static checks
---------------------

.. agent-skill::
   :id: run_static_checks
   :description: Run static checks with local tools and fallback to Breeze
   :local: uv run --project airflow-core ruff check .
   :fallback: breeze exec prek run --stage pre-commit

Realistic Airflow Workflow Example
----------------------------------

.. agent-skill::
   :id: run_single_core_test
   :description: Run one airflow-core pytest target locally, fallback to Breeze when needed
   :local: uv run --project airflow-core pytest airflow-core/tests/cli/test_cli_parser.py -xvs
   :fallback: breeze run pytest airflow-core/tests/cli/test_cli_parser.py -xvs
