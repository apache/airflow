.. Licensed to the Apache Software Foundation (ASF) under one
   or more contributor license agreements.  See the NOTICE file
   distributed with this work for additional information
   regarding copyright ownership.  The ASF licenses this file
   to you under the Apache License, Version 2.0 (the
   "License"); you may not use this file except in compliance
   with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing,
   software distributed under the License is distributed on an
   "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
   KIND, either express or implied.  See the License for the
   specific language governing permissions and limitations
   under the License.

.. NOTE: This file is the RST-format equivalent of the YAML workflow files in this
   directory. It is a PoC for embedding agent skills directly in contributor
   documentation, following the long-term direction of using ``contributing-docs/*.rst``
   as the source of truth (as suggested by Jarek in issue #62500).
   The YAML files remain the current source of truth; this file is a comparison artifact.

Agent Skills for Airflow Contributors
======================================

This document defines the executable workflows that AI agents use when helping
contributors work on the Apache Airflow repository. Each skill maps to a specific
contributor task and encodes the correct commands for both host and Breeze environments.

These skills are parsed from this file and compiled into
``contributing-docs/agent_skills/skills.json`` by ``scripts/ci/prek/generate_agent_skills.py``.

.. _agent-skill-format:

Skill Format
------------

Each skill uses the ``.. agent-skill::`` directive with the following options:

- ``:id:`` — stable identifier (must be unique)
- ``:category:`` — one of ``environment``, ``testing``, ``linting``, ``documentation``
- ``:description:`` — one-sentence summary of what the skill does
- ``:local:`` — command to run on the host machine
- ``:breeze:`` — command to run inside the Breeze container (optional)
- ``:prereqs:`` — comma-separated list of skill IDs that must run first (optional)
- ``:fallback:`` — command to use when ``:local:`` cannot run (e.g., missing system deps)
- ``:params:`` — comma-separated list of ``name:required`` pairs (optional)
- ``:expected-output:`` — string that signals success (optional)

Setting Up the Environment
--------------------------

Start the Airflow Breeze development environment before running tests or checks.

.. agent-skill::
   :id: setup-breeze-environment
   :category: environment
   :description: Start the Airflow Breeze development environment
   :local: breeze start-airflow
   :breeze: echo "Already inside Breeze"
   :expected-output: Airflow webserver is ready

When running database integration tests that require Postgres, pass ``--backend postgres``
to the ``breeze start-airflow`` command instead.

Running Static Checks
---------------------

Always run fast static checks before committing to catch formatting, import, and
type errors early.

.. agent-skill::
   :id: run-static-checks
   :category: linting
   :description: Run fast static checks with prek before committing
   :local: prek run --from-ref {target_branch} --stage pre-commit
   :breeze: prek run --from-ref {target_branch} --stage pre-commit
   :prereqs: setup-breeze-environment
   :params: target_branch:required
   :expected-output: All checks passed.

The ``target_branch`` parameter is the branch the PR will be merged into — usually
``main``, but may be ``v3-1-test`` for patch releases.

Running Manual Checks
---------------------

Run slower, more thorough checks before opening a PR. These are not run on every
commit because they take longer.

.. agent-skill::
   :id: run-manual-checks
   :category: linting
   :description: Run slower manual checks with prek before opening a PR
   :local: prek run --from-ref {target_branch} --stage manual
   :breeze: prek run --from-ref {target_branch} --stage manual
   :prereqs: run-static-checks
   :params: target_branch:required
   :expected-output: All checks passed.

Running a Single Test
---------------------

Run a targeted test for a changed module or provider. Try uv first (faster,
debuggable in IDE); fall back to Breeze when system dependencies are missing.

.. agent-skill::
   :id: run-single-test
   :category: testing
   :description: Run a targeted test for a changed module or provider
   :local: uv run --project {project} pytest {test_path} -xvs
   :breeze: pytest {test_path} -xvs
   :fallback: breeze run pytest {test_path} -xvs
   :prereqs: setup-breeze-environment
   :params: project:required,test_path:required
   :expected-output: passed

The ``project`` parameter is the path to the distribution folder containing
``pyproject.toml`` (e.g. ``airflow-core``, ``providers/amazon``).
The ``fallback`` command is used on the host when system dependencies such as
``mysql`` or ``kubernetes`` libraries are not available locally.

Building Documentation
----------------------

Build Airflow documentation locally inside Breeze.

.. agent-skill::
   :id: build-docs
   :category: documentation
   :description: Build Airflow documentation locally inside Breeze
   :local: breeze build-docs
   :breeze: breeze build-docs
   :fallback: breeze build-docs --package-filter {package}
   :prereqs: setup-breeze-environment
   :params: package:optional
   :expected-output: Build finished.

Pass a ``package`` parameter (e.g. ``apache-airflow-providers-amazon``) to build
only that package's docs, which is significantly faster than building all docs.

.. rubric:: Format comparison note

The YAML workflow files in this directory (``*.yaml``) and this RST file represent
two alternative source formats for the same agent skills. Key differences:

.. list-table::
   :header-rows: 1
   :widths: 30 35 35

   * - Feature
     - YAML (``*.yaml``)
     - RST (this file)
   * - Source of truth
     - One file per skill
     - All skills in one doc
   * - Human readability
     - High (dedicated file)
     - High (embedded in prose)
   * - Prose context
     - None
     - Full narrative around each skill
   * - Aligns with Airflow docs
     - No
     - Yes (``contributing-docs/``)
   * - Conditional steps
     - ``condition:`` per step
     - ``:fallback:`` option
   * - Rich parameters
     - Full schema with defaults
     - Compact ``name:required`` pairs
