 .. Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

 ..   http://www.apache.org/licenses/LICENSE-2.0

 .. Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

Executable Documentation PoC
============================

This file is the source of truth for developer workflows consumed by AI agents.

Running tests
-------------

.. agent-skill::
   :id: run-tests
   :description: Run Airflow tests using correct environment host or Breeze
   :local: uv run --project airflow-core pytest airflow-core/tests/cli/test_cli_parser.py -xvs
   :fallback: breeze run pytest airflow-core/tests/cli/test_cli_parser.py -xvs

Running static checks
---------------------

.. agent-skill::
   :id: run-static-checks
   :description: Run static checks first locally and fallback to Breeze
   :local: uv run --project airflow-core ruff check .
   :fallback: breeze run prek run --stage pre-commit

Realistic Airflow Workflow Example
----------------------------------

.. agent-skill::
   :id: run-single-core-test
   :description: Run one airflow-core pytest target with host first then Breeze fallback
   :local: uv run --project airflow-core pytest airflow-core/tests/cli/test_cli_parser.py -xvs
   :fallback: breeze run pytest airflow-core/tests/cli/test_cli_parser.py -xvs
