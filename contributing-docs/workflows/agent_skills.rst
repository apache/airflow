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

Agent Skills Command Registry
==============================

.. agent-skill::
   :id: setup-breeze-environment
   :category: environment
   :description: Start the Airflow Breeze development environment
   :local: breeze start-airflow
   :breeze: echo "Already inside Breeze"
   :expected-output: Airflow webserver is ready

.. agent-skill::
   :id: run-static-checks
   :category: linting
   :description: Run fast static checks with prek before committing
   :local: prek run --from-ref {target_branch} --stage pre-commit
   :breeze: prek run --from-ref {target_branch} --stage pre-commit
   :prereqs: setup-breeze-environment
   :params: target_branch:required
   :expected-output: All checks passed.

.. agent-skill::
   :id: run-manual-checks
   :category: linting
   :description: Run slower manual checks with prek before opening a PR
   :local: prek run --from-ref {target_branch} --stage manual
   :breeze: prek run --from-ref {target_branch} --stage manual
   :prereqs: run-static-checks
   :params: target_branch:required
   :expected-output: All checks passed.

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

.. agent-skill::
   :id: run-db-test
   :category: testing
   :description: Run tests marked with @pytest.mark.db_test. Always uses Breeze — never try uv first.
   :local: breeze run pytest {test_path} -xvs
   :breeze: pytest {test_path} -xvs
   :prereqs: setup-breeze-environment
   :params: test_path:required
   :expected-output: passed

.. agent-skill::
   :id: format-and-lint
   :category: linting
   :description: Format and lint a Python file with ruff after writing or editing it
   :local: uv run --project {project} ruff format {file_path} && uv run --project {project} ruff check --fix {file_path}
   :params: project:required,file_path:required
   :expected-output: All checks passed

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
