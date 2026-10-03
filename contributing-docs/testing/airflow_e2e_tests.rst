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

Airflow End-2-End Tests
=======================

Airflow End-2-End (E2E) Tests are comprehensive tests that validate the entire Airflow system, These tests ensure that Airflow functions correctly by running real dags.
These E2E tests uses production docker image to run all Airflow components (scheduler, api-server, triggerer).

Running E2E Tests
-----------------

1. Ensure you have the prod image built locally. To build the prod image, you can run the following command:

.. code-block:: bash

    breeze prod-image build --python <python_version>

2. To run the Airflow E2E tests, you can use the following command:

.. code-block:: bash

    cd ./airflow-e2e-tests && uv run pytest ./tests/

3. Using breeze to run the E2E tests:

.. code-block:: bash

    breeze testing airflow-e2e-tests

    # Run with custom Docker image
    DOCKER_IMAGE=<replace-image> breeze testing airflow-e2e-tests


Adding new E2E Tests
--------------------

1. To add new Dags for E2E tests, you can add them to the dags folder in ``./airflow-e2e-tests/tests/dags`` and update
the DAG_IDS constant in ``./airflow-e2e-tests/tests/basic_tests/test_example_dags.py`` file to include the new Dag IDs.
This will trigger Dag execution and validate that the Dag runs successfully.

2. To add new test cases, create a new test file or add it to any existing files in ``./airflow-e2e-tests/tests/*``.


Language SDK modes
------------------

The ``go_sdk``, ``ts_sdk`` and ``java_sdk`` modes run Dags whose stub tasks run task handlers written in a Language
SDK. Run one with ``breeze testing airflow-e2e-tests --e2e-test-mode go_sdk``. Each builds its bundles in a
toolchain container, or with the host toolchain when ``LANG_SDK_NATIVE_TOOLCHAIN=true`` is set, as CI does.

Before the first test, the tests wait until the Dag processor has bound each stub task on a queue it routes to the
artifact that registers its task handler, and until the mode's Language SDK Dag files that fail to import are exactly
the ones that are meant to. Import errors in other files of the Dags folder, such as the stock example Dags, are not
checked. A Dag file with an import error is left out of the binding check. If that does not happen in time, the run
fails with the stub tasks that have no binding and the text of any import error nobody expected.

A Dag file that must fail goes in a test bundle, not in a user-facing example, which the tests pin. The Go test
bundle takes it in ``./airflow-e2e-tests/go-test-bundle/dags``, and the ``go_sdk`` mode copies every Dag file there.
The Java test bundle takes it in ``./airflow-e2e-tests/java-test-bundle/src/resources/dags``, and
``_setup_java_sdk_integration`` in ``conftest.py`` must also copy it, next to ``java_test_dags.py``, and add it to
``lang_sdk_dag_files``. An import error takes its whole Dag file, so keep one Dag file for each import error you
expect, and list the file in the ``expected_import_errors`` of the mode in ``conftest.py``.

Let the failing Dag file fail through its stub tasks, such as a task that no artifact registers. Do not make it fail
through an artifact the Dag processor cannot probe. A failed probe records no answer, so every parse of every Dag
file that uses its coordinator probes the artifact again, and every "no artifact registers it" error names it. That
breaks the check that a later parse probes nothing, and the exact import error text.

A new Language SDK mode joins ``LANG_SDK_E2E_MODES`` in ``constants.py``, records its ``lang_sdk_queues``,
``lang_sdk_dag_files`` and ``expected_import_errors`` in its setup in ``conftest.py``, and puts
``*LANG_SDK_E2E_SHARED_FILES`` in its file group in ``dev/breeze/src/airflow_breeze/utils/selective_checks.py``.

The same CI job also runs the unit tests that pack and probe the real example of its language, after the e2e tests.
To run one yourself, install the toolchain (Go, Node.js 22 with pnpm, or a JDK) and run:

.. code-block:: bash

    AIRFLOW_LANG_SDK_REAL_PROBE_TESTS=1 uv run --project airflow-core --with-editable shared/secrets_masker \
        pytest airflow-core/tests/unit/dag_processing/test_task_handler_processor_go.py

Without the toolchain, the test is skipped. In CI, where the ``CI`` variable is set, it fails instead.


Airflow E2E tests in CI
-----------------------

Airflow E2E tests are run in CI to ensure that the Airflow system is functioning correctly. Find the E2E test
workflow in ``.github/workflows/airflow-e2e-tests.yml``.

It uses prod image built in the previous step to run the tests.

After the test run, the logs are collected and stored as artifacts in the CI job with the name ``e2e-test-logs``.


Manually running E2E tests in CI
--------------------------------
To manually run the E2E tests in CI with workflow_dispatch event. use the ``Airflow E2E Tests`` workflow

Provide the image tag to be used for e2e tests. You can use any prod image tag or tag from the dockerhub. This can be useful to
test the e2e tests against rc candidate.
