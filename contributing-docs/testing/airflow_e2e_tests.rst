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

1. To Add new DAGs for E2E tests, you can add them to the dags folder in ``./airflow-e2e-tests/tests/dags`` and update
the DAG_IDS constant in ``./airflow-e2e-tests/tests/basic_tests/test_example_dags.py`` file to include the new DAGs ids.
This will trigger DAG execution and validate the DAG run successfully.

2. To add new test cases, create a new test file or add it any existing files in the ``./airflow-e2e-tests/tests/*``


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
