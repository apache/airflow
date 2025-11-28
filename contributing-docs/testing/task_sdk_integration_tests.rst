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

Task SDK Integration Tests
==========================

Task SDK Integration Tests are a specialized type of test that verify the integration between the
`Apache Airflow Task SDK <../../task-sdk/>`__ and a running Airflow instance (mainly the execution API server).
These tests ensure that the Task SDK can properly communicate with Airflow's execution API server
and that the integration between the two works correctly in a realistic environment.

What are Task SDK Integration Tests?
------------------------------------

The Task SDK Integration Tests differ from regular unit tests in several key ways:

**Purpose & Scope**
  These tests verify that the Task SDK package can successfully communicate with a complete Airflow
  environment, mainly the Airflow Execution API server. They test the actual
  integration points between the SDK and Airflow core, which is also known as Task Execution Interface.

**Real Environment Testing**
  Unlike unit tests that use mocks and stubs, these integration tests spin up actual Airflow services
  using Docker Compose to create a realistic testing environment that closely mirrors real deployments.

**Cross-Package Validation**
  Since the Task SDK is distributed as a separate Python package (``apache-airflow-task-sdk``),
  these tests ensure that the SDK works correctly with different versions of Airflow core and
  that API compatibility is maintained.

**End-to-End Communication**
  The tests verify complete request/response cycles between the Task SDK API client and Airflow's
  execution API, including authentication, API versioning, and error handling (some of it is yet to come).

Why Task SDK Integration?
-------------------------

**API Compatibility Assurance**
  The Task SDK communicates with Airflow through well defined APIs for task execution. These tests ensure that
  changes to either the SDK or Airflow core don't break the interface contract between the two.

**Real World Scenario Testing**
  While unit tests verify individual components work correctly, integration tests validate
  that the entire system works together as expected in deployment scenarios.

**Quicker Interface Issue resolution**
  These tests catch integration issues early in the development cycle, preventing breaking
  changes reaching a release.

**Version Compatibility Matrix**
  As both Airflow and the Task SDK evolve, these tests help ensure compatibility across
  different version combinations (to come soon).

Running Task SDK Integration Tests
----------------------------------

There are multiple ways to run Task SDK Integration Tests depending based on your preferences.

Using Breeze
............

The simplest way to run Task SDK Integration Tests is using Breeze, which provides CI like
reproducibility:

.. code-block:: bash

   # Run all Task SDK integration tests
   breeze testing task-sdk-integration-tests

   # Run specific test files
   breeze testing task-sdk-integration-tests task_sdk_tests/test_task_sdk_health.py

   # Run with custom Docker image
   DOCKER_IMAGE=my-custom-airflow-image:latest breeze testing task-sdk-integration-tests

Running in Your Current Virtual Environment
...........................................

Since you're already working in the Airflow repository, you can run Task SDK Integration Tests
directly:

**Run Tests**

.. code-block:: bash

   # Navigate to task-sdk-tests directory and run tests
   cd task-sdk-tests/
   uv run pytest -s

   # Run specific test file
   cd task-sdk-tests/
   uv run pytest tests/task_sdk_tests/test_task_sdk_health.py -s

   # Keep containers running for debugging
   cd task-sdk-tests/
   SKIP_DOCKER_COMPOSE_DELETION=1 uv run pytest -s

**Optional: Set Custom Docker Image**

.. code-block:: bash

   # Use a different Airflow image for testing
   cd task-sdk-tests/
   DOCKER_IMAGE=my-custom-airflow:latest uv run pytest -s


Debugging Failed Tests
......................

When tests fail, the logs from all running containers are automatically dumped to the console
and the Docker Compose deployment is shut down. To debug issues more effectively:

.. code-block:: bash

   # Run with maximum verbosity
   cd task-sdk-tests/
   uv run pytest tests/task_sdk_tests/ -vvv -s --tb=long

   # Keep containers running for inspection (local environment)
   cd task-sdk-tests/
   SKIP_DOCKER_COMPOSE_DELETION=1 uv run pytest tests/task_sdk_tests/test_task_sdk_health.py::test_task_sdk_health

   # Keep containers running for inspection (using Breeze)
   breeze testing task-sdk-integration-tests --skip-docker-compose-deletion

   # Inspect container logs (when containers are still running)
   cd task-sdk-tests/docker
   docker-compose logs airflow-apiserver
   docker-compose logs airflow-scheduler
   docker-compose logs postgres

   # Access running containers for interactive debugging
   docker-compose exec airflow-apiserver bash

.. tip::
   **Container Cleanup Control**: By default, the Docker Compose deployment is deleted after tests
   complete to keep your system clean. To keep containers running for debugging:

   - **Local environment**: Export ``SKIP_DOCKER_COMPOSE_DELETION=1`` before running tests
   - **Breeze environment**: Use the ``--skip-docker-compose-deletion`` flag

   Remember to manually clean up containers when done: ``cd task-sdk-tests/docker && docker-compose down -v``

Testing Custom Airflow Images
..............................

To test the Task SDK against custom Airflow builds:

.. code-block:: bash

   # Build your custom Airflow image first
   cd /path/to/airflow
   docker build -t my-custom-airflow:latest -f Dockerfile .

   # Use custom image for integration tests
   export DOCKER_IMAGE=my-custom-airflow:latest
   cd task-sdk-tests
   uv run pytest tests/task_sdk_tests/

Common Issues and Solutions
---------------------------

**Port Conflicts**
  If port 8080 is already in use, change the host port:

  .. code-block:: bash

     export HOST_PORT=localhost:9090
     export TASK_SDK_HOST_PORT=localhost:9090

**Memory Issues**
  These tests require sufficient memory for multiple containers. Ensure Docker has at least 4GB RAM allocated.

Files and Directories
---------------------

The Task SDK Integration Tests are organized as follows:

.. code-block::

   task-sdk-tests/
   ├── pyproject.toml                    # Test package configuration and dependencies
   ├── docker/
   │   └── docker-compose.yaml           # Airflow services configuration
   └── tests/
       └── task_sdk_tests/
           ├── conftest.py                # Test configuration and setup
           ├── constants.py               # Test constants and configuration
           ├── test_task_sdk_health.py    # Main integration test
           └── __init__.py

**Key Files:**

- **docker-compose.yaml**: Defines the complete Airflow environment (postgres, scheduler, api-server)
- **test_task_sdk_health.py**: Main test that verifies Task SDK can communicate with Airflow API
- **conftest.py**: Handles Task SDK installation and test environment setup
- **constants.py**: Configuration constants for Docker images, ports, and API versions

-----

For other kinds of tests look at `Testing document <../09_testing.rst>`__
