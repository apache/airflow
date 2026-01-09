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

Prerequisite - build PROD image
...............................

.. note::

   The task-sdk integration tests are using locally built production images started in docker-compose by
   Pytest. This means that while the tests are running in the environment that you start it from (usually
   local development environment), you need to first build the images that you want to test against.

You also need to make sure that your assets are built first.
.. code-block:: bash

   # From the Airflow repository root
   breeze ui compile-assets

Then, you should build the base image once before running the tests. You can do it using Breeze:

.. code-block:: bash

   # From the Airflow repository root
   breeze prod-image build --python 3.10

The first build may take a while as it needs to download base image, build Python, install dependencies
and set up the environment. Subsequent builds will be much faster as they will use cached layers.

You can choose other Python versions supported by Airflow by changing the ``--python`` argument.

If you use ``breeze`` to run the integration tests and you do not have the image built before,
``breeze`` will prompt you to build it, and the building will proceed automatically after 20 seconds
if you do not answer ``no``.

This will build the right image ``ghcr.io/apache/airflow/main/prod/python3.10.latest`` (with the right
Python version) that will be used to run the tests. The ``breeze prod image build`` command by default -
when run from sources of airflow - will use the local sources and build the image using ``uv``
to speed up the build process. Also, when building from sources it will check if the assets are built
and will error if they are not. However it will not check if the assets are up to date - so make sure
to run the ``breeze ui compile-assets`` command above if you have changed any UI sources
and did not build your assets after that.

.. tip::

    Note that you do not need to rebuild the image every time you run the tests and change Python sources -
    because the docker-compose setup we use in tests will automatically mount the local Python sources into the
    container, so you can iterate quickly without rebuilding the image. However, if you want to test changes
    that require new image (like modifying dependencies, system packages, rebuilding UI etc.) you will need
    to rebuild the image with the ``breeze prod image build`` command.

After you build the image, there are several ways to run Task SDK Integration Tests,
depending based on your preferences. The ways are listed below.

Using Breeze
............

The simplest way to run Task SDK Integration Tests is using Breeze, which provides CI like
reproducibility

.. code-block:: bash

   # Run all Task SDK integration tests
   breeze testing task-sdk-integration-tests

   # Run specific test files
   breeze testing task-sdk-integration-tests task_sdk_tests/test_task_sdk_health.py

   # Run with custom Docker image
   DOCKER_IMAGE=my-custom-airflow-image:latest breeze testing task-sdk-integration-tests

Using uv
........

Since you're already working in the Airflow repository, you can run Task SDK Integration Tests directly.
Make sure you have ``uv`` installed in your environment and make sure to run all these commands
in the ``task-sdk-integration-tests`` directory. You can also run ``uv sync`` in the directory
first to make sure that your virtual environment is up to date.

.. code-block::

    # Navigate to task-sdk-integration-tests directory
    cd task-sdk-integration-tests

    # Sync dependencies
    uv sync

All the ``uv`` and ``docker compose`` commands below should be run from within the
``task-sdk-integration-tests`` directory.

**Run Tests**

.. code-block:: bash

   # Navigate to task-sdk-integration-tests directory and run tests
   uv run pytest -s

   # Run specific test file
   uv run pytest tests/task_sdk_tests/test_task_sdk_health.py -s

**Optional: Set Custom Docker Image**

.. code-block:: bash

   # Use a different Airflow image for testing
   DOCKER_IMAGE=my-custom-airflow:latest uv run pytest -s

By default when you run your tests locally, the Docker Compose deployment is kept between the sessions,
your local sources are mounted into the containers and the Airflow services are restarted automatically with hot reloading when any Python sources change.

This allows for quick iterations without rebuilding the image or restarting the containers.

Generated .env file
...................

When you run the tests an .env file is generated in the task-sdk-integration-tests directory.
This file contains environment variables used by docker-compose to configure the services.
You can inspect or modify this file if you need to change any configurations so that you can
also debug issues by running ``docker compose`` commands directly.

When running the tests with ``VERBOSE=1`` environment variable set or ``--verbose`` flag passed to breeze command,
the docker-compose commands used to start the services are also printed to the console and you can copy
them to run them directly.

Stopping docker-compose
.......................

When you finish testing (or when you updated dependencies and rebuild your images),
you likely want to stop the running containers. You can stop the the running containers by running:

.. code-block:: bash

   # Stop and remove containers
   docker-compose down -v --remove-orphans

and with breeze:


.. code-block:: bash

   # Using Breeze to stop docker compose
   breeze testing task-sdk-integration-tests --down

Docker compose will be automatically started again next time you run the tests.

Running tests in the way CI does it
....................................

Our CI runs the tests in a clean environment every time without mounting local sources. This means that
any changes you have locally will not be visible inside the containers. You can reproduce it locally by adding
``--skip-mounting-local-volumes`` to breeze command or by setting ``SKIP_MOUNTING_LOCAL_VOLUMES=1`` in your
environment when running tests locally. Before that however make sure that your PROD image is rebuilt
using latest sources. When you disable mounting local volumes, the containers will be stopped by default
when the tests end, you can disable that by setting SKIP_DOCKER_COMPOSE_DELETION=1 in your environment
or passing --skip-docker-compose-deletion to breeze command.

.. code-block:: bash

   # Keep containers running for debugging
   SKIP_MOUNTING_LOCAL_VOLUMES=1 uv run pytest -s

or

.. code-block:: bash

  # Using Breeze to keep containers running
  breeze testing task-sdk-integration-tests --skip-mounting-local-volumes

Debugging Failed Tests
......................

When tests fail, the logs from all running containers are automatically dumped to the console
and the Docker Compose deployment is shut down. To debug issues more effectively:

.. code-block:: bash

   # Run with maximum verbosity
   uv run pytest tests/task_sdk_tests/ -vvv -s --tb=long

   # Print logs from all services
   docker-compose logs

   # Inspect container logs (when containers are still running - which is default)
   # The -f flag follows the logs in real-time so you can open several terminals to monitor different services
   docker-compose logs -f airflow-apiserver
   docker-compose logs -f airflow-scheduler
   docker-compose logs -f postgres

   # Access running containers for interactive debugging
   docker-compose exec -it airflow-apiserver bash

Every time you save airflow source code, the components running inside the container will be restarted
automatically (hot reloaded). You can disable this behaviour by setting SKIP_MOUNTING_LOCAL_VOLUMES=1
as described above (but then your sources will not be mounted).


Using Other Airflow Images
..........................

To test the Task SDK against other Airflow images (for example with different Python or custom build
images) - you can specify it by setting the DOCKER_IMAGE environment variable before running the tests.

Note that when you are running tests using breeze, the right image is built automatically for you
for the Python version you specify in breeze command. So you do not need to set DOCKER_IMAGE
when using breeze. Using custom image is only supported when you are running tests using uv directly.

.. code-block:: bash

   # Build your custom Airflow image first
   cd /path/to/airflow
   docker build -t my-custom-airflow:latest -f Dockerfile .

   # Use custom image for integration tests
   export DOCKER_IMAGE=my-custom-airflow:latest
   uv run pytest tests/task_sdk_tests/ -s

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

   task-sdk-integration-tests/
   ├── pyproject.toml                    # Test package configuration and dependencies
   ├── docker-compose.yaml               # Airflow services configuration
   ├── docker-compose-local.yaml         # local mounted volumes configuration
   ├── .env                              # Generated (.gitignored) environment variables for docker-compose
   └── tests/
       └── task_sdk_tests/
           ├── conftest.py                # Test configuration and setup
           ├── constants.py               # Test constants and configuration
           ├── test_task_sdk_health.py    # Main integration test
           └── __init__.py

**Key Files:**

- **docker-compose.yaml**: Defines the complete Airflow environment (postgres, scheduler, api-server)
- **docker-compose-local.yaml**: Defines mounts used to mount local sources to the containers for local testing
- **.env**: Generated environment variables for docker-compose configuration
- **test_task_sdk_health.py**: Main test that verifies Task SDK can communicate with Airflow API
- **conftest.py**: Handles Task SDK installation and test environment setup
- **constants.py**: Configuration constants for Docker images, ports, and API versions

Manual docker compose commands
------------------------------

The commands run by pytest to start the docker compose environment are printed to the console
when running the tests with VERBOSE=1 environment variable set or --verbose flag passed to breeze command.

However some basic commands that you can use to manage the docker compose environment manually are:

.. code-block:: bash

   # Start the environment without manually mounted volumes
   docker-compose up -d -f docker-compose.yaml

   # Start the environment with manually mounted volumes
   docker-compose up -d -f docker-compose.yaml -f docker-compose-local.yaml

   # Stop the environment
   docker-compose down -v --remove-orphans

   # See running commands
   docker-compose ps

   # Access running containers for interactive debugging
   docker-compose exec -it airflow-apiserver bash

   # Print logs from all services
   docker-compose logs

   # Print logs from specific service
   # The -f flag follows the logs in real-time so you can open several terminals to monitor different services
   docker-compose logs -f airflow-apiserver



For other kinds of tests look at `Testing document <../09_testing.rst>`__
