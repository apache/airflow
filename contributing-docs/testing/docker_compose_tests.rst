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

Airflow Docker Compose Tests
============================

This document describes how to run tests for Airflow Docker Compose deployment.

.. contents:: :local:

Running Docker Compose Tests with Breeze
----------------------------------------

We also test in CI whether the Docker Compose that we expose in our documentation via
`Running Airflow in Docker <https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html>`_
works as expected. Those tests are run in CI ("Test docker-compose quick start")
and you can run them locally as well.

The way the tests work:

1. They first build the Airflow production image
2. Then they take the Docker Compose file of ours and use the image to start it
3. Then they perform some simple DAG trigger tests which checks whether Airflow is up and can process
   an example DAG

This is done in a local environment, not in the Breeze CI image. It uses ``COMPOSE_PROJECT_NAME`` set to
``quick-start`` to avoid conflicts with other docker compose deployments you might have.

The complete test can be performed using Breeze. The prerequisite to that
is to have ``docker-compose`` (Docker Compose v1) or ``docker compose`` plugin (Docker Compose v2)
available on the path.

Running complete test with breeze:

.. code-block:: bash

    breeze prod-image build --python 3.8
    breeze testing docker-compose-tests

In case the test fails, it will dump the logs from the running containers to the console and it
will shutdown the Docker Compose deployment. In case you want to debug the Docker Compose deployment
created for the test, you can pass ``--skip-docker-compose-deletion`` flag to Breeze or
export ``SKIP_DOCKER_COMPOSE_DELETION`` set to "true" variable and the deployment
will not be deleted after the test.

You can also specify maximum timeout for the containers with ``--wait-for-containers-timeout`` flag.
You can also add ``-s`` option to the command pass it to underlying pytest command
to see the output of the test as it happens (it can be also set via
``WAIT_FOR_CONTAINERS_TIMEOUT`` environment variable)

The test can be also run manually with ``pytest docker_tests/test_docker_compose_quick_start.py``
command, provided that you have a local airflow venv with ``dev`` extra set and the
``DOCKER_IMAGE`` environment variable is set to the image you want to test. The variable defaults
to ``ghcr.io/apache/airflow/main/prod/python3.8:latest`` which is built by default
when you run ``breeze prod-image build --python 3.8``. also the switches ``--skip-docker-compose-deletion``
and ``--wait-for-containers-timeout`` can only be passed via environment variables.

If you want to debug the deployment using ``docker compose`` commands after ``SKIP_DOCKER_COMPOSE_DELETION``
was used, you should set ``COMPOSE_PROJECT_NAME`` to ``quick-start`` because this is what the test uses:

.. code-block:: bash

    export COMPOSE_PROJECT_NAME=quick-start

You can also add ``--project-name quick-start`` to the ``docker compose`` commands you run.
When the test will be re-run it will automatically stop previous deployment and start a new one.

Running Docker Compose deployment manually
------------------------------------------

You can also (independently of Pytest test) run docker-compose deployment manually with the image you built using
the prod image build command above.

.. code-block:: bash

    export AIRFLOW_IMAGE_NAME=ghcr.io/apache/airflow/main/prod/python3.8:latest

and follow the instructions in the
`Running Airflow in Docker <https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html>`_
but make sure to use the docker-compose file from the sources in
``docs/apache-airflow/stable/howto/docker-compose/`` folder.

Then, the usual ``docker compose`` and ``docker`` commands can be used to debug such running instances.
The test performs a simple API call to trigger a DAG and wait for it, but you can follow our
documentation to connect to such running docker compose instances and test it manually.

-----

For other kinds of tests look at `Testing document <../09_testing.rst>`__
