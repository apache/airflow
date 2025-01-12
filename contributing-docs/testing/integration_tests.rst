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

Airflow Integration Tests
=========================

Integration tests in Airflow check the interactions between Airflow components and external services
that could run as separate Docker containers, without connecting to an external API on the internet.
These tests require ``airflow`` Docker image and extra images with integrations (such as ``celery``, ``mongodb``, etc.).
The integration tests are all stored in the ``tests/integration`` folder, and similarly to the unit tests they all run
using `pytest <http://doc.pytest.org/en/latest/>`_, but they are skipped by default unless ``--integration`` flag is passed to pytest.

**The outline for this document in GitHub is available at top-right corner button (with 3-dots and 3 lines).**

Enabling Integrations
---------------------

Airflow integration tests cannot be run in the local virtualenv. They can only run in the Breeze
environment and in the CI, with their respective integrations enabled. See `CI <../../dev/breeze/doc/ci/README.md>`_ for
details about Airflow CI.

When you initiate a Breeze environment, by default, all integrations are disabled. This enables only unit tests
to be executed in Breeze. You can enable an integration by passing the ``--integration <INTEGRATION>``
switch when starting Breeze, either with ``breeze shell`` or with ``breeze start-airflow``. As there's no need to simulate
a full setup of Airflow during integration tests, using ``breeze shell`` (or simply ``breeze``) to run them is
sufficient. You can specify multiple integrations by repeating the ``--integration`` switch, or by using the ``--integration all-testable`` switch
that enables all testable integrations. You may use ``--integration all`` switch to enable all integrations that
includes also non-testable integrations such as openlineage.

NOTE: Every integration requires a separate container with the corresponding integration image.
These containers take precious resources on your PC, mainly the memory. The started integrations are not stopped
until you stop the Breeze environment with the ``breeze down`` command.

The following integrations are available:

.. BEGIN AUTO-GENERATED INTEGRATION LIST

+--------------+-------------------------------------------------------+
| Identifier   | Description                                           |
+==============+=======================================================+
| cassandra    | Integration required for Cassandra hooks.             |
+--------------+-------------------------------------------------------+
| celery       | Integration required for Celery executor tests.       |
+--------------+-------------------------------------------------------+
| drill        | Integration required for drill operator and hook.     |
+--------------+-------------------------------------------------------+
| kafka        | Integration required for Kafka hooks.                 |
+--------------+-------------------------------------------------------+
| kerberos     | Integration that provides Kerberos authentication.    |
+--------------+-------------------------------------------------------+
| keycloak     | Integration for manual testing of multi-team Airflow. |
+--------------+-------------------------------------------------------+
| mongo        | Integration required for MongoDB hooks.               |
+--------------+-------------------------------------------------------+
| mssql        | Integration required for mssql hooks.                 |
+--------------+-------------------------------------------------------+
| openlineage  | Integration required for Openlineage hooks.           |
+--------------+-------------------------------------------------------+
| otel         | Integration required for OTEL/opentelemetry hooks.    |
+--------------+-------------------------------------------------------+
| pinot        | Integration required for Apache Pinot hooks.          |
+--------------+-------------------------------------------------------+
| qdrant       | Integration required for Qdrant tests.                |
+--------------+-------------------------------------------------------+
| redis        | Integration required for Redis tests.                 |
+--------------+-------------------------------------------------------+
| statsd       | Integration required for Statsd hooks.                |
+--------------+-------------------------------------------------------+
| trino        | Integration required for Trino hooks.                 |
+--------------+-------------------------------------------------------+
| ydb          | Integration required for YDB tests.                   |
+--------------+-------------------------------------------------------+

.. END AUTO-GENERATED INTEGRATION LIST'

To start a shell with ``mongo`` integration enabled, enter:

.. code-block:: bash

    breeze --integration mongo

You could add multiple ``--integration`` options as the types of the integrations that you want to enable.
For example, to start a shell with both ``mongo`` and ``cassandra`` integrations enabled, enter:

.. code-block:: bash

    breeze --integration mongo --integration cassandra


To start all testable integrations, enter:

.. code-block:: bash

    breeze --integration all-testable

To start all integrations, enter:

.. code-block:: bash

    breeze --integration all

Note that Kerberos is a special kind of integration. Some tests run differently when
Kerberos integration is enabled (they retrieve and use a Kerberos authentication token) and differently when the
Kerberos integration is disabled (they neither retrieve nor use the token). Therefore, one of the test jobs
for the CI system should run all tests with the Kerberos integration enabled to test both scenarios.

Running Integration Tests
-------------------------

All integration tests are marked with a custom pytest marker ``pytest.mark.integration``.
The marker has a single parameter - the name of integration.

Example of the ``celery`` integration test:

.. code-block:: python

    @pytest.mark.integration("celery")
    def test_real_ping(self):
        hook = RedisHook(redis_conn_id="redis_default")
        redis = hook.get_conn()

        assert redis.ping(), "Connection to Redis with PING works."

The markers can be specified at the test level or the class level (then all tests in this class
require an integration). You can add multiple markers with different integrations for tests that
require more than one integration.

If such a marked test does not have a required integration enabled, it is skipped.
The skip message clearly says what is needed to use the test.

To run all tests with a certain integration, use the custom pytest flag ``--integration``.
You can pass several integration flags if you want to enable several integrations at once.

**NOTE:** If an integration is not enabled in Breeze or CI,
the affected test will be skipped.

To run only ``mongo`` integration tests:

.. code-block:: bash

    pytest --integration mongo tests/integration

To run integration tests for ``mongo`` and ``celery``:

.. code-block:: bash

    pytest --integration mongo --integration celery tests/integration


Here is an example of the collection limited to the ``providers/apache`` sub-directory:

.. code-block:: bash

    pytest --integration cassandra tests/integrations/providers/apache

Running Integration Tests from the Host
---------------------------------------

You can also run integration tests using Breeze from the host.

Runs all integration tests:

  .. code-block:: bash

       breeze testing integration-tests  --db-reset --integration all-testable

Runs all mongo DB tests:

  .. code-block:: bash

       breeze testing integration-tests --db-reset --integration mongo

Writing Integration Tests
-------------------------
Before creating the integration tests, you'd like to make the integration itself (i.e., the service) available for use.
For that, you'll first need to create a Docker Compose YAML file under ``scripts/ci/docker-compose``, named
``integration-<INTEGRATION>.yml``. The file should define one service for the integration, and another one
for the Airflow instance that depends on it. It is recommended to stick to the following guidelines:


1. Name the ``services::<INTEGRATION>::container_name`` as the service's name and give it an appropriate description under
``services::<INTEGRATION>::labels:breeze.description``, so it would be easier to detect it in Docker for debugging
purposes.

2. Use an official stable release of the service with a pinned version. When there are number of possibilities for an
image, you should probably pick the latest version that is supported by Airflow.

3. Set the ``services::<INTEGRATION>::restart`` to "on-failure".

4. For integrations that require persisting data (for example, databases), define a volume at ``volumes::<VOLUME_NAME>``
and mount the volume to the data path on the container by listing it under ``services:<INTEGRATION>::volumes``
(see example).

5. Check what ports should be exposed to use the service - carefully validate that these ports are not in use by other
integrations (consult the community what to do if such case happens). To avoid conflicts with host's ports, it is a
good practice to prefix the corresponding host port with a number (usually 2), parametrize it and to list the parameter
under ``# Initialise base variables`` section in ``dev/breeze/src/airflow_breeze/global_constants.py``.

6. In some cases you might need to change the entrypoint of the service's container, for example, by setting
``stdin_open: true``.

7. In the Airflow service definition, ensure that it depends on the integration's service (``depands_on``) and set
the env. var. ``INTEGRATION-<INTEGRATION>`` to true.

8. If you need to mount a file (for example, a configuration file), you could put it at ``scripts/ci/docker-compose``
(or a subfolder of this path) and list it under ``services::<INTEGRATION>::volumes``.

For example, ``integration-drill.yml`` looks as follows:

  .. code-block:: yaml

      version: "3.8"
      services:
        drill:
          container_name: drill
          image: "apache/drill:1.21.1-openjdk-17"
          labels:
            breeze.description: "Integration required for drill operator and hook."
          volumes:
            - drill-db-volume:/data
            - ./drill/drill-override.conf:/opt/drill/conf/drill-override.conf
          restart: "on-failure"
          ports:
            - "${DRILL_HOST_PORT}:8047"
          stdin_open: true
        airflow:
          depends_on:
            - drill
          environment:
            - INTEGRATION_DRILL=true
      volumes:
        drill-db-volume:


In the example above, ``DRILL_HOST_PORT = "28047"`` has been added to ``dev/breeze/src/airflow_breeze/global_constants.py``.

Then, you'll also need to set the host port as an env. var. for Docker commands in ``dev/breeze/src/airflow_breeze/params/shell_params.py``
under the property ``env_variables_for_docker_commands``.
For the example above, the following statement was added:

.. code-block:: python

    _set_var(_env, "DRILL_HOST_PORT", None, DRILL_HOST_PORT)

The final setup for the integration would be adding a netcat to check that upon setting the integration, it is possible
to access the service in the internal port.

For that, you'll need to add the following in ``scripts/in_container/check_environment.sh`` under "Checking backend and integrations".
The code block for ``drill`` in this file looks as follows:

.. code-block:: bash

    if [[ ${INTEGRATION_DRILL} == "true" ]]; then
        check_service "drill" "run_nc drill 8047" 50
    fi

Then, create the integration test file under ``tests/integration`` - remember to prefix the file name with ``test_``,
and to use the ``@pytest.mark.integration`` decorator. It is recommended to define setup and teardown methods
(``setup_method`` and ``teardown_method``, respectively) - you could look at existing integration tests to learn more.

Before pushing to GitHub, make sure to run static checks (``breeze static-checks --only-my-changes``) to apply linters
on the Python logic, as well as to update the commands images under ``dev/breeze/docs/images``.

When writing integration tests for components that also require Kerberos, you could enforce auto-enabling the latter by
updating ``compose_file()`` method in ``airflow_breeze.params.shell_params.ShellParams``. For example, to ensure that
Kerberos is active for ``trino`` integration tests, the following code has been introduced:

.. code-block:: python

        if "trino" in integrations and "kerberos" not in integrations:
            get_console().print(
                "[warning]Adding `kerberos` integration as it is implicitly needed by trino",
            )
            compose_file_list.append(DOCKER_COMPOSE_DIR / "integration-kerberos.yml")



-----

For other kinds of tests look at `Testing document <../09_testing.rst>`__
