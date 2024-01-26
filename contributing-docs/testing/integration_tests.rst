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

Some of the tests in Airflow are integration tests. These tests require ``airflow`` Docker
image and extra images with integrations (such as ``celery``, ``mongodb``, etc.).
The integration tests are all stored in the ``tests/integration`` folder.

.. contents:: :local:

Enabling Integrations
---------------------

Airflow integration tests cannot be run in the local virtualenv. They can only run in the Breeze
environment with enabled integrations and in the CI. See `CI <CI.rst>`_ for details about Airflow CI.

When you are in the Breeze environment, by default, all integrations are disabled. This enables only true unit tests
to be executed in Breeze. You can enable the integration by passing the ``--integration <INTEGRATION>``
switch when starting Breeze. You can specify multiple integrations by repeating the ``--integration`` switch
or using the ``--integration all-testable`` switch that enables all testable integrations and
``--integration all`` switch that enables all integrations.

NOTE: Every integration requires a separate container with the corresponding integration image.
These containers take precious resources on your PC, mainly the memory. The started integrations are not stopped
until you stop the Breeze environment with the ``stop`` command and started with the ``start`` command.

The following integrations are available:

.. BEGIN AUTO-GENERATED INTEGRATION LIST

+--------------+----------------------------------------------------+
| Identifier   | Description                                        |
+==============+====================================================+
| cassandra    | Integration required for Cassandra hooks.          |
+--------------+----------------------------------------------------+
| celery       | Integration required for Celery executor tests.    |
+--------------+----------------------------------------------------+
| kafka        | Integration required for Kafka hooks.              |
+--------------+----------------------------------------------------+
| kerberos     | Integration that provides Kerberos authentication. |
+--------------+----------------------------------------------------+
| mongo        | Integration required for MongoDB hooks.            |
+--------------+----------------------------------------------------+
| openlineage  | Integration required for Openlineage hooks.        |
+--------------+----------------------------------------------------+
| otel         | Integration required for OTEL/opentelemetry hooks. |
+--------------+----------------------------------------------------+
| pinot        | Integration required for Apache Pinot hooks.       |
+--------------+----------------------------------------------------+
| statsd       | Integration required for Satsd hooks.              |
+--------------+----------------------------------------------------+
| trino        | Integration required for Trino hooks.              |
+--------------+----------------------------------------------------+

.. END AUTO-GENERATED INTEGRATION LIST'

To start the ``mongo`` integration only, enter:

.. code-block:: bash

    breeze --integration mongo

To start ``mongo`` and ``cassandra`` integrations, enter:

.. code-block:: bash

    breeze --integration mongo --integration cassandra

To start all testable integrations, enter:

.. code-block:: bash

    breeze --integration all-testable

To start all integrations, enter:

.. code-block:: bash

    breeze --integration all-testable

Note that Kerberos is a special kind of integration. Some tests run differently when
Kerberos integration is enabled (they retrieve and use a Kerberos authentication token) and differently when the
Kerberos integration is disabled (they neither retrieve nor use the token). Therefore, one of the test jobs
for the CI system should run all tests with the Kerberos integration enabled to test both scenarios.

Running Integration Tests
-------------------------

All tests using an integration are marked with a custom pytest marker ``pytest.mark.integration``.
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

-----

For other kinds of tests look at `Testing document <../09_testing.rst>`__
