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

Airflow System Tests
====================

System tests verify the correctness of Airflow Operators by running them in Dags and allowing to communicate with
external services. A system test tries to look as close to a regular Dag as possible, and it generally checks the
"happy path" (a scenario featuring no errors) ensuring that the Operator works as expected.

System tests need to communicate with external services/systems that are available
if you have appropriate credentials configured for your tests.

**The outline for this document in GitHub is available at top-right corner button (with 3-dots and 3 lines).**

Purpose of System Tests
-----------------------

The purpose of these tests is to:

- assure high quality of providers and their integration with Airflow core,
- avoid regression in providers when doing changes to the Airflow,
- autogenerate documentation for Operators from code,
- provide runnable example Dags with use cases for different Operators,
- serve both as examples and test files.
- the excerpts from these system tests are used to generate documentation

Configuration
-------------

Environment for System Tests
............................

**Prerequisites:** You may need to set some variables to run system tests.

Usually you must set-up ``SYSTEM_TESTS_ENV_ID`` to a random value to avoid conflicts with other tests and
set it before running test command.

.. code-block:: bash

  SYSTEM_TESTS_ENV_ID=SOME_RANDOM_VALUE

Running the System Tests
------------------------

There are multiple ways of running system tests. Each system test is a self-contained Dag, so it can be run as any
other Dag. Some tests may require access to external services, enabled APIs or specific permissions. Make sure to
prepare your  environment correctly, depending on the system tests you want to run - some may require additional
configuration which should be documented by the relevant providers in their subdirectory
``tests/system/<provider_name>/README.md``.

Running as Airflow Dags
.......................

If you have a working Airflow environment with a scheduler and a webserver, you can import system test files into
your Airflow instance as Dags and they will be automatically triggered. If the setup of the environment is correct
(depending on the type of tests you want to run), they should be executed without any issues. The instructions on
how to set up the environment is documented in each provider's system tests directory. Make sure that all resource
required by the tests are also imported.

Running via Pytest
..................

Running system tests with pytest is the easiest with Breeze. Thanks to it, you don't need to bother about setting up
the correct environment, that is able to execute the tests.
You can either run them using your IDE (if you have installed plugin/widget supporting pytest) or using the following
example of command:

For core:

.. code-block:: bash

  pytest --system airflow-core/tests/system/example_empty.py

For providers:

.. code-block:: bash

  pytest --system providers/google/tests/system/google/cloud/bigquery/example_bigquery_queries.py


Running via Breeze
..................

You can also run system tests using Breeze. To do so, you need to run the following command:

For core:

.. code-block:: bash

  breeze testing system-tests airflow-core/tests/system/example_empty.py


If you need to add some initialization of environment variables when entering Breeze, you can add a
``environment_variables.env`` file in the ``files/airflow-breeze-config/environment_variables.env`` file.

It will be automatically sourced when entering the Breeze environment. You can also add some additional
initialization commands in the  ``files/airflow-breeze-config/init.sh`` file if you want to execute
something always at the time of entering Breeze.

For system tests run in Breeze, you can also forward authentication from the host to your Breeze container.
You can specify the ``--forward-credentials`` flag when starting Breeze. Then, it will also forward the
most commonly used credentials stored in your ``home`` directory. Use this feature with care as it makes
your personal credentials visible to anything that you have installed inside the Docker container.

Currently forwarded credentials are:
  * credentials stored in ``${HOME}/.aws`` for ``aws`` - Amazon Web Services client
  * credentials stored in ``${HOME}/.azure`` for ``az`` - Microsoft Azure client
  * credentials stored in ``${HOME}/.config`` for ``gcloud`` - Google Cloud client (among others)
  * credentials stored in ``${HOME}/.docker`` for ``docker`` client
  * credentials stored in ``${HOME}/.snowsql`` for ``snowsql`` - SnowSQL (Snowflake CLI client)

------

For other kinds of tests look at `Testing document <../09_testing.rst>`__
