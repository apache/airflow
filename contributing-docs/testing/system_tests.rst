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

System tests need to communicate with external services/systems that are available
if you have appropriate credentials configured for your tests.

**The outline for this document in GitHub is available at top-right corner button (with 3-dots and 3 lines).**

Purpose of System Tests
-----------------------

Airflow system tests are pretty special because they serve three purposes:

* they are runnable tests that communicate with external services
* they are example_dags that users can just copy&paste and use as starting points for their own DAGs
* the excerpts from these system tests are used to generate documentation

Old System Tests
----------------

The system tests derive from the ``tests_common.test_utils.system_test_class.SystemTests`` class.

Old versions of System tests should also be marked with ``@pytest.marker.system(SYSTEM)`` where ``system``
designates the system to be tested (for example, ``google.cloud``). These tests are skipped by default.

For older version of the tests, you can execute the system tests by providing the
``--system SYSTEM`` flag to ``pytest``. You can specify several --system flags if you want to execute
tests for several systems.

The system tests execute a specified example DAG file that runs the DAG end-to-end.

New System Tests
----------------

The new system tests follows
[AIP-47 New Design of System Tests](https://cwiki.apache.org/confluence/display/AIRFLOW/AIP-47+New+design+of+Airflow+System+Tests)
and those system tests do not require separate ``pytest`` flag to be executed, they also don't need a separate
class to run - all the code is kept in the system test class that is also an executable DAG, it is discoverable
by ``pytest`` and it is executable as Python script.

Environment for System Tests
----------------------------

**Prerequisites:** You may need to set some variables to run system tests. If you need to
add some initialization of environment variables to Breeze, you can add a
``variables.env`` file in the ``files/airflow-breeze-config/variables.env`` file. It will be automatically
sourced when entering the Breeze environment. You can also add some additional
initialization commands in this file if you want to execute something
always at the time of entering Breeze.

There are several typical operations you might want to perform such as:

* generating a file with the random value used across the whole Breeze session (this is useful if
  you want to use this random number in names of resources that you create in your service
* generate variables that will be used as the name of your resources
* decrypt any variables and resources you keep as encrypted in your configuration files
* install additional packages that are needed in case you are doing tests with 1.10.* Airflow series
  (see below)

Example variables.env file is shown here (this is part of the variables.env file that is used to
run Google Cloud system tests.

.. code-block:: bash

  # Build variables. This file is sourced by Breeze.
  # Also it is sourced during continuous integration build in Cloud Build

  # Auto-export all variables
  set -a

  echo
  echo "Reading variables"
  echo

  # Generate random number that will be used across your session
  RANDOM_FILE="/random.txt"

  if [[ ! -f "${RANDOM_FILE}" ]]; then
      echo "${RANDOM}" > "${RANDOM_FILE}"
  fi

  RANDOM_POSTFIX=$(cat "${RANDOM_FILE}")

Forwarding Authentication from the Host
---------------------------------------

For system tests, you can also forward authentication from the host to your Breeze container. You can specify
the ``--forward-credentials`` flag when starting Breeze. Then, it will also forward the most commonly used
credentials stored in your ``home`` directory. Use this feature with care as it makes your personal credentials
visible to anything that you have installed inside the Docker container.

Currently forwarded credentials are:
  * credentials stored in ``${HOME}/.aws`` for ``aws`` - Amazon Web Services client
  * credentials stored in ``${HOME}/.azure`` for ``az`` - Microsoft Azure client
  * credentials stored in ``${HOME}/.config`` for ``gcloud`` - Google Cloud client (among others)
  * credentials stored in ``${HOME}/.docker`` for ``docker`` client
  * credentials stored in ``${HOME}/.snowsql`` for ``snowsql`` - SnowSQL (Snowflake CLI client)

Running the System Tests
------------------------

Running the tests and developing tests in `Test documentation <../../tests/system/README.md>`__

------

For other kinds of tests look at `Testing document <../09_testing.rst>`__
