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
The system tests derive from the ``tests.test_utils.system_test_class.SystemTests`` class. They should also
be marked with ``@pytest.marker.system(SYSTEM)`` where ``system`` designates the system
to be tested (for example, ``google.cloud``). These tests are skipped by default.

You can execute the system tests by providing the ``--system SYSTEM`` flag to ``pytest``. You can
specify several --system flags if you want to execute tests for several systems.

The system tests execute a specified example DAG file that runs the DAG end-to-end.

.. contents:: :local:

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


To execute system tests, specify the ``--system SYSTEM``
flag where ``SYSTEM`` is a system to run the system tests for. It can be repeated.


Forwarding Authentication from the Host
----------------------------------------------------

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

Adding a New System Test
--------------------------

We are working on automating system tests execution (AIP-4) but for now, system tests are skipped when
tests are run in our CI system. But to enable the test automation, we encourage you to add system
tests whenever an operator/hook/sensor is added/modified in a given system.

* To add your own system tests, derive them from the
  ``tests.test_utils.system_tests_class.SystemTest`` class and mark with the
  ``@pytest.mark.system(SYSTEM_NAME)`` marker. The system name should follow the path defined in
  the ``providers`` package (for example, the system tests from ``tests.providers.google.cloud``
  package should be marked with ``@pytest.mark.system("google.cloud")``.

* If your system tests need some credential files to be available for an
  authentication with external systems, make sure to keep these credentials in the
  ``files/airflow-breeze-config/keys`` directory. Mark your tests with
  ``@pytest.mark.credential_file(<FILE>)`` so that they are skipped if such a credential file is not there.
  The tests should read the right credentials and authenticate them on their own. The credentials are read
  in Breeze from the ``/files`` directory. The local "files" folder is mounted to the "/files" folder in Breeze.

* If your system tests are long-running ones (i.e., require more than 20-30 minutes
  to complete), mark them with the ```@pytest.markers.long_running`` marker.
  Such tests are skipped by default unless you specify the ``--long-running`` flag to pytest.

* The system test itself (python class) does not have any logic. Such a test runs
  the DAG specified by its ID. This DAG should contain the actual DAG logic
  to execute. Make sure to define the DAG in ``providers/<SYSTEM_NAME>/example_dags``. These example DAGs
  are also used to take some snippets of code out of them when documentation is generated. So, having these
  DAGs runnable is a great way to make sure the documentation is describing a working example. Inside
  your test class/test method, simply use ``self.run_dag(<DAG_ID>,<DAG_FOLDER>)`` to run the DAG. Then,
  the system class will take care about running the DAG. Note that the DAG_FOLDER should be
  a subdirectory of the ``tests.test_utils.AIRFLOW_MAIN_FOLDER`` + ``providers/<SYSTEM_NAME>/example_dags``.


A simple example of a system test is available in:

``tests/providers/google/cloud/operators/test_compute_system.py``.

It runs two DAGs defined in ``airflow.providers.google.cloud.example_dags.example_compute.py``.


The typical system test session
-------------------------------

Here is the typical session that you need to do to run system tests:

1. Enter breeze

.. code-block:: bash

   breeze down
   breeze --python 3.8 --db-reset --forward-credentials

This will:

* stop the whole environment (i.e. recreates metadata database from the scratch)
* run Breeze with:

  * python 3.8 version
  * resetting the Airflow database
  * forward your local credentials to Breeze

3. Run the tests:

.. code-block:: bash

   pytest -o faulthandler_timeout=2400 \
      --system=google tests/providers/google/cloud/operators/test_compute_system.py

-----

For other kinds of tests look at `Testing document <../09_testing.rst>`__
