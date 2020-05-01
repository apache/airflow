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

.. contents:: :local:

Airflow Test Infrastructure
===========================

* **Unit tests** are Python tests that do not require any additional integrations.
  Unit tests are available both in the `Breeze environment <BREEZE.rst>`__
  and local virtualenv.

* **Integration tests** are available in the Breeze development environment
  that is also used for Airflow CI tests. Integration tests are special tests that require
  additional services running, such as Postgres, MySQL, Kerberos, etc. Currently, these tests are not
  marked as integration tests but soon they will be separated by ``pytest`` annotations.

* **System tests** are automatic tests that use external systems like
  Google Cloud Platform. These tests are intended for an end-to-end DAG execution.
  The tests can be executed on both the current version of Apache Airflow and any of the older
  versions from 1.10.* series.

This document is about running Python tests. Before the tests are run, use
`static code checks <STATIC_CODE_CHECKS.rst>`__ that enable catching typical errors in the code.

Airflow Unit Tests
==================

All tests for Apache Airflow are run using `pytest <http://doc.pytest.org/en/latest/>`_ .

Writing Unit Tests
------------------

Follow the guidelines when writing unit tests:

* For standard unit tests that do not require integrations with external systems, make sure to simulate all communications.
* All Airflow tests are run with ``pytest``. Make sure to set your IDE/runners (see below) to use ``pytest`` by default.
* For new tests, use standard "asserts" of Python and ``pytest`` decorators/context managers for testing
  rather than ``unittest`` ones. See `Pytest docs <http://doc.pytest.org/en/latest/assert.html>`_ for details.
* Use a parameterized framework for tests that have variations in parameters.

**NOTE:** We plan to convert all unit tests to standard "asserts" semi-automatically but this will be done later
in Airflow 2.0 development phase. That will include setUp/tearDown/context managers and decorators.

Running Unit Tests from IDE
---------------------------

To run unit tests from the IDE, create the `local virtualenv <LOCAL_VIRTUALENV.rst>`_,
select it as the default project's environment, then configure your test runner:

.. image:: images/configure_test_runner.png
    :align: center
    :alt: Configuring test runner

and run unit tests as follows:

.. image:: images/running_unittests.png
    :align: center
    :alt: Running unit tests

**NOTE:** You can run the unit tests in the standalone local virtualenv
(with no Breeze installed) if they do not have dependencies such as
Postgres/MySQL/Hadoop/etc.


Running Unit Tests
--------------------------------
To run unit, integration, and system tests from the Breeze and your
virtualenv, you can use the `pytest <http://doc.pytest.org/en/latest/>`_ framework.

Custom ``pytest`` plugin run ``airflow db init`` and ``airflow db reset`` the first
time you launch them. So, you can count on the database being initialized. Currently,
when you run tests not supported **in the local virtualenv, they may either fail
or provide an error message**.

There are many available options for selecting a specific test in ``pytest``. Details can be found
in the official documentation but here are a few basic examples:

.. code-block:: bash

    pytest -k "TestCore and not check"

This runs the ``TestCore`` class but skips tests of this class that include 'check' in their names.
For better performance (due to a test collection), run:

.. code-block:: bash

    pytest tests/tests_core.py -k "TestCore and not bash".

This flag is useful when used to run a single test like this:

.. code-block:: bash

    pytest tests/tests_core.py -k "test_check_operators"

This can also be done by specifying a full path to the test:

.. code-block:: bash

    pytest tests/test_core.py::TestCore::test_check_operators

To run the whole test class, enter:

.. code-block:: bash

    pytest tests/test_core.py::TestCore

You can use all available ``pytest`` flags. For example, to increase a log level
for debugging purposes, enter:

.. code-block:: bash

    pytest --log-level=DEBUG tests/test_core.py::TestCore


Running Tests for a Specified Target Using Breeze from the Host
---------------------------------------------------------------

If you wish to only run tests and not to drop into shell, apply the
``-t``, ``--test-target`` flag. You can add extra pytest flags after ``--`` in the command line.

.. code-block:: bash

     ./breeze test-target tests/hooks/test_druid_hook.py -- --logging-level=DEBUG

You can run the whole test suite with a special '.' test target:

.. code-block:: bash

    ./breeze test-target .

You can also specify individual tests or a group of tests:

.. code-block:: bash

    ./breeze test-target tests/test_core.py::TestCore


Airflow Integration Tests
=========================

Some of the tests in Airflow are integration tests. These tests require not only ``airflow`` Docker
image but also extra images with integrations (such as ``redis``, ``mongodb``, etc.).


Enabling Integrations
---------------------

Airflow integration tests cannot be run in the local virtualenv. They can only run in the Breeze
environment with enabled integrations and in the CI. See `<CI.yml>`_ for details about Airflow CI.

When you are in the Breeze environment, by default all integrations are disabled. This enables only true unit tests
to be executed in Breeze. You can enable the integration by passing the ``--integration <INTEGRATION>``
switch when starting Breeze. You can specify multiple integrations by repeating the ``--integration`` switch
or by using the ``--integration all`` switch that enables all integrations.

NOTE: Every integration requires a separate container with the corresponding integration image.
They take precious resources on your PC, mainly the memory. The started integrations are not stopped
until you stop the Breeze environment with the ``stop`` command  and restart it
via ``restart`` command.

The following integrations are available:

.. list-table:: Airflow Test Integrations
   :widths: 15 80
   :header-rows: 1

   * - Integration
     - Description
   * - cassandra
     - Integration required for Cassandra hooks
   * - kerberos
     - Integration that provides Kerberos authentication
   * - mongo
     - Integration required for MongoDB hooks
   * - openldap
     - Integration required for OpenLDAP hooks
   * - rabbitmq
     - Integration required for Celery executor tests
   * - redis
     - Integration required for Celery executor tests

To start the ``mongo`` integration only, enter:

.. code-block:: bash

    ./breeze --integration mongo

To start ``mongo`` and ``cassandra`` integrations, enter:

.. code-block:: bash

    ./breeze --integration mongo --integration cassandra

To start all integrations, enter:

.. code-block:: bash

    ./breeze --integration all

In the CI environment, integrations can be enabled by specifying the ``ENABLED_INTEGRATIONS`` variable
storing a space-separated list of integrations to start. Thanks to that, we can run integration and
integration-less tests separately in different jobs, which is desired from the memory usage point of view.

Note that Kerberos is a special kind of integration. Some tests run differently when
Kerberos integration is enabled (they retrieve and use a Kerberos authentication token) and differently when the
Kerberos integration is disabled (they neither retrieve nor use the token). Therefore, one of the test jobs
for the CI system should run all tests with the Kerberos integration enabled to test both scenarios.

Running Integration Tests
-------------------------

All tests using an integration are marked with a custom pytest marker ``pytest.mark.integration``.
The marker has a single parameter - the name of integration.

Example of the ``redis`` integration test:

.. code-block:: python

    @pytest.mark.integration("redis")
    def test_real_ping(self):
        hook = RedisHook(redis_conn_id='redis_default')
        redis = hook.get_conn()

        self.assertTrue(redis.ping(), 'Connection to Redis with PING works.')

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

    pytest --integration mongo

To run integration tests for ``mongo`` and ``rabbitmq``:

.. code-block:: bash

    pytest --integration mongo --integration rabbitmq

Note that collecting all tests takes some time. So, if you know where your tests are located, you can
speed up the test collection significantly by providing the folder where the tests are located.

Here is an example of the collection limited to the ``providers/apache`` directory:

.. code-block:: bash

    pytest --integration cassandra tests/providers/apache/

Running Backend-Specific Tests
------------------------------

Tests that are using a specific backend are marked with a custom pytest marker ``pytest.mark.backend``.
The marker has a single parameter - the name of a backend. It corresponds to the ``--backend`` switch of
the Breeze environment (one of ``mysql``, ``sqlite``, or ``postgres``). Backend-specific tests only run when
the Breeze environment is running with the right backend. If you specify more than one backend
in the marker, the test runs for all specified backends.

Example of the ``postgres`` only test:

.. code-block:: python

    @pytest.mark.backend("postgres")
    def test_copy_expert(self):
        ...


Example of the ``postgres,mysql`` test (they are skipped with the ``sqlite`` backend):

.. code-block:: python

    @pytest.mark.backend("postgres", "mysql")
    def test_celery_executor(self):
        ...


You can use the custom ``--backend`` switch in pytest to only run tests specific for that backend.
Here is an example of running only postgres-specific backend tests:

.. code-block:: bash

    pytest --backend postgres

Running Long running tests
--------------------------

Some of the tests rung for a long time. Such tests are marked with ``@pytest.mark.long_running`` annotation.
Those tests are skipped by default. You can enable them with ``--include-long-running`` flag. You
can also decide to only run tests with ``-m long-running`` flags to run only those tests.

Quarantined tests
-----------------

Some of our tests are quarantined. This means that this test will be run in isolation and that it will be
re-run several times. Also when quarantined tests fail, the whole test suite will not fail. The quarantined
tests are usually flaky tests that need some attention and fix.

Those tests are marked with ``@pytest.mark.quarantined`` annotation.
Those tests are skipped by default. You can enable them with ``--include-quarantined`` flag. You
can also decide to only run tests with ``-m quarantined`` flag to run only those tests.

Running Tests with Kubernetes
-----------------------------

Starting Kubernetes Cluster when Starting Breeze
................................................

To run Kubernetes in Breeze, you can start Breeze with the ``--kind-cluster-start`` switch. This
automatically creates a Kind Kubernetes cluster in the same ``docker`` engine that is used to run Breeze.
Setting up the Kubernetes cluster takes some time so the cluster continues running
until it is stopped with the ``--kind-cluster-stop`` switch or until the ``--kind-cluster-recreate``
switch is used rather than ``--kind-cluster-start``. Starting Breeze with the Kind Cluster automatically
sets ``runtime`` to ``kubernetes`` (see below).

The cluster name follows the pattern ``airflow-python-X.Y.Z-vA.B.C`` where X.Y.Z is a Python version
and A.B.C is a Kubernetes version. This way you can have multiple clusters set up and running at the same
time for different Python versions and different Kubernetes versions.

The Control Plane is available from inside the Docker image via ``<CLUSTER_NAME>-control-plane:6443``
host:port, the worker of the Kind Cluster is available at  <CLUSTER_NAME>-worker
and the webserver port for the worker is 30809.

After the Kubernetes Cluster is started, you need to deploy Airflow to the cluster:

1. Build the image.
2. Load it to the Kubernetes cluster.
3. Deploy the Airflow application.

It can be done with a single script: ``./scripts/ci/in_container/deploy_airflow_to_kubernetes.sh``.

You can, however, work separately on the image in Kubernetes and deploying the Airflow app in the cluster.

Building and Loading Airflow Images to Kubernetes Cluster
..............................................................

Use the script ``./scripts/ci/in_container/kubernetes/docker/rebuild_airflow_image.sh`` that does the following:

1. Rebuilds the latest ``apache/airflow:master-pythonX.Y-ci`` images using the latest sources.
2. Builds a new Kubernetes image based on the  ``apache/airflow:master-pythonX.Y-ci`` using
   necessary scripts added to run in Kubernetes. The image is tagged as
   ``apache/airflow:master-pythonX.Y-ci-kubernetes``.
3. Loads the image to the Kind Cluster using the ``kind load`` command.

Deploying the Airflow Application in the Kubernetes Cluster
...........................................................

Use the script ``./scripts/ci/in_container/kubernetes/app/deploy_app.sh`` that does the following:

1. Prepares Kubernetes resources by processing a template from the ``template`` directory and replacing
   variables with the right images and locations:
   - configmaps.yaml
   - airflow.yaml
2. Uses the existing resources without replacing any variables inside:
   - secrets.yaml
   - postgres.yaml
   - volumes.yaml
3. Applies all the resources to the Kind Cluster.
4. Waits for all the applications to be ready and reachable.

After the deployment is finished, you can run Kubernetes tests immediately in the same way as other tests.
The Kubernetes tests are available in the ``tests/runtime/kubernetes`` folder.

You can run all the integration tests for Kubernetes with ``pytest tests/runtime/kubernetes``.


Running Runtime-Specific Tests
------------------------------

Tests using a specific runtime are marked with a custom pytest marker ``pytest.mark.runtime``.
The marker has a single parameter - the name of a runtime. At the moment the only supported runtime is
``kubernetes``. This runtime is set when you run Breeze with one of the ``--kind-cluster-*`` flags.
Runtime-specific tests run only when the selectd runtime is started.


.. code-block:: python

    @pytest.mark.runtime("kubernetes")
    class TestKubernetesExecutor(unittest.TestCase):


You can use the custom ``--runtime`` switch in pytest to only run tests specific for that backend.

To run only kubernetes-runtime backend tests, enter:

.. code-block:: bash

    pytest --runtime kubernetes

**NOTE:** For convenience and faster search, all runtime tests are stored in the ``tests.runtime`` package. In this case, you
can speed up the collection of tests by running:

.. code-block:: bash

    pytest --runtime kubernetes tests/runtime

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

See more details about adding new system tests below.

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

  # install any packages from dist folder if they are available
  if [[ ${RUN_AIRFLOW_1_10:=} == "true" ]]; then
      pip install /dist/apache_airflow_providers_{google,postgres,mysql}*.whl || true
  fi

To execute system tests, specify the ``--system SYSTEM`
flag where ``SYSTEM`` is a system to run the system tests for. It can be repeated.


Forwarding Authentication from the Host
----------------------------------------------------

For system tests, you can also forward authentication from the host to your Breeze container. You can specify
the ``--forward-credentials`` flag when starting Breeze. Then, it will also forward the most commonly used
credentials stored in your ``home`` directory. Use this feature with care as it makes your personal credentials
visible to anything that you have installed inside the Docker container.

Currently forwarded credentials are:
  * all credentials stored in ``${HOME}/.config`` (for example, GCP credentials)
  * credentials stored in ``${HOME}/.gsutil`` for ``gsutil`` tool from GCS
  * credentials stored in ``${HOME}/.aws``, ``${HOME}/.boto``, and ``${HOME}/.s3`` (for AWS authentication)
  * credentials stored in ``${HOME}/.docker`` for docker
  * credentials stored in ``${HOME}/.kube`` for kubectl


Adding a New System Test
--------------------------

We are working on automating system tests execution (AIP-4) but for now, system tests are skipped when
tests are run in our CI system. But to enable the test automation, we encourage you to add system
tests whenever an operator/hook/sensor is added/modified in a given system.

* To add your own system tests, derive them from the
  ``tests.test_utils.system_tests_class.SystemTest` class and mark with the
  ``@pytest.mark.system(SYSTEM_NAME)`` marker. The system name should follow the path defined in
  the ``providers`` package (for example, the system tests from ``tests.providers.google.cloud``
  package should be marked with ``@pytest.mark.system("google.cloud")``.

* If your system tests need some credential files to be available for an
  authentication with external systems, make sure to keep these credentials in the
  ``files/airflow-breeze-config/keys`` directory. Mark your tests with
  ``@pytest.mark.credential_file(<FILE>)`` so that they are skipped if such a credential file is not there.
  The tests should read the right credentials and authenticate them on their own. The credentials are read
  in Breeze from the ``/files`` directory. The local "files" folder is mounted to the "/files" folder in Breeze.

* If your system tests are long-runnin ones (i.e., require more than 20-30 minutes
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

It runs two DAGs defined in ``airflow.providers.google.cloud.example_dags.example_compute.py`` and
``airflow.providers.google.cloud.example_dags.example_compute_igm.py``.

Preparing backport packages for System Tests for Airflow 1.10.* series
----------------------------------------------------------------------

To run system tests with old Airflow version you need to prepare backport packages. This
can be done by running ``./scripts/ci/ci_prepare_backport_packages.sh <PACKAGES TO BUILD>``. For
example the below command will build google postgres and mysql packages:

.. code-block:: bash

  ./scripts/ci/ci_prepare_backport_packages.sh google postgres mysql

Those packages will be prepared in ./dist folder. This folder is mapped to /dist folder
when you enter Breeze, so it is easy to automate installing those packages for testing.


Installing backported for Airflow 1.10.* series
-----------------------------------------------

The tests can be executed against the master version of Airflow but they also work
with older versions. This is especially useful to test back-ported operators
from Airflow 2.0 to 1.10.* versions.

To run the tests for Airflow 1.10.* series, you need to run Breeze with
``--install-airflow-version=<VERSION>`` to install a different version of Airflow.
If ``current`` is specified (default), then the current version of Airflow is used.
Otherwise, the released version of Airflow is installed.

The ``-install-airflow-version=<VERSION>`` command make sure that the current (from sources) version of
Airflow is removed and the released version of Airflow from ``Pypi`` is installed. Note that tests sources
are not removed and they can be used to run tests (unit tests and system tests) against the
freshly installed version.

You should automate installing of the backport packages in your own
``./files/airflow-breeze-config/variables.env`` file. You should make it depend on
``RUN_AIRFLOW_1_10`` variable value equals to "true" so that
the installation of backport packages is only performed when you install airflow 1.10.*.
The backport packages are available in ``/dist`` directory if they were prepared as described
in the previous chapter.

Typically the command in you variables.env file will be similar to:

.. code-block:: bash

  # install any packages from dist folder if they are available
  if [[ ${RUN_AIRFLOW_1_10:=} == "true" ]]; then
      pip install /dist/apache_airflow_providers_{google,postgres,mysql}*.whl || true
  fi

The command above will automatically install backported google, postgres, and mysql packages if they
were prepared before entering the breeze.


Running system tests for backported packages in Airflow 1.10.* series
---------------------------------------------------------------------

Once you installed 1.10.* Airflow version with ``--install-airflow-version`` and prepared and
installed the required packages via ``variables.env`` it should be as easy as running
``pytest --system=<SYSTEM_NAME> TEST_NAME``. Note that we have default timeout for running
system tests set to 8 minutes and some system tests might take much longer to run and you might
want to add ``-o faulthandler_timeout=2400`` (2400s = 40 minutes for example) to your
pytest command.

The typical system test session
---------------------------

Here is the typical session that you need to do to run system tests:

1. Prepare backport packages

.. code-block:: bash

  ./scripts/ci/ci_prepare_backport_packages.sh google postgres mysql

2. Enter breeze with installing Airflow 1.10.*, forwarding credentials and installing
   backported packages (you need an appropriate line in ``./files/airflow-breeze-config/variables.env``)

.. code-block:: bash

   ./breeze --install-airflow-version 1.10.9 --python 3.6 --db-reset --forward-credentials restart

This will:

* install Airflow 1.10.9
* restarts the whole environment (i.e. recreates metadata database from the scratch)
* run Breeze with python 3.6 version
* reset the Airflow database
* forward your local credentials to Breeze

3. Run the tests:

.. code-block:: bash

   pytest -o faulthandler_timeout=2400 \
      --system=google tests/providers/google/cloud/operators/test_compute_system.py


Iteration with System Tests if your resources are slow to create
----------------------------------------------------------------

When you want to iterate on system tests, you might want to create slow resources first.

If you need to set up some external resources for your tests (for example compute instances in Google Cloud)
you should set them up and teardown in the setUp/tearDown methods of your tests.
Since those resources might be slow to create you might want to add some helpers that
set them up and tear them down separately via manual operations. This way you can iterate on
the tests without waiting for setUp and tearDown with every test.

In this case, you should build in a mechanism to skip setUp and tearDown in case you manually
created the resources. A somewhat complex example of that can be found in
``tests.providers.google.cloud.operators.test_cloud_sql_system.py`` and the helper is
available in ``tests.providers.google.cloud.operators.test_cloud_sql_system_helper.py``.

When the helper is run with ``--action create`` to create cloud sql instances which are very slow
to create and set-up so that you can iterate on running the system tests without
losing the time for creating theme every time. A temporary file is created to prevent from
setting up and tearing down the instances when running the test.

This example also shows how you can use the random number generated at the entry of Breeze if you
have it in your variables.env (see the previous chapter). In the case of Cloud SQL, you cannot reuse the
same instance name for a week so we generate a random number that is used across the whole session
and store it in ``/random.txt`` file so that the names are unique during tests.


!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! Important !!!!!!!!!!!!!!!!!!!!!!!!!!!!

Do not forget to delete manually created resources before leaving the
Breeze session. They are usually expensive to run.

!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! Important !!!!!!!!!!!!!!!!!!!!!!!!!!!!

Note that in case you have to update your backported operators or system tests (they are part of
the backport packageS) you need to rebuild the packages outside of breeze and
``pip remove/pip install`` those packages to get them installed. This is not needed
if you run system tests with ``current`` airflow version, so it is better to iterate with the
system tests with the ``current`` version and fix all problems there and only afterwards run
the tests with Airflow 1.10.*

The typical session then looks as follows:

1. Prepare backport packages

.. code-block:: bash

  ./scripts/ci/ci_prepare_backport_packages.sh google postgres mysql

2. Enter breeze with installing Airflow 1.10.*, forwarding credentials and installing
   backported packages (you need an appropriate line in ``./files/airflow-breeze-config/variables.env``)

.. code-block:: bash

   ./breeze --install-airflow-version 1.10.9 --python 3.6 --db-reset --forward-credentials restart

3. Run create action in helper (to create slowly created resources):

.. code-block:: bash

    python tests/providers/google/cloud/operators/test_cloud_sql_system_helper.py --action create

4. Run the tests:

.. code-block:: bash

   pytest -o faulthandler_timeout=2400 \
      --system=google tests/providers/google/cloud/operators/test_compute_system.py

5. In case you are running backport packages tests you need to rebuild and reinstall a package
   every time you change the operators/hooks or example_dags. The example below shows reinstallation
   of the google package:

In the host:

.. code-block:: bash

  ./scripts/ci/ci_prepare_backport_packages.sh google

In the container:

.. code-block:: bash

  pip uninstall apache-airflow-providers-google
  pip install /dist/apache_airflow_providers_google-*.whl

The points 4. and 5. can be repeated multiple times without leaving the container

6. Run delete action in helper:

.. code-block:: bash

    python tests/providers/google/cloud/operators/test_cloud_sql_system_helper.py --action delete


Local and Remote Debugging in IDE
=================================

One of the great benefits of using the local virtualenv and Breeze is an option to run
local debugging in your IDE graphical interface.

When you run example DAGs, even if you run them using unit tests within IDE, they are run in a separate
container. This makes it a little harder to use with IDE built-in debuggers.
Fortunately, IntelliJ/PyCharm provides an effective remote debugging feature (but only in paid versions).
See additional details on
`remote debugging <https://www.jetbrains.com/help/pycharm/remote-debugging-with-product.html>`_.

You can set up your remote debugging session as follows:

.. image:: images/setup_remote_debugging.png
    :align: center
    :alt: Setup remote debugging

Note that on macOS, you have to use a real IP address of your host rather than the default
localhost because on macOS the container runs in a virtual machine with a different IP address.

Make sure to configure source code mapping in the remote debugging configuration to map
your local sources to the ``/opt/airflow`` location of the sources within the container:

.. image:: images/source_code_mapping_ide.png
    :align: center
    :alt: Source code mapping

Setup VM on GCP with SSH forwarding
-----------------------------------

Below are the steps you need to take to set up your virtual machine in the Google Cloud Platform.

1. The next steps will assume that you have configured environment variables with the name of the network and
   a virtual machine, project ID and the zone where the virtual machine will be created

    .. code-block:: bash

      PROJECT_ID="<PROJECT_ID>"
      GCP_ZONE="europe-west3-a"
      GCP_NETWORK_NAME="airflow-debugging"
      GCP_INSTANCE_NAME="airflow-debugging-ci"

2. It is necessary to configure the network and firewall for your machine.
   The firewall must have unblocked access to port 22 for SSH traffic and any other port for the debugger.
   In the example for the debugger, we will use port 5555.

    .. code-block:: bash

      gcloud compute --project="${PROJECT_ID}" networks create "${GCP_NETWORK_NAME}" \
        --subnet-mode=auto

      gcloud compute --project="${PROJECT_ID}" firewall-rules create "${GCP_NETWORK_NAME}-allow-ssh" \
        --network "${GCP_NETWORK_NAME}" \
        --allow tcp:22 \
        --source-ranges 0.0.0.0/0

      gcloud compute --project="${PROJECT_ID}" firewall-rules create "${GCP_NETWORK_NAME}-allow-debugger" \
        --network "${GCP_NETWORK_NAME}" \
        --allow tcp:5555 \
        --source-ranges 0.0.0.0/0

3. If you have a network, you can create a virtual machine. To save costs, you can create a `Preemptible
   virtual machine <https://cloud.google.com/preemptible-vms>` that is automatically deleted for up
   to 24 hours.

    .. code-block:: bash

      gcloud beta compute --project="${PROJECT_ID}" instances create "${GCP_INSTANCE_NAME}" \
        --zone="${GCP_ZONE}" \
        --machine-type=f1-micro \
        --subnet="${GCP_NETWORK_NAME}" \
        --image=debian-10-buster-v20200210 \
        --image-project=debian-cloud \
        --preemptible

    To check the public IP address of the machine, you can run the command

    .. code-block:: bash

      gcloud compute --project="${PROJECT_ID}" instances describe "${GCP_INSTANCE_NAME}" \
        --zone="${GCP_ZONE}" \
        --format='value(networkInterfaces[].accessConfigs[0].natIP.notnull().list())'

4. The SSH Deamon's default configuration does not allow traffic forwarding to public addresses.
   To change it, modify the ``GatewayPorts`` options in the ``/etc/ssh/sshd_config`` file to ``Yes``
   and restart the SSH daemon.

    .. code-block:: bash

      gcloud beta compute --project="${PROJECT_ID}" ssh "${GCP_INSTANCE_NAME}" \
        --zone="${GCP_ZONE}" -- \
        sudo sed -i "s/#\?\s*GatewayPorts no/GatewayPorts Yes/" /etc/ssh/sshd_config

      gcloud beta compute --project="${PROJECT_ID}" ssh "${GCP_INSTANCE_NAME}" \
        --zone="${GCP_ZONE}" -- \
        sudo service sshd restart

5. To start port forwarding, run the following command:

    .. code-block:: bash

      gcloud beta compute --project="${PROJECT_ID}" ssh "${GCP_INSTANCE_NAME}" \
        --zone="${GCP_ZONE}" -- \
        -N \
        -R 0.0.0.0:5555:localhost:5555 \
        -v

If you have finished using the virtual machine, remember to delete it.

    .. code-block:: bash

      gcloud beta compute --project="${PROJECT_ID}" instances delete "${GCP_INSTANCE_NAME}" \
        --zone="${GCP_ZONE}"

You can use the GCP service for free if you use the `Free Tier <https://cloud.google.com/free>`__.

DAG Testing
===========

To ease and speed up the process of developing DAGs, you can use
py:class:`~airflow.executors.debug_executor.DebugExecutor`, which is a single process executor
for debugging purposes. Using this executor, you can run and debug DAGs from your IDE.

To set up the IDE:

1. Add ``main`` block at the end of your DAG file to make it runnable.
It will run a backfill job:

.. code-block:: python

  if __name__ == '__main__':
    dag.clear(reset_dag_runs=True)
    dag.run()


2. Set up ``AIRFLOW__CORE__EXECUTOR=DebugExecutor`` in the run configuration of your IDE.
   Make sure to also set up all environment variables required by your DAG.

3. Run and debug the DAG file.

Additionally, ``DebugExecutor`` can be used in a fail-fast mode that will make
all other running or scheduled tasks fail immediately. To enable this option, set
``AIRFLOW__DEBUG__FAIL_FAST=True`` or adjust ``fail_fast`` option in your ``airflow.cfg``.

Also, with the Airflow CLI command ``airflow dags test``, you can execute one complete run of a DAG:

.. code-block:: bash

    # airflow dags test [dag_id] [execution_date]
    airflow dags test example_branch_operator 2018-01-01

By default ``/files/dags`` folder is mounted from your local ``<AIRFLOW_SOURCES>/files/dags`` and this is
the directory used by airflow scheduler and webserver to scan dags for. You can place your dags there
to test them.

The DAGs can be run in the master version of Airflow but they also work
with older versions.

To run the tests for Airflow 1.10.* series, you need to run Breeze with
``--install-airflow-version==<VERSION>`` to install a different version of Airflow.
If ``current`` is specified (default), then the current version of Airflow is used.
Otherwise, the released version of Airflow is installed.

You should also consider running it with ``restart`` command when you change the installed version.
This will clean-up the database so that you start with a clean DB and not DB installed in a previous version.
So typically you'd run it like ``breeze --install-airflow-version=1.10.9 restart``.

BASH Unit Testing (BATS)
========================

We have started adding tests to cover Bash scripts we have in our codebase.
The tests are placed in the ``tests\bats`` folder.
They require BAT CLI to be installed if you want to run them on your
host or via a Docker image.

Installing BATS CLI
---------------------

You can find an installation guide as well as information on how to write
the bash tests in `BATS Installation <https://github.com/bats-core/bats-core#installation>`_.

Running BATS Tests on the Host
------------------------------

To run all tests:

```
bats -r tests/bats/
```

To run a single test:

```
bats tests/bats/your_test_file.bats
```

Running BATS Tests via Docker
-----------------------------

To run all tests:

```
docker run -it --workdir /airflow -v $(pwd):/airflow  bats/bats:latest -r /airflow/tests/bats
```

To run a single test:

```
docker run -it --workdir /airflow -v $(pwd):/airflow  bats/bats:latest /airflow/tests/bats/your_test_file.bats
```

Using BATS
----------

You can read more about using BATS CLI and writing tests in
`BATS Usage <https://github.com/bats-core/bats-core#usage>`_.
