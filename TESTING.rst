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
  that is also used for Airflow Travis CI tests. Integration tests are special tests that require
  additional services running, such as Postgres, MySQL, Kerberos, etc. Currently, these tests are not
  marked as integration tests but soon they will be clearly separated by ``pytest`` annotations.

* **System tests** are automatic tests that use external systems like
  Google Cloud Platform. These tests are intended for an end-to-end DAG execution.
  The tests can be executed on both current version of Apache Airflow, and any of the older
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

Some of the tests in Airflow are integration tests. These tests require not only ``airflow-testing`` Docker
image but also extra images with integrations (such as ``redis``, ``mongodb``, etc.).


Enabling Integrations
---------------------

Airflow integration tests cannot be run in the local virtualenv. They can only run in the Breeze
environment with enabled integrations and in Travis CI.

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

Note that Kerberos is a special kind of integration. There are some tests that run differently when
Kerberos integration is enabled (they retrieve and use a Kerberos authentication token) and differently when the
Kerberos integration is disabled (they neither retrieve nor use the token). Therefore, one of the test jobs
for the CI system should run all tests with the Kerberos integration enabled to test both scenarios.

Running Integration Tests
-------------------------

All tests using an integration are marked with a custom pytest marker ``pytest.mark.integration``.
The marker has a single parameter - the name of an integration.

Example of the ``redis`` integration test:

.. code-block:: python

    @pytest.mark.integration("redis")
    def test_real_ping(self):
        hook = RedisHook(redis_conn_id='redis_default')
        redis = hook.get_conn()

        self.assertTrue(redis.ping(), 'Connection to Redis with PING works.')

The markers can be specified at the test level or at the class level (then all tests in this class
require an integration). You can add multiple markers with different integrations for tests that
require more than one integration.

If such a marked test does not have a required integration enabled, it is skipped.
The skip message clearly says what is needed to use the test.

To run all tests with a certain integration, use the custom pytest flag ``--integrations``,
where you can pass integrations as comma-separated values. You can also specify ``all`` to start
tests for all integrations.

**NOTE:** If an integration is not enabled in Breeze or Travis CI,
the affected test will be skipped.

To run only ``mongo`` integration tests:

.. code-block:: bash

    pytest --integrations mongo

To run integration tests fot ``mongo`` and ``rabbitmq``:

.. code-block:: bash

    pytest --integrations mongo,rabbitmq

To runs all integration tests:

.. code-block:: bash

    pytest --integrations all

Note that collecting all tests takes some time. So, if you know where your tests are located, you can
speed up the test collection significantly by providing the folder where the tests are located.

Here is an example of the collection limited to the ``providers/apache`` directory:

.. code-block:: bash

    pytest --integrations cassandra tests/providers/apache/

Running Backend-Specific Tests
------------------------------

Tests that are using a specific backend are marked with a custom pytest marker ``pytest.mark.backend``.
The marker has a single parameter - the name of a backend. It corresponds to the ``--backend`` switch of
the Breeze environment (one of ``mysql``, ``sqlite``, or ``postgres``). Backen-specific tests only run when
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

Running Tests with Kubernetes
-----------------------------

Starting Kubernetes Cluster when Starting Breeze
................................................

To run Kubernetes in Breeze, you can start Breeze with the ``--kind-cluster-start`` switch. This
automatically creates a Kind Kubernetes cluster in the same ``docker`` engine that is used to run Breeze.
Setting up the Kubernetes cluster takes some time so the cluster continues running
until the it is stopped with the ``--kind-cluster-stop`` switch or until the ``--kind-cluster-recreate``
switch is used rather than ``--kind-cluster-start``. Starting Breeze with the Kind Cluster automatically
sets ``runtime`` to ``kubernetes`` (see below).

The cluster name follows the pattern ``airflow-python-X.Y.Z-vA.B.C`` where X.Y.Z is a Python version
and A.B.C is a Kubernetes version. This way you can have multiple clusters set up and running at the same
time for different Python versions and different Kubernetes versions.

The Control Plane is available from inside the Docker image via ``<CLUSTER_NAME>-control-plane:6443``
host:port, the worker of the Kind Cluster is available at  <CLUSTER_NAME>-worker
and webserver port for the worker is 30809.

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

Travis CI Testing Framework
===========================

Airflow test suite is based on Travis CI framework as running all of the tests
locally requires significant setup. You can set up Travis CI in your fork of
Airflow by following the
`Travis CI Getting Started guide <https://docs.travis-ci.com/user/getting-started/>`__.

Consider using Travis CI framework if you submit multiple pull requests
and want to speed up your builds.

There are two different options available for running Travis CI, and they are
set up on GitHub as separate components:

-   **Travis CI GitHub App** (new version)
-   **Travis CI GitHub Services** (legacy version)

Travis CI GitHub App (new version)
----------------------------------

1.  Once `installed <https://github.com/apps/travis-ci/installations/new/permissions?target_id=47426163>`__,
    configure the Travis CI GitHub App at
    `Configure Travis CI <https://github.com/settings/installations>`__.

2.  Set repository access to either "All repositories" for convenience, or "Only
    select repositories" and choose ``USERNAME/airflow`` in the drop-down menu.

3.   Access Travis CI for your fork at `<https://travis-ci.com/USERNAME/airflow>`__.

Travis CI GitHub Services (legacy version)
------------------------------------------

**NOTE:** The apache/airflow project is still using the legacy version.

Travis CI GitHub Services version uses an Authorized OAuth App.

1.  Once installed, configure the Travis CI Authorized OAuth App at
    `Travis CI OAuth APP <https://github.com/settings/connections/applications/88c5b97de2dbfc50f3ac>`__.

2.  If you are a GitHub admin, click the **Grant** button next to your
    organization; otherwise, click the **Request** button. For the Travis CI
    Authorized OAuth App, you may have to grant access to the forked
    ``ORGANIZATION/airflow`` repo even though it is public.

3.  Access Travis CI for your fork at
    `<https://travis-ci.org/ORGANIZATION/airflow>`_.

Creating New Projects in Travis CI
----------------------------------

If you need to create a new project in Travis CI, use travis-ci.com for both
private repos and open source.

The travis-ci.org site for open source projects is now legacy and you should not use it.

..
    There is a second Authorized OAuth App available called **Travis CI for Open Source** used
    for the legacy travis-ci.org service. Don't use it for new projects!

More information:

-  `Open Source on travis-ci.com <https://docs.travis-ci.com/user/open-source-on-travis-ci-com/>`__.
-  `Legacy GitHub Services to GitHub Apps Migration Guide <https://docs.travis-ci.com/user/legacy-services-to-github-apps-migration-guide/>`__.
-  `Migrating Multiple Repositories to GitHub Apps Guide <https://docs.travis-ci.com/user/travis-migrate-to-apps-gem-guide/>`__.

Airflow System Tests
====================

System tests need to communicate with external services/systems that are available
if you have appropriate credentials configured for your tests.
The system tests derive from the ``tests.test_utils.system_test_class.SystemTests`` class. They should also
be marked with ``@pytest.marker.system(SYSTEM)`` where ``system`` designates the system
to be tested (for example, ``google.cloud``). These tests are skipped by default.
You can execute the system tests by providing the ``--systems SYSTEMS`` flag to ``pytest``.

The system tests execute a specified example DAG file that runs the DAG end-to-end.

See more details about adding new system tests below.

Running System Tests
--------------------
**Prerequisites:** You may need to set some variables to run system tests. If you need to
add some intialization of environment variables to Breeze, you can always add a
``variables.env`` file in the ``files/airflow-breeze-config/variables.env`` file. It will be automatically
sourced when entering the Breeze environment.

To execute system tests, specify the ``--systems SYSTEMS``
flag where ``SYSTEMS`` is a coma-separated list of systems to run the system tests for.

Forwarding Authentication from the Host
----------------------------------------------------

For system tests, you can also forward authentication from the host to your Breeze container. You can specify
the ``--forward-credentials`` flag when starting Breeze. Then, it will also forward the most commonly used
credentials stored in your ``home`` directory. Use this feature with care as it makes your personal credentials
visible to anything that you have installed inside the Docker container.

Currently forwarded credentials are:
  * all credentials stored in ``${HOME}/.config`` (for example, GCP credentials)
  * credentials stored in ``${HOME}/.gsutil`` for ``gsutil`` tool from GCS
  * credentials stored in ``${HOME}/.boto`` and ``${HOME}/.s3`` (for AWS authentication)
  * credentials stored in ``${HOME}/.docker`` for docker
  * credentials stored in ``${HOME}/.kube`` for kubectl
  * credentials stored in ``${HOME}/.ssh`` for SSH


Adding a New System Test
--------------------------

We are working on automating system tests execution (AIP-4) but for now system tests are skipped when
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
  The tests should read the right credentials and authenticate on their own. The credentials are read
  in Breeze from the ``/files`` directory. The local "files" folder is mounted to the "/files" folder in Breeze.
* If your system tests are long-lasting ones (i.e., require more than 20-30 minutes
  to complete), mark them with the ```@pytest.markers.long_running`` marker.
  Such tests are skipped by default unless you specify the ``--long-lasting`` flag to pytest.
* The system test itself (python class) does not have any logic. Such a test runs
  the DAG specified by its ID. This DAG should contain the actual DAG logic
  to execute. Make sure to define the DAG in ``providers/<SYSTEM_NAME>/example_dags``. These example DAGs
  are also used to take some snippets of code out of them when documentation is generated. So, having these
  DAGs runnable is a great way to make sure the documenation is describing a working example. Inside
  your test class/test method, simply use ``self.run_dag(<DAG_ID>,<DAG_FOLDER>)`` to run the DAG. Then,
  the system class will take care about running the DAG. Note that the DAG_FOLDER should be
  a subdirectory of the ``tests.test_utils.AIRFLOW_MAIN_FOLDER`` + ``providers/<SYSTEM_NAME>/example_dags``.

An example of a system test is available in:

``airflow.tests.providers.google.operators.test_natunal_language_system.CloudNaturalLanguageExampleDagsTest``.

It runs the DAG defined in ``airflow.providers.google.cloud.example_dags.example_natural_language.py``.

Running Tests for Older Airflow Versions
----------------------------------------

The tests can be executed against the master version of Airflow but they also work
with older versions. This is especially useful to test back-ported operators
from Airflow 2.0 to 1.10.* versions.

To run the tests for Airflow 1.10.* series, you need to run Breeze with
``--install-airflow-version==<VERSION>`` to install a different version of Airflow.
If ``current`` is specified (default), then the current version of Airflow is used.
Otherwise, the released version of Airflow is installed.

The commands make sure that the source version of master Airflow is removed and the released version of
Airflow from ``Pypi`` is installed. Note that tests sources are not removed and they can be used
to run tests (unit tests and system tests) against the freshly installed version.

This works best for system tests: all the system tests should work for at least latest released 1.10.x
Airflow version. Some of the unit and integration tests might also work in the same
fashion but it is not necessary or expected.

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

Note that on macOS, you have to use a real IP address of your host rather than default
localhost because on macOS the container runs in a virtual machine with a different IP address.

Make sure to configure source code mapping in the remote debugging configuration to map
your local sources to the ``/opt/airflow`` location of the sources within the container:

.. image:: images/source_code_mapping_ide.png
    :align: center
    :alt: Source code mapping

DAG Testing
===========

To ease and speed up process of developing DAGs, you can use
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

You should also consider running it with ``restart`` command when you change installed version.
This will clean-up the database so that you start with a clean DB and not DB installed in a previous version.
So typically you'd run it like ``breeze --install-ariflow-version=1.10.9 restart``.

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
