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

Airflow Test Infrastructure
===========================

Airflow features a comprehensive testing infrastructure encompassing multiple testing methodologies designed to
ensure reliability and functionality across different deployment scenarios and integrations. The testing framework
includes:

* `Unit tests <testing/unit_tests.rst>`__ are Python tests that do not require any additional integrations.
  Unit tests are available both in the `Breeze environment <../dev/breeze/doc/README.rst>`__
  and `local virtualenv <07_local_virtualenv.rst>`__. Note that in order for a pull request to be reviewed,
  **it should have a unit test**, unless a test is not required i.e. for documentation changes.

* `Integration tests <testing/integration_tests.rst>`__ are available in the
  `Breeze environment <../dev/breeze/doc/README.rst>`__ that is also used for Airflow CI tests.
  Integration tests are special tests that require additional services running, such as Postgres,
  MySQL, Kerberos, etc.

* `Docker Compose tests <testing/docker_compose_tests.rst>`__ are tests we run to check if our quick
  start docker-compose works.

* `Kubernetes tests <testing/k8s_tests.rst>`__ are tests we run to check if our Kubernetes
  deployment and Kubernetes Pod Operator works.

* `Helm unit tests <testing/helm_unit_tests.rst>`__ are tests we run to verify if Helm Chart is
  rendered correctly for various configuration parameters.

* `System tests <testing/system_tests.rst>`__ are automatic tests that use external systems like
  Google Cloud and AWS. These tests are intended for an end-to-end Dag execution.

* `Task SDK integration tests <testing/task_sdk_integration_tests.rst>`__ are specialized tests that verify
  the integration between the Apache Airflow Task SDK package and a running Airflow instance.

* `Airflow Ctl integration tests <testing/airflow_ctl_integration_tests.rst>`__ are tests we run to verify
  if the ``airflowctl`` command-line tool works correctly with a running Airflow instance.

You can also run other kinds of tests when you are developing Airflow packages:

* `Testing distributions <testing/testing_distributions.rst>`__ is a document that describes how to
  manually build and test pre-release candidate distributions of Airflow and providers.

* `Python client tests <testing/python_client_tests.rst>`__ are tests we run to check if the Python API
  client works correctly.

* `Dag testing <testing/dag_testing.rst>`__ is a document that describes how to test Dags in a local environment
  with ``dag.test()``.

------

You can learn how to `build documentation <../docs/README.md>`__ as you will likely need to update
documentation as part of your PR.

You can also learn about `working with git <10_working_with_git.rst>`__ as you will need to understand how
git branching works and how to rebase your PR.
