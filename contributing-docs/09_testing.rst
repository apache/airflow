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

* `Unit tests <testing/unit_tests.rst>`__ are Python tests that do not require any additional integrations.
  Unit tests are available both in the `Breeze environment <../dev/breeze/doc/README.rst>`__
  and `local virtualenv <07_local_virtualenv.rst>`__. More about unit tests.

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

You can also run other kind of tests when you are developing airflow packages:

* `Testing packages <testing/testing_packages.rst>`__ is a document that describes how to
  manually build and test pre-release candidate packages of airflow and providers.

* `Dag testing <testing/dag_testing.rst>`__ is a document that describes how to test Dags in a local environment
  with ``DebugExecutor``. Note, that this is a legacy method - you can now use dag.test() method to test Dags.

------

You can learn how to `build documentation <../docs/README.rst>`__ as you will likely need to update
documentation as part of your PR.

You can also learn about `working with git <10_working_with_git.rst>`__ as you will need to understand how
git branching works and how to rebase your PR.
