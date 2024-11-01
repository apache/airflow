
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

Helm Unit Tests
---------------

On the Airflow Project, we have decided to stick with pythonic testing for our Helm chart. This makes our chart
easier to test, easier to modify, and able to run with the same testing infrastructure. To add Helm unit tests
add them in ``helm_tests``.

.. code-block:: python

    class TestBaseChartTest: ...

To render the chart create a YAML string with the nested dictionary of options you wish to test. You can then
use our ``render_chart`` function to render the object of interest into a testable Python dictionary. Once the chart
has been rendered, you can use the ``render_k8s_object`` function to create a k8s model object. It simultaneously
ensures that the object created properly conforms to the expected resource spec and allows you to use object values
instead of nested dictionaries.

Example test here:

.. code-block:: python

    from tests.charts.common.helm_template_generator import render_chart, render_k8s_object

    git_sync_basic = """
    dags:
      gitSync:
      enabled: true
    """


    class TestGitSyncScheduler:
        def test_basic(self):
            helm_settings = yaml.safe_load(git_sync_basic)
            res = render_chart(
                "GIT-SYNC",
                helm_settings,
                show_only=["templates/scheduler/scheduler-deployment.yaml"],
            )
            dep: k8s.V1Deployment = render_k8s_object(res[0], k8s.V1Deployment)
            assert "dags" == dep.spec.template.spec.volumes[1].name


To execute all Helm tests using breeze command and utilize parallel pytest tests, you can run the
following command (but it takes quite a long time even in a multi-processor machine).

.. code-block:: bash

    breeze testing helm-tests

You can also execute tests from a selected package only. Tests in ``tests/chart`` are grouped by packages
so rather than running all tests, you can run only tests from a selected package. For example:

.. code-block:: bash

    breeze testing helm-tests --helm-test-package basic

Will run all tests from ``tests-charts/basic`` package.


You can also run Helm tests individually via the usual ``breeze`` command. Just enter breeze and run the
tests with pytest as you would do with regular unit tests (you can add ``-n auto`` command to run Helm
tests in parallel - unlike most of the regular unit tests of ours that require a database, the Helm tests are
perfectly safe to be run in parallel (and if you have multiple processors, you can gain significant
speedups when using parallel runs):

.. code-block:: bash

    breeze

This enters breeze container.

.. code-block:: bash

    pytest helm_tests -n auto

This runs all chart tests using all processors you have available.

.. code-block:: bash

    pytest helm_tests/test_airflow_common.py -n auto

This will run all tests from ``tests_airflow_common.py`` file using all processors you have available.

.. code-block:: bash

    pytest helm_tests/test_airflow_common.py

This will run all tests from ``tests_airflow_common.py`` file sequentially.

-----

For other kinds of tests look at `Testing document <../09_testing.rst>`__
