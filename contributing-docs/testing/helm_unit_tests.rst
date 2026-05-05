
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

On the Apache Airflow Project, we have decided to stick with pythonic testing for our Helm chart. This makes our chart
easier to test, easier to modify, and able to run with the same testing infrastructure. To add Helm unit tests
add them under ``helm-tests/tests/helm_tests`` directory.

.. code-block:: python

    class TestBaseChartTest: ...

To render the chart create a dictionary with options you wish to test. You can then use them with our ``render_chart``
function to render the object of interest as a testable Python dictionary. Once the chart has been rendered,
you can use ``jmespath.search`` function for searching desirable value of rendered chart to test.

Example test here:

.. code-block:: python

    from chart_utils.helm_template_generator import render_chart


    class TestGitSyncScheduler:
        def test_basic(self):
            docs = render_chart(
                name="GIT-SYNC",
                values={"dags": {"gitSync": {"enabled": True}}},
                show_only=["templates/scheduler/scheduler-deployment.yaml"],
            )

            assert "dags" == jmespath.search("spec.template.spec.volumes[1].name", docs[0])


To execute all Helm tests using breeze command and utilize parallel pytest tests, you can run the
following command (it takes quite a long time even on a multi-processor machine).

.. code-block:: bash

    breeze testing helm-tests

You can also execute tests from a selected package only. Tests in ``helm-tests/tests/helm_tests`` are grouped
by packages so rather than running all tests, you can run only tests from a selected package. For example:

.. code-block:: bash

    breeze testing helm-tests --test-type airflow_aux

Will run all tests from ``helm-tests/tests/helm_tests/airflow_aux`` package.


You can also run Helm tests individually via the usual ``breeze`` command. Just enter breeze and run the
tests with pytest as you would do with regular unit tests (you can add ``-n auto`` command to run Helm
tests in parallel - unlike most of the regular unit tests of ours that require a database, the Helm tests are
perfectly safe to be run in parallel (and if you have multiple processors, you can gain significant
speedups when using parallel runs)):

.. code-block:: bash

    breeze

This enters breeze container.

.. code-block:: bash

    pytest helm-tests -n auto

This runs all chart tests using all processors you have available.

.. code-block:: bash

    pytest helm-tests/tests/helm_tests/airflow_aux/test_airflow_common.py -n auto

This will run all tests from ``tests_airflow_common.py`` file using all processors you have available.

.. code-block:: bash

    pytest helm-tests/tests/helm_tests/airflow_aux/test_airflow_common.py

This will run all tests from ``tests_airflow_common.py`` file sequentially.

-----

For other kinds of tests look at `Testing document <../09_testing.rst>`__
