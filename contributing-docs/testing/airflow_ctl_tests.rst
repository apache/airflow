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

Airflow Ctl (airflowctl) Tests
===============================

This document describes how to run tests for the Airflow Ctl (airflowctl) command-line tool.

Similar to other integration tests, the Airflow Ctl tests are run in the Breeze environment.
The tests are using the `Docker Compose <../airflow-core/doc/howto/docker-compose/docker-compose.yaml>`__ and running a full working Airflow environment.
These tests run on a scheduled basis in CI and can also be run locally using Breeze.
We have them to ensure the ``airflowctl`` tool is working as expected and interacting with Airflow instances correctly.
Airflow Ctl integration tests are located in the root directory of Apache Airflow project.

.. note::

   The integration tests ensure that the ``airflowctl`` tool behaves as expected when interacting with Airflow instances.
   They validate the tool's functionality in a controlled environment, providing confidence in its reliability.

Running Airflow Ctl Tests with Breeze
-------------------------------------

Firstly, checkout the tag or branch you want to run the tests on.
Then, you can run the tests using the following command:

.. code-block:: bash

   breeze testing airflow-ctl-integration-test

.. note::

   The above command runs the integration tests for ``airflowctl`` in the Breeze environment.
   Ensure that the correct tag or branch is checked out before executing the tests.
