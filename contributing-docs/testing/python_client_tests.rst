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

Airflow Python API Client Tests
===============================

This document describes how to run tests for the Airflow Python API client.

Running Python API Client Tests with Breeze
-------------------------------------------

The Python API client tests are run in the Breeze environment. The tests are run in the same way as the other tests in Breeze.

The way the tests work:

1. The Airflow Python API client package is first built into a wheel file and placed in the dist folder.
2. The ``breeze testing python-api-client-tests`` command is used to initiate the tests.
3. This command installs the package from the dist folder.
4. Example Dags are then parsed and executed to validate the Python API client.
5. The webserver is started with the credentials admin/admin, and tests are run against the webserver.

If you have python client repository not cloned, you can clone it by running the following command:

.. code-block:: bash

    git clone https://github.com/apache/airflow-client-python.git

To build the package, you can run the following command:

.. code-block:: bash

    breeze release-management prepare-python-client --distribution-format both
          --python-client-repo ./airflow-client-python
