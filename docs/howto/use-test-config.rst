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



Using the Test Mode Configuration
=================================

Airflow has a fixed set of "test mode" configuration options. You can load these
at any time by calling ``airflow.configuration.load_test_config()``. Please **note** that this operation is **not reversible**.

Some options for example, DAG_FOLDER, are loaded before you have a chance to call load_test_config().
In order to eagerly load the test configuration, set test_mode in airflow.cfg:

.. code-block:: ini

  [tests]
  unit_test_mode = True

Due to Airflow's automatic environment variable expansion (see :doc:`set-config`), you can also set the environment variable ``AIRFLOW__CORE__UNIT_TEST_MODE`` to temporarily overwrite airflow.cfg.
