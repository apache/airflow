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

Operators and Hooks Reference
=============================

Here's the list of the operators and hooks which are available in this release in the ``apache-airflow`` package.

Airflow has many more integrations available for separate installation as
:doc:`apache-airflow-providers:index`.

For details see: :doc:`apache-airflow-providers:operators-and-hooks-ref/index`.

**Base:**

.. list-table::
   :header-rows: 1

   * - Module
     - Guides

   * - :mod:`airflow.hooks.base`
     -

   * - :mod:`airflow.models.baseoperator`
     -

   * - :mod:`airflow.sensors.base`
     -

**Operators:**

.. list-table::
   :header-rows: 1

   * - Operators
     - Guides

   * - :mod:`airflow.operators.bash`
     - :doc:`How to use <howto/operator/bash>`

   * - :mod:`airflow.operators.branch`
     -

   * - :mod:`airflow.operators.datetime`
     - :doc:`How to use <howto/operator/datetime>`

   * - :mod:`airflow.operators.empty`
     -

   * - :mod:`airflow.operators.email`
     -

   * - :mod:`airflow.operators.generic_transfer`
     -

   * - :mod:`airflow.operators.latest_only`
     -

   * - :mod:`airflow.operators.python`
     - :doc:`How to use <howto/operator/python>`

   * - :mod:`airflow.operators.trigger_dagrun`
     -

**Sensors:**

.. list-table::
   :header-rows: 1

   * - Sensors
     - Guides

   * - :mod:`airflow.sensors.bash`
     - :ref:`How to use <howto/operator:BashSensor>`

   * - :mod:`airflow.sensors.date_time`
     - :doc:`How to use <howto/operator/datetime>`

   * - :mod:`airflow.sensors.external_task`
     - :doc:`How to use <howto/operator/external_task_sensor>`

   * - :mod:`airflow.sensors.filesystem`
     - :ref:`How to use <howto/operator:FileSensor>`

   * - :mod:`airflow.sensors.python`
     - :ref:`How to use <howto/operator:PythonSensor>`

   * - :mod:`airflow.sensors.time_delta`
     - :ref:`How to use <howto/operator:TimeDeltaSensor>`

   * - :mod:`airflow.sensors.time_sensor`
     - :ref:`How to use <howto/operator:TimeSensor>`

   * - :mod:`airflow.sensors.weekday`
     - :ref:`How to use <howto/operator:BashSensor>`


**Hooks:**

.. list-table::
   :header-rows: 1

   * - Hooks
     - Guides

   * - :mod:`airflow.hooks.filesystem`
     -

   * - :mod:`airflow.hooks.subprocess`
     -
