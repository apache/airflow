
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

Here's the list of the operators and hooks which are available in this release.

Note that commonly used operators and sensors (such as ``BashOperator``, ``PythonOperator``, ``ExternalTaskSensor``, etc.) are provided by the ``apache-airflow-providers-standard`` package.

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

   * - :mod:`airflow.providers.standard.operators.bash`
     - :doc:`How to use <apache-airflow-providers-standard:operators/bash>`

   * - :mod:`airflow.providers.standard.operators.python`
     - :doc:`How to use <apache-airflow-providers-standard:operators/python>`

   * - :mod:`airflow.providers.standard.operators.datetime`
     - :doc:`How to use <apache-airflow-providers-standard:operators/datetime>`

   * - :mod:`airflow.providers.standard.operators.empty`
     -

   * - :mod:`airflow.providers.common.sql.operators.generic_transfer.GenericTransfer`
     - :doc:`How to use <apache-airflow-providers-common-sql:operators>`

   * - :mod:`airflow.providers.standard.operators.latest_only`
     - :doc:`How to use <apache-airflow-providers-standard:operators/latest_only>`

   * - :mod:`airflow.providers.standard.operators.trigger_dagrun`
     - :doc:`How to use <apache-airflow-providers-standard:operators/trigger_dag_run>`

**Sensors:**

.. list-table::
   :header-rows: 1

   * - Sensors
     - Guides

   * - :mod:`airflow.providers.standard.sensors.bash`
     - :doc:`How to use <apache-airflow-providers-standard:sensors/bash>`

   * - :mod:`airflow.providers.standard.sensors.python`
     - :doc:`How to use <apache-airflow-providers-standard:sensors/python>`

   * - :mod:`airflow.providers.standard.sensors.filesystem`
     - :doc:`How to use <apache-airflow-providers-standard:sensors/file>`

   * - :mod:`airflow.providers.standard.sensors.date_time`
     - :doc:`How to use <apache-airflow-providers-standard:sensors/datetime>`

   * - :mod:`airflow.providers.standard.sensors.external_task`
     - :doc:`How to use <apache-airflow-providers-standard:sensors/external_task_sensor>`




**Hooks:**

.. list-table::
   :header-rows: 1

   * - Hooks
     - Guides

   * - :mod:`airflow.providers.standard.hooks.filesystem`
     -

   * - :mod:`airflow.providers.standard.hooks.subprocess`
     -
