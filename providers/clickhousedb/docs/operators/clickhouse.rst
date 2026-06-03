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

.. _howto/operator:ClickHouseOperator:

SQLExecuteQueryOperator for ClickHouse
======================================
Use the :class:`~airflow.providers.common.sql.operators.sql.SQLExecuteQueryOperator` to execute
SQL commands in a `ClickHouse <https://clickhouse.com/>`__ database.

Because :class:`~airflow.providers.clickhousedb.hooks.clickhouse.ClickHouseHook` extends
:class:`~airflow.providers.common.sql.hooks.sql.DbApiHook`, no dedicated ClickHouse operator is
needed — the generic ``SQLExecuteQueryOperator`` handles DDL, DML, and analytical queries.

Using the Operator
^^^^^^^^^^^^^^^^^^

Set the ``conn_id`` argument to the ID of a
:ref:`ClickHouse connection <howto/connection:clickhouse>`.

.. list-table:: ClickHouse Airflow Connection Metadata
   :widths: 25 25
   :header-rows: 1

   * - Parameter
     - Input
   * - Host: string
     - ClickHouse server hostname
   * - Port: integer
     - HTTP port (default: ``8123``, TLS: ``8443``)
   * - Login: string
     - ClickHouse username (default: ``default``)
   * - Password: string
     - ClickHouse user password
   * - Database (Schema): string
     - Default database (default: ``default``)
   * - Extra: JSON dict
     - ``secure``, ``verify``, ``connect_timeout``, ``send_receive_timeout``, ``compress``, ``client_name``

An example usage of the SQLExecuteQueryOperator to connect to ClickHouse:

.. exampleinclude:: /../../clickhousedb/tests/system/clickhouse/example_clickhouse.py
    :language: python
    :start-after: [START howto_operator_clickhouse]
    :end-before: [END howto_operator_clickhouse]
    :dedent: 4

Querying Data
^^^^^^^^^^^^^

Use a ``handler`` to return query results:

.. exampleinclude:: /../../clickhousedb/tests/system/clickhouse/example_clickhouse.py
    :language: python
    :start-after: [START howto_operator_clickhouse_query]
    :end-before: [END howto_operator_clickhouse_query]
    :dedent: 4

.. note::

    ``session_settings`` passed to :class:`~airflow.providers.clickhousedb.hooks.clickhouse.ClickHouseHook`
    directly (via ``hook_params``) take precedence over any ``session_settings`` defined in the
    connection's ``extra`` JSON field. Conflicting keys are resolved in favor of the constructor argument.
