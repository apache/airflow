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

.. _howto/operator:SnowflakeOperator:

SQLExecuteQueryOperator for Snowflake
=====================================

Use the :class:`SQLExecuteQueryOperator <airflow.providers.common.sql.operators.sql>` to execute
SQL commands in a `Snowflake <https://docs.snowflake.com/en/>`__ database.


Using the Operator
^^^^^^^^^^^^^^^^^^

Use the ``conn_id`` argument to connect to your Snowflake instance where
the connection metadata is structured as follows:

.. list-table:: Snowflake Airflow Connection Metadata
   :widths: 25 25
   :header-rows: 1

   * - Parameter
     - Input
   * - Login: string
     - Snowflake user name
   * - Password: string
     - Password for Snowflake user
   * - Schema: string
     - Set schema to execute SQL operations on by default
   * - Extra: dictionary
     - ``warehouse``, ``account``, ``database``, ``region``, ``role``, ``authenticator``

An example usage of the SQLExecuteQueryOperator to connect to Snowflake is as follows:

.. exampleinclude:: /../../snowflake/tests/system/snowflake/example_snowflake.py
    :language: python
    :start-after: [START howto_operator_snowflake]
    :end-before: [END howto_operator_snowflake]
    :dedent: 4


.. note::

  Parameters that can be passed onto the operator will be given priority over the parameters already given
  in the Airflow connection metadata (such as ``schema``, ``role``, ``database`` and so forth).

.. _howto/operator:SnowflakeCheckOperator:

SnowflakeCheckOperator
^^^^^^^^^^^^^^^^^^^^^^

To perform checks against Snowflake you can use
:class:`~airflow.providers.snowflake.operators.snowflake.SnowflakeCheckOperator`

This operator expects a SQL query that will return a single row. Each value on
that first row is evaluated using Python ``bool`` casting. If any of the values
return ``False`` the check fails and errors out.

.. exampleinclude:: /../../snowflake/tests/system/snowflake/example_snowflake.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_snowflake_check]
    :end-before: [END howto_operator_snowflake_check]

.. _howto/operator:SnowflakeValueCheckOperator:

SnowflakeValueCheckOperator
^^^^^^^^^^^^^^^^^^^^^^^^^^^

To perform a simple value check using SQL code you can use
:class:`~airflow.providers.snowflake.operators.snowflake.SnowflakeValueCheckOperator`

This operator expects a SQL query that will return a single row. That value is
evaluated against ``pass_value``, which can be either a string or numeric value.
If numeric, you can also specify ``tolerance``.

.. exampleinclude:: /../../snowflake/tests/system/snowflake/example_snowflake.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_snowflake_value_check]
    :end-before: [END howto_operator_snowflake_value_check]

.. _howto/operator:SnowflakeIntervalCheckOperator:

SnowflakeIntervalCheckOperator
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To check that the values of metrics given as SQL expressions are within a certain
tolerance of the ones from ``days_back`` before you can use
:class:`~airflow.providers.snowflake.operators.snowflake.SnowflakeIntervalCheckOperator`

.. exampleinclude:: /../../snowflake/tests/system/snowflake/example_snowflake.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_snowflake_interval_check]
    :end-before: [END howto_operator_snowflake_interval_check]


SnowflakeSqlApiOperator
=======================

Use the :class:`SnowflakeSqlApiHook <airflow.providers.snowflake.operators.snowflake>` to execute
SQL commands in a `Snowflake <https://docs.snowflake.com/en/>`__ database.

You can also run this operator in deferrable mode by setting ``deferrable`` param to ``True``.
This will ensure that the task is deferred from the Airflow worker slot and polling for the task status happens on the trigger.

Using the Operator
^^^^^^^^^^^^^^^^^^

Use the ``snowflake_conn_id`` argument to connect to your Snowflake instance where
the connection metadata is structured as follows:

.. list-table:: Snowflake Airflow Connection Metadata
   :widths: 25 25
   :header-rows: 1

   * - Parameter
     - Input
   * - Login: string
     - Snowflake user name. If using `OAuth connection <https://docs.snowflake.com/en/developer-guide/sql-api/authenticating#using-oauth>`__ this is the ``client_id``
   * - Password: string
     - Password for Snowflake user. If using OAuth this is the ``client_secret``
   * - Schema: string
     - Set schema to execute SQL operations on by default
   * - Extra: dictionary
     - ``warehouse``, ``account``, ``database``, ``region``, ``role``, ``authenticator``, ``refresh_token``. If using OAuth must specify ``refresh_token`` (`obtained here <https://community.snowflake.com/s/article/HOW-TO-OAUTH-TOKEN-GENERATION-USING-SNOWFLAKE-CUSTOM-OAUTH>`__)

An example usage of the SnowflakeSqlApiHook is as follows:

.. exampleinclude:: /../../snowflake/tests/system/snowflake/example_snowflake.py
    :language: python
    :start-after: [START howto_snowflake_sql_api_operator]
    :end-before: [END howto_snowflake_sql_api_operator]
    :dedent: 4


.. note::

  Parameters that can be passed onto the operator will be given priority over the parameters already given
  in the Airflow connection metadata (such as ``schema``, ``role``, ``database`` and so forth).

Durable execution
^^^^^^^^^^^^^^^^^^

``SnowflakeSqlApiOperator`` submits one or more SQL statements and then polls their statement
handles to completion on the worker. By default the operator runs in a *durable* mode that makes
this crash-safe: the statement handles are persisted to :doc:`task state store
<apache-airflow:core-concepts/task-state-store>` before polling begins, so if the worker crashes
or is preempted and the task is retried, the operator reconnects to the statements that are
already executing in Snowflake instead of resubmitting the SQL.

On retry the operator checks the prior statements' state:

* if any handle is still running, the operator reconnects and continues polling -- handles that
  already finished are not re-run, only the ones still in progress are waited on
* if every handle already succeeded, the operator returns immediately without resubmitting
* if any handle failed, or a handle has expired past Snowflake's retention window, the operator
  submits the SQL fresh

Because Snowflake's SQL API has no way to retry or repair a single failed statement within a
multi-statement request, a genuine failure always resubmits the whole request batch, matching the
operator's all-or-nothing submission semantics.

Durable execution requires Airflow 3.3 or newer, since it relies on the task state store. On
earlier Airflow versions the flag is a no-op and the operator always submits fresh SQL on retry,
exactly as before. If the task state store is unavailable at runtime, the operator logs that crash
recovery is disabled and behaves the same way.

To opt out and always submit fresh SQL on retry, set ``durable=False``:

.. code-block:: python

  api_operator = SnowflakeSqlApiOperator(
      task_id="snowflake_sql_api",
      snowflake_conn_id="snowflake_default",
      sql="select * from table",
      statement_count=1,
      durable=False,
  )

Durable execution applies to the synchronous path. When ``deferrable=True`` is set, the Triggerer
already tracks the statement handles across the wait, so deferrable mode takes precedence and
``durable`` has no effect.
