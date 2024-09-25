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

SQLExecuteQueryOperator to connect to Snowflake
===============================================

Use the :class:`SQLExecuteQueryOperator <airflow.providers.common.sql.operators.sql>` to execute
SQL commands in a `Snowflake <https://docs.snowflake.com/en/>`__ database.

.. warning::
    Previously, SnowflakeOperator was used to perform this kind of operation. But at the moment SnowflakeOperator is deprecated and will be removed in future versions of the provider. Please consider to switch to SQLExecuteQueryOperator as soon as possible.


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

.. exampleinclude:: /../../providers/tests/system/snowflake/example_snowflake.py
    :language: python
    :start-after: [START howto_operator_snowflake]
    :end-before: [END howto_operator_snowflake]
    :dedent: 4


.. note::

  Parameters that can be passed onto the operator will be given priority over the parameters already given
  in the Airflow connection metadata (such as ``schema``, ``role``, ``database`` and so forth).


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

.. exampleinclude:: /../../providers/tests/system/snowflake/example_snowflake.py
    :language: python
    :start-after: [START howto_snowflake_sql_api_operator]
    :end-before: [END howto_snowflake_sql_api_operator]
    :dedent: 4


.. note::

  Parameters that can be passed onto the operator will be given priority over the parameters already given
  in the Airflow connection metadata (such as ``schema``, ``role``, ``database`` and so forth).
