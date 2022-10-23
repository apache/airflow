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

SnowflakeOperator
=================

Use the :class:`SnowflakeOperator <airflow.providers.snowflake.operators.snowflake>` to execute
SQL commands in a `Snowflake <https://docs.snowflake.com/en/>`__ database.


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
     - Snowflake user name
   * - Password: string
     - Password for Snowflake user
   * - Schema: string
     - Set schema to execute SQL operations on by default
   * - Extra: dictionary
     - ``warehouse``, ``account``, ``database``, ``region``, ``role``, ``authenticator``

An example usage of the SnowflakeOperator is as follows:

.. exampleinclude:: /../../tests/system/providers/snowflake/example_snowflake.py
    :language: python
    :start-after: [START howto_operator_snowflake]
    :end-before: [END howto_operator_snowflake]
    :dedent: 4


.. note::

  Parameters that can be passed onto the operator will be given priority over the parameters already given
  in the Airflow connection metadata (such as ``schema``, ``role``, ``database`` and so forth).
