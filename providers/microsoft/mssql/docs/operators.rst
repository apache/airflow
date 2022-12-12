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

.. _howto/operator:MsSqlOperator:

MsSqlOperator
=============

The purpose of MSSQL Operator is to define tasks involving interactions with the MSSQL database.

Use the :class:`~airflow.providers.microsoft.mssql.operators.mssql.MsSqlOperator` to execute
SQL commands in MSSQL database.

Common Database Operations with MsSqlOperator
------------------------------------------------

To use the mssql operator to carry out SQL request, two parameters are required: ``sql`` and ``mssql_conn_id``.
These two parameters are eventually fed to the MSSQL hook object that interacts directly with the MSSQL database.

Creating a MSSQL database table
----------------------------------

The code snippets below are based on Airflow-2.2

An example usage of the MsSqlOperator is as follows:

.. exampleinclude:: /../../tests/system/providers/microsoft/mssql/example_mssql.py
    :language: python
    :start-after: [START howto_operator_mssql]
    :end-before: [END howto_operator_mssql]

You can also use an external file to execute the SQL commands. Script folder must be at the same level as DAG.py file.
This way you can easily maintain the SQL queries separated from the code.

.. exampleinclude:: /../../tests/system/providers/microsoft/mssql/example_mssql.py
    :language: python
    :start-after: [START mssql_operator_howto_guide_create_table_mssql_from_external_file]
    :end-before: [END mssql_operator_howto_guide_create_table_mssql_from_external_file]


Your ``dags/create_table.sql`` should look like this:

.. code-block::sql

      -- create Users table
      CREATE TABLE Users (
        user_id INT NOT NULL IDENTITY(1,1) PRIMARY KEY,
        username TEXT,
        description TEXT
    );


Inserting data into a MSSQL database table
---------------------------------------------
We can then create a MsSqlOperator task that populate the ``Users`` table.

.. exampleinclude:: /../../tests/system/providers/microsoft/mssql/example_mssql.py
    :language: python
    :start-after: [START mssql_operator_howto_guide_populate_user_table]
    :end-before: [END mssql_operator_howto_guide_populate_user_table]


Fetching records from your MSSQL database table
--------------------------------------------------

Fetching records from your MSSQL database table can be as simple as:

.. exampleinclude:: /../../tests/system/providers/microsoft/mssql/example_mssql.py
    :language: python
    :start-after: [START mssql_operator_howto_guide_get_all_countries]
    :end-before: [END mssql_operator_howto_guide_get_all_countries]


Passing Parameters into MsSqlOperator
----------------------------------------

MsSqlOperator provides ``parameters`` attribute which makes it possible to dynamically inject values into your
SQL requests during runtime.

To find the countries in Asian continent:

.. exampleinclude:: /../../tests/system/providers/microsoft/mssql/example_mssql.py
    :language: python
    :start-after: [START mssql_operator_howto_guide_params_passing_get_query]
    :end-before: [END mssql_operator_howto_guide_params_passing_get_query]


The complete MSSQL Operator DAG
----------------------------------

When we put everything together, our DAG should look like this:

.. exampleinclude:: /../../tests/system/providers/microsoft/mssql/example_mssql.py
    :language: python
    :start-after: [START mssql_operator_howto_guide]
    :end-before: [END mssql_operator_howto_guide]
