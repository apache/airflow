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

.. _howto/operator:TeradataOperator:

TeradataOperator
================

The purpose of TeradataOperator is to define tasks involving interactions with the Teradata.

To execute arbitrary SQL in an Teradata, use the
:class:`~airflow.providers.teradata.operators.teradata.TeradataOperator`.

Common Database Operations with TeradataOperator
------------------------------------------------

Creating a Teradata database table
----------------------------------

An example usage of the TeradataOperator is as follows:

.. exampleinclude:: /../../tests/system/providers/teradata/example_teradata.py
    :language: python
    :dedent: 4
    :start-after: [START teradata_operator_howto_guide_create_table]
    :end-before: [END teradata_operator_howto_guide_create_table]

You can also use an external file to execute the SQL commands. External file must be at the same level as DAG.py file.
This way you can easily maintain the SQL queries separated from the code.

.. exampleinclude:: /../../tests/system/providers/teradata/example_teradata.py
    :language: python
    :start-after: [START teradata_operator_howto_guide_create_table_from_external_file]
    :end-before: [END teradata_operator_howto_guide_create_table_from_external_file]


Your ``dags/create_table.sql`` should look like this:

.. code-block:: sql

      -- create Users table
      CREATE TABLE Users, FALLBACK (
        username   varchar(50),
        description           varchar(256)
    );


Inserting data into a Teradata database table
---------------------------------------------
We can then create a TeradataOperator task that populate the ``Users`` table.

.. exampleinclude:: /../../tests/system/providers/teradata/example_teradata.py
    :language: python
    :start-after: [START teradata_operator_howto_guide_populate_table]
    :end-before: [END teradata_operator_howto_guide_populate_table]


Fetching records from your Teradata database table
--------------------------------------------------

Fetching records from your Teradata database table can be as simple as:

.. exampleinclude:: /../../tests/system/providers/teradata/example_teradata.py
    :language: python
    :start-after: [START teradata_operator_howto_guide_get_all_countries]
    :end-before: [END teradata_operator_howto_guide_get_all_countries]


Passing Parameters into TeradataOperator
----------------------------------------

TeradataOperator provides ``parameters`` attribute which makes it possible to dynamically inject values into your
SQL requests during runtime.

To find the countries in Asian continent:

.. exampleinclude:: /../../tests/system/providers/teradata/example_teradata.py
    :language: python
    :start-after: [START teradata_operator_howto_guide_params_passing_get_query]
    :end-before: [END teradata_operator_howto_guide_params_passing_get_query]


Dropping a Teradata database table
--------------------------------------------------

We can then create a TeradataOperator task that drops the ``Users`` table.

.. exampleinclude:: /../../tests/system/providers/teradata/example_teradata.py
    :language: python
    :start-after: [START teradata_operator_howto_guide_drop_users_table]
    :end-before: [END teradata_operator_howto_guide_drop_users_table]

The complete Teradata Operator DAG
----------------------------------

When we put everything together, our DAG should look like this:

.. exampleinclude:: /../../tests/system/providers/teradata/example_teradata.py
    :language: python
    :start-after: [START teradata_operator_howto_guide]
    :end-before: [END teradata_operator_howto_guide]
