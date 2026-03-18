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

.. exampleinclude:: /../../teradata/tests/system/teradata/example_teradata.py
    :language: python
    :dedent: 4
    :start-after: [START teradata_operator_howto_guide_create_table]
    :end-before: [END teradata_operator_howto_guide_create_table]

You can also use an external file to execute the SQL commands. External file must be at the same level as DAG.py file.
This way you can easily maintain the SQL queries separated from the code.

.. exampleinclude:: /../../teradata/tests/system/teradata/example_teradata.py
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

.. exampleinclude:: /../../teradata/tests/system/teradata/example_teradata.py
    :language: python
    :start-after: [START teradata_operator_howto_guide_populate_table]
    :end-before: [END teradata_operator_howto_guide_populate_table]


Fetching records from your Teradata database table
--------------------------------------------------

Fetching records from your Teradata database table can be as simple as:

.. exampleinclude:: /../../teradata/tests/system/teradata/example_teradata.py
    :language: python
    :start-after: [START teradata_operator_howto_guide_get_all_countries]
    :end-before: [END teradata_operator_howto_guide_get_all_countries]


Passing Parameters into TeradataOperator
----------------------------------------

TeradataOperator provides ``parameters`` attribute which makes it possible to dynamically inject values into your
SQL requests during runtime.

To find the countries in Asian continent:

.. exampleinclude:: /../../teradata/tests/system/teradata/example_teradata.py
    :language: python
    :start-after: [START teradata_operator_howto_guide_params_passing_get_query]
    :end-before: [END teradata_operator_howto_guide_params_passing_get_query]


Dropping a Teradata database table
--------------------------------------------------

We can then create a TeradataOperator task that drops the ``Users`` table.

.. exampleinclude:: /../../teradata/tests/system/teradata/example_teradata.py
    :language: python
    :start-after: [START teradata_operator_howto_guide_drop_users_table]
    :end-before: [END teradata_operator_howto_guide_drop_users_table]

The complete Teradata Operator Dag
----------------------------------

When we put everything together, our Dag should look like this:

.. exampleinclude:: /../../teradata/tests/system/teradata/example_teradata.py
    :language: python
    :start-after: [START teradata_operator_howto_guide]
    :end-before: [END teradata_operator_howto_guide]

TeradataStoredProcedureOperator
===============================

The purpose of TeradataStoredProcedureOperator is to define tasks involving executing teradata
stored procedures.

Execute a Stored Procedure in a Teradata database
-------------------------------------------------

To execute a Stored Procedure in an Teradata, use the
:class:`~airflow.providers.teradata.operators.teradata.TeradataStoredProcedureOperator`.

Assume a stored procedure exists in the database that looks like this:

    .. code-block:: sql

        REPLACE PROCEDURE TEST_PROCEDURE (
            IN val_in INTEGER,
            INOUT val_in_out INTEGER,
            OUT val_out INTEGER,
            OUT value_str_out varchar(100)
        )
            BEGIN
                set val_out = val_in * 2;
                set val_in_out = val_in_out * 4;
                set value_str_out = 'string output';
            END;
        /

This stored procedure takes an integer argument, val_in, as input.
It operates with a single inout argument, val_in_out, which serves as both input and output.
Additionally, it returns an integer argument, val_out, and a string argument, value_str_out.

This stored procedure can be invoked using
:class:`~airflow.providers.teradata.operators.teradata.TeradataStoredProcedureOperator` in various manners.

One approach involves passing parameters positionally as a list, with output parameters specified as Python data types:

.. exampleinclude:: /../../teradata/tests/system/teradata/example_teradata_call_sp.py
    :language: python
    :start-after: [START howto_call_teradata_stored_procedure_operator_with_types]
    :end-before: [END howto_call_teradata_stored_procedure_operator_with_types]

Alternatively, parameters can be passed positionally as a list, with output parameters designated as placeholders:

.. exampleinclude:: /../../teradata/tests/system/teradata/example_teradata_call_sp.py
    :language: python
    :start-after: [START howto_call_teradata_stored_procedure_operator_with_place_holder]
    :end-before: [END howto_call_teradata_stored_procedure_operator_with_place_holder]

Another method entails passing parameters positionally as a dictionary:

.. exampleinclude:: /../../teradata/tests/system/teradata/example_teradata_call_sp.py
    :language: python
    :start-after: [START howto_call_teradata_stored_procedure_operator_with_dict_input]
    :end-before: [END howto_call_teradata_stored_procedure_operator_with_dict_input]

Assume a stored procedure exists in the database that looks like this:

    .. code-block:: sql

       REPLACE PROCEDURE GetTimestampOutParameter (OUT out_timestamp TIMESTAMP)
          BEGIN
              -- Assign current timestamp to the OUT parameter
              SET out_timestamp = CURRENT_TIMESTAMP;
          END;
        /

This stored procedure yields a singular timestamp argument, out_timestamp, and is callable through
:class:`~airflow.providers.teradata.operators.teradata.TeradataStoredProcedureOperator`
with parameters passed positionally as a list:

.. exampleinclude:: /../../teradata/tests/system/teradata/example_teradata_call_sp.py
    :language: python
    :start-after: [START howto_call_teradata_stored_procedure_operator_timestamp]
    :end-before: [END howto_call_teradata_stored_procedure_operator_timestamp]


Assume a stored procedure exists in the database that looks like this:

    .. code-block:: sql

        REPLACE PROCEDURE
        TEST_PROCEDURE (IN val_in INTEGER, OUT val_out INTEGER)
          BEGIN
            DECLARE cur1 CURSOR WITH RETURN FOR SELECT * from DBC.DBCINFO ORDER BY 1 ;
            DECLARE cur2 CURSOR WITH RETURN FOR SELECT infodata, infokey from DBC.DBCINFO order by 1 ;
            open cur1 ;
            open cur2 ;
            set val_out = val_in * 2;
          END;
        /

This stored procedure takes a single integer argument, val_in, as input and produces a single integer argument, val_out.
Additionally, it yields two cursors representing the outputs of select queries.
This stored procedure can be invoked using
:class:`~airflow.providers.teradata.operators.teradata.TeradataStoredProcedureOperator`
with parameters passed positionally as a list:

.. exampleinclude:: /../../teradata/tests/system/teradata/example_teradata_call_sp.py
    :language: python
    :start-after: [START howto_teradata_stored_procedure_operator_with_in_out_dynamic_result]
    :end-before: [END howto_teradata_stored_procedure_operator_with_in_out_dynamic_result]

The complete TeradataStoredProcedureOperator Dag
------------------------------------------------

When we put everything together, our Dag should look like this:

.. exampleinclude:: /../../teradata/tests/system/teradata/example_teradata_call_sp.py
    :language: python
    :start-after: [START howto_teradata_operator_for_sp]
    :end-before: [END howto_teradata_operator_for_sp]
