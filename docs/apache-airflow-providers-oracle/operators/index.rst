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


.. _howto/operators:oracle:

Oracle Operators
================
The Oracle connection type provides connection to a Oracle database.

Execute SQL in an Oracle database
---------------------------------

To execute arbitrary SQL in an Oracle database, use the
:class:`~airflow.providers.oracle.operators.oracle.OracleOperator`.

An example of executing a simple query is as follows:

.. exampleinclude:: /../../providers/src/airflow/providers/oracle/example_dags/example_oracle.py
    :language: python
    :start-after: [START howto_oracle_operator]
    :end-before: [END howto_oracle_operator]


Execute a Stored Procedure in an Oracle database
------------------------------------------------

To execute a Stored Procedure in an Oracle database, use the
:class:`~airflow.providers.oracle.operators.oracle.OracleStoredProcedureOperator`.

Assume a stored procedure exists in the database that looks like this:

    .. code-block:: sql

        CREATE OR REPLACE PROCEDURE
        TEST_PROCEDURE (val_in IN INT, val_out OUT INT) AS
        BEGIN
        val_out := val_in * 2;
        END;
        /

This stored procedure accepts a single integer argument, val_in, and outputs
a single integer argument, val_out. This can be represented with the following
call using :class:`~airflow.providers.oracle.operators.oracle.OracleStoredProcedureOperator`
with parameters passed positionally as a list:

.. exampleinclude:: /../../providers/src/airflow/providers/oracle/example_dags/example_oracle.py
    :language: python
    :start-after: [START howto_oracle_stored_procedure_operator_with_list_inout]
    :end-before: [END howto_oracle_stored_procedure_operator_with_list_inout]


Alternatively, parameters can be passed as keyword arguments using a dictionary
as well.

.. exampleinclude:: /../../providers/src/airflow/providers/oracle/example_dags/example_oracle.py
    :language: python
    :start-after: [START howto_oracle_stored_procedure_operator_with_dict_inout]
    :end-before: [END howto_oracle_stored_procedure_operator_with_dict_inout]

Both input and output will be passed to xcom provided that xcom push is requested.

More on stored procedure execution can be found in `oracledb documentation
<https://python-oracledb.readthedocs.io/en/latest/user_guide/plsql_execution.html#pl-sql-stored-procedures>`_.
