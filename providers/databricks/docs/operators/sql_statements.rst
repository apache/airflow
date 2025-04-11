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

.. _howto/operator:DatabricksSQLStatementsOperator:


DatabricksSQLStatementsOperator
===============================

Use the :class:`~airflow.providers.databricks.operators.databricks.DatabricksSQLStatementsOperator` to submit a
Databricks SQL Statement to Databricks using the
`Databricks SQL Statement Execution API <https://docs.databricks.com/api/workspace/statementexecution>`_.


Using the Operator
------------------

The ``DatabricksSQLStatementsOperator`` submits SQL statements to Databricks using the
`/api/2.0/sql/statements/ <https://docs.databricks.com/api/workspace/statementexecution/executestatement>`_ endpoint.
It supports configurable execution parameters such as warehouse selection, catalog, schema, and parameterized queries.
The operator can either synchronously poll for query completion or run in a deferrable mode for improved efficiency.

The only required parameters for using the operator are:

* ``statement`` - The SQL statement to execute. The statement can optionally be parameterized, see parameters.
* ``warehouse_id`` - Warehouse upon which to execute a statement.

All other parameters are optional and described in the documentation for ``DatabricksSQLStatementsOperator`` including
but not limited to:

* ``catalog``
* ``schema``
* ``parameters``

Examples
--------

An example usage of the ``DatabricksSQLStatementsOperator`` is as follows:

.. exampleinclude:: /../../databricks/tests/system/databricks/example_databricks.py
    :language: python
    :start-after: [START howto_operator_sql_statements]
    :end-before: [END howto_operator_sql_statements]
