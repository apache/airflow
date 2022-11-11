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

SQL Operators
=============

These operators perform various queries against a SQL database, including
column- and table-level data quality checks.

.. _howto/operator:SQLExecuteQueryOperator:

Execute SQL query
~~~~~~~~~~~~~~~~~

Use the :class:`~airflow.providers.common.sql.operators.sql.SQLExecuteQueryOperator` to run SQL query against
different databases. Parameters of the operators are:

- ``sql`` - single string, list of strings or string pointing to a template file to be executed;
- ``autocommit`` (optional) if True, each command is automatically committed (default: False);
- ``parameters`` (optional) the parameters to render the SQL query with.
- ``handler`` (optional) the function that will be applied to the cursor. If it's ``None`` results won't returned (default: fetch_all_handler).
- ``split_statements`` (optional) if split single SQL string into statements and run separately (default: False).
- ``return_last`` (optional) depends ``split_statements`` and if it's ``True`` this parameter is used to return the result of only last statement or all split statements (default: True).

The example below shows how to instantiate the SQLExecuteQueryOperator task.

.. exampleinclude:: /../../tests/system/providers/common/sql/example_sql_execute_query.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_sql_execute_query]
    :end-before: [END howto_operator_sql_execute_query]

.. _howto/operator:SQLColumnCheckOperator:

Check SQL Table Columns
~~~~~~~~~~~~~~~~~~~~~~~

Use the :class:`~airflow.providers.common.sql.operators.sql.SQLColumnCheckOperator` to run data quality
checks against columns of a given table. As well as a connection ID and table, a column_mapping
describing the relationship between columns and tests to run must be supplied. An example column
mapping is a set of three nested dictionaries and looks like:

.. code-block:: python

        column_mapping = {
            "col_name": {
                "null_check": {
                    "equal_to": 0,
                },
                "min": {
                    "greater_than": 5,
                    "leq_to": 10,
                    "tolerance": 0.2,
                },
                "max": {"less_than": 1000, "geq_to": 10, "tolerance": 0.01},
            }
        }

Where col_name is the name of the column to run checks on, and each entry in its dictionary is a check.
The valid checks are:

- null_check: checks the number of NULL values in the column
- distinct_check: checks the COUNT of values in the column that are distinct
- unique_check: checks the number of distinct values in a column against the number of rows
- min: checks the minimum value in the column
- max: checks the maximum value in the column

Each entry in the check's dictionary is either a condition for success of the check or the tolerance. The
conditions for success are:

- greater_than
- geq_to
- less_than
- leq_to
- equal_to

When specifying conditions, equal_to is not compatible with other conditions. Both a lower- and an upper-
bound condition may be specified in the same check. The tolerance is a percentage that the result may
be out of bounds but still considered successful.



The below example demonstrates how to instantiate the SQLColumnCheckOperator task.

.. exampleinclude:: /../../tests/system/providers/common/sql/example_sql_column_table_check.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_sql_column_check]
    :end-before: [END howto_operator_sql_column_check]

.. _howto/operator:SQLTableCheckOperator:

Check SQL Table Values
~~~~~~~~~~~~~~~~~~~~~~~

Use the :class:`~airflow.providers.common.sql.operators.sql.SQLTableCheckOperator` to run data quality
checks against a given table. As well as a connection ID and table, a checks dictionary
describing the relationship between the table and tests to run must be supplied. An example
checks argument is a set of two nested dictionaries and looks like:

.. code-block:: python

        checks = (
            {
                "row_count_check": {
                    "check_statement": "COUNT(*) = 1000",
                },
                "column_sum_check": {"check_statement": "col_a + col_b < col_c"},
            },
        )

The first set of keys are the check names, which are referenced in the templated query the operator builds.
The dictionary key under the check name must be check_statement, with the value a SQL statement that
resolves to a boolean (this can be any string or int that resolves to a boolean in
airflow.operators.sql.parse_boolean).

The below example demonstrates how to instantiate the SQLTableCheckOperator task.

.. exampleinclude:: /../../tests/system/providers/common/sql/example_sql_column_table_check.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_sql_table_check]
    :end-before: [END howto_operator_sql_table_check]
