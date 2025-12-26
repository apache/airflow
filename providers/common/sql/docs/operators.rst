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

.. exampleinclude:: /../tests/system/common/sql/example_sql_execute_query.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_sql_execute_query]
    :end-before: [END howto_operator_sql_execute_query]

.. _howto/operator:SQLColumnCheckOperator:

Check SQL Table Columns
~~~~~~~~~~~~~~~~~~~~~~~

Use the :class:`~airflow.providers.common.sql.operators.sql.SQLColumnCheckOperator` to run data quality
checks against columns of a given table. As well as a connection ID and table, a column_mapping
describing the relationship between columns and tests to run must be supplied. An example column mapping
is a set of three nested dictionaries and looks like:

.. code-block:: python

        column_mapping = {
            "col_name": {
                "null_check": {"equal_to": 0, "partition_clause": "other_col LIKE 'this'"},
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

Each entry in the check's dictionary is either a condition for success of the check, the tolerance,
or a partition clause. The conditions for success are:

- greater_than
- geq_to
- less_than
- leq_to
- equal_to

When specifying conditions, equal_to is not compatible with other conditions. Both a lower- and an upper-
bound condition may be specified in the same check. The tolerance is a percentage that the result may
be out of bounds but still considered successful.

The partition clauses may be given at the operator level as a parameter where it partitions all checks,
at the column level in the column mapping where it partitions all checks for that column, or at the
check level for a column where it partitions just that check.

A database may also be specified if not using the database from the supplied connection.

The accept_none argument, true by default, will convert None values returned by the query to 0s, allowing
empty tables to return valid integers.

The below example demonstrates how to instantiate the SQLColumnCheckOperator task.

.. exampleinclude:: /../tests/system/common/sql/example_sql_column_table_check.py
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
                "column_sum_check": {
                    "check_statement": "col_a + col_b < col_c",
                    "partition_clause": "col_a IS NOT NULL",
                },
            },
        )

The first set of keys are the check names, which are referenced in the templated query the operator builds.
A dictionary key under the check name must include check_statement and the value a SQL statement that
resolves to a boolean (this can be any string or int that resolves to a boolean in
airflow.operators.sql.parse_boolean). The other possible key to supply is partition_clause, which is a
check level statement that will partition the data in the table using a WHERE clause for that check.
This statement is compatible with the parameter partition_clause, where the latter filters across all
checks.

The below example demonstrates how to instantiate the SQLTableCheckOperator task.

.. exampleinclude:: /../tests/system/common/sql/example_sql_column_table_check.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_sql_table_check]
    :end-before: [END howto_operator_sql_table_check]


.. _howto/operator:SQLValueCheckOperator:

Check value against expected
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Use the :class:`~airflow.providers.common.sql.operators.sql.SQLValueCheckOperator` to compare a SQL query result
against an expected value, with some optionally specified tolerance for numeric results.
The parameters for this operator are:

- ``sql`` - the sql query to be executed, as a templated string.
- ``pass_value`` - the expected value to compare the query result against.
- ``tolerance`` (optional) - numerical tolerance for comparisons involving numeric values.
- ``conn_id`` (optional) - the connection ID used to connect to the database.
- ``database`` (optional) - name of the database which overwrites the name defined in the connection.

The below example demonstrates how to instantiate the SQLValueCheckOperator task.

.. exampleinclude:: /../tests/system/common/sql/example_sql_value_check.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_sql_value_check]
    :end-before: [END howto_operator_sql_value_check]

.. _howto/operator:SQLThresholdCheckOperator:

Check values against a threshold
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Use the :class:`~airflow.providers.common.sql.operators.sql.SQLThresholdCheckOperator` to compare a specific SQL query result against defined minimum and maximum thresholds.
Both thresholds can either be a numeric value or another SQL query that evaluates to a numeric value.
This operator requires a connection ID, along with the SQL query to execute, and allows optional specification of a database, if the one from the connection_id should be overridden.
The parameters are:
- ``sql`` - the sql query to be executed, as a templated string.
- ``min_threshold`` The minimum threshold that is checked against. Either as a numeric value or templated sql query.
- ``max_threshold`` The maximum threshold that is checked against. Either as a numeric value or templated sql query.
- ``conn_id`` (optional) The connection ID used to connect to the database.
- ``database`` (optional) name of the database which overwrites the one from the connection.


The below example demonstrates how to instantiate the SQLThresholdCheckOperator task.

.. exampleinclude:: /../tests/system/common/sql/example_sql_threshold_check.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_sql_threshold_check]
    :end-before: [END howto_operator_sql_threshold_check]

If the value returned by the query, is within the thresholds, the task passes. Otherwise, it fails.

.. _howto/operator:SQLInsertRowsOperator:

Insert rows into Table
~~~~~~~~~~~~~~~~~~~~~~

Use the :class:`~airflow.providers.common.sql.operators.sql.SQLInsertRowsOperator` to insert rows into a database table
directly from Python data structures or an XCom. Parameters of the operator are:

- ``table_name`` - name of the table in which the rows will be inserted (templated).
- ``conn_id`` - the Airflow connection ID used to connect to the database.
- ``schema`` (optional) - the schema in which the table is defined.
- ``database`` (optional) - name of the database which overrides the one defined in the connection.
- ``columns`` (optional) - list of columns to use for the insert when passing a list of dictionaries.
- ``ignored_columns`` (optional) - list of columns to ignore for the insert, if no columns are specified,
  columns will be dynamically resolved from the metadata.
- ``rows`` - rows to insert, a list of tuples.
- ``rows_processor`` (optional) - a function applied to the rows before inserting them.
- ``preoperator`` (optional) - SQL statement or list of statements to execute before inserting data (templated).
- ``postoperator`` (optional) - SQL statement or list of statements to execute after inserting data (templated).
- ``hook_params`` (optional) - dictionary of additional parameters passed to the underlying hook.
- ``insert_args`` (optional) - dictionary of additional arguments passed to the hook's ``insert_rows`` method,
  can include ``replace``, ``executemany``, ``fast_executemany``, ``autocommit``, and others supported by the hook.

The example below shows how to instantiate the SQLInsertRowsOperator task.

.. exampleinclude:: /../tests/system/common/sql/example_sql_insert_rows.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_sql_insert_rows]
    :end-before: [END howto_operator_sql_insert_rows]

.. _howto/operator:GenericTransfer:

Generic Transfer
~~~~~~~~~~~~~~~~

Use the :class:`~airflow.providers.common.sql.operators.generic_transfer.GenericTransfer` to transfer data between
between two connections.

.. exampleinclude:: /../tests/system/common/sql/example_generic_transfer.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_generic_transfer]
    :end-before: [END howto_operator_generic_transfer]
