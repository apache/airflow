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

.. _howto/connection:sql:

Connecting to SQL Databases
===========================

The common SQL provider has no connection type of its own. Its operators, sensor and
``@task.sql`` decorator work with a connection from a database provider: a ``postgres``
connection from the Postgres provider, a ``snowflake`` connection from the Snowflake provider,
and so on. Create the connection as the database provider documents it, then pass its ID to
the operator.

.. code-block:: python

    from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

    create_table = SQLExecuteQueryOperator(
        task_id="create_table",
        conn_id="my_postgres",
        sql="CREATE TABLE IF NOT EXISTS users (id INT, name TEXT)",
    )

The same task runs against MySQL, Snowflake or Trino by pointing ``conn_id`` at a connection
of that type. The SQL itself still has to be valid for that database.

Setting up the connection
-------------------------

1. Install the provider for your database, for example ``apache-airflow-providers-postgres``.
   :doc:`supported-database-types` lists the providers that work with common SQL.
2. Create a connection of that provider's type. The fields and extras are described on the
   provider's own connection page, such as
   :doc:`the Postgres connection <apache-airflow-providers-postgres:connections/postgres>`.
3. Pass the connection ID as ``conn_id``. There is no default connection ID, so every task has
   to name one. ``GenericTransfer`` takes two: ``source_conn_id`` and ``destination_conn_id``.

How the hook is chosen
----------------------

At run time, a common SQL operator looks up the connection and instantiates the hook class that
the database provider registered for that connection type. The hook has to be a subclass of
:class:`~airflow.providers.common.sql.hooks.sql.DbApiHook`, otherwise the task fails with an
error naming the hook class it found.

To see which hook a connection type resolves to, run:

.. code-block:: bash

    airflow providers hooks

If a connection type is missing from that list, its provider is not installed in the
environment the task runs in. If the hook is listed but is not a ``DbApiHook``, that connection
type cannot be used with common SQL (an HTTP or cloud storage connection, for example).

Overriding connection settings per task
---------------------------------------

To change how one task connects without editing a connection other tasks share, set
``database`` or ``hook_params`` on the operator.

``database``
    Runs the task against a different database from the one set in the connection. For most
    connection types this replaces the connection's ``schema`` field, which is where Postgres,
    MySQL and similar providers store the database name.

``hook_params``
    A dictionary passed as keyword arguments to the hook's constructor. Which keys a hook
    accepts is up to the database provider. For example, the Snowflake hook accepts
    ``warehouse`` and ``role``:

    .. code-block:: python

        SQLExecuteQueryOperator(
            task_id="nightly_rollup",
            conn_id="snowflake_default",
            sql="CALL rollup_daily_sales()",
            hook_params={"warehouse": "REPORTING_WH", "role": "REPORTER"},
        )

    For operators in ``airflow.providers.common.sql.operators.sql``, the connection's extras are
    merged into ``hook_params`` before the hook is built, and a key set in ``hook_params`` wins
    over the same key in the extras. ``SQLSensor`` takes ``hook_params`` too, and
    ``GenericTransfer`` takes ``source_hook_params`` and ``destination_hook_params``.

Connection extras read by common SQL
------------------------------------

Every ``DbApiHook`` reads the following keys from the connection's extras, on top of whatever
the database provider documents. They control the SQL that ``insert_rows``,
``SQLInsertRowsOperator`` and ``GenericTransfer`` generate, and they matter most for generic
connection types such as ODBC and JDBC, where Airflow cannot tell from the connection alone
which database is on the other end.

.. list-table::
   :header-rows: 1
   :widths: 25 20 55

   * - Extra
     - Default
     - Effect
   * - ``placeholder``
     - ``%s``
     - Parameter placeholder in generated statements. Only ``%s`` and ``?`` are accepted; any
       other value is ignored with a warning. Some hooks set their own default, such as ``?``
       for SQLite.
   * - ``sqlalchemy_scheme``
     - none (``mssql+pyodbc`` for ODBC)
     - SQLAlchemy scheme for connection types that have none of their own, such as ODBC and
       JDBC. The :doc:`dialect <dialects>` is taken from it, so ``mssql+pyodbc`` selects the
       ``mssql`` dialect. It takes precedence over ``dialect``.
   * - ``dialect``
     - ``default``
     - Name of the dialect to use, such as ``mssql`` or ``postgresql``. Read only when neither the
       connection URI nor ``sqlalchemy_scheme`` gives a dialect, as with a JDBC connection whose
       ``host`` holds a ``jdbc:`` URL.
   * - ``insert_statement_format``
     - ``INSERT INTO {} {} VALUES ({})``
     - Template for insert statements. The three fields are the table, the column list and the
       placeholders.
   * - ``replace_statement_format``
     - ``REPLACE INTO {} {} VALUES ({})``
     - Template for upsert statements, used when ``replace=True``.
   * - ``escape_word_format``
     - ``"{}"``
     - How a column name is quoted when it needs escaping, for example ``[{}]`` for SQL Server.
   * - ``escape_column_names``
     - ``false``
     - Quote every column name. By default only reserved words and names containing special
       characters are quoted.

For example, an ODBC connection to SQL Server that uses pyodbc's ``?`` placeholders and
bracket-quoted column names can set:

.. code-block:: json

    {
        "sqlalchemy_scheme": "mssql+pyodbc",
        "placeholder": "?",
        "escape_word_format": "[{}]"
    }

``mssql+pyodbc`` is already the ODBC default. An ODBC connection to any other database has to
change it, because setting ``dialect`` alone keeps the ``mssql`` dialect.

The ``mssql`` and ``postgresql`` dialects are registered by the Microsoft SQL Server and Postgres
providers. If the provider for a dialect is not installed, the hook uses the default dialect
without a warning, so an upsert through ODBC to SQL Server generates ``REPLACE INTO`` instead of
``MERGE`` unless ``apache-airflow-providers-microsoft-mssql`` is installed.
