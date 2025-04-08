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

SQL Dialects
=============

The :class:`~airflow.providers.common.sql.dialects.dialect.Dialect` offers an abstraction layer between the
:class:`~airflow.providers.common.sql.hooks.sql.DbApiHook` implementation and the database.  For some database multiple
connection types are available, like native, ODBC and or JDBC.  As the :class:`~airflow.providers.odbc.hooks.odbc.OdbcHook`
and the :class:`~airflow.providers.jdbc.hooks.jdbc.JdbcHook` are generic hooks which allows you to interact with any
database that has a driver for it, it needed an abstraction layer which allows us to run specialized queries
depending of the database to which we connect and that's why dialects where introduced.

The default :class:`~airflow.providers.common.sql.dialects.dialect.Dialect` class has following operations
available which underneath use SQLAlchemy to execute, but can be overloaded with specialized implementations
per database:

- ``placeholder`` specifies the database specific placeholder used in prepared statements (default: ``%s``);
- ``inspector`` returns the SQLAlchemy inspector which allows us to retrieve database metadata;
- ``extract_schema_from_table`` allows us to extract the schema name from a string.
- ``get_column_names`` returns the column names for the given table and schema (optional) using the SQLAlchemy inspector.
- ``get_primary_keys`` returns the primary keys for the given table and schema (optional) using the SQLAlchemy inspector.
- ``get_target_fields`` returns the columns names that aren't identity or auto incremented columns, this will be used by the insert_rows method of the :class:`~airflow.providers.common.sql.hooks.sql.DbApiHook` if the target_fields parameter wasn't specified and the Airflow property ``core.dbapihook_resolve_target_fields`` is set to True (default: False).
- ``reserved_words`` returns the reserved words in SQL for the target database using the SQLAlchemy inspector.
- ``generate_insert_sql`` generates the insert SQL statement for the target database.
- ``generate_replace_sql`` generates the upsert SQL statement for the target database.

At the moment there are only 3 dialects available:

- ``default`` :class:`~airflow.providers.common.sql.dialects.dialect.Dialect` reuses the generic functionality that was already available in the :class:`~airflow.providers.common.sql.hooks.sql.DbApiHook`;
- ``mssql`` :class:`~airflow.providers.microsoft.mssql.dialects.mssql.MsSqlDialect` specialized for Microsoft SQL Server;
- ``postgresql`` :class:`~airflow.providers.postgres.dialects.postgres.PostgresDialect` specialized for PostgreSQL;

The dialect to be used will be derived from the connection string, which sometimes won't be possible.  There is always
the possibility to specify the dialect name through the extra options of the connection:

.. code-block::

  dialect_name: 'mssql'

If a specific dialect isn't available for a database, the default one will be used, same when a non-existing dialect name is specified.
