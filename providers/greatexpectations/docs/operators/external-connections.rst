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

Manage Data Source credentials with Airflow Connections
========================================================

The Great Expectations Airflow Provider includes convenience functions to retrieve connection credentials
from other Airflow provider Connections. These functions allow you to reuse existing Airflow connections
when configuring data sources in your GX BatchDefinitions and Checkpoints.

Supported Connection Types
---------------------------

The following external Connections are supported:

.. list-table::
   :header-rows: 1
   :widths: 30 40 30

   * - Connection Type
     - API Function
     - External Provider Documentation
   * - Amazon Redshift
     - ``build_redshift_connection_string(conn_id, schema=None)``
     - `Amazon Provider <https://airflow.apache.org/docs/apache-airflow-providers-amazon/stable/connections/redshift.html>`__
   * - MySQL
     - ``build_mysql_connection_string(conn_id, schema=None)``
     - `MySQL Provider <https://airflow.apache.org/docs/apache-airflow-providers-mysql/stable/connections/mysql.html>`__
   * - Microsoft SQL Server
     - ``build_mssql_connection_string(conn_id, schema=None)``
     - `Microsoft SQL Server Provider <https://airflow.apache.org/docs/apache-airflow-providers-microsoft-mssql/stable/connections/mssql.html>`__
   * - PostgreSQL
     - ``build_postgres_connection_string(conn_id, schema=None)``
     - `PostgreSQL Provider <https://airflow.apache.org/docs/apache-airflow-providers-postgres/stable/connections/postgres.html>`__
   * - Snowflake
     - ``build_snowflake_connection_string(conn_id, schema=None)``
     - `Snowflake Provider <https://airflow.apache.org/docs/apache-airflow-providers-snowflake/stable/connections/snowflake.html>`__
   * - Snowflake (Key-based Auth)
     - ``build_snowflake_key_connection(conn_id, schema=None)``
     - `Snowflake Provider <https://airflow.apache.org/docs/apache-airflow-providers-snowflake/stable/connections/snowflake.html>`__
   * - Google Cloud BigQuery
     - ``build_gcpbigquery_connection_string(conn_id, schema=None)``
     - `Google Cloud Provider <https://airflow.apache.org/docs/apache-airflow-providers-google/stable/connections/gcp.html>`__
   * - SQLite
     - ``build_sqlite_connection_string(conn_id)``
     - `SQLite Provider <https://airflow.apache.org/docs/apache-airflow-providers-sqlite/stable/connections/sqlite.html>`__
   * - AWS Athena
     - ``build_aws_connection_string(conn_id, schema=None, database=None, s3_path=None, region=None)``
     - `Amazon Provider <https://airflow.apache.org/docs/apache-airflow-providers-amazon/stable/connections/aws.html>`__

Usage
-----

To use these functions, first install the Airflow Provider that maintains the connection you need,
and use the Airflow UI to configure the Connection with your credentials. Then, import the function
you need from ``airflow.providers.greatexpectations.common.external_connections`` and use it within
your ``configure_batch_definition`` or ``configure_checkpoint`` function.

PostgreSQL Example
^^^^^^^^^^^^^^^^^^

.. code-block:: python

    from __future__ import annotations

    from typing import TYPE_CHECKING

    from airflow.providers.greatexpectations.common.external_connections import (
        build_postgres_connection_string,
    )

    if TYPE_CHECKING:
        from great_expectations.data_context import AbstractDataContext
        from great_expectations.core.batch_definition import BatchDefinition


    def configure_postgres_batch_definition(
        context: AbstractDataContext,
    ) -> BatchDefinition:
        task_id = "example_task"
        table_name = "example_table"
        postgres_conn_id = "example_conn_id"
        return (
            context.data_sources.add_postgres(
                name=task_id,
                connection_string=build_postgres_connection_string(conn_id=postgres_conn_id),
            )
            .add_table_asset(
                name=task_id,
                table_name=table_name,
            )
            .add_batch_definition_whole_table(task_id)
        )


Function Reference
------------------

Amazon Redshift
^^^^^^^^^^^^^^^

``build_redshift_connection_string(conn_id: str, schema: str | None = None) -> str``

Build a connection string for Amazon Redshift connections.

**Parameters:**

* ``conn_id``: Airflow connection ID for the Redshift connection
* ``schema`` (optional): Schema override (used as database name)

**Returns:** Connection string for Redshift

For information on configuring the Airflow connection, see the
`Amazon Redshift Connection documentation <https://airflow.apache.org/docs/apache-airflow-providers-amazon/stable/connections/redshift.html>`__.


MySQL
^^^^^

``build_mysql_connection_string(conn_id: str, schema: str | None = None) -> str``

Build a connection string for MySQL connections.

**Parameters:**

* ``conn_id``: Airflow connection ID for the MySQL connection
* ``schema`` (optional): Schema override (used as database name)

**Returns:** Connection string for MySQL

For information on configuring the Airflow connection, see the
`MySQL Connection documentation <https://airflow.apache.org/docs/apache-airflow-providers-mysql/stable/connections/mysql.html>`__.


Microsoft SQL Server
^^^^^^^^^^^^^^^^^^^^

``build_mssql_connection_string(conn_id: str, schema: str | None = None) -> str``

Build a connection string for Microsoft SQL Server connections.

**Parameters:**

* ``conn_id``: Airflow connection ID for the MSSQL connection
* ``schema`` (optional): Schema override (used as database name, defaults to "master")

**Returns:** Connection string for MSSQL

**Note:** The driver is retrieved from the connection extras, defaulting to "ODBC Driver 17 for SQL Server".

For information on configuring the Airflow connection, see the
`Microsoft SQL Server Connection documentation <https://airflow.apache.org/docs/apache-airflow-providers-microsoft-mssql/stable/connections/mssql.html>`__.


PostgreSQL
^^^^^^^^^^

``build_postgres_connection_string(conn_id: str, schema: str | None = None) -> str``

Build a connection string for PostgreSQL connections.

**Parameters:**

* ``conn_id``: Airflow connection ID for the PostgreSQL connection
* ``schema`` (optional): Schema override (used as database name)

**Returns:** Connection string for PostgreSQL

For information on configuring the Airflow connection, see the
`PostgreSQL Connection documentation <https://airflow.apache.org/docs/apache-airflow-providers-postgres/stable/connections/postgres.html>`__.


Snowflake
^^^^^^^^^

``build_snowflake_connection_string(conn_id: str, schema: str | None = None) -> str``

Build a connection string for Snowflake connections using username/password authentication.

**Parameters:**

* ``conn_id``: Airflow connection ID for the Snowflake connection
* ``schema`` (optional): Schema override

**Returns:** Connection string for Snowflake

**Note:** This function attempts to use ``SnowflakeHook`` if the Snowflake provider package is installed.
If not available, it constructs the connection string manually from the connection parameters.

For information on configuring the Airflow connection, see the
`Snowflake Connection documentation <https://airflow.apache.org/docs/apache-airflow-providers-snowflake/stable/connections/snowflake.html>`__.


Snowflake (Key-based Authentication)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

``build_snowflake_key_connection(conn_id: str, schema: str | None = None) -> SnowflakeKeyConnection``

Build a key-based connection configuration for Snowflake connections using private key authentication.

**Parameters:**

* ``conn_id``: Airflow connection ID for the Snowflake connection
* ``schema`` (optional): Schema override

**Returns:** ``SnowflakeKeyConnection`` object containing key-based connection configuration

**Requirements:**

* Requires the Snowflake provider package (``apache-airflow-providers-snowflake``)
* The connection must have a ``private_key_file`` configured in the extras

For information on configuring private key authentication, see the
`Snowflake Connection documentation <https://airflow.apache.org/docs/apache-airflow-providers-snowflake/stable/connections/snowflake.html>`__.


Google Cloud BigQuery
^^^^^^^^^^^^^^^^^^^^^

``build_gcpbigquery_connection_string(conn_id: str, schema: str | None = None) -> str``

Build a connection string for Google Cloud BigQuery connections.

**Parameters:**

* ``conn_id``: Airflow connection ID for the BigQuery connection
* ``schema`` (optional): Schema override

**Returns:** Connection string for BigQuery

For information on configuring the Airflow connection, see the
`Google Cloud Connection documentation <https://airflow.apache.org/docs/apache-airflow-providers-google/stable/connections/gcp.html>`__.


SQLite
^^^^^^

``build_sqlite_connection_string(conn_id: str) -> str``

Build a connection string for SQLite connections.

**Parameters:**

* ``conn_id``: Airflow connection ID for the SQLite connection

**Returns:** Connection string for SQLite

For information on configuring the Airflow connection, see the
`SQLite Connection documentation <https://airflow.apache.org/docs/apache-airflow-providers-sqlite/stable/connections/sqlite.html>`__.


AWS Athena
^^^^^^^^^^

``build_aws_connection_string(conn_id: str, schema: str | None = None, database: str | None = None, s3_path: str | None = None, region: str | None = None) -> str``

Build a connection string for AWS Athena connections.

**Parameters:**

* ``conn_id``: Airflow connection ID for the AWS connection
* ``schema`` (optional): Schema override
* ``database`` (optional): Database name for Athena
* ``s3_path`` (required): S3 staging directory path
* ``region`` (required): AWS region

**Returns:** Connection string for AWS Athena

For information on configuring the Airflow connection, see the
`AWS Connection documentation <https://airflow.apache.org/docs/apache-airflow-providers-amazon/stable/connections/aws.html>`__
and `Athena Connection documentation <https://airflow.apache.org/docs/apache-airflow-providers-amazon/stable/connections/athena.html>`__.
