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



.. _howto/connection:mssql:

MSSQL Connection
======================
The MSSQL connection type enables connection to `Microsoft SQL Server <https://www.microsoft.com/en-in/sql-server/>`__.

Default Connection IDs
----------------------

MSSQL Hook uses parameter ``mssql_conn_id`` for the connection ID. The default value is ``mssql_default``.

Configuring the Connection
--------------------------
Host (required)
    The host to connect to.

Schema (optional)
    Specify the schema name to be used in the database.

Login (required)
    Specify the user name to connect.

Password (required)
    Specify the password to connect.

Port (required)
    The port to connect.

Extra (optional)
    Specify the extra parameters (as json dictionary) that can be used in MSSQL
    connection.

    More details on all MSSQL parameters supported can be found in
    `MSSQL documentation <https://docs.microsoft.com/en-us/sql/connect/jdbc/setting-the-connection-properties?view=sql-server-ver15>`_.

When specifying the connection as URI (in :envvar:`AIRFLOW_CONN_{CONN_ID}` variable) you should specify it
following the standard syntax of DB connections - where extras are passed as parameters
of the URI. Note that all components of the URI should be URL-encoded.

For example:

.. code-block:: bash

   export AIRFLOW_CONN_MSSQL_DEFAULT='mssql://username:password@server.com:1433/database_name'

If serializing with JSON:

.. code-block:: bash

    export AIRFLOW_CONN_MSSQL_DEFAULT='{
        "conn_type": "mssql",
        "login": "username",
        "password": "password",
        "host": "server.com",
        "port": 1433,
        "schema": "database_name"
    }'

Choosing the DBAPI driver
-------------------------

By default the hook connects with `pymssql <https://pypi.org/project/pymssql/>`__. Microsoft's own
`mssql-python <https://pypi.org/project/mssql-python/>`__ driver is available as an alternative. Compared
with pymssql it supports Microsoft Entra ID authentication, and it uses Microsoft's ODBC driver (installed
with it) instead of FreeTDS. Switching changes a few things, which are listed in the notes below. For
example, SQL written with ``%s`` placeholders has to be rewritten to use ``?``.

To use it, install the extra:

.. code-block:: bash

    pip install 'apache-airflow-providers-microsoft-mssql[mssql-python]'

and set ``dbapi_driver`` to ``mssql_python`` in the connection extra. As a URI, the extras go in as query parameters:

.. code-block:: bash

    export AIRFLOW_CONN_MSSQL_DEFAULT='mssql://username:password@example.com:1433/database_name?dbapi_driver=mssql_python&TrustServerCertificate=yes'

If serializing with JSON:


.. code-block:: bash

    export AIRFLOW_CONN_MSSQL_DEFAULT='{
        "conn_type": "mssql",
        "login": "username",
        "password": "password",
        "host": "example.com",
        "port": 1433,
        "schema": "database_name",
        "extra": {"dbapi_driver": "mssql_python", "TrustServerCertificate": "yes"}
    }'

Notes
~~~~~

* ``dbapi_driver`` accepts ``pymssql`` (the default) or ``mssql_python`` and is case-insensitive.
* Additional driver options, such as ``Encrypt``, ``TrustServerCertificate`` and
  ``Authentication``, can be supplied through connection extras.
* The default SQLAlchemy scheme is ``mssql+mssqlpython``, which requires SQLAlchemy 2.1
  or newer. Override it using ``sqlalchemy_scheme`` in the connection extra or hook argument.
  The Airflow constraint files pin SQLAlchemy 2.0 and Airflow 2.11 requires it below 2.0, so on a
  constrained install ``get_conn()`` works, but methods that create a SQLAlchemy engine, such as
  ``get_sqlalchemy_engine()``, fail with ``NoSuchModuleError`` until SQLAlchemy is upgraded past the
  constraints.
* Use ``?`` placeholders with positional parameters or ``%(name)s`` with named parameters.
  Plain ``%s`` placeholders are not supported. The hook defaults to ``?`` for generated statements.
* Driver-level connection pooling is enabled by default.
* Connection extras are passed to the driver as connection string keywords. ``get_conn()`` logs a warning
  and ignores keywords that mssql-python does not know, but the SQLAlchemy URL is rejected by the driver.
  Remove options that only pymssql understands, such as ``charset`` or ``tds_version``, when you switch.
* ``MsSqlToHiveOperator`` and ``MSSQLToGCSOperator`` map column types with pymssql type codes, so they
  need the default ``pymssql`` driver for now.
