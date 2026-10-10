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

By default the hook connects with `pymssql <https://pypi.org/project/pymssql/>`__. To use Microsoft's
`mssql-python <https://pypi.org/project/mssql-python/>`__ driver instead, install the extra:

.. code-block:: bash

    pip install 'apache-airflow-providers-microsoft-mssql[mssql-python]'

and set ``dbapi_driver`` to ``mssql_python`` in the connection extra:

.. code-block:: bash

    export AIRFLOW_CONN_MSSQL_DEFAULT='{
        "conn_type": "mssql",
        "login": "username",
        "password": "password",
        "host": "server.com",
        "port": 1433,
        "schema": "database_name",
        "extra": {"dbapi_driver": "mssql_python", "TrustServerCertificate": "yes"}
    }'

Things to know when using ``mssql_python``:

* ``dbapi_driver`` accepts ``pymssql`` (the default) or ``mssql_python``. The name is case-insensitive.
* Every other extra is passed to ``mssql_python.connect`` as a connection string keyword, for example
  ``Encrypt``, ``TrustServerCertificate`` or ``Authentication`` (for Microsoft Entra ID sign-in). The
  host and port are sent as ``Server=host,port``.
* The SQLAlchemy scheme defaults to ``mssql+mssqlpython``, which needs SQLAlchemy 2.1 or newer. It can still
  be changed with the ``sqlalchemy_scheme`` extra or the hook argument.
* The driver does not understand the plain ``%s`` parameter marker. Write SQL parameters as ``?`` with a
  list or tuple, or as ``%(name)s`` with a dict. The hook's default placeholder for generated statements
  is ``?`` and can be overridden with the ``placeholder`` extra.
* ``mssql-python`` pools connections at the driver level by default.
