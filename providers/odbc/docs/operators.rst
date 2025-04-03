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

.. _howto/operator:OdbcOperator:

OdbcOperator
============

Open Database Connectivity (ODBC) is a standard API for accessing database
management systems (DBMS).


Prerequisite Tasks
^^^^^^^^^^^^^^^^^^

To use this operator you need:

  * Install the python module ``pyodbc``:
    .. code-block:: bash

        pip install apache-airflow[odbc]

  * Have the ODBC driver for your database installed.
  * Configure an ODBC Data Source Name (DSN) if required by your database.

Once these prerequisites are satisfied you should be able to run
this Python snippet (replacing the variables values with the ones
related to your driver).

Other error messages will inform you in case the ``pyodbc`` module
is missing or the driver is not available. A ``Connection Refused``
error means that the connection string is pointing to a host where no
database is listening for new connections.

  .. code-block:: python

    import pyodbc

    driver = "{ODBC Driver 17 for SQL Server}"
    server = "localhost"
    database = "testdb"
    username = "user"
    password = "password"

    conn_str = (
        f"DRIVER={driver};" f"SERVER={server};" f"DATABASE={database};" f"UID={username};" f"PWD={password};"
    )

    conn = pyodbc.connect(conn_str)

Usage
^^^^^
Use the :class:`~airflow.providers.common.sql.operators.SQLExecuteQueryOperator` to execute
commands against a database (or data storage) accessible via an ODBC driver.

The :doc:`ODBC Connection <connections/odbc>` must be passed as
``conn_id``.

.. exampleinclude:: /../../odbc/tests/system/odbc/example_odbc.py
    :language: python
    :start-after: [START howto_operator_odbc]
    :end-before: [END howto_operator_odbc]


The parameter ``sql`` can receive a string or a list of strings.
Each string can be an SQL statement or a reference to a template file.
Template references are recognized by ending in '.sql'.

The parameter ``autocommit`` if set to ``True`` will execute a commit after
each command (default is ``False``).

Templating
----------

You can use :ref:`Jinja templates <concepts:jinja-templating>` to parameterize
``sql``.

.. exampleinclude:: /../../odbc/tests/system/odbc/example_odbc.py
    :language: python
    :start-after: [START howto_operator_odbc_template]
    :end-before: [END howto_operator_odbc_template]
