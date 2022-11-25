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

.. _howto/operator:JdbcOperator:

JdbcOperator
============

Java Database Connectivity (JDBC) is an application programming interface
(API) for the programming language Java, which defines how a client may
access a database.

Prerequisite Tasks
^^^^^^^^^^^^^^^^^^

To use this operator you need:

  * Install the python module jaydebeapi:
    .. code-block:: bash

        pip install apache-airflow[jdbc]

  * Install a `JVM <https://adoptium.net/installation/>`_ and
    add a ``JAVA_HOME`` env variable.
  * Have the JDBC driver for your database installed.

Once these prerequisites are satisfied you should be able to run
this Python snippet (replacing the variables values with the ones
related to your driver).

Other error messages will inform you in case the ``jaydebeapi`` module
is missing or the driver is not available. A ``Connection Refused``
error means that the connection string is pointing to host where no
database is listening for new connections.

  .. code-block:: python

    driver_class = "com.exasol.jdbc.EXADriver"
    driver_path = "/opt/airflow/drivers/exasol/EXASolution_JDBC-7.0.2/exajdbc.jar"
    connection_url = "jdbc:exa:localhost"
    credentials = ["", ""]

    conn = jaydebeapi.connect(
        driver_class,
        connection_url,
        credentials,
        driver_path,
    )

Usage
^^^^^
Use the :class:`~airflow.providers.jdbc.operators.jdbc` to execute
commands against a database (or data storage) accessible via a JDBC driver.

The :doc:`JDBC Connection <connections/jdbc>` must be passed as
``jdbc_conn_id``.

.. exampleinclude:: /../../tests/system/providers/jdbc/example_jdbc_queries.py
    :language: python
    :start-after: [START howto_operator_jdbc]
    :end-before: [END howto_operator_jdbc]

The parameter ``sql`` can receive a string or a list of strings.
Each string can be an SQL statement or a reference to a template file.
Template reference are recognized by ending in '.sql'.

The parameter ``autocommit`` if set to ``True`` will execute a commit after
each command (default is ``False``)

Templating
----------

You can use :ref:`Jinja templates <concepts:jinja-templating>` to parameterize
``sql``.

.. exampleinclude:: /../../tests/system/providers/jdbc/example_jdbc_queries.py
    :language: python
    :start-after: [START howto_operator_jdbc_template]
    :end-before: [END howto_operator_jdbc_template]
