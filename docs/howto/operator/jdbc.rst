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

Use the :class:`~airflow.providers.jdbc.operators.jdbc` to execute
commands against a database (or data storage) accessible via a JDBC driver.

The :ref:`JDBC Connection <jdbc-connection>` must be passed as
``jdbc_conn_id``.

.. exampleinclude:: /../airflow/example_dags/example_jdbc_operator.py
    :language: python
    :start-after: [START howto_operator_jdbc]
    :end-before: [END howto_operator_jdbc]

The parameter ``sql`` can receive a string or a list of strings.
Each string can be an SQL statement or a reference to a template file.
Template reference are recognized by ending in '.sql'.

The parameter ``autocommit`` if set to ``True`` will execute a commit after
each command (default is ``False``)

Prerequisite Tasks
------------------
To use this operator you need:

  * Install the python module jaydebeapi:
    .. code-block:: bash

      pip install jaydebeapy

  * Install a JVM.
  * Have the JDBC driver for your database installed.

Templating
----------

You can use :ref:`Jinja templates <jinja-templating>` to parameterize
``sql``.

.. exampleinclude:: /../airflow/example_dags/example_jdbc_operator.py
    :language: python
    :start-after: [START howto_operator_jdbc_template]
    :end-before: [END howto_operator_jdbc_template]
