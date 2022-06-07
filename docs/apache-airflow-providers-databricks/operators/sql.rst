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

.. _howto/operator:DatabricksSqlOperator:


DatabricksSqlOperator
=====================

Use the :class:`~airflow.providers.databricks.operators.databricks_sql.DatabricksSqlOperator` to execute SQL
on a `Databricks SQL endpoint  <https://docs.databricks.com/sql/admin/sql-endpoints.html>`_ or a
`Databricks cluster <https://docs.databricks.com/clusters/index.html>`_.


Using the Operator
------------------

Operator executes given SQL queries against configured endpoint. The only required parameters are:

* ``sql`` - SQL queries to execute. There are 3 ways of specifying SQL queries:

  1. Simple string with SQL statement.
  2. List of strings representing SQL statements.
  3. Name of the file with SQL queries. File must have ``.sql`` extension. Each query should finish with ``;<new_line>``

* One of ``sql_endpoint_name`` (name of Databricks SQL endpoint to use) or ``http_path`` (HTTP path for Databricks SQL endpoint or Databricks cluster).

Other parameters are optional and could be found in the class documentation.

Examples
--------

Selecting data
^^^^^^^^^^^^^^

An example usage of the DatabricksSqlOperator to select data from a table is as follows:

.. exampleinclude:: /../../tests/system/providers/databricks/example_databricks_sql.py
    :language: python
    :start-after: [START howto_operator_databricks_sql_select]
    :end-before: [END howto_operator_databricks_sql_select]

Selecting data into a file
^^^^^^^^^^^^^^^^^^^^^^^^^^

An example usage of the DatabricksSqlOperator to select data from a table and store in a file is as follows:

.. exampleinclude:: /../../tests/system/providers/databricks/example_databricks_sql.py
    :language: python
    :start-after: [START howto_operator_databricks_sql_select_file]
    :end-before: [END howto_operator_databricks_sql_select_file]

Executing multiple statements
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

An example usage of the DatabricksSqlOperator to perform multiple SQL statements is as follows:

.. exampleinclude:: /../../tests/system/providers/databricks/example_databricks_sql.py
    :language: python
    :start-after: [START howto_operator_databricks_sql_multiple]
    :end-before: [END howto_operator_databricks_sql_multiple]


Executing multiple statements from a file
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

An example usage of the DatabricksSqlOperator to perform statements from a file is as follows:

.. exampleinclude:: /../../tests/system/providers/databricks/example_databricks_sql.py
    :language: python
    :start-after: [START howto_operator_databricks_sql_multiple_file]
    :end-before: [END howto_operator_databricks_sql_multiple_file]
