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

Operator executes given SQL queries against configured endpoint.  There are 3 ways of specifying SQL queries:

1. Simple string with SQL statement.
2. List of strings representing SQL statements.
3. Name of the file with SQL queries. File must have ``.sql`` extension. Each query should finish with ``;<new_line>``

.. list-table::
   :widths: 15 25
   :header-rows: 1

   * - Parameter
     - Input
   * - sql: str or list[str]
     - Required parameter specifying a queries to execute.
   * - sql_endpoint_name: str
     - Optional name of Databricks SQL endpoint to use. If not specified, ``http_path`` should be provided.
   * - http_path: str
     - Optional HTTP path for Databricks SQL endpoint or Databricks cluster. If not specified, it should be provided in Databricks connection, or the ``sql_endpoint_name`` parameter must be set.
   * - parameters: dict[str, any]
     - Optional parameters that will be used to substitute variable(s) in SQL query.
   * - session_configuration: dict[str,str]
     - optional dict specifying Spark configuration parameters that will be set for the session.
   * - output_path: str
     - Optional path to the file to which results will be written.
   * - output_format: str
     - Name of the format which will be used to write results.  Supported values are (case-insensitive): ``JSON`` (array of JSON objects), ``JSONL`` (each row as JSON object on a separate line), ``CSV`` (default).
   * - csv_params: dict[str, any]
     - Optional dictionary with parameters to customize Python CSV writer.
   * - do_xcom_push: boolean
     - whether we should push query results (last query if multiple queries are provided) to xcom. Default: false

Examples
--------

Selecting data
^^^^^^^^^^^^^^

An example usage of the DatabricksSqlOperator to select data from a table is as follows:

.. exampleinclude:: /../../airflow/providers/databricks/example_dags/example_databricks_sql.py
    :language: python
    :start-after: [START howto_operator_databricks_sql_select]
    :end-before: [END howto_operator_databricks_sql_select]

Selecting data into a file
^^^^^^^^^^^^^^^^^^^^^^^^^^

An example usage of the DatabricksSqlOperator to select data from a table and store in a file is as follows:

.. exampleinclude:: /../../airflow/providers/databricks/example_dags/example_databricks_sql.py
    :language: python
    :start-after: [START howto_operator_databricks_sql_select_file]
    :end-before: [END howto_operator_databricks_sql_select_file]

Executing multiple statements
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

An example usage of the DatabricksSqlOperator to perform multiple SQL statements is as follows:

.. exampleinclude:: /../../airflow/providers/databricks/example_dags/example_databricks_sql.py
    :language: python
    :start-after: [START howto_operator_databricks_sql_multiple]
    :end-before: [END howto_operator_databricks_sql_multiple]


Executing multiple statements from a file
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

An example usage of the DatabricksSqlOperator to perform statements from a file is as follows:

.. exampleinclude:: /../../airflow/providers/databricks/example_dags/example_databricks_sql.py
    :language: python
    :start-after: [START howto_operator_databricks_sql_multiple_file]
    :end-before: [END howto_operator_databricks_sql_multiple_file]
