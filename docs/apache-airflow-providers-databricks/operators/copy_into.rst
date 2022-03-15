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

.. _howto/operator:DatabricksSqlCopyIntoOperator:


DatabricksCopyIntoOperator
==========================

Use the :class:`~airflow.providers.databricks.operators.databricks_sql.DatabricksCopyIntoOperator` to import
data into Databricks table using `COPY INTO <https://docs.databricks.com/sql/language-manual/delta-copy-into.html>`_
command.


Using the Operator
------------------

Operator loads data from a specified location into a table using a configured endpoint.

.. list-table::
   :widths: 15 25
   :header-rows: 1

   * - Parameter
     - Input
   * - table_name: str
     - Required name of the table.
   * - file_location: str
     - Required location of files to import.
   * - file_format: str
     - Required file format. Supported formats are ``CSV``, ``JSON``, ``AVRO``, ``ORC``, ``PARQUET``, ``TEXT``, ``BINARYFILE``.
   * - sql_endpoint_name: str
     - Optional name of Databricks SQL endpoint to use. If not specified, ``http_path`` should be provided.
   * - http_path: str
     - Optional HTTP path for Databricks SQL endpoint or Databricks cluster. If not specified, it should be provided in Databricks connection, or the ``sql_endpoint_name`` parameter must be set.
   * - session_configuration: dict[str,str]
     - optional dict specifying Spark configuration parameters that will be set for the session.
   * - files: Optional[List[str]]
     - optional list of files to import. Can't be specified together with ``pattern``.
   * - pattern: Optional[str]
     - optional regex string to match file names to import. Can't be specified together with ``files``.
   * - expression_list: Optional[str]
     - optional string that will be used in the ``SELECT`` expression.
   * - credential: Optional[Dict[str, str]]
     - optional credential configuration for authentication against a specified location
   * - encryption: Optional[Dict[str, str]]
     - optional encryption configuration for a specified location
   * - format_options: Optional[Dict[str, str]]
     - optional dictionary with options specific for a given file format.
   * - force_copy: Optional[bool]
     - optional bool to control forcing of data import (could be also specified in ``copy_options``).
   * - copy_options: Optional[Dict[str, str]]
     - optional dictionary of copy options. Right now only ``force`` option is supported.
   * - validate: Optional[Union[bool, int]]
     - optional validation configuration. ``True`` forces validation of all rows, positive number - only N first rows. (requires Preview channel)

Examples
--------

Importing CSV data
^^^^^^^^^^^^^^^^^^

An example usage of the DatabricksCopyIntoOperator to import CSV data into a table is as follows:

.. exampleinclude:: /../../airflow/providers/databricks/example_dags/example_databricks_sql.py
    :language: python
    :start-after: [START howto_operator_databricks_copy_into]
    :end-before: [END howto_operator_databricks_copy_into]
