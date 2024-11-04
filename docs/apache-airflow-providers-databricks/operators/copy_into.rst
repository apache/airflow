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

Operator loads data from a specified location into a table using a configured endpoint.  The only required parameters are:

* ``table_name`` - string with the table name
* ``file_location`` - string with the URI of data to load
* ``file_format`` - string specifying the file format of data to load. Supported formats are ``CSV``, ``JSON``, ``AVRO``, ``ORC``, ``PARQUET``, ``TEXT``, ``BINARYFILE``.
* One of ``sql_endpoint_name`` (name of Databricks SQL endpoint to use) or ``http_path`` (HTTP path for Databricks SQL endpoint or Databricks cluster).

Other parameters are optional and could be found in the class documentation.

Examples
--------

Importing CSV data
^^^^^^^^^^^^^^^^^^

An example usage of the DatabricksCopyIntoOperator to import CSV data into a table is as follows:

.. exampleinclude:: /../../providers/tests/system/databricks/example_databricks_sql.py
    :language: python
    :start-after: [START howto_operator_databricks_copy_into]
    :end-before: [END howto_operator_databricks_copy_into]
