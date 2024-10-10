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



.. _howto/operator:SqliteOperator:

SQLExecuteQueryOperator to connect to Sqlite
============================================

Use the :class:`SQLExecuteQueryOperator<airflow.providers.common.sql.operators.sql>` to execute
Sqlite commands in a `Sqlite <https://sqlite.org/lang.html>`__ database.

.. warning::
    Previously, SqliteOperator was used to perform this kind of operation. But at the moment SqliteOperator is deprecated and will be removed in future versions of the provider. Please consider to switch to SQLExecuteQueryOperator as soon as possible.

Using the Operator
^^^^^^^^^^^^^^^^^^

Use the ``conn_id`` argument to connect to your Sqlite instance where
the connection metadata is structured as follows:

.. list-table:: Sqlite Airflow Connection Metadata
   :widths: 25 25
   :header-rows: 1

   * - Parameter
     - Input
   * - Host: string
     - Sqlite database file

An example usage of the SQLExecuteQueryOperator to connect to Sqlite is as follows:

.. exampleinclude:: /../../providers/tests/system/sqlite/example_sqlite.py
    :language: python
    :start-after: [START howto_operator_sqlite]
    :end-before: [END howto_operator_sqlite]

Furthermore, you can use an external file to execute the SQL commands. Script folder must be at the same level as DAG.py file.

.. exampleinclude:: /../../providers/tests/system/sqlite/example_sqlite.py
    :language: python
    :start-after: [START howto_operator_sqlite_external_file]
    :end-before: [END howto_operator_sqlite_external_file]

Reference
^^^^^^^^^
For further information, look at:

* `Sqlite Documentation <https://www.sqlite.org/index.html>`__

.. note::

  Parameters given via SQLExecuteQueryOperator() are given first-place priority
  relative to parameters set via Airflow connection metadata (such as ``schema``, ``login``, ``password`` etc).
