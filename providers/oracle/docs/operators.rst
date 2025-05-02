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



.. _howto/operator:OracleOperator:

SQLExecuteQueryOperator to connect to Oracle
============================================

Use the :class:`SQLExecuteQueryOperator<airflow.providers.common.sql.operators.sql>` to execute
Oracle commands in a `Oracle <https://docs.oracle.com/en/>`__ database.

.. note::
    Previously, ``OracleStoredProcedureOperator`` was used to perform this kind of operation. After deprecation this has been removed. Please use ``SQLExecuteQueryOperator`` instead.

Using the Operator
^^^^^^^^^^^^^^^^^^

Use the ``conn_id`` argument to connect to your Oracle instance where
the connection metadata is structured as follows:

.. list-table:: Oracle Airflow Connection Metadata
   :widths: 25 25
   :header-rows: 1

   * - Parameter
     - Input
   * - Host: string
     - Oracle database hostname
   * - Schema: string
     - Schema to execute SQL operations on by default
   * - Login: string
     - Oracle database user
   * - Password: string
     - Oracle database user password
   * - Port: int
     - Oracle database port (default: 1521)
   * - Extra: JSON
     - Additional connection configuration, such as DSN string:
       ``{"dsn": "(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=<hostname>)(PORT=1521))(CONNECT_DATA=(SERVICE_NAME=<service_name>)))"}``

An example usage of the SQLExecuteQueryOperator to connect to Oracle is as follows:

.. exampleinclude:: /../../oracle/tests/system/oracle/example_oracle.py
    :language: python
    :start-after: [START howto_operator_oracle]
    :end-before: [END howto_operator_oracle]


Reference
^^^^^^^^^
For further information, look at:

* `Oracle Documentation <https://docs.oracle.com/en/>`__

.. note::

  Parameters given via SQLExecuteQueryOperator() are given first-place priority
  relative to parameters set via Airflow connection metadata (such as ``schema``, ``login``, ``password`` etc).
