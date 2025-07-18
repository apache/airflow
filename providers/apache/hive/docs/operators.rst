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



.. _howto/operator:HiveOperator:

SQLExecuteQueryOperator to connect to Apache Hive
====================================================

Use the :class:`SQLExecuteQueryOperator<airflow.providers.common.sql.operators.sql>` to execute
Hive commands in an `Apache Hive <https://cwiki.apache.org/confluence/display/Hive/Home>`__ database.

.. note::
    Previously, ``HiveOperator`` was used to perform this kind of operation.
    After deprecation this has been removed. Please use ``SQLExecuteQueryOperator`` instead.

.. note::
    Make sure you have installed the ``apache-airflow-providers-apache-hive`` package
    to enable Hive support.

Using the Operator
^^^^^^^^^^^^^^^^^^

Use the ``conn_id`` argument to connect to your Apache Hive instance where
the connection metadata is structured as follows:

.. list-table:: Hive Airflow Connection Metadata
   :widths: 25 25
   :header-rows: 1

   * - Parameter
     - Input
   * - Host: string
     - HiveServer2 hostname or IP address
   * - Schema: string
     - Default database name (optional)
   * - Login: string
     - Hive username (if applicable)
   * - Password: string
     - Hive password (if applicable)
   * - Port: int
     - HiveServer2 port (default: 10000)
   * - Extra: JSON
     - Additional connection configuration, such as the authentication method:
       ``{"auth": "NOSASL"}``

An example usage of the SQLExecuteQueryOperator to connect to Apache Hive is as follows:

.. exampleinclude:: /../tests/system/apache/hive/example_hive.py
    :language: python
    :start-after: [START howto_operator_hive]
    :end-before: [END howto_operator_hive]

Reference
^^^^^^^^^
For further information, look at:

* `Apache Hive Documentation <https://cwiki.apache.org/confluence/display/Hive/Home>`__

.. note::

  Parameters provided directly via SQLExecuteQueryOperator() take precedence
  over those specified in the Airflow connection metadata (such as ``schema``, ``login``, ``password``, etc).
