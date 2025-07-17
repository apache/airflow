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

.. _howto/operator:ImpalaOperator:

SQLExecuteQueryOperator to connect to Apache Impala
=====================================================

Use the :class:`SQLExecuteQueryOperator<airflow.providers.common.sql.operators.sql.SQLExecuteQueryOperator>` to execute SQL queries against an
`Apache Impala <https://impala.apache.org/>`__ cluster.

.. note::
    Previously, a dedicated operator for Impala might have been used.
    After deprecation, please use the ``SQLExecuteQueryOperator`` instead.

.. note::
    Make sure you have installed the ``apache-airflow-providers-apache-impala`` package to enable Impala support.

Using the Operator
^^^^^^^^^^^^^^^^^^

Use the ``conn_id`` argument to connect to your Apache Impala instance where
the connection metadata is structured as follows:

.. list-table:: Impala Airflow Connection Metadata
   :widths: 25 25
   :header-rows: 1

   * - Parameter
     - Input
   * - Host: string
     - Impala daemon hostname or IP address
   * - Schema: string
     - The default database name (optional)
   * - Login: string
     - Username for authentication (if applicable)
   * - Password: string
     - Password for authentication (if applicable)
   * - Port: int
     - Impala service port (default: 21050)
   * - Extra: JSON
     - Additional connection configuration, such as:
       ``{"use_ssl": false, "auth": "NOSASL"}``

An example usage of the SQLExecuteQueryOperator to connect to Apache Impala is as follows:

.. exampleinclude:: /../tests/system/apache/impala/example_impala.py
   :language: python
   :start-after: [START howto_operator_impala]
   :end-before: [END howto_operator_impala]

Reference
^^^^^^^^^
For further information, see:

* `Apache Impala Documentation <https://impala.apache.org/docs/>`__

.. note::
  Parameters provided directly via SQLExecuteQueryOperator() take precedence over those specified
  in the Airflow connection metadata (such as ``schema``, ``login``, ``password``, etc).
