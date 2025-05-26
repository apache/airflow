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

.. _howto/operator:DruidOperator:

SQLExecuteQueryOperator to connect to Apache Druid
====================================================

Use the :class:`SQLExecuteQueryOperator<airflow.providers.common.sql.operators.sql.SQLExecuteQueryOperator>` to execute SQL queries against an
`Apache Druid <https://druid.apache.org/>`__ cluster.

.. note::
    Previously, a dedicated operator for Druid might have been used.
    After deprecation, please use the ``SQLExecuteQueryOperator`` instead.

.. note::
    Make sure you have installed the ``apache-airflow-providers-apache-druid`` package to enable Druid support.

Using the Operator
^^^^^^^^^^^^^^^^^^

Use the ``conn_id`` argument to connect to your Apache Druid instance where
the connection metadata is structured as follows:

.. list-table:: Druid Airflow Connection Metadata
   :widths: 25 25
   :header-rows: 1

   * - Parameter
     - Input
   * - Host: string
     - Druid broker hostname or IP address
   * - Schema: string
     - Not applicable (leave blank)
   * - Login: string
     - Not applicable (leave blank)
   * - Password: string
     - Not applicable (leave blank)
   * - Port: int
     - Druid broker port (default: 8082)
   * - Extra: JSON
     - Additional connection configuration, such as:
       ``{"endpoint": "/druid/v2/sql/", "method": "POST"}``

An example usage of the SQLExecuteQueryOperator to connect to Apache Druid is as follows:

.. exampleinclude:: /../tests/system/apache/druid/example_druid.py
   :language: python
   :start-after: [START howto_operator_druid]
   :end-before: [END howto_operator_druid]

Reference
^^^^^^^^^
For further information, see:

* `Apache Druid Documentation <https://druid.apache.org/docs/latest/>`__

.. note::
  Parameters provided directly via SQLExecuteQueryOperator() take precedence over those specified
  in the Airflow connection metadata (such as ``schema``, ``login``, ``password``, etc).
