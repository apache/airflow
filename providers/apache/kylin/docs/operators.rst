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

.. _howto/operator:KylinOperator:

SQLExecuteQueryOperator to connect to Apache Kylin
===================================================

Use the :class:`SQLExecuteQueryOperator<airflow.providers.common.sql.operators.sql.SQLExecuteQueryOperator>` to execute SQL queries against an
`Apache Kylin <https://kylin.apache.org/docs/overview>`__ cluster.

.. note::
    There is no dedicated operator for Apache Kylin.
    Please use the ``SQLExecuteQueryOperator`` instead.

.. note::
    Make sure you have installed the necessary provider package (e.g. ``apache-airflow-providers-apache-kylin``)
    to enable Apache Kylin support.

Using the Operator
^^^^^^^^^^^^^^^^^^

Use the ``conn_id`` argument to connect to your Apache Kylin instance where
the connection metadata is structured as follows:

.. list-table:: Kylin Airflow Connection Metadata
   :widths: 25 25
   :header-rows: 1

   * - Parameter
     - Input
   * - Host: string
     - Kylin server hostname or IP address
   * - Schema: string
     - The default project name (optional)
   * - Login: string
     - Username for authentication (default: ADMIN)
   * - Password: string
     - Password for authentication (default: KYLIN)
   * - Port: int
     - Kylin service port (default: 7070)
   * - Extra: JSON
     - Additional connection configuration, such as:
       ``{"use_ssl": false}``

An example usage of the SQLExecuteQueryOperator to connect to Apache Kylin is as follows:

.. exampleinclude:: /../tests/system/apache/kylin/example_kylin.py
   :language: python
   :start-after: [START howto_operator_kylin]
   :end-before: [END howto_operator_kylin]

Reference
^^^^^^^^^
For further information, see:

* `Apache Kylin Documentation <https://kylin.apache.org/docs/>`__

.. note::
  Parameters provided directly via SQLExecuteQueryOperator() take precedence over those specified
  in the Airflow connection metadata (such as ``schema``, ``login``, ``password``, etc).
