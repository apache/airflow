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

.. _howto/operator:PinotOperator:

SQLExecuteQueryOperator to connect to Apache Pinot
==================================================

Use the :class:`SQLExecuteQueryOperator<airflow.providers.common.sql.operators.sql.SQLExecuteQueryOperator>` to execute SQL queries against an
`Apache Pinot <https://pinot.apache.org/>`__ cluster.

.. note::
    There is no dedicated operator for Apache Pinot.
    Please use the ``SQLExecuteQueryOperator`` instead.

.. note::
    Make sure you have installed the necessary provider package (e.g. ``apache-airflow-providers-apache-pinot``)
    to enable Apache Pinot support.

Using the Operator
------------------

Use the ``conn_id`` argument to connect to your Apache Pinot instance where
the connection metadata is structured as follows:

.. list-table:: Pinot Airflow Connection Metadata
   :widths: 25 25
   :header-rows: 1

   * - Parameter
     - Input
   * - Host: string
     - Pinot broker hostname or IP address
   * - Port: int
     - Pinot broker port (default: 8000)
   * - Schema: string
     - (Not used)
   * - Extra: JSON
     - Optional fields such as: ``{"endpoint": "query/sql"}``

An example usage of the SQLExecuteQueryOperator to connect to Apache Pinot is as follows:

.. exampleinclude:: /../tests/system/apache/pinot/example_pinot.py
   :language: python
   :start-after: [START howto_operator_pinot]
   :end-before: [END howto_operator_pinot]

Reference
---------

For further information, see:

* `Apache Pinot Documentation <https://docs.pinot.apache.org/>`__

.. note::
  Parameters provided directly via SQLExecuteQueryOperator() take precedence over those specified
  in the Airflow connection metadata.
