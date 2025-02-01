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



.. _howto/operator:PrestoOperator:

SQLExecuteQueryOperator to connect to Presto
============================================

Use the :class:`SQLExecuteQueryOperator<airflow.providers.common.sql.operators.sql>` to execute
Presto commands in a `Presto <https://prestodb.io/docs/current/>`__ database.


Using the Operator
^^^^^^^^^^^^^^^^^^

Use the ``conn_id`` argument to connect to your Presto instance where
the connection metadata is structured as follows:

.. list-table:: Presto Airflow Connection Metadata
   :widths: 25 25
   :header-rows: 1

   * - Parameter
     - Input
   * - Host: string
     - Presto server hostname or container name
   * - Schema: string
     - Schema to execute SQL operations on by default
   * - Login: string
     - Presto user (required)
   * - Password: string
     - Presto user password (if authentication is enabled)
   * - Port: int
     - Presto server port (default: 8080)
   * - Extra: JSON
     - Additional parameters such as `{"user": "airflow_user"}`


An example usage of the SQLExecuteQueryOperator to connect to Presto is as follows:

.. exampleinclude:: /../../providers/presto/tests/system/presto/example_presto.py
    :language: python
    :start-after: [START howto_operator_presto]
    :end-before: [END howto_operator_presto]


Reference
^^^^^^^^^
For further information, look at:

* `Presto Documentation <https://prestodb.io/docs/current/>`__

.. note::

  Parameters given via SQLExecuteQueryOperator() are given first-place priority
  relative to parameters set via Airflow connection metadata (such as ``schema``, ``login``, ``password`` etc).