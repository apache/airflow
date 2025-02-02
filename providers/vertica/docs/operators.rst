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


.. _howto/operator:VerticaOperator:

SQLExecuteQueryOperator to connect to Vertica
=============================================

Use the :class:`SQLExecuteQueryOperator <airflow.providers.common.sql.operators.sql>` to execute
Vertica commands in a `Vertica <https://www.vertica.com/documentation/>`__ database.

.. note::
    If you previously used other legacy operators to handle Vertica interactions, you can now use
    ``SQLExecuteQueryOperator`` for both stored procedures and raw SQL execution.

Using the Operator
^^^^^^^^^^^^^^^^^^

Use the ``conn_id`` argument to connect to your Vertica instance where
the connection metadata is structured as follows:

.. list-table:: Vertica Airflow Connection Metadata
   :widths: 25 25
   :header-rows: 1

   * - Parameter
     - Input
   * - Host: string
     - Vertica database hostname or container name (if running in Docker network)
   * - Schema: string
     - Schema to execute SQL operations on by default
   * - Login: string
     - Vertica database user (often ``dbadmin`` if using community Docker image)
   * - Password: string
     - Vertica database user password
   * - Port: int
     - Vertica database port (default: 5433)
   * - Extra: JSON
     - Additional connection configuration (e.g. TLS settings):  
       ``{"tlsmode": "disable"}``

An example usage of the ``SQLExecuteQueryOperator`` to connect to Vertica is as follows:

.. exampleinclude:: /../../providers/vertica/tests/system/vertica/example_vertica.py
    :language: python
    :start-after: [START howto_operator_vertica]
    :end-before: [END howto_operator_vertica]

Furthermore, you can use an external file to execute the SQL commands (e.g. DROP TABLE). The script
folder must be at the same level as the DAG.py file.

.. exampleinclude:: /../../providers/vertica/tests/system/vertica/example_vertica.py
    :language: python
    :start-after: [START howto_operator_vertica_external_file]
    :end-before: [END howto_operator_vertica_external_file]

Reference
^^^^^^^^^

For further information, look at:

* `Vertica Documentation <https://www.vertica.com/documentation/>`__

.. note::

  Parameters given via ``SQLExecuteQueryOperator()`` take first-place priority
  relative to parameters set via the Airflow connection metadata (such as
  ``schema``, ``login``, ``password``, etc).
