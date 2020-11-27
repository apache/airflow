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



.. _howto/operator:MySqlOperator:

MySqlOperator
=============

Use the :class:`~airflow.providers.mysql.operators.MySqlOperator` to execute
SQL commands in a `MySql <https://dev.mysql.com/doc/>`__ database.


Using the Operator
^^^^^^^^^^^^^^^^^^

Use the ``mysql_conn_id`` argument to connect to your MySql instance where
the connection metadata is structured as follows:

.. list-table:: MySql Airflow Connection Metadata
   :widths: 25 25
   :header-rows: 1

   * - Parameter
     - Input
   * - Host: string
     - MySql hostname
   * - Schema: string
     - Set schema to execute Sql operations on by default
   * - Login: string
     - MySql user
   * - Password: string
     - MySql user password
   * - Port: int
     - MySql port

An example usage of the MySqlOperator is as follows:

.. exampleinclude:: /../../airflow/providers/mysql/example_dags/example_mysql.py
    :language: python
    :start-after: [START howto_operator_mysql]
    :end-before: [END howto_operator_mysql]

You can also use an external file to execute the SQL commands. Script folder must be at the same level as DAG.py file.

.. exampleinclude:: /../../airflow/providers/mysql/example_dags/example_mysql.py
    :language: python
    :start-after: [START howto_operator_mysql_external_file]
    :end-before: [END howto_operator_mysql_external_file]

.. note::

  Parameters that can be passed onto the operator will be given priority over the parameters already given
  in the Airflow connection metadata (such as ``schema``, ``login``, ``password`` and so forth).
