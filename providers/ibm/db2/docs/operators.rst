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

IBM Db2 Operators
=================

.. _howto/operator:Db2Operator:

Db2Operator
-----------

Use the :class:`~airflow.providers.ibm.db2.operators.db2.Db2Operator` to execute
SQL commands in an IBM Db2 database.

Using the Operator
^^^^^^^^^^^^^^^^^^

Use the ``db2_conn_id`` parameter to specify the connection to use.

An example usage of the Db2Operator is as follows:

.. exampleinclude:: /../../providers/ibm/db2/example_dags/db2_example_dag.py
    :language: python
    :start-after: [START howto_operator_db2]
    :end-before: [END howto_operator_db2]

Reference
^^^^^^^^^

For further information, see the IBM Db2 documentation:

* `IBM Db2 Documentation <https://www.ibm.com/docs/en/db2>`__
