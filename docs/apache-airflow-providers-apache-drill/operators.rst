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


Connect to Apache Drill via SQLExecuteQueryOperator
===================================================

Prerequisite
------------

Use :class:`~airflow.providers.common.sql.operators.SQLExecuteQueryOperator`,
to execute SQL commands in  `Drill <https://drill.apache.org/>`__ query engine.

To connect to Drill, you must configure a :doc:`Drill Connection <connections/drill>` and can pass that as ``conn_id`` to SQLExecuteQueryOperator.

.. warning::
    Previously, DrillOperator was used to perform this kind of operation. But at the moment DrillOperator is deprecated and will be removed in future versions of the provider. Please consider to switch to SQLExecuteQueryOperator as soon as possible.

SQLExecuteQueryOperator
-----------------------

Executes one or more SQL queries on an Apache Drill server.
The ``sql`` parameter can be templated and be an external ``.sql`` file.

Using the operator
""""""""""""""""""

.. exampleinclude:: /../../providers/tests/system/apache/drill/example_drill_dag.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_drill]
    :end-before: [END howto_operator_drill]

Reference
"""""""""

For further information, see `the Drill documentation on querying data <http://apache.github.io/drill/docs/query-data/>`_.
