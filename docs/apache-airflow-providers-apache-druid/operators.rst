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


Apache Druid Operators
======================

Prerequisite
------------

To use :class:`~airflow.providers.apache.druid.operators.druid.DruidOperator`,
you must configure a Druid Connection first.

DruidOperator
-------------------

Submit a task directly to Druid, you need to provide the filepath to the Druid index specification ``json_index_file``, and the connection id of the Druid overlord ``druid_ingest_conn_id`` which accepts index jobs in Airflow Connections.
In addition, you can provide the ingestion type ``ingestion_type`` to determine whether the job is Batch Ingestion or SQL-based ingestion.

There is also a example content of the Druid Ingestion specification below.

For parameter definition take a look at :class:`~airflow.providers.apache.druid.operators.druid.DruidOperator`.

Using the operator
""""""""""""""""""

.. exampleinclude:: /../../providers/tests/system/apache/druid/example_druid_dag.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_druid_submit]
    :end-before: [END howto_operator_druid_submit]

Reference
"""""""""

For more information, please refer to `Apache Druid Ingestion spec reference <https://druid.apache.org/docs/latest/ingestion/ingestion-spec.html>`_.
