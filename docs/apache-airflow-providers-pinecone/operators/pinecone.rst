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

Operators
---------

.. _howto/operator:PineconeIngestOperator:

Ingest data into a pinecone index
=================================

Use the :class:`~airflow.providers.pinecone.operators.pinecone.PineconeIngestOperator` to
interact with Pinecone APIs to ingest vectors.


Using the Operator
^^^^^^^^^^^^^^^^^^

The PineconeIngestOperator requires the ``vectors`` as an input ingest into Pinecone. Use the ``conn_id`` parameter to
specify the Pinecone connection to use to connect to your account. The vectors could also contain metadata referencing
the original text corresponding to the vectors that could be ingested into the database.

An example using the operator in this way:

.. exampleinclude:: /../../providers/tests/system/pinecone/example_dag_pinecone.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_pinecone_ingest]
    :end-before: [END howto_operator_pinecone_ingest]

.. _howto/operator:CreatePodIndexOperator:

Create a Pod based Index
========================

Use the :class:`~airflow.providers.pinecone.operators.pinecone.CreatePodIndexOperator` to
interact with Pinecone APIs to create a Pod based Index.

Using the Operator
^^^^^^^^^^^^^^^^^^

The ``CreatePodIndexOperator`` requires the index details as well as the pod configuration details. ``api_key``, ``environment`` can be
passed via arguments to the operator or via the connection.

An example using the operator in this way:

.. exampleinclude:: /../../providers/tests/system/pinecone/example_create_pod_index.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_create_pod_index]
    :end-before: [END howto_operator_create_pod_index]


.. _howto/operator:CreateServerlessIndexOperator:

Create a Serverless Index
=========================

Use the :class:`~airflow.providers.pinecone.operators.pinecone.CreateServerlessIndexOperator` to
interact with Pinecone APIs to create a Pod based Index.

Using the Operator
^^^^^^^^^^^^^^^^^^

The ``CreateServerlessIndexOperator``  requires the index details as well as the Serverless configuration details. ``api_key``, ``environment`` can be
passed via arguments to the operator or via the connection.

An example using the operator in this way:

.. exampleinclude:: /../../providers/tests/system/pinecone/example_create_serverless_index.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_create_serverless_index]
    :end-before: [END howto_operator_create_serverless_index]
