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

.. _howto/operator:PineconeIngestOperator:

PineconeIngestOperator
======================

Use the :class:`~airflow.providers.pinecone.operators.pinecone.PineconeIngestOperator` to
interact with Pinecone APIs to ingest vectors.


Using the Operator
^^^^^^^^^^^^^^^^^^

The PineconeIngestOperator requires the ``vectors`` as an input ingest into Pinecone. Use the ``conn_id`` parameter to
specify the Pinecone connection to use to connect to your account. The vectors could also contain metadata referencing
the original text corresponding to the vectors that could be ingested into the database.

An example using the operator in this way:

.. exampleinclude:: /../../tests/system/providers/pinecone/example_dag_pinecone.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_pinecone_ingest]
    :end-before: [END howto_operator_pinecone_ingest]
