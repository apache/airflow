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

======================================================
Amazon Neptune Documentation
======================================================

`Amazon Neptune is a fast, reliable, fully managed graph database service that makes it easy to build and run
applications that work with highly connected datasets. The core of Neptune is a purpose-built,
high-performance graph database engine that is optimized for storing billions of relationships and
querying the graph with milliseconds latency. Neptune supports the popular graph query languages
Apache TinkerPop Gremlin and W3C's SPARQL, allowing you to build queries that efficiently navigate highly connected
datasets. Neptune powers graph use cases such as recommendation engines, fraud detection, knowledge graphs,
drug discovery, and network security.`

Prerequisite Tasks
------------------

.. include:: _partials/prerequisite_tasks.rst

Operators
---------

.. _howto/operator:NeptuneStartDbOperator:

Start a database cluster
====================================

To start an Amazon Neptune DB cluster you can use
:class:`~airflow.providers.amazon.aws.operators.neptune.NeptuneStartDbOperator`.

.. exampleinclude:: /../../tests/system/providers/amazon/aws/example_neptune_cluster.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_neptune_start_db]
    :end-before: [END howto_operator_neptune_start_db]


.. _howto/operator:NeptuneStopDbOperator:

Stop a database cluster
===================================

To stop an Amazon Neptune DB cluster you can use
:class:`~airflow.providers.amazon.aws.operators.neptune.NeptuneStopDbOperator`.

.. exampleinclude:: /../../tests/system/providers/amazon/aws/example_neptune_cluster.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_neptune_stop_db]
    :end-before: [END howto_operator_neptune_stop_db]

Reference
---------

* `AWS boto3 library documentation for Neptune <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/neptune.html>`__
