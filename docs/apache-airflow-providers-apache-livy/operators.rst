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



Apache Livy Operators
=====================

Apache Livy is a service that enables easy interaction with a Spark cluster over a REST interface.
It enables easy submission of Spark jobs or snippets of Spark code, synchronous or asynchronous result retrieval,
as well as Spark Context management, all via a simple REST interface or an RPC client library.

LivyOperator
------------

This operator wraps the Apache Livy batch REST API, allowing to submit a Spark application to the underlying cluster.

.. exampleinclude:: /../../tests/system/providers/apache/livy/example_livy.py
    :language: python
    :start-after: [START create_livy]
    :end-before: [END create_livy]

You can also run this operator in deferrable mode by setting the parameter ``deferrable`` to True.
This will lead to efficient utilization of Airflow workers as polling for job status happens on
the triggerer asynchronously. Note that this will need triggerer to be available on your Airflow deployment.

.. exampleinclude:: /../../tests/system/providers/apache/livy/example_livy.py
    :language: python
    :start-after: [START create_livy_deferrable]
    :end-before: [END create_livy_deferrable]

Reference
"""""""""

For further information, look at `Apache Livy <https://livy.apache.org/>`_.
