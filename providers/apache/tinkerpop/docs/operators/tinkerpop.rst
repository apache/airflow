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


Apache TinkerPop Operators
==========================

.. _howto/operator:`GremlinOperator`:

GremlinOperator
---------------

Executes Gremlin queries on a remote Gremlin Server.

For parameter definition take a look at :class:`~airflow.providers.apache.tinkerpop.operators.gremlin.GremlinOperator`.

Using the operator
""""""""""""""""""

An example usage of the GremlinOperator to query from gremlin server is as follows:

.. exampleinclude:: /../tests/system/apache/tinkerpop/example_gremlin_dag.py
    :language: python
    :dedent: 4
    :start-after: [START run_query_gremlin_operator]
    :end-before: [END run_query_gremlin_operator]


Reference
"""""""""

For further information, look at: https://tinkerpop.apache.org/gremlin.html
