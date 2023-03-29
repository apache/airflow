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

.. _howto/hook:elasticsearch_python_hook:

ElasticsearchPythonHook
========================

Elasticsearch Hook that is using the native Python client to communicate with Elasticsearch

Parameters
------------
hosts
  A list of a single or many Elasticsearch instances. Example: ``["http://localhost:9200"]``.
es_conn_args
  Additional arguments you might need to enter to connect to Elasticsearch.
  Example: ``{"ca_cert":"/path/to/cert", "basic_auth": "(user, pass)"}``

  For all possible configurations, consult with Elasticsearch documentation.
  Reference: https://www.elastic.co/guide/en/elasticsearch/client/python-api/current/connecting.html

Usage Example
---------------------

.. exampleinclude:: /../../tests/system/providers/elasticsearch/example_elasticsearch_query.py
    :language: python
    :start-after: [START howto_elasticsearch_python_hook]
    :end-before: [END howto_elasticsearch_python_hook]
