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

=============
OpenSearch
=============

`OpenSearch <https://opensearch.org/>`__


Operators
---------

.. _howto/operator:OpenSearchCreateIndexOperator:

Create an Index in OpenSearch
=============================

Use :class:`~airflow.providers.opensearch.operators.opensearch.OpenSearchCreateIndexOperator`
to create a new index in an OpenSearch domain.



.. exampleinclude:: /../../providers/tests/system/opensearch/example_opensearch.py
    :language: python
    :start-after: [START howto_operator_opensearch_create_index]
    :dedent: 4
    :end-before: [END howto_operator_opensearch_create_index]


.. _howto/operator:OpenSearchAddDocumentOperator:

Add a Document to an Index on OpenSearch
========================================

Use :class:`~airflow.providers.opensearch.operators.opensearch.OpenSearchAddDocumentOperator`
to add single documents to an OpenSearch Index

.. exampleinclude:: /../../providers/tests/system/opensearch/example_opensearch.py
    :language: python
    :start-after: [START howto_operator_opensearch_add_document]
    :dedent: 4
    :end-before: [END howto_operator_opensearch_add_document]


.. _howto/operator:OpenSearchQueryOperator:

Run a query against an OpenSearch Index
=======================================

Use :class:`~airflow.providers.opensearch.operators.opensearch.OpenSearchQueryOperator`
to run a query against an OpenSearch index.

.. exampleinclude:: /../../providers/tests/system/opensearch/example_opensearch.py
    :language: python
    :start-after: [START howto_operator_opensearch_query]
    :dedent: 4
    :end-before: [END howto_operator_opensearch_query]
