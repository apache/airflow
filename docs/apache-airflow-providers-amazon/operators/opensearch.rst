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

====================
Amazon OpenSearch
====================

`Amazon Open Search <https://aws.amazon.com/opensearch-service/>`__ Amazon OpenSearch Service makes it
easy for you to perform interactive log analytics, real-time application monitoring, website search, and more.
OpenSearch is an open source, distributed search and analytics suite derived from Elasticsearch.

Prerequisite Tasks
------------------

.. include:: ../../_partials/prerequisite_tasks.rst

Operators
---------

.. _howto/operator:OpenSearchCreateIndexOperator:

Create an Index on an Open Search Domain
=================================================

Use the :class:`OpenSearchAddDocumentOperator <airflow.providers.amazon.aws.operators.opensearch>` to add
a new document to a specified Index on an Amazon OpenSearch cluster.


.. exampleinclude:: /../../tests/system/providers/amazon/aws/example_opensearch.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_opensearch_document]
    :end-before: [END howto_operator_opensearch_document]

.. _howto/operator:OpenSearchAddDocumentOperator:

Add a document to an Index on an Open Search Domain
=================================================

Use the :class:`OpenSearchCreateIndexOperator <airflow.providers.amazon.aws.operators.opensearch>` to create a new
index on an Open Search Cluster


.. exampleinclude:: /../../tests/system/providers/amazon/aws/example_opensearch.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_opensearch_index]
    :end-before: [END howto_operator_opensearch_index]


.. _howto/operator:OpenSearchSearchOperator:

Run a query on an Amazon OpenSearch cluster
=================================================

Use the :class:`OpenSearchSearchOperator <airflow.providers.amazon.aws.operators.opensearch>` to run
search queries against an Amazon OpenSearch cluster on a given index.


.. exampleinclude:: /../../tests/system/providers/amazon/aws/example_opensearch.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_opensearch_search]
    :end-before: [END howto_operator_opensearch_search]




Reference
---------
* `Open Search High Level Client <https://opensearch.org/docs/latest/clients/python-high-level/>`__
* `Open Search Low Level Client <https://opensearch.org/docs/latest/clients/python-low-level/>`__
