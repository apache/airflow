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
=========

The operators use :class:`~airflow.providers.search.hooks.search.SearchHook` and work the same
against Elasticsearch and OpenSearch.

.. _howto/operator:SearchQueryOperator:

SearchQueryOperator
-------------------

Runs a search and returns the response, which is pushed to XCom.

.. code-block:: python

    from airflow.providers.search.operators.search import SearchQueryOperator

    find_failures = SearchQueryOperator(
        task_id="find_failures",
        index_name="orders-*",
        query={"query": {"term": {"status": "failed"}}, "size": 100},
    )

.. _howto/operator:SearchCreateIndexOperator:

SearchCreateIndexOperator
-------------------------

Creates an index, optionally with settings and mappings.

.. code-block:: python

    from airflow.providers.search.operators.search import SearchCreateIndexOperator

    create_index = SearchCreateIndexOperator(
        task_id="create_index",
        index_name="orders",
        index_body={"mappings": {"properties": {"status": {"type": "keyword"}}}},
    )

.. _howto/operator:SearchAddDocumentOperator:

SearchAddDocumentOperator
-------------------------

Adds a document, or replaces the one with the same id.

.. code-block:: python

    from airflow.providers.search.operators.search import SearchAddDocumentOperator

    add_order = SearchAddDocumentOperator(
        task_id="add_order",
        index_name="orders",
        doc_id="{{ ds_nodash }}",
        document={"status": "created", "date": "{{ ds }}"},
    )

SQL
---

``SearchHook`` is also a ``DbApiHook`` over the engine's SQL endpoint (``_sql`` on Elasticsearch,
``_plugins/_sql`` on OpenSearch), so ``SQLExecuteQueryOperator`` accepts a ``search`` connection.
Query parameters (``?`` placeholders) work on Elasticsearch only.

.. code-block:: python

    from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

    count_failures = SQLExecuteQueryOperator(
        task_id="count_failures",
        conn_id="search_default",
        sql="SELECT COUNT(*) FROM orders WHERE status = 'failed'",
    )
