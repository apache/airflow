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

ElasticsearchHook
=================

:class:`~airflow.providers.elasticsearch.hooks.elasticsearch.ElasticsearchHook` provides enhanced functionality for interacting with Elasticsearch clusters with better Airflow integration.

Features
--------

* **Airflow Connection Integration**: Uses standard Airflow connections with environment variable fallback
* **Bulk Operations**: Support for bulk, streaming bulk, and parallel bulk operations
* **Index Management**: Create, delete, and check index existence
* **Data Processing**: Convert search results to pandas DataFrames
* **Context Manager**: Automatic connection cleanup
* **Enhanced Error Handling**: Better connection testing and error reporting

Configuration
-------------

The hook supports configuration through:

1. **Airflow Connections** (primary)
2. **Environment Variables** (fallback)
3. **Default Values** (last resort)

**Environment Variables:**

* ``ELASTICSEARCH_HOST``
* ``ELASTICSEARCH_PORT``
* ``ELASTICSEARCH_USERNAME``
* ``ELASTICSEARCH_PASSWORD``
* ``ELASTICSEARCH_USE_SSL``
* ``ELASTICSEARCH_VERIFY_CERTS``
* ``ELASTICSEARCH_TIMEOUT``
* ``ELASTICSEARCH_MAX_RETRIES``

Usage Examples
--------------

**Basic Setup:**

.. code-block:: python

    from airflow.providers.elasticsearch.hooks.elasticsearch import ElasticsearchHook

    # Using default connection
    hook = ElasticsearchHook()

    # Using specific connection
    hook = ElasticsearchHook(elasticsearch_conn_id="my_es_conn")

**Connection Testing:**

.. code-block:: python

    # Test connection
    if hook.test_connection():
        print("Connection successful!")

**Index Operations:**

.. code-block:: python

    # Create index with mappings and settings
    hook.create_index(
        index_name="my_index",
        mappings={"properties": {"field": {"type": "text"}}},
        settings={"number_of_shards": 1},
    )

    # Check if index exists
    exists = hook.index_exists("my_index")

    # Delete index
    hook.delete_index("old_index")

**Bulk Operations:**

.. code-block:: python

    actions = [
        {"_index": "my_index", "_source": {"field": "value1"}},
        {"_index": "my_index", "_source": {"field": "value2"}},
    ]

    # Standard bulk
    success_count, errors = hook.bulk(actions)

    # Streaming bulk
    for success, info in hook.streaming_bulk(actions):
        if not success:
            print(f"Failed: {info}")

**Pandas Integration:**

.. code-block:: python

    # Convert search results to DataFrame
    df = hook.search_to_pandas(index="my_index", query={"match_all": {}})

    # Scan large datasets to DataFrame
    df = hook.scan_to_pandas(index="large_index", query={"range": {"date": {"gte": "2023-01-01"}}})

**Data Migration:**

.. code-block:: python

    # Reindex data between indices
    success_count, errors = hook.reindex(source_index="old_index", target_index="new_index")

**Context Manager:**

.. code-block:: python

    # Automatic cleanup
    with ElasticsearchHook() as hook:
        results = hook.search({"match_all": {}}, "my_index")

API Reference
-------------

.. currentmodule:: airflow.providers.elasticsearch.hooks.elasticsearch

.. autoclass:: ElasticsearchHook
    :members:
    :undoc-members:
    :show-inheritance:
