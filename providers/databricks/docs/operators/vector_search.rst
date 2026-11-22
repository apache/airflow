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

.. _howto/operator:DatabricksVectorSearchSyncOperator:

Synchronize a Databricks Vector Search index
============================================

Use :class:`~airflow.providers.databricks.operators.vector_search.DatabricksVectorSearchSyncOperator`
to request a synchronization of an existing Delta Sync index with a ``TRIGGERED`` pipeline through the
`Vector Search sync API <https://docs.databricks.com/api/vector-search/v1/sync-vector-index>`_.
Continuous indexes synchronize automatically, and Direct Vector Access indexes use the upsert API.

Pass the index's three-level Unity Catalog name as ``index_name``. The operator uses the
:ref:`Databricks connection <howto/connection:databricks>` for authentication; both ``index_name``
and ``databricks_conn_id`` support templating.

.. exampleinclude:: /../../databricks/tests/system/databricks/example_databricks_vector_search.py
    :language: python
    :start-after: [START howto_operator_databricks_vector_search_sync]
    :end-before: [END howto_operator_databricks_vector_search_sync]

The operator returns ``None`` when the request succeeds. Synchronization continues in Databricks;
task success does not guarantee that source-table changes are already searchable. Verify data
freshness before running downstream tasks that depend on the updated index.

HTTP requests are attempted once by default. ``databricks_retry_limit``, ``databricks_retry_delay``
and ``databricks_retry_args`` configure HTTP retries. A timeout can occur after Databricks accepts
the request, so an HTTP retry, task retry or task clear can submit another synchronization.
The example sets ``retries=0`` to override any Dag-level retry default.
