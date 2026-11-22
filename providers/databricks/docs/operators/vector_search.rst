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

By default, the operator waits for the index to report an online state without pending updates,
following the Databricks SDK's
`wait_until_ready(wait_for_updates=True) <https://api-docs.databricks.com/python/ai-search/databricks.ai_search.html#databricks.ai_search.index.AISearchIndex.wait_until_ready>`_
convention. Index failures and unrecognized states fail the task. This state check does not identify
a particular sync execution and can finish immediately if the index already reports a qualifying state.

``polling_period_seconds`` defaults to 30 seconds. ``timeout`` defaults to 3600 seconds and bounds the
wait after submission, including status requests and retries. Set ``deferrable=True`` to release the
worker while waiting. A timeout does not cancel the synchronization in Databricks.

Set ``wait_for_completion=False`` to return as soon as the sync request succeeds, without polling or
deferring. Both modes return ``None``.

HTTP requests are attempted once by default. ``databricks_retry_limit``, ``databricks_retry_delay``
and ``databricks_retry_args`` configure HTTP retries. A timeout can occur after Databricks accepts
the request, so an HTTP retry, task retry or task clear can submit another synchronization.
The example sets ``retries=0`` to override any Dag-level retry default.
