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

Vector Search system-test prerequisites
======================================

``example_databricks_vector_search.py`` requires an existing Delta Sync index whose pipeline mode
is ``TRIGGERED``. Set ``VECTOR_SEARCH_INDEX_NAME`` to its three-level Unity Catalog name and configure
``databricks_default`` with an identity permitted to synchronize it.

Create the fixture using the `Databricks index setup guide
<https://docs.databricks.com/aws/en/ai-search/create-ai-search>`_. Use a dedicated test index: running
the test starts its synchronization and can incur Databricks compute charges.

Create a local initialization script at ``dev/vector_search_system_init.sh`` that exports
``VECTOR_SEARCH_INDEX_NAME`` and ``AIRFLOW_CONN_DATABRICKS_DEFAULT`` for the test environment.
Keep credentials out of commits. Run the example through the Breeze system-test harness:

.. code-block:: bash

    SYSTEM_TESTS_ENV_ID=vector_search \
    BREEZE_INIT_COMMAND='source /opt/airflow/dev/vector_search_system_init.sh' \
    breeze testing system-tests --python 3.11 --forward-credentials \
      --mount-sources all --keep-env-variables --test-timeout 1200 -- \
      providers/databricks/tests/system/databricks/example_databricks_vector_search.py -v -s

A passing test proves that Databricks accepted the sync request and the index reported an online
state without pending updates. This follows the SDK's state-based wait; it does not identify a
particular sync execution or independently verify search-result freshness. The example uses a
triggerer and waits for up to 600 seconds after submission.

The test uses the existing endpoint, index and source table without creating or deleting resources.
Fixture provisioning and deletion remain the responsibility of the test-environment owner.
