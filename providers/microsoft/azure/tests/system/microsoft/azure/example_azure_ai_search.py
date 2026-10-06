#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""
System test for AzureAISearchHook.

Requires a real Azure AI Search service, reachable through the ``azure_ai_search_default``
connection (``AIRFLOW_CONN_AZURE_AI_SEARCH_DEFAULT``) with an admin API key as password: the
test creates its own index, writes and reads documents through the hook and deletes the index.
"""

from __future__ import annotations

import os
from datetime import datetime

from airflow.providers.common.compat.sdk import DAG, TriggerRule, task
from airflow.providers.microsoft.azure.hooks.ai_search import AzureAISearchHook

DAG_ID = "example_azure_ai_search"
ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID") or "default"
INDEX = f"airflow-system-test-{ENV_ID}"
DOCUMENTS = [
    {"id": "1", "title": "Annual report 2025", "category": "report"},
    {"id": "2", "title": "Quarterly report Q1", "category": "report"},
    {"id": "3", "title": "Meeting notes", "category": "memo"},
]


@task
def create_index():
    """Create the index of this test; the hook works on documents, not on indexes."""
    from azure.core.credentials import AzureKeyCredential
    from azure.search.documents.indexes import SearchIndexClient
    from azure.search.documents.indexes.models import (
        SearchableField,
        SearchFieldDataType,
        SearchIndex,
        SimpleField,
    )

    conn = AzureAISearchHook.get_connection(AzureAISearchHook.default_conn_name)
    client = SearchIndexClient(
        f"https://{conn.host.removeprefix('https://')}", AzureKeyCredential(conn.password)
    )
    client.create_or_update_index(
        SearchIndex(
            name=INDEX,
            fields=[
                SimpleField(name="id", type=SearchFieldDataType.String, key=True),
                SearchableField(name="title", type=SearchFieldDataType.String),
                SimpleField(name="category", type=SearchFieldDataType.String, filterable=True),
            ],
        )
    )


# [START howto_hook_azure_ai_search_upload]
@task
async def upload_documents() -> int:
    """Insert the documents, replacing the ones whose key already exists."""
    hook = AzureAISearchHook(index=INDEX)
    return await hook.upload(DOCUMENTS)


# [END howto_hook_azure_ai_search_upload]


# [START howto_hook_azure_ai_search_search]
@task(retries=3)
async def find_reports() -> list[str]:
    """Return the keys of the reports; retries while the service still indexes the write."""
    hook = AzureAISearchHook(index=INDEX)
    reports = [
        document["id"]
        async for document in hook.search(select=["id"], filter="category eq 'report'", order_by=["id"])
    ]
    if len(reports) < 2:
        raise ValueError(f"Expected 2 reports, found {reports}")
    return reports


# [END howto_hook_azure_ai_search_search]


# [START howto_hook_azure_ai_search_merge]
@task
async def archive_reports(keys: list[str]) -> int:
    """Change one field of the reports, then count the documents that have it."""
    hook = AzureAISearchHook(index=INDEX)
    await hook.merge([{"id": key, "category": "archive"} for key in keys])
    return await hook.count(filter="category eq 'archive'")


# [END howto_hook_azure_ai_search_merge]


# [START howto_hook_azure_ai_search_get_document]
@task
async def read_first_document(keys: list[str]) -> dict:
    hook = AzureAISearchHook(index=INDEX)
    document = await hook.get_document(keys[0], select=["id", "title"])
    if document is None:
        raise ValueError(f"Document {keys[0]!r} is missing")
    return document


# [END howto_hook_azure_ai_search_get_document]


# [START howto_hook_azure_ai_search_delete]
@task
async def delete_documents() -> int:
    """Delete the documents by key; only the key field is needed."""
    hook = AzureAISearchHook(index=INDEX)
    return await hook.delete({"id": document["id"]} for document in DOCUMENTS)


# [END howto_hook_azure_ai_search_delete]


@task(trigger_rule=TriggerRule.ALL_DONE)
def delete_index():
    from azure.core.credentials import AzureKeyCredential
    from azure.search.documents.indexes import SearchIndexClient

    conn = AzureAISearchHook.get_connection(AzureAISearchHook.default_conn_name)
    client = SearchIndexClient(
        f"https://{conn.host.removeprefix('https://')}", AzureKeyCredential(conn.password)
    )
    client.delete_index(INDEX)


with DAG(
    DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example"],
) as dag:
    reports = find_reports()
    (
        create_index()
        >> upload_documents()
        >> reports
        >> archive_reports(reports)
        >> read_first_document(reports)
        >> delete_documents()
        >> delete_index()
    )

    from tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: contributing-docs/testing/system_tests.rst)
test_run = get_test_run(dag)
