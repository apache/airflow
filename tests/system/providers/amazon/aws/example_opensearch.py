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
from __future__ import annotations

from datetime import datetime

import boto3
from opensearch_dsl import Keyword, Search, Text
from opensearch_dsl.document import Document

from airflow import DAG
from airflow.decorators import task
from airflow.models.baseoperator import chain
from airflow.providers.amazon.aws.operators.opensearch import (
    OpenSearchAddDocumentOperator,
    OpenSearchCreateIndexOperator,
    OpenSearchQueryOperator,
)
from airflow.utils.trigger_rule import TriggerRule
from tests.system.providers.amazon.aws.utils import ENV_ID_KEY, SystemTestContextBuilder

DAG_ID = "example_opensearch"
INDEX_NAME = "example-index"

sys_test_context_task = SystemTestContextBuilder().build()


class TestDoc(Document):
    title = Text(fields={"raw": Keyword()})
    media_type = Text()

    class Index:
        name = INDEX_NAME

    def save(self, **kwargs):
        return super().save(**kwargs)


@task
def create_open_search_cluster():
    env_id = test_context[ENV_ID_KEY]
    opensearch = boto3.client("opensearch")
    opensearch.create_domain(
        DomainName=f"{env_id}-opensearch-cluster",
        EngineVersion="2.7",
        ClusterConfig={
            "InstanceType": "t3.small.search",
            "InstanceCount": 1,
            "DedicatedMasterEnabled": False,
            "ZoneAwarenessEnabled": False,
        },
    )


@task(trigger_rule=TriggerRule.ALL_DONE)
def delete_os_cluster(env_id: str):
    boto3.client("opensearch").delete_domain(DomainName=f"{env_id}-opensearch-cluster")


with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2023, 9, 1),
    schedule="@once",
    catchup=False,
    tags=["example"],
) as dag:
    test_context = sys_test_context_task()
    create_cluster = create_open_search_cluster()
    # [START howto_operator_opensearch_index]
    create_index = OpenSearchCreateIndexOperator(
        task_id="create_index_example",
        index_name=INDEX_NAME,
        index_body={"settings": {"index": {"number_of_shards": 1}}},
    )
    # [END howto_operator_opensearch_index]

    # [START howto_operator_opensearch_document]
    add_low_doc = OpenSearchAddDocumentOperator(
        task_id="add_low_level_document",
        index_name=INDEX_NAME,
        document={"title": "MontyPython", "media_type": "Movie"},
        doc_id=1,
    )
    add_high_doc = OpenSearchAddDocumentOperator(
        task_id="add_high_level_document",
        doc_class=TestDoc(meta={"id": 2}, title="Top Gun", media_type="Movie"),
    )

    # [END howto_operator_opensearch_document]

    # [START howto_operator_opensearch_search]
    search_low_docs = OpenSearchQueryOperator(
        task_id="search_low_level",
        index_name=INDEX_NAME,
        query={
            "size": 5,
            "query": {"multi_match": {"query": "MontyPython", "fields": ["title^2", "media_type"]}},
        },
    )

    search_high_docs = OpenSearchQueryOperator(
        task_id="search_high",
        search_object=Search(index=INDEX_NAME)
        .filter("term", media_type="Movie")
        .query("match", title="Top Gun"),
    )

    # [END howto_operator_opensearch_search]

    remove_cluster = delete_os_cluster(env_id=test_context[ENV_ID_KEY])

    chain(
        # TEST SETUP
        test_context,
        create_cluster,
        # TEST BODY
        create_index,
        add_low_doc,
        add_high_doc,
        search_low_docs,
        search_high_docs,
        # TEST TEAR DOWN
        remove_cluster,
    )
    from tests.system.utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
