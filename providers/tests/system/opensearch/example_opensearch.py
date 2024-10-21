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

# TODO: FIXME - therea are a number of typing issues in those opensearch examples and they should be fixed
# mypy: disable-error-code="call-arg,attr-defined"

from __future__ import annotations

from datetime import datetime, timedelta

from opensearchpy import Integer, Text
from opensearchpy.helpers.document import Document
from opensearchpy.helpers.search import Search

from airflow.models.baseoperator import chain
from airflow.models.dag import DAG
from airflow.providers.opensearch.operators.opensearch import (
    OpenSearchAddDocumentOperator,
    OpenSearchCreateIndexOperator,
    OpenSearchQueryOperator,
)

DAG_ID = "example_opensearch"
INDEX_NAME = "example_index"

default_args = {
    "owner": "airflow",
    "depend_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


class LogDocument(Document):
    log_group_id = Integer()
    logger = Text()
    message = Text()

    class Index:
        name = INDEX_NAME

    def save(self, **kwargs):
        super().save(**kwargs)


def load_connections():
    # Connections needed for this example dag to finish
    from airflow.models import Connection
    from airflow.utils import db

    db.merge_conn(
        Connection(
            conn_id="opensearch_test", conn_type="opensearch", host="127.0.0.1", login="test", password="test"
        )
    )


with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2021, 1, 1),
    schedule="@once",
    catchup=False,
    tags=["example"],
    default_args=default_args,
    description="Examples of OpenSearch Operators",
) as dag:
    # [START howto_operator_opensearch_create_index]
    create_index = OpenSearchCreateIndexOperator(
        task_id="create_index",
        index_name=INDEX_NAME,
        index_body={"settings": {"index": {"number_of_shards": 1}}},
    )
    # [END howto_operator_opensearch_create_index]

    # [START howto_operator_opensearch_add_document]
    add_document_by_args = OpenSearchAddDocumentOperator(
        task_id="add_document_with_args",
        index_name=INDEX_NAME,
        doc_id=1,
        document={"log_group_id": 1, "logger": "python", "message": "hello world"},
    )

    add_document_by_class = OpenSearchAddDocumentOperator(
        task_id="add_document_by_class",
        doc_class=LogDocument(log_group_id=2, logger="airflow", message="hello airflow"),
    )
    # [END howto_operator_opensearch_add_document]

    # [START howto_operator_opensearch_query]
    search_low_level = OpenSearchQueryOperator(
        task_id="low_level_query",
        index_name="system_test",
        query={"query": {"bool": {"must": {"match": {"message": "hello world"}}}}},
    )
    search = Search()
    search._index = [INDEX_NAME]
    search_object = search.filter("term", logger="airflow").query("match", message="hello airflow")

    search_high_level = OpenSearchQueryOperator(task_id="high_level_query", search_object=search_object)
    # [END howto_operator_opensearch_query]

    chain(create_index, add_document_by_class, add_document_by_args, search_high_level, search_low_level)

    from tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
