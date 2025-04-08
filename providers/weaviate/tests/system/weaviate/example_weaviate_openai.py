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

import json
from pathlib import Path

import pendulum

from airflow.decorators import dag, setup, task, teardown
from airflow.providers.openai.operators.openai import OpenAIEmbeddingOperator
from airflow.providers.weaviate.hooks.weaviate import WeaviateHook
from airflow.providers.weaviate.operators.weaviate import WeaviateIngestOperator

COLLECTION_NAME = "Weaviate_openai_example_collection"


@dag(
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["example", "weaviate"],
)
def example_weaviate_openai():
    """
    Example DAG which creates embeddings using OpenAIEmbeddingOperator and the uses WeaviateIngestOperator to insert embeddings to Weaviate .
    """

    @setup
    @task
    def create_weaviate_collection():
        """
        Example task to create collection without any Vectorizer. You're expected to provide custom vectors for your data.
        """
        weaviate_hook = WeaviateHook()
        # collection definition object. Weaviate's autoschema feature will infer properties when importing.
        weaviate_hook.create_collection(COLLECTION_NAME)

    @setup
    @task
    def get_data_to_embed():
        data = json.load(Path("jeopardy_data_without_vectors.json").open())
        return [item["Question"] for item in data]

    data_to_embed = get_data_to_embed()

    embed_data = OpenAIEmbeddingOperator.partial(
        task_id="embedding_using_xcom_data",
        conn_id="openai_default",
        model="text-embedding-ada-002",
    ).expand(input_text=data_to_embed["return_value"])

    @task
    def update_vector_data_in_json(**kwargs):
        ti = kwargs["ti"]
        data = json.load(Path("jeopardy_data_without_vectors.json").open())
        embedded_data = ti.xcom_pull(task_ids="embedding_using_xcom_data", key="return_value")
        for i, vector in enumerate(embedded_data):
            data[i]["Vector"] = vector
        return data

    update_vector_data_in_json = update_vector_data_in_json()

    perform_ingestion = WeaviateIngestOperator(
        task_id="perform_ingestion",
        conn_id="weaviate_default",
        collection_name=COLLECTION_NAME,
        input_data=update_vector_data_in_json["return_value"],
    )

    embed_query = OpenAIEmbeddingOperator(
        task_id="embed_query",
        conn_id="openai_default",
        input_text="biology",
        model="text-embedding-ada-002",
    )

    @task
    def query_weaviate(**kwargs):
        ti = kwargs["ti"]
        query_vector = ti.xcom_pull(task_ids="embed_query", key="return_value")
        weaviate_hook = WeaviateHook()
        properties = ["question", "answer", "category"]
        response = weaviate_hook.query_with_vector(query_vector, COLLECTION_NAME, properties)
        assert "In 1953 Watson & Crick built a model" in response.objects[0].properties["question"]

    @teardown
    @task
    def delete_weaviate_collection():
        """
        Example task to delete a weaviate collection
        """
        weaviate_hook = WeaviateHook()
        # collection definition object. Weaviate's autoschema feature will infer properties when importing.

        weaviate_hook.delete_collections([COLLECTION_NAME])

    (
        create_weaviate_collection()
        >> embed_data
        >> update_vector_data_in_json
        >> perform_ingestion
        >> embed_query
        >> query_weaviate()
        >> delete_weaviate_collection()
    )


example_weaviate_openai()


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
