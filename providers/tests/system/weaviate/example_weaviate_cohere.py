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

import pendulum

from airflow.decorators import dag, setup, task, teardown
from airflow.providers.cohere.operators.embedding import CohereEmbeddingOperator
from airflow.providers.weaviate.operators.weaviate import WeaviateIngestOperator

COLLECTION_NAME = "weaviate_cohere_example_collection"


@dag(
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["example", "weaviate", "cohere"],
)
def example_weaviate_cohere():
    """
    Example DAG which creates embeddings using CohereEmbeddingOperator and the uses WeaviateIngestOperator to insert embeddings to Weaviate .
    """

    @setup
    @task
    def create_weaviate_collection():
        """
        Example task to create collection without any Vectorizer. You're expected to provide custom vectors for your data.
        """
        from airflow.providers.weaviate.hooks.weaviate import WeaviateHook

        weaviate_hook = WeaviateHook()
        # Collection definition object. Weaviate's autoschema feature will infer properties when importing.
        weaviate_hook.create_collection(name=COLLECTION_NAME, vectorizer_config=None)

    @setup
    @task
    def get_data_to_embed():
        import json
        from pathlib import Path

        data = json.load(Path("jeopardy_data_without_vectors.json").open())
        return [[item["Question"]] for item in data]

    data_to_embed = get_data_to_embed()

    embed_data = CohereEmbeddingOperator.partial(
        task_id="embedding_using_xcom_data",
    ).expand(input_text=data_to_embed["return_value"])

    @task
    def update_vector_data_in_json(**kwargs):
        import json
        from pathlib import Path

        ti = kwargs["ti"]
        data = json.load(Path("jeopardy_data_without_vectors.json").open())
        embedded_data = ti.xcom_pull(
            task_ids="embedding_using_xcom_data", key="return_value"
        )
        for i, vector in enumerate(embedded_data):
            data[i]["Vector"] = vector[0]
        return data

    update_vector_data_in_json = update_vector_data_in_json()

    perform_ingestion = WeaviateIngestOperator(
        task_id="perform_ingestion",
        conn_id="weaviate_default",
        collection_name=COLLECTION_NAME,
        input_data=update_vector_data_in_json["return_value"],
    )

    embed_query = CohereEmbeddingOperator(
        task_id="embed_query",
        input_text=["biology"],
    )

    @teardown
    @task
    def delete_weaviate_collections():
        """
        Example task to delete a weaviate collection
        """
        from airflow.providers.weaviate.hooks.weaviate import WeaviateHook

        weaviate_hook = WeaviateHook()
        # collection definition object. Weaviate's autoschema feature will infer properties when importing.

        weaviate_hook.delete_collections([COLLECTION_NAME])

    (
        create_weaviate_collection()
        >> embed_data
        >> update_vector_data_in_json
        >> perform_ingestion
        >> embed_query
        >> delete_weaviate_collections()
    )


example_weaviate_cohere()


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
