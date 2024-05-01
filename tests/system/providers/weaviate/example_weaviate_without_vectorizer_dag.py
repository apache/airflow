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
from airflow.providers.openai.operators.openai import OpenAIEmbeddingOperator
from airflow.providers.weaviate.operators.weaviate import WeaviateIngestOperator

class_name = "Weaviate_example_without_vectorizer_class"


@dag(
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["example", "weaviate"],
)
def example_weaviate_without_vectorizer_dag():
    """
    Example DAG which uses WeaviateIngestOperator to insert embeddings to Weaviate without vectorizer and then query to verify the response
    """

    @setup
    @task
    def create_weaviate_class():
        """
        Example task to create class without any Vectorizer. You're expected to provide custom vectors for your data.
        """
        from airflow.providers.weaviate.hooks.weaviate import WeaviateHook

        weaviate_hook = WeaviateHook()
        # Class definition object. Weaviate's autoschema feature will infer properties when importing.
        class_obj = {
            "class": class_name,
            "vectorizer": "none",
        }
        weaviate_hook.create_class(class_obj)

    @setup
    @task
    def get_data_without_vectors():
        import json
        from pathlib import Path

        data = json.load(Path("jeopardy_data_with_vectors.json").open())
        return data

    data_to_ingest = get_data_without_vectors()

    perform_ingestion = WeaviateIngestOperator(
        task_id="perform_ingestion",
        conn_id="weaviate_default",
        class_name=class_name,
        input_data=data_to_ingest["return_value"],
    )

    embedd_query = OpenAIEmbeddingOperator(
        task_id="embedd_query",
        conn_id="openai_default",
        input_text="biology",
        model="text-embedding-ada-002",
    )

    @task
    def query_weaviate(**kwargs):
        from airflow.providers.weaviate.hooks.weaviate import WeaviateHook

        ti = kwargs["ti"]
        query_vector = ti.xcom_pull(task_ids="embedd_query", key="return_value")
        weaviate_hook = WeaviateHook()
        properties = ["question", "answer", "category"]
        response = weaviate_hook.query_with_vector(
            query_vector, "Weaviate_example_without_vectorizer_class", *properties
        )
        assert "In 1953 Watson & Crick built a model" in response["data"]["Get"][class_name][0]["question"]

    @teardown
    @task
    def delete_weaviate_class():
        """
        Example task to delete a weaviate class
        """
        from airflow.providers.weaviate.hooks.weaviate import WeaviateHook

        weaviate_hook = WeaviateHook()
        # Class definition object. Weaviate's autoschema feature will infer properties when importing.

        weaviate_hook.delete_classes([class_name])

    (
        create_weaviate_class()
        >> perform_ingestion
        >> embedd_query
        >> query_weaviate()
        >> delete_weaviate_class()
    )


example_weaviate_without_vectorizer_dag()


from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
