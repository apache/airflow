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
from weaviate.classes.config import DataType, Property
from weaviate.collections.classes.config import Configure

from airflow.decorators import dag, task, teardown

COLLECTION_NAME = "QuestionWithOpenAIVectorizerUsingHook"


@dag(
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["example", "weaviate"],
)
def example_weaviate_dag_using_hook():
    """Example Weaviate DAG demonstrating usage of the hook."""

    @task()
    def create_collection_with_vectorizer():
        """
        Example task to create collection with OpenAI Vectorizer responsible for vectorining data using Weaviate cluster.
        """
        from airflow.providers.weaviate.hooks.weaviate import WeaviateHook

        weaviate_hook = WeaviateHook()
        weaviate_hook.create_collection(
            COLLECTION_NAME,
            description="Information from a Jeopardy! question",
            properties=[
                Property(name="question", description="The question", data_type=DataType.TEXT),
                Property(name="answer", description="The answer", data_type=DataType.TEXT),
                Property(name="category", description="The category", data_type=DataType.TEXT),
            ],
            vectorizer_config=Configure.Vectorizer.text2vec_openai(),
        )

    @task()
    def create_collection_without_vectorizer():
        """
        Example task to create collection without any Vectorizer. You're expected to provide custom vectors for your data.
        """
        from airflow.providers.weaviate.hooks.weaviate import WeaviateHook

        weaviate_hook = WeaviateHook()
        # collection definition object. Weaviate's autoschema feature will infer properties when importing.
        weaviate_hook.create_collection(
            "QuestionWithoutVectorizerUsingHook",
            vectorizer_config=None,
        )

    @task(trigger_rule="all_done")
    def store_data_without_vectors_in_xcom():
        import json
        from pathlib import Path

        data = json.load(Path("jeopardy_data_without_vectors.json").open())
        return data

    @task(trigger_rule="all_done")
    def store_data_with_vectors_in_xcom():
        import json
        from pathlib import Path

        data = json.load(Path("jeopardy_data_with_vectors.json").open())
        return data

    @task(trigger_rule="all_done")
    def batch_data_without_vectors(data: list):
        from airflow.providers.weaviate.hooks.weaviate import WeaviateHook

        weaviate_hook = WeaviateHook()
        weaviate_hook.batch_data(COLLECTION_NAME, data)

    @task(trigger_rule="all_done")
    def batch_data_with_vectors(data: list):
        from airflow.providers.weaviate.hooks.weaviate import WeaviateHook

        weaviate_hook = WeaviateHook()
        weaviate_hook.batch_data("QuestionWithoutVectorizerUsingHook", data)

    @teardown
    @task
    def delete_weaviate_collection_vector():
        """
        Example task to delete a weaviate collection
        """
        from airflow.providers.weaviate.hooks.weaviate import WeaviateHook

        weaviate_hook = WeaviateHook()
        # collection definition object. Weaviate's autoschema feature will infer properties when importing.

        weaviate_hook.delete_collections([COLLECTION_NAME])

    @teardown
    @task
    def delete_weaviate_collection_without_vector():
        """
        Example task to delete a weaviate collection
        """
        from airflow.providers.weaviate.hooks.weaviate import WeaviateHook

        weaviate_hook = WeaviateHook()
        # collection definition object. Weaviate's autoschema feature will infer properties when importing.

        weaviate_hook.delete_collections(["QuestionWithoutVectorizerUsingHook"])

    data_with_vectors = store_data_with_vectors_in_xcom()
    (
        create_collection_without_vectorizer()
        >> batch_data_with_vectors(data_with_vectors["return_value"])
        >> delete_weaviate_collection_vector()
    )

    data_without_vectors = store_data_without_vectors_in_xcom()
    (
        create_collection_with_vectorizer()
        >> batch_data_without_vectors(data_without_vectors["return_value"])
        >> delete_weaviate_collection_without_vector()
    )


example_weaviate_dag_using_hook()

from dev.tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
