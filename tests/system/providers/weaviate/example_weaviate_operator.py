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

from airflow.decorators import dag, task, teardown
from airflow.providers.weaviate.operators.weaviate import (
    WeaviateDocumentIngestOperator,
    WeaviateIngestOperator,
)

sample_data_with_vector = [
    {
        "Answer": "Liver",
        "Category": "SCIENCE",
        "Question": "This organ removes excess glucose from the blood & stores it as glycogen",
        "Vector": [
            -0.006632288,
            -0.0042016874,
            0.030541966,
        ],
    },
    {
        "Answer": "Elephant",
        "Category": "ANIMALS",
        "Question": "It's the only living mammal in the order Proboseidea",
        "Vector": [
            -0.0166891,
            -0.00092290324,
            -0.0125168245,
        ],
    },
    {
        "Answer": "the nose or snout",
        "Category": "ANIMALS",
        "Question": "The gavial looks very much like a crocodile except for this bodily feature",
        "Vector": [
            -0.015592773,
            0.019883318,
            0.017782344,
        ],
    },
]

sample_data_without_vector = [
    {
        "Answer": "Liver",
        "Category": "SCIENCE",
        "Question": "This organ removes excess glucose from the blood & stores it as glycogen",
    },
    {
        "Answer": "Elephant",
        "Category": "ANIMALS",
        "Question": "It's the only living mammal in the order Proboseidea",
    },
    {
        "Answer": "the nose or snout",
        "Category": "ANIMALS",
        "Question": "The gavial looks very much like a crocodile except for this bodily feature",
    },
]


def get_data_with_vectors(*args, **kwargs):
    return sample_data_with_vector


def get_data_without_vectors(*args, **kwargs):
    return sample_data_without_vector


@dag(
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["example", "weaviate"],
)
def example_weaviate_using_operator():
    """
    Example Weaviate DAG demonstrating usage of the operator.
    """

    # Example tasks to create a Weaviate class without vectorizers, store data with custom vectors in XCOM, and call
    # WeaviateIngestOperator to ingest data with those custom vectors.
    @task()
    def create_class_without_vectorizer():
        """
        Example task to create class without any Vectorizer. You're expected to provide custom vectors for your data.
        """
        from airflow.providers.weaviate.hooks.weaviate import WeaviateHook

        weaviate_hook = WeaviateHook()
        # Class definition object. Weaviate's autoschema feature will infer properties when importing.
        class_obj = {
            "class": "QuestionWithoutVectorizerUsingOperator",
            "vectorizer": "none",
        }
        weaviate_hook.create_class(class_obj)

    @task(trigger_rule="all_done")
    def store_data_with_vectors_in_xcom():
        return sample_data_with_vector

    # [START howto_operator_weaviate_embedding_and_ingest_xcom_data_with_vectors]
    batch_data_with_vectors_xcom_data = WeaviateIngestOperator(
        task_id="batch_data_with_vectors_xcom_data",
        conn_id="weaviate_default",
        class_name="QuestionWithoutVectorizerUsingOperator",
        input_json=store_data_with_vectors_in_xcom(),
        trigger_rule="all_done",
    )
    # [END howto_operator_weaviate_embedding_and_ingest_xcom_data_with_vectors]

    # [START howto_operator_weaviate_embedding_and_ingest_callable_data_with_vectors]
    batch_data_with_vectors_callable_data = WeaviateIngestOperator(
        task_id="batch_data_with_vectors_callable_data",
        conn_id="weaviate_default",
        class_name="QuestionWithoutVectorizerUsingOperator",
        input_json=get_data_with_vectors(),
        trigger_rule="all_done",
    )
    # [END howto_operator_weaviate_embedding_and_ingest_callable_data_with_vectors]

    # Example tasks to create class with OpenAI vectorizer, store data without vectors in XCOM, and call
    # WeaviateIngestOperator to ingest data by internally generating OpenAI vectors while ingesting.
    @task()
    def create_class_with_vectorizer():
        """
        Example task to create class with OpenAI Vectorizer responsible for vectorining data using Weaviate cluster.
        """
        from airflow.providers.weaviate.hooks.weaviate import WeaviateHook

        weaviate_hook = WeaviateHook()
        class_obj = {
            "class": "QuestionWithOpenAIVectorizerUsingOperator",
            "description": "Information from a Jeopardy! question",  # description of the class
            "properties": [
                {
                    "dataType": ["text"],
                    "description": "The question",
                    "name": "question",
                },
                {
                    "dataType": ["text"],
                    "description": "The answer",
                    "name": "answer",
                },
                {
                    "dataType": ["text"],
                    "description": "The category",
                    "name": "category",
                },
            ],
            "vectorizer": "text2vec-openai",
        }
        weaviate_hook.create_class(class_obj)

    @task()
    def create_class_for_doc_data_with_vectorizer():
        """
        Example task to create class with OpenAI Vectorizer responsible for vectorining data using Weaviate cluster.
        """
        from airflow.providers.weaviate.hooks.weaviate import WeaviateHook

        weaviate_hook = WeaviateHook()
        class_obj = {
            "class": "QuestionWithOpenAIVectorizerUsingOperatorDocs",
            "description": "Information from a Jeopardy! question",  # description of the class
            "properties": [
                {
                    "dataType": ["text"],
                    "description": "The question",
                    "name": "question",
                },
                {
                    "dataType": ["text"],
                    "description": "The answer",
                    "name": "answer",
                },
                {
                    "dataType": ["text"],
                    "description": "The category",
                    "name": "category",
                },
                {
                    "dataType": ["text"],
                    "description": "URL for source document",
                    "name": "docLink",
                },
            ],
            "vectorizer": "text2vec-openai",
        }
        weaviate_hook.create_class(class_obj)

    @task(trigger_rule="all_done")
    def store_data_without_vectors_in_xcom():
        import json
        from pathlib import Path

        data = json.load(Path("jeopardy_data_without_vectors.json").open())
        return data

    @task(trigger_rule="all_done")
    def store_doc_data_without_vectors_in_xcom():
        import json
        from pathlib import Path

        data = json.load(Path("jeopardy_doc_data_without_vectors.json").open())
        return data

    xcom_data_without_vectors = store_data_without_vectors_in_xcom()
    xcom_doc_data_without_vectors = store_doc_data_without_vectors_in_xcom()

    # [START howto_operator_weaviate_ingest_xcom_data_without_vectors]
    batch_data_without_vectors_xcom_data = WeaviateIngestOperator(
        task_id="batch_data_without_vectors_xcom_data",
        conn_id="weaviate_default",
        class_name="QuestionWithOpenAIVectorizerUsingOperator",
        input_json=xcom_data_without_vectors["return_value"],
        trigger_rule="all_done",
    )
    # [END howto_operator_weaviate_ingest_xcom_data_without_vectors]

    # [START howto_operator_weaviate_ingest_callable_data_without_vectors]
    batch_data_without_vectors_callable_data = WeaviateIngestOperator(
        task_id="batch_data_without_vectors_callable_data",
        conn_id="weaviate_default",
        class_name="QuestionWithOpenAIVectorizerUsingOperator",
        input_json=get_data_without_vectors(),
        trigger_rule="all_done",
    )
    # [END howto_operator_weaviate_ingest_callable_data_without_vectors]

    create_or_replace_document_objects_without_vectors = WeaviateDocumentIngestOperator(
        task_id="create_or_replace_document_objects_without_vectors_xcom_data",
        existing="replace",
        document_column="docLink",
        conn_id="weaviate_default",
        class_name="QuestionWithOpenAIVectorizerUsingOperatorDocs",
        batch_config_params={"batch_size": 1000},
        input_data=xcom_doc_data_without_vectors["return_value"],
        trigger_rule="all_done",
    )

    @teardown
    @task
    def delete_weaviate_class_Vector():
        """
        Example task to delete a weaviate class
        """
        from airflow.providers.weaviate.hooks.weaviate import WeaviateHook

        weaviate_hook = WeaviateHook()
        # Class definition object. Weaviate's autoschema feature will infer properties when importing.

        weaviate_hook.delete_classes(
            [
                "QuestionWithOpenAIVectorizerUsingOperator",
            ]
        )

    @teardown
    @task
    def delete_weaviate_class_without_Vector():
        """
        Example task to delete a weaviate class
        """
        from airflow.providers.weaviate.hooks.weaviate import WeaviateHook

        weaviate_hook = WeaviateHook()
        # Class definition object. Weaviate's autoschema feature will infer properties when importing.

        weaviate_hook.delete_classes(
            [
                "QuestionWithoutVectorizerUsingOperator",
            ]
        )

    @teardown
    @task
    def delete_weaviate_docs_class_without_Vector():
        """
        Example task to delete a weaviate class
        """
        from airflow.providers.weaviate.hooks.weaviate import WeaviateHook

        weaviate_hook = WeaviateHook()
        # Class definition object. Weaviate's autoschema feature will infer properties when importing.

        weaviate_hook.delete_classes(["QuestionWithOpenAIVectorizerUsingOperatorDocs"])

    (
        create_class_without_vectorizer()
        >> [batch_data_with_vectors_xcom_data, batch_data_with_vectors_callable_data]
        >> delete_weaviate_class_without_Vector()
    )
    (
        create_class_for_doc_data_with_vectorizer()
        >> [create_or_replace_document_objects_without_vectors]
        >> delete_weaviate_docs_class_without_Vector()
    )
    (
        create_class_with_vectorizer()
        >> [
            batch_data_without_vectors_xcom_data,
            batch_data_without_vectors_callable_data,
        ]
        >> delete_weaviate_class_Vector()
    )


example_weaviate_using_operator()


from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
