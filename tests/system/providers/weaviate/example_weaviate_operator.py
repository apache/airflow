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

from airflow.decorators import dag, task, teardown
from airflow.providers.weaviate.hooks.weaviate import WeaviateHook
from airflow.providers.weaviate.operators.weaviate import WeaviateIngestOperator


def get_data_with_vectors(*args, **kwargs):
    data = json.load(Path("./data/jeopardy_data_with_vectors.json").open())
    return data


def get_data_without_vectors(*args, **kwargs):
    data = json.load(Path("./data/jeopardy_data_without_vectors.json").open())
    return data


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

    # Example tasks to create a Weaviate class without vectorizers, store data with custom vectors in XCOM,
    # and call WeaviateIngestOperator to ingest data with those custom vectors. There are 2 tasks for
    # WeaviateIngestOperator, one of them gets input data from the XCOM and the other gets input data
    # from a callable.
    @task()
    def create_class_without_vectorizer():
        """
        Example task to create class without any Vectorizer. You're expected to provide custom vectors
         for your data.
        """
        weaviate_hook = WeaviateHook()
        # Class definition object. Weaviate's autoschema feature will infer properties when importing.
        class_obj = {
            "class": "QuestionWithoutVectorizerUsingOperator",
            "vectorizer": "none",
        }
        weaviate_hook.create_class(class_obj)

    @task(trigger_rule="all_done")
    def store_data_with_vectors_in_xcom():
        data = json.load(Path("./data/jeopardy_data_with_vectors.json").open())
        return data

    xcom_data_with_vectors = store_data_with_vectors_in_xcom()

    # [START howto_operator_weaviate_embedding_and_ingest_xcom_data_with_vectors]
    batch_data_with_vectors_xcom_data = WeaviateIngestOperator(
        task_id="batch_data_with_vectors_xcom_data",
        conn_id="weaviate_default",
        class_name="QuestionWithoutVectorizerUsingOperator",
        input_json=xcom_data_with_vectors["return_value"],
        trigger_rule="all_done",
    )
    # [END howto_operator_weaviate_embedding_and_ingest_xcom_data_with_vectors]

    # [START howto_operator_weaviate_embedding_and_ingest_callable_data_with_vectors]
    batch_data_with_vectors_callable_data = WeaviateIngestOperator(
        task_id="batch_data_with_vectors_callable_data",
        conn_id="weaviate_default",
        class_name="QuestionWithoutVectorizerUsingOperator",
        input_callable=get_data_with_vectors,
        input_callable_args=[],
        input_callable_kwargs={},
        trigger_rule="all_done",
    )
    # [END howto_operator_weaviate_embedding_and_ingest_callable_data_with_vectors]

    # Example tasks to create class with OpenAI vectorizer, store data without vectors in XCOM, and call
    # WeaviateIngestOperator to ingest data by internally generating OpenAI vectors while ingesting.
    # There are 2 tasks for WeaviateIngestOperator, one of them gets input data from the XCOM and the other
    # gets input data from a callable.
    @task()
    def create_class_with_vectorizer():
        """
        Example task to create class with OpenAI Vectorizer responsible for vectorining data using Weaviate
         cluster.
        """
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

    @task(trigger_rule="all_done")
    def store_data_without_vectors_in_xcom():
        data = json.load(Path("./data/jeopardy_data_without_vectors.json").open())
        return data

    xcom_data_without_vectors = store_data_without_vectors_in_xcom()

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
        input_callable=get_data_without_vectors,
        input_callable_args=[],
        input_callable_kwargs={},
        trigger_rule="all_done",
    )
    # [END howto_operator_weaviate_ingest_callable_data_without_vectors]

    @teardown
    @task
    def delete_weaviate_class_Vector():
        """
        Example task to delete a weaviate class
        """
        weaviate_hook = WeaviateHook()
        # Class definition object. Weaviate's autoschema feature will infer properties when importing.

        weaviate_hook.delete_class("QuestionWithOpenAIVectorizerUsingOperator")

    @teardown
    @task
    def delete_weaviate_class_without_Vector():
        """
        Example task to delete a weaviate class
        """
        weaviate_hook = WeaviateHook()
        # Class definition object. Weaviate's autoschema feature will infer properties when importing.

        weaviate_hook.delete_class("QuestionWithoutVectorizerUsingOperator")

    (
        create_class_without_vectorizer()
        >> [
            batch_data_with_vectors_xcom_data,
            batch_data_with_vectors_callable_data,
        ]
        >> delete_weaviate_class_without_Vector()
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
