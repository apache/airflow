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

from airflow.decorators import dag, task
from airflow.providers.openai.hooks.openai import OpenAIHook
from airflow.providers.openai.operators.openai import OpenAIEmbeddingOperator


def input_text_callable(
    input_arg1: str,
    input_arg2: str,
    input_kwarg1: str = "default_kwarg1_value",
    input_kwarg2: str = "default_kwarg1_value",
):
    text = " ".join([input_arg1, input_arg2, input_kwarg1, input_kwarg2])
    return text


@dag(
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["example", "openai"],
)
def example_openai_dag():
    """
    ### TaskFlow API Tutorial Documentation
    This is a simple data pipeline example which demonstrates the use of
    the TaskFlow API using three simple tasks for Extract, Transform, and Load.
    Documentation that goes along with the Airflow TaskFlow API tutorial is
    located
    [here](https://airflow.apache.org/docs/apache-airflow/stable/tutorial_taskflow_api.html)
    """

    texts = [
        "On Kernel-Target Alignment. We describe a family of global optimization procedures",
        " that automatically decompose optimization problems into smaller loosely coupled",
        " problems, then combine the solutions of these with message passing algorithms.",
    ]

    @task()
    def create_embeddings_using_hook():
        """
        #### Extract task
        A simple Extract task to get data ready for the rest of the data
        pipeline. In this case, getting data is simulated by reading from a
        hardcoded JSON string.
        """
        openai_hook = OpenAIHook()
        embeddings = openai_hook.create_embeddings(texts[0])
        return embeddings

    @task()
    def task_to_store_input_text_in_xcom():
        return texts[0]

    # [START howto_operator_openai_embedding]
    OpenAIEmbeddingOperator(
        task_id="embedding_using_xcom_data",
        conn_id="openai_default",
        input_text=task_to_store_input_text_in_xcom(),
        model="text-embedding-ada-002",
    )

    OpenAIEmbeddingOperator(
        task_id="embedding_using_callable",
        conn_id="openai_default",
        input_text=input_text_callable(
            "input_arg1_value",
            "input2_value",
            input_kwarg1="input_kwarg1_value",
            input_kwarg2="input_kwarg2_value",
        ),
        model="text-embedding-ada-002",
    )
    OpenAIEmbeddingOperator(
        task_id="embedding_using_text",
        conn_id="openai_default",
        input_text=texts,
        model="text-embedding-ada-002",
    )
    # [END howto_operator_openai_embedding]

    create_embeddings_using_hook()


example_openai_dag()


from dev.tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
