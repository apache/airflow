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

import os
from datetime import datetime

from airflow import DAG

try:
    from airflow.sdk import task, teardown
except ImportError:
    # Airflow 2 path
    from airflow.decorators import setup, task, teardown  # type: ignore[attr-defined,no-redef]
from airflow.providers.cohere.operators.embedding import CohereEmbeddingOperator
from airflow.providers.pinecone.operators.pinecone import PineconeIngestOperator, \
    CreateServerlessIndexOperator
from airflow.providers.pinecone.hooks.pinecone import PineconeHook

index_name = os.getenv("INDEX_NAME", "example-pinecone-index")
namespace = os.getenv("NAMESPACE", "example-pinecone-index")
data = [
    "Alice Ann Munro is a Canadian short story writer who won the Nobel Prize in Literature in 2013. Munro's work has been described as revolutionizing the architecture of short stories, especially in its tendency to move forward and backward in time."
]

with DAG(
    "example_pinecone_cohere",
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    create_index = CreateServerlessIndexOperator(
        task_id="pinecone_create_serverless_index",
        index_name=index_name,
        dimension=1024,
        cloud="aws",
        region="us-west-2",
        metric="cosine",
    )

    embed_task = CohereEmbeddingOperator(
        task_id="embed_task",
        input_text=data,
    )

    @task
    def transform_output(embedding_output) -> list[dict]:
        # Convert each embedding to a map with an ID and the embedding vector
        return [dict(id=str(i), values=embedding) for i, embedding in enumerate(embedding_output)]

    transformed_output = transform_output(embed_task.output)

    perform_ingestion = PineconeIngestOperator(
        task_id="perform_ingestion",
        index_name=index_name,
        input_vectors=transformed_output,
        namespace=namespace,
        batch_size=1,
    )

    @teardown
    @task
    def delete_index():

        hook = PineconeHook()
        hook.delete_index(index_name=index_name)

    create_index >> embed_task >> transformed_output >> perform_ingestion >> delete_index()


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
