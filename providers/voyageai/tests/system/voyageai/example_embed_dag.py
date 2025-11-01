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

from pendulum import datetime

from airflow.decorators import dag, task
from airflow.providers.voyageai.operators.embedding import VoyageEmbeddingOperator


# [START howto_operator_voyageai_embed]
@dag(
    dag_id="voyage_ai_example_dag",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["voyageai", "example"],
)
def voyage_ai_example_dag():
    """A simple DAG to demonstrate the VoyageEmbeddingOperator."""
    texts_to_embed = [
        "This is a test sentence from an Airflow DAG.",
        "The Voyage AI provider makes embedding easy.",
    ]

    generate_embeddings = VoyageEmbeddingOperator(
        task_id="generate_embeddings",
        conn_id="voyageai_default",
        input_texts=texts_to_embed,
        model="voyage-2",
    )

    @task
    def process_embeddings(embeddings: list):
        if not embeddings:
            raise ValueError("No embeddings were returned.")

        print(f"Successfully received {len(embeddings)} embedding vectors.")
        print(f"Dimension of the first vector: {len(embeddings[0])}")

    process_embeddings(embeddings=generate_embeddings.output)


voyage_ai_example_dag()
# [END howto_operator_voyageai_embed]

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
