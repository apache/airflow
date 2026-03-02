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

from datetime import timedelta

import pendulum
from weaviate.collections.classes.config import Configure

try:
    from airflow.sdk import dag, setup, task, teardown
except ImportError:
    # Airflow 2 path
    from airflow.decorators import dag, setup, task, teardown  # type: ignore[attr-defined,no-redef]
from airflow.providers.weaviate.operators.weaviate import WeaviateIngestOperator

COLLECTION_NAMES = ["Weaviate_DTM_example_collection_1", "Weaviate_DTM_example_collection_2"]

default_args = {
    "retries": 5,
    "retry_delay": timedelta(seconds=15),
    "pool": "weaviate_pool",
}


@dag(
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    default_args=default_args,
    catchup=False,
    tags=["example", "weaviate"],
)
def example_weaviate_dynamic_mapping_dag():
    """
    Example DAG which uses WeaviateIngestOperator to insert embeddings to Weaviate using dynamic mapping"""

    @setup
    @task
    def create_weaviate_collection(data):
        """
        Example task to create collection without any Vectorizer. You're expected to provide custom vectors for your data.
        """
        from airflow.providers.weaviate.hooks.weaviate import WeaviateHook

        weaviate_hook = WeaviateHook()
        # collection definition object. Weaviate's autoschema feature will infer properties when importing.
        weaviate_hook.create_collection(data[0], vectorizer_config=data[1])

    @setup
    @task
    def get_data_to_ingest():
        import json
        from pathlib import Path

        file1 = json.load(Path("jeopardy_data_with_vectors.json").open())
        file2 = json.load(Path("jeopardy_data_without_vectors.json").open())
        return [file1, file2]

    get_data_to_ingest = get_data_to_ingest()

    perform_ingestion = WeaviateIngestOperator.partial(
        task_id="perform_ingestion",
        conn_id="weaviate_default",
    ).expand(
        collection_name=COLLECTION_NAMES,
        input_data=get_data_to_ingest["return_value"],
    )

    @teardown
    @task
    def delete_weaviate_collection(collection_name):
        """
        Example task to delete a weaviate collection
        """

        from airflow.providers.weaviate.hooks.weaviate import WeaviateHook

        weaviate_hook = WeaviateHook()
        # collection definition object. Weaviate's autoschema feature will infer properties when importing.

        weaviate_hook.delete_collections([collection_name])

    (
        create_weaviate_collection.expand(
            data=[[COLLECTION_NAMES[0], None], [COLLECTION_NAMES[1], Configure.Vectorizer.text2vec_openai()]]
        )
        >> perform_ingestion
        >> delete_weaviate_collection.expand(collection_name=COLLECTION_NAMES)
    )


example_weaviate_dynamic_mapping_dag()

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
