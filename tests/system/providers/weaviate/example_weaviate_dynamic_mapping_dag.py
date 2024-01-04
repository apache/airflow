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

import pendulum
from airflow.decorators import dag, setup, task, teardown
from airflow.providers.weaviate.operators.weaviate import WeaviateIngestOperator


@dag(
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["example", "weaviate"],
)
def example_weaviate_dynamic_mapping_dag():
    """
    Example DAG which uses WeaviateIngestOperator to insert embeddings to Weaviate using dynamic mapping"""

    @setup
    @task
    def create_weaviate_class(data):
        """
        Example task to create class without any Vectorizer. You're expected to provide custom vectors for your data.
        """
        from airflow.providers.weaviate.hooks.weaviate import WeaviateHook

        weaviate_hook = WeaviateHook()
        # Class definition object. Weaviate's autoschema feature will infer properties when importing.
        class_obj = {
            "class": data[0],
            "vectorizer": data[1],
        }
        weaviate_hook.create_class(class_obj)

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
        class_name=["example1", "example2"],
        input_data=get_data_to_ingest["return_value"],
    )

    @teardown
    @task
    def delete_weaviate_class(class_name):
        """
        Example task to delete a weaviate class
        """

        from airflow.providers.weaviate.hooks.weaviate import WeaviateHook
        weaviate_hook = WeaviateHook()
        # Class definition object. Weaviate's autoschema feature will infer properties when importing.

        weaviate_hook.delete_classes([class_name])

    (
        create_weaviate_class.expand(data=[["example1", "none"], ["example2", "text2vec-openai"]])
        >> perform_ingestion
        >> delete_weaviate_class.expand(class_name=["example1", "example2"])
    )


example_weaviate_dynamic_mapping_dag()

from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
