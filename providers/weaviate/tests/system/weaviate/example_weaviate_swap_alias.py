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
from weaviate.classes.config import Configure

try:
    from airflow.sdk import dag, task, teardown
except ImportError:
    # Airflow 2 path
    from airflow.decorators import dag, task, teardown  # type: ignore[attr-defined,no-redef]
from airflow.providers.weaviate.operators.weaviate import WeaviateIngestOperator, WeaviateSwapAliasOperator

ALIAS_NAME = "QuestionsAlias"
OLD_COLLECTION = "QuestionsAliasV1"
NEW_COLLECTION = "QuestionsAliasV2"

sample_data = [
    {"Question": "This organ removes excess glucose from the blood", "Vector": [-0.0066, -0.0042, 0.0305]},
    {
        "Question": "It's the only living mammal in the order Proboseidea",
        "Vector": [-0.0167, -0.0009, -0.0125],
    },
]


@dag(
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["example", "weaviate"],
)
def example_weaviate_swap_alias():
    """Example DAG re-indexing into a new collection and swapping an alias to it without downtime."""

    @task()
    def create_collections():
        from airflow.providers.weaviate.hooks.weaviate import WeaviateHook

        hook = WeaviateHook()
        for name in (OLD_COLLECTION, NEW_COLLECTION):
            hook.create_collection(name, vector_config=Configure.Vectors.self_provided())

    point_alias_to_old_collection = WeaviateSwapAliasOperator(
        task_id="point_alias_to_old_collection",
        conn_id="weaviate_default",
        alias_name=ALIAS_NAME,
        target_collection=OLD_COLLECTION,
    )

    ingest_into_new_collection = WeaviateIngestOperator(
        task_id="ingest_into_new_collection",
        conn_id="weaviate_default",
        collection_name=NEW_COLLECTION,
        input_data=sample_data,
    )

    # [START howto_operator_weaviate_swap_alias]
    swap_alias_to_new_collection = WeaviateSwapAliasOperator(
        task_id="swap_alias_to_new_collection",
        conn_id="weaviate_default",
        alias_name=ALIAS_NAME,
        target_collection=NEW_COLLECTION,
        delete_previous_collection=True,
    )
    # [END howto_operator_weaviate_swap_alias]

    @teardown
    @task
    def delete_alias_and_collections():
        from airflow.providers.weaviate.hooks.weaviate import WeaviateHook

        hook = WeaviateHook()
        hook.delete_alias(ALIAS_NAME)
        hook.delete_collections([OLD_COLLECTION, NEW_COLLECTION])

    (
        create_collections()
        >> point_alias_to_old_collection
        >> ingest_into_new_collection
        >> swap_alias_to_new_collection
        >> delete_alias_and_collections()
    )


example_dag = example_weaviate_swap_alias()


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: contributing-docs/testing/system_tests.rst)
test_run = get_test_run(example_dag)
