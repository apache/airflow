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
from datetime import datetime

import boto3

from airflow.providers.amazon.aws.operators.opensearch_serverless import (
    OpenSearchServerlessCreateCollectionOperator,
)
from airflow.providers.amazon.aws.sensors.opensearch_serverless import (
    OpenSearchServerlessCollectionActiveSensor,
)
from airflow.providers.common.compat.sdk import DAG, chain

from system.amazon.aws.utils import ENV_ID_KEY, SystemTestContextBuilder
from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS

if AIRFLOW_V_3_0_PLUS:
    from airflow.sdk import TriggerRule, task
else:
    from airflow.decorators import task  # type: ignore[attr-defined,no-redef]
    from airflow.utils.trigger_rule import TriggerRule  # type: ignore[no-redef,attr-defined]

DAG_ID = "example_opensearch_serverless"

sys_test_context_task = SystemTestContextBuilder().build()


@task
def create_encryption_policy(collection_name: str, policy_name: str):
    boto3.client("opensearchserverless").create_security_policy(
        name=policy_name,
        type="encryption",
        policy=json.dumps(
            {
                "Rules": [{"ResourceType": "collection", "Resource": [f"collection/{collection_name}"]}],
                "AWSOwnedKey": True,
            }
        ),
    )


@task(trigger_rule=TriggerRule.ALL_DONE)
def delete_encryption_policy(policy_name: str):
    boto3.client("opensearchserverless").delete_security_policy(name=policy_name, type="encryption")


@task(trigger_rule=TriggerRule.ALL_DONE)
def delete_collection(collection_id: str):
    boto3.client("opensearchserverless").delete_collection(id=collection_id)


with DAG(
    dag_id=DAG_ID,
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:
    test_context = sys_test_context_task()
    env_id = test_context[ENV_ID_KEY]
    collection_name = f"{env_id}-collection"
    encryption_policy_name = f"{env_id}-enc-policy"

    # [START howto_operator_opensearch_serverless_create_collection]
    create_collection = OpenSearchServerlessCreateCollectionOperator(
        task_id="create_collection",
        collection_name=collection_name,
        collection_type="SEARCH",
    )
    # [END howto_operator_opensearch_serverless_create_collection]

    wait_for_collection = OpenSearchServerlessCollectionActiveSensor(
        task_id="wait_for_collection",
        collection_name=collection_name,
        poke_interval=30,
        timeout=600,
    )

    chain(
        # TEST SETUP
        test_context,
        create_encryption_policy(collection_name, encryption_policy_name),
        # TEST BODY
        create_collection,
        wait_for_collection,
        # TEST TEARDOWN
        delete_collection(create_collection.output),
        delete_encryption_policy(encryption_policy_name),
    )

    from tests_common.test_utils.watcher import watcher

    list(dag.tasks) >> watcher()

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

test_run = get_test_run(dag)
