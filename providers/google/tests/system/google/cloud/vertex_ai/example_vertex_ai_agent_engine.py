#
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

"""
Example Airflow Dag for Google Vertex AI Agent Engine operations.
"""

from __future__ import annotations

import os
from datetime import datetime
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from vertexai._genai import types as vertexai_types

from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator, GCSDeleteBucketOperator
from airflow.providers.google.cloud.operators.vertex_ai.agent_engine import (
    CheckQueryAgentEngineOperator,
    CreateAgentEngineOperator,
    DeleteAgentEngineOperator,
    GetAgentEngineOperator,
    QueryAgentEngineOperator,
    UpdateAgentEngineOperator,
)

try:
    from airflow.sdk import DAG, TriggerRule
except ImportError:
    # Compatibility for Airflow < 3.1
    from airflow.models.dag import DAG  # type: ignore[attr-defined,no-redef,assignment]
    from airflow.utils.trigger_rule import TriggerRule  # type: ignore[no-redef,attr-defined]

DAG_ID = "vertex_ai_agent_engine_operations"
LOCATION = "us-central1"
ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID", "default")
PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT", "default")
CONTAINER_URI = os.environ.get(
    "SYSTEM_TESTS_VERTEX_AI_AGENT_ENGINE_CONTAINER_URI",
    "us-central1-docker.pkg.dev/example-project/example-repository/example-agent:latest",
)

AGENT_ENGINE_ID = "{{ task_instance.xcom_pull(task_ids='create_agent_engine')['name'].split('/')[-1] }}"
QUERY_OPERATION_NAME = "{{ task_instance.xcom_pull(task_ids='query_agent_engine')['job_name'] }}"
BUCKET_NAME = f"bucket-{DAG_ID}-{ENV_ID}".replace("_", "-")
DISPLAY_NAME = f"airflow-agent-engine-{ENV_ID}"

QUERY_CONFIG: vertexai_types.RunQueryJobAgentEngineConfigDict = {
    "query": "Respond with a short acknowledgement.",
    "output_gcs_uri": f"gs://{BUCKET_NAME}/query-output/",
}

with DAG(
    DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example", "vertex_ai", "agent_engine"],
) as dag:
    # [START how_to_cloud_vertex_ai_create_agent_engine_operator]
    create_agent_engine = CreateAgentEngineOperator(
        task_id="create_agent_engine",
        project_id=PROJECT_ID,
        location=LOCATION,
        config={
            "display_name": DISPLAY_NAME,
            "description": "Airflow system test Agent Engine",
            "agent_framework": "custom",
            "min_instances": 0,
            "max_instances": 1,
            "resource_limits": {"cpu": "1", "memory": "1Gi"},
            "container_spec": {"image_uri": CONTAINER_URI},
            "class_methods": [
                {
                    "name": "query",
                    "api_mode": "",
                },
            ],
        },
    )
    # [END how_to_cloud_vertex_ai_create_agent_engine_operator]

    # [START how_to_cloud_vertex_ai_get_agent_engine_operator]
    get_agent_engine = GetAgentEngineOperator(
        task_id="get_agent_engine",
        project_id=PROJECT_ID,
        location=LOCATION,
        agent_engine_id=AGENT_ENGINE_ID,
    )
    # [END how_to_cloud_vertex_ai_get_agent_engine_operator]

    create_bucket = GCSCreateBucketOperator(
        task_id="create_bucket",
        bucket_name=BUCKET_NAME,
        project_id=PROJECT_ID,
    )

    # [START how_to_cloud_vertex_ai_query_agent_engine_operator]
    query_agent_engine = QueryAgentEngineOperator(
        task_id="query_agent_engine",
        project_id=PROJECT_ID,
        location=LOCATION,
        agent_engine_id=AGENT_ENGINE_ID,
        config=QUERY_CONFIG,
    )
    # [END how_to_cloud_vertex_ai_query_agent_engine_operator]

    # [START how_to_cloud_vertex_ai_check_query_agent_engine_operator]
    check_query_agent_engine = CheckQueryAgentEngineOperator(
        task_id="check_query_agent_engine",
        project_id=PROJECT_ID,
        location=LOCATION,
        operation_name=QUERY_OPERATION_NAME,
        config={"retrieve_result": True},
        deferrable=True,
    )
    # [END how_to_cloud_vertex_ai_check_query_agent_engine_operator]

    # [START how_to_cloud_vertex_ai_update_agent_engine_operator]
    update_agent_engine = UpdateAgentEngineOperator(
        task_id="update_agent_engine",
        project_id=PROJECT_ID,
        location=LOCATION,
        agent_engine_id=AGENT_ENGINE_ID,
        config={
            "display_name": f"{DISPLAY_NAME}-updated",
            "description": "Updated Airflow system test Agent Engine",
        },
    )
    # [END how_to_cloud_vertex_ai_update_agent_engine_operator]

    # [START how_to_cloud_vertex_ai_delete_agent_engine_operator]
    delete_agent_engine = DeleteAgentEngineOperator(
        task_id="delete_agent_engine",
        project_id=PROJECT_ID,
        location=LOCATION,
        agent_engine_id=AGENT_ENGINE_ID,
        force=True,
        deferrable=True,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    # [END how_to_cloud_vertex_ai_delete_agent_engine_operator]

    delete_bucket = GCSDeleteBucketOperator(
        task_id="delete_bucket",
        bucket_name=BUCKET_NAME,
        force=True,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    (
        create_agent_engine
        >> get_agent_engine
        >> create_bucket
        >> query_agent_engine
        >> check_query_agent_engine
        >> update_agent_engine
        >> delete_agent_engine
        >> delete_bucket
    )

    # ### Everything below this line is not part of example ###
    # ### Just for system tests purpose ###
    from tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the Dag
    list(dag.tasks) >> watcher()

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example Dag with pytest (see: contributing-docs/testing/system_tests.rst)
test_run = get_test_run(dag)
