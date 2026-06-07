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

from datetime import datetime

from airflow.providers.amazon.aws.operators.bedrock import (
    BedrockCreateAgentRuntimeOperator,
    BedrockDeleteAgentRuntimeOperator,
    BedrockInvokeAgentRuntimeOperator,
)

from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS

if AIRFLOW_V_3_0_PLUS:
    from airflow.sdk import DAG, chain
else:
    from airflow.models.baseoperator import chain  # type: ignore[attr-defined,no-redef]
    from airflow.models.dag import DAG  # type: ignore[attr-defined,no-redef,assignment]

try:
    from airflow.sdk import TriggerRule
except ImportError:
    from airflow.utils.trigger_rule import TriggerRule  # type: ignore[no-redef,attr-defined]

from system.amazon.aws.utils import SystemTestContextBuilder

# Externally fetched variables:
ROLE_ARN_KEY = "ROLE_ARN"
CONTAINER_URI_KEY = "CONTAINER_URI"

sys_test_context_task = (
    SystemTestContextBuilder().add_variable(ROLE_ARN_KEY).add_variable(CONTAINER_URI_KEY).build()
)

DAG_ID = "example_bedrock_agentcore"

with DAG(
    dag_id=DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
) as dag:
    test_context = sys_test_context_task()
    env_id = test_context["ENV_ID"]

    runtime_name = f"airflow-agentcore-{env_id}"

    # [START howto_operator_bedrock_create_agent_runtime]
    create_agent_runtime = BedrockCreateAgentRuntimeOperator(
        task_id="create_agent_runtime",
        agent_runtime_name=runtime_name,
        agent_runtime_artifact={
            "containerConfiguration": {
                "containerUri": test_context[CONTAINER_URI_KEY],
            },
        },
        role_arn=test_context[ROLE_ARN_KEY],
        network_configuration={"networkMode": "PUBLIC"},
        deferrable=True,
    )
    # [END howto_operator_bedrock_create_agent_runtime]

    # [START howto_operator_bedrock_invoke_agent_runtime]
    invoke_agent_runtime = BedrockInvokeAgentRuntimeOperator(
        task_id="invoke_agent_runtime",
        agent_runtime_arn=create_agent_runtime.output,
        payload={"prompt": "Hello from Airflow"},
        botocore_config={"read_timeout": 300},
    )
    # [END howto_operator_bedrock_invoke_agent_runtime]

    # [START howto_operator_bedrock_delete_agent_runtime]
    delete_agent_runtime = BedrockDeleteAgentRuntimeOperator(
        task_id="delete_agent_runtime",
        agent_runtime_id="{{ task_instance.xcom_pull('create_agent_runtime').split('/')[-1] }}",
        trigger_rule=TriggerRule.ALL_DONE,
    )
    # [END howto_operator_bedrock_delete_agent_runtime]

    chain(
        # TEST SETUP
        test_context,
        # TEST BODY
        create_agent_runtime,
        invoke_agent_runtime,
        # TEST TEARDOWN
        delete_agent_runtime,
    )

    from tests_common.test_utils.watcher import watcher

    list(dag.tasks) >> watcher()

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

test_run = get_test_run(dag)
