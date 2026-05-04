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
    BedrockCreateGuardrailOperator,
    BedrockCreateGuardrailVersionOperator,
    BedrockDeleteGuardrailOperator,
)
from airflow.providers.common.compat.sdk import DAG, chain

from system.amazon.aws.utils import ENV_ID_KEY, SystemTestContextBuilder
from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS

if AIRFLOW_V_3_0_PLUS:
    from airflow.sdk import TriggerRule
else:
    from airflow.utils.trigger_rule import TriggerRule  # type: ignore[no-redef,attr-defined]

DAG_ID = "example_bedrock_guardrail"

sys_test_context_task = SystemTestContextBuilder().build()

with DAG(
    dag_id=DAG_ID,
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:
    test_context = sys_test_context_task()
    env_id = test_context[ENV_ID_KEY]

    # [START howto_operator_bedrock_create_guardrail]
    create_guardrail = BedrockCreateGuardrailOperator(
        task_id="create_guardrail",
        guardrail_name=f"{env_id}-guardrail",
        blocked_input_messaging="Sorry, this input is not allowed.",
        blocked_outputs_messaging="Sorry, I cannot respond to that.",
        content_policy_config={
            "filtersConfig": [
                {"type": "HATE", "inputStrength": "HIGH", "outputStrength": "HIGH"},
                {"type": "VIOLENCE", "inputStrength": "HIGH", "outputStrength": "HIGH"},
            ]
        },
    )
    # [END howto_operator_bedrock_create_guardrail]

    # [START howto_operator_bedrock_delete_guardrail]
    delete_guardrail = BedrockDeleteGuardrailOperator(
        task_id="delete_guardrail",
        guardrail_identifier=create_guardrail.output,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    # [END howto_operator_bedrock_delete_guardrail]

    # [START howto_operator_bedrock_create_guardrail_version]
    create_guardrail_version = BedrockCreateGuardrailVersionOperator(
        task_id="create_guardrail_version",
        guardrail_identifier=create_guardrail.output,
    )
    # [END howto_operator_bedrock_create_guardrail_version]

    chain(
        test_context,
        create_guardrail,
        create_guardrail_version,
        delete_guardrail,
    )

    from tests_common.test_utils.watcher import watcher

    list(dag.tasks) >> watcher()

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

test_run = get_test_run(dag)
