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

from airflow.models.baseoperator import chain
from airflow.models.dag import DAG
from airflow.providers.amazon.aws.operators.eventbridge import (
    EventBridgeDisableRuleOperator,
    EventBridgeEnableRuleOperator,
    EventBridgePutEventsOperator,
    EventBridgePutRuleOperator,
)

from providers.tests.system.amazon.aws.utils import ENV_ID_KEY, SystemTestContextBuilder

DAG_ID = "example_eventbridge"
ENTRIES = [
    {
        "Detail": '{"event-name": "custom-event"}',
        "EventBusName": "custom-bus",
        "Source": "example.myapp",
        "DetailType": "Sample Custom Event",
    }
]


sys_test_context_task = SystemTestContextBuilder().build()

with DAG(
    dag_id=DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    tags=["example"],
    catchup=False,
) as dag:
    test_context = sys_test_context_task()

    env_id = test_context[ENV_ID_KEY]

    # [START howto_operator_eventbridge_put_events]
    put_events = EventBridgePutEventsOperator(task_id="put_events_task", entries=ENTRIES)
    # [END howto_operator_eventbridge_put_events]

    # [START howto_operator_eventbridge_put_rule]
    put_rule = EventBridgePutRuleOperator(
        task_id="put_rule_task",
        name="example_rule",
        event_pattern='{"source": ["example.myapp"]}',
        description="This rule matches events from example.myapp.",
        state="DISABLED",
    )
    # [END howto_operator_eventbridge_put_rule]

    # [START howto_operator_eventbridge_enable_rule]
    enable_rule = EventBridgeEnableRuleOperator(task_id="enable_rule_task", name="example_rule")
    # [END howto_operator_eventbridge_enable_rule]

    # [START howto_operator_eventbridge_disable_rule]
    disable_rule = EventBridgeDisableRuleOperator(
        task_id="disable_rule_task",
        name="example_rule",
    )
    # [END howto_operator_eventbridge_disable_rule]

    chain(test_context, put_events, put_rule, enable_rule, disable_rule)


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
