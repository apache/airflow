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
from airflow.providers.amazon.aws.operators.appflow import (
    AppflowRecordsShortCircuitOperator,
    AppflowRunAfterOperator,
    AppflowRunBeforeOperator,
    AppflowRunDailyOperator,
    AppflowRunFullOperator,
)
from airflow.providers.standard.operators.bash import BashOperator
from tests.system.providers.amazon.aws.utils import SystemTestContextBuilder

sys_test_context_task = SystemTestContextBuilder().build()

DAG_ID = "example_appflow"

with DAG(
    DAG_ID,
    schedule="@once",
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=["example"],
) as dag:
    test_context = sys_test_context_task()
    env_id = test_context["ENV_ID"]

    source_name = "salesforce"
    flow_name = f"{env_id}-salesforce-campaign"

    # [START howto_operator_appflow_run_full]
    campaign_dump_full = AppflowRunFullOperator(
        task_id="campaign_dump_full",
        source=source_name,
        flow_name=flow_name,
    )
    # [END howto_operator_appflow_run_full]

    # [START howto_operator_appflow_run_daily]
    campaign_dump_daily = AppflowRunDailyOperator(
        task_id="campaign_dump_daily",
        source=source_name,
        flow_name=flow_name,
        source_field="LastModifiedDate",
        filter_date="{{ ds }}",
    )
    # [END howto_operator_appflow_run_daily]

    # [START howto_operator_appflow_run_before]
    campaign_dump_before = AppflowRunBeforeOperator(
        task_id="campaign_dump_before",
        source=source_name,
        flow_name=flow_name,
        source_field="LastModifiedDate",
        filter_date="{{ ds }}",
    )
    # [END howto_operator_appflow_run_before]

    # [START howto_operator_appflow_run_after]
    campaign_dump_after = AppflowRunAfterOperator(
        task_id="campaign_dump_after",
        source=source_name,
        flow_name=flow_name,
        source_field="LastModifiedDate",
        filter_date="3000-01-01",  # Future date, so no records to dump
    )
    # [END howto_operator_appflow_run_after]

    # [START howto_operator_appflow_shortcircuit]
    campaign_dump_short_circuit = AppflowRecordsShortCircuitOperator(
        task_id="campaign_dump_short_circuit",
        flow_name=flow_name,
        appflow_run_task_id="campaign_dump_after",  # Should shortcircuit, no records expected
    )
    # [END howto_operator_appflow_shortcircuit]

    should_be_skipped = BashOperator(
        task_id="should_be_skipped",
        bash_command="echo 1",
    )

    chain(
        # TEST SETUP
        test_context,
        # TEST BODY
        campaign_dump_full,
        campaign_dump_daily,
        campaign_dump_before,
        campaign_dump_after,
        campaign_dump_short_circuit,
        should_be_skipped,
    )

    from tests.system.utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
