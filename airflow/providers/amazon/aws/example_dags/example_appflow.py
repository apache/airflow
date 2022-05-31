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

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.operators.appflow import (
    AppflowRecordsShortCircuit,
    AppflowRunAfterOperator,
    AppflowRunBeforeOperator,
    AppflowRunDailyOperator,
    AppflowRunFullOperator,
    AppflowRunOperator,
)

SOURCE_NAME = "salesforce"
FLOW_NAME = "salesforce-campaign"

with DAG(
    "example_appflow",
    schedule_interval=None,
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=["example"],
) as dag:

    # [START howto_appflow_run]
    run = AppflowRunOperator(
        task_id="campaign-dump",
        source=SOURCE_NAME,
        name=FLOW_NAME,
    )
    # [END howto_appflow_run]

    # [START howto_appflow_run_full]
    run_full = AppflowRunFullOperator(
        task_id="campaign-dump-full",
        source=SOURCE_NAME,
        name=FLOW_NAME,
    )
    # [END howto_appflow_run_full]

    # [START howto_appflow_run_daily]
    run_daily = AppflowRunDailyOperator(
        task_id="campaign-dump-daily",
        source=SOURCE_NAME,
        name=FLOW_NAME,
        source_field="LastModifiedDate",
        dt="{{ ds }}",
    )
    # [END howto_appflow_run_daily]

    # [START howto_appflow_run_before]
    run_before = AppflowRunBeforeOperator(
        task_id="campaign-dump-before",
        source=SOURCE_NAME,
        name=FLOW_NAME,
        source_field="LastModifiedDate",
        dt="{{ ds }}",
    )
    # [END howto_appflow_run_before]

    # [START howto_appflow_run_after]
    run_after = AppflowRunAfterOperator(
        task_id="campaign-dump-after",
        source=SOURCE_NAME,
        name=FLOW_NAME,
        source_field="LastModifiedDate",
        dt="3000-01-01",  # Future date, so no records to dump
    )
    # [END howto_appflow_run_after]

    # [START howto_appflow_shortcircuit]
    has_records = AppflowRecordsShortCircuit(
        task_id="campaign-dump-short-ciruit",
        flow_name=FLOW_NAME,
        appflow_run_task_id="campaign-dump-after",  # Should shortcircuit, no records expected
    )
    # [END howto_appflow_shortcircuit]

    skipped = BashOperator(
        task_id="should_be_skipped",
        bash_command="echo 1",
    )

    run >> run_full >> run_daily >> run_before >> run_after >> has_records >> skipped
