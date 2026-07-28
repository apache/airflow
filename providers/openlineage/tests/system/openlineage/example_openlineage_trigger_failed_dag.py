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
Simple DAG that triggers another simple DAG that fails.

It checks:
    - task's trigger_dag_id
    - DAGRun START and FAIL events, for the triggered DAG
    - propagation of OL parent and root info from DAGRun conf
    - error message run facet on DAG FAIL event
    - airflow state run facet on DAG FAIL event showing failed state
"""

from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.trigger_rule import TriggerRule

from system.openlineage.constants import DEFAULT_DAGRUN_TIMEOUT
from system.openlineage.expected_events import get_expected_event_file_path
from system.openlineage.operator import OpenLineageTestOperator

DAG_ID = "openlineage_trigger_failed_dag"

with DAG(
    dagrun_timeout=DEFAULT_DAGRUN_TIMEOUT,
    dag_id=DAG_ID,
    start_date=datetime(2021, 1, 1),
    schedule=None,
    catchup=False,
    default_args={"retries": 0},
) as dag:
    trigger_dagrun = TriggerDagRunOperator(
        task_id="trigger_dagrun",
        trigger_dag_id="openlineage_trigger_failed_dag_child__notrigger",
        trigger_run_id=f"openlineage_trigger_failed_dag_triggering_child_{datetime.now().isoformat()}",
        wait_for_completion=True,
        conf={
            "some_config": "value1",
            "openlineage": {
                "parentRunId": "3bb703d1-09c1-4a42-8da5-35a0b3216072",
                "parentJobNamespace": "prod_biz",
                "parentJobName": "get_files",
                "rootParentRunId": "9d3b14f7-de91-40b6-aeef-e887e2c7673e",
                "rootParentJobNamespace": "prod_analytics",
                "rootParentJobName": "generate_report_sales_e2e",
            },
        },
        poke_interval=10,
    )

    empty_task_on_fail = EmptyOperator(
        task_id="empty_task_on_fail",
        trigger_rule=TriggerRule.ONE_FAILED,
    )

    check_events = OpenLineageTestOperator(
        task_id="check_events",
        file_path=get_expected_event_file_path(DAG_ID),
    )

    trigger_dagrun >> empty_task_on_fail >> check_events


with DAG(
    dagrun_timeout=DEFAULT_DAGRUN_TIMEOUT,
    dag_id="openlineage_trigger_failed_dag_child__notrigger",
    start_date=datetime(2021, 1, 1),
    schedule=None,
    catchup=False,
    tags=["first", "second@", "with'quote", 'z"e'],
    doc_md="MD DAG doc",
    description="DAG description",
    default_args={"retries": 0},
) as child_dag:
    failing_task = BashOperator(task_id="failing_task", bash_command="exit 1;")


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: contributing-docs/testing/system_tests.rst)
test_run = get_test_run(dag)
