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
Simple DAG that triggers another simple DAG.

It checks:
    - task's trigger_dag_id
    - DAGRun START and COMPLETE events, for the triggered DAG
    - propagation of OL parent and root info, including job types, from DAGRun conf
    - for a Dag triggered with parent and root information whose job types miss a field (a job type needs
      `processingType`, `integration` and `jobType`), the parent and root of its run and task come from the conf
      without a `jobType` facet, and in its task `lineage_root_job_type(ti)` is None, called directly and rendered
      through `tojson`, while `lineage_job_type()` is still the type of a task
"""

from __future__ import annotations

import json
from datetime import datetime

from airflow import DAG
from airflow.providers.openlineage.plugins.macros import lineage_job_type, lineage_root_job_type
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

from system.openlineage.constants import DEFAULT_DAGRUN_TIMEOUT
from system.openlineage.expected_events import get_expected_event_file_path
from system.openlineage.operator import OpenLineageTestOperator, check_values

DAG_ID = "openlineage_trigger_dag"
PARTIAL_CHILD_DAG_ID = "openlineage_trigger_dag_partial_child__notrigger"


def check_partial_macros(root_job_type: str, ti) -> None:
    check_values(
        {
            "lineage_job_type()": (
                lineage_job_type(),
                {"processingType": "BATCH", "integration": "AIRFLOW", "jobType": "TASK"},
            ),
            "lineage_root_job_type(ti)": (lineage_root_job_type(ti), None),
            "lineage_root_job_type(ti) | tojson": (json.loads(root_job_type), None),
        }
    )


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
        trigger_dag_id="openlineage_trigger_dag_child__notrigger",
        trigger_run_id=f"openlineage_trigger_dag_triggering_child_{datetime.now().isoformat()}",
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
                "parentJobType": {"processingType": "BATCH", "integration": "SPARK", "jobType": "JOB"},
                "rootParentJobType": {
                    "processingType": "STREAMING",
                    "integration": "CUSTOM",
                    "jobType": "PIPELINE",
                },
            },
        },
        poke_interval=5,
    )

    trigger_partial = TriggerDagRunOperator(
        task_id="trigger_partial",
        trigger_dag_id=PARTIAL_CHILD_DAG_ID,
        conf={
            "openlineage": {
                "parentRunId": "3bb703d1-09c1-4a42-8da5-35a0b3216072",
                "parentJobNamespace": "prod_biz",
                "parentJobName": "get_files",
                "rootParentRunId": "9d3b14f7-de91-40b6-aeef-e887e2c7673e",
                "rootParentJobNamespace": "prod_analytics",
                "rootParentJobName": "generate_report_sales_e2e",
                "parentJobType": {"processingType": "BATCH", "integration": "SPARK"},
                "rootParentJobType": {"processingType": "STREAMING", "jobType": "PIPELINE"},
            },
        },
        wait_for_completion=True,
        # Waits in the triggerer, without holding a worker slot
        deferrable=True,
        poke_interval=2,
    )

    check_events = OpenLineageTestOperator(
        task_id="check_events",
        file_path=get_expected_event_file_path(DAG_ID),
        # START when the task starts and again when it resumes after waiting for the Dag run it triggered
        event_count_assertions={rf"{DAG_ID}\.trigger_partial\.event\.start": ">=2"},
    )

    [trigger_dagrun, trigger_partial] >> check_events


with DAG(
    dagrun_timeout=DEFAULT_DAGRUN_TIMEOUT,
    dag_id="openlineage_trigger_dag_child__notrigger",
    start_date=datetime(2021, 1, 1),
    schedule=None,
    catchup=False,
    tags=["first", "second@", "with'quote", 'z"e'],
    doc_md="MD DAG doc",
    description="DAG description",
    default_args={"retries": 0},
) as child_dag:
    do_nothing_task = BashOperator(task_id="do_nothing_task", bash_command="sleep 10;")


with DAG(
    dagrun_timeout=DEFAULT_DAGRUN_TIMEOUT,
    dag_id=PARTIAL_CHILD_DAG_ID,
    start_date=datetime(2021, 1, 1),
    schedule=None,
    catchup=False,
    default_args={"retries": 0},
) as partial_child_dag:
    PythonOperator(
        task_id="check_macros",
        python_callable=check_partial_macros,
        op_kwargs={
            "root_job_type": "{{ macros.OpenLineageProviderPlugin.lineage_root_job_type(task_instance) | tojson }}"
        },
    )


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: contributing-docs/testing/system_tests.rst)
test_run = get_test_run(dag)
