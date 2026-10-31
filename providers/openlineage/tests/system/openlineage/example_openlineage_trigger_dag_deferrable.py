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
Simple DAG that triggers another simple DAG in deferrable mode.

It checks:
    - task's trigger_dag_id, trigger_run_id, deferrable attribute
    - DAGRun START and COMPLETE events, for the triggered DAG
    - automatic injection of OL parent and root info, including job types, to DAGRun conf
    - multiple levels of triggering: the root stays the first Dag after two hops
    - in a task of the last Dag, the OpenLineage macros, called directly and rendered in templates through
      `tojson`: `lineage_job_type()` is the type of a task, `lineage_root_job_type(ti)` the type of a Dag, and
      `lineage_root_job_name(ti)` and `lineage_root_run_id(ti)` are this Dag and its Dag run's runId, which this
      Dag passes down in the conf to compare
"""

from __future__ import annotations

import json
from datetime import datetime

from airflow import DAG
from airflow.providers.openlineage.plugins.macros import lineage_job_type, lineage_root_job_type
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

from system.openlineage.constants import DEFAULT_DAGRUN_TIMEOUT
from system.openlineage.expected_events import get_expected_event_file_path
from system.openlineage.operator import OpenLineageTestOperator, check_values

DAG_ID = "openlineage_trigger_dag_deferrable"
MACROS = "macros.OpenLineageProviderPlugin"
TASK_JOB_TYPE = {"processingType": "BATCH", "integration": "AIRFLOW", "jobType": "TASK"}
DAG_JOB_TYPE = {"processingType": "BATCH", "integration": "AIRFLOW", "jobType": "DAG"}


def check_macros(
    job_type: str, root_job_type: str, root_job_name: str, root_run_id: str, expected_root_run_id: str, ti
) -> None:
    check_values(
        {
            "lineage_job_type()": (lineage_job_type(), TASK_JOB_TYPE),
            "lineage_root_job_type(ti)": (lineage_root_job_type(ti), DAG_JOB_TYPE),
            "lineage_job_type() | tojson": (json.loads(job_type), TASK_JOB_TYPE),
            "lineage_root_job_type(ti) | tojson": (json.loads(root_job_type), DAG_JOB_TYPE),
            "lineage_root_job_name(ti)": (root_job_name, DAG_ID),
            "lineage_root_run_id(ti)": (root_run_id, expected_root_run_id),
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
        trigger_dag_id="openlineage_trigger_dag_deferrable_child__notrigger",
        wait_for_completion=True,
        # expected_root_run_id is not OpenLineage information, only this Dag run's runId for the last Dag to compare
        conf={
            "some_config": "value1",
            "expected_root_run_id": f"{{{{ {MACROS}.lineage_root_run_id(task_instance) }}}}",
        },
        poke_interval=5,
        deferrable=True,
    )

    check_events = OpenLineageTestOperator(
        task_id="check_events",
        file_path=get_expected_event_file_path(DAG_ID),
        event_count_assertions={"openlineage_trigger_dag_deferrable.trigger_dagrun.event.start": ">=2"},
    )

    trigger_dagrun >> check_events


with DAG(
    dagrun_timeout=DEFAULT_DAGRUN_TIMEOUT,
    dag_id="openlineage_trigger_dag_deferrable_child__notrigger",
    start_date=datetime(2021, 1, 1),
    schedule=None,
    catchup=False,
    default_args={"retries": 0},
) as child_dag:
    trigger_dagrun2 = TriggerDagRunOperator(
        task_id="trigger_dagrun2",
        trigger_dag_id="openlineage_trigger_dag_deferrable_child2__notrigger",
        conf={"expected_root_run_id": "{{ dag_run.conf['expected_root_run_id'] }}"},
        wait_for_completion=True,
        poke_interval=5,
    )


with DAG(
    dagrun_timeout=DEFAULT_DAGRUN_TIMEOUT,
    dag_id="openlineage_trigger_dag_deferrable_child2__notrigger",
    start_date=datetime(2021, 1, 1),
    schedule=None,
    catchup=False,
    default_args={"retries": 0},
) as child_dag2:
    PythonOperator(
        task_id="check_macros",
        python_callable=check_macros,
        op_kwargs={
            "job_type": f"{{{{ {MACROS}.lineage_job_type() | tojson }}}}",
            "root_job_type": f"{{{{ {MACROS}.lineage_root_job_type(task_instance) | tojson }}}}",
            "root_job_name": f"{{{{ {MACROS}.lineage_root_job_name(task_instance) }}}}",
            "root_run_id": f"{{{{ {MACROS}.lineage_root_run_id(task_instance) }}}}",
            "expected_root_run_id": "{{ dag_run.conf['expected_root_run_id'] }}",
        },
    )


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: contributing-docs/testing/system_tests.rst)
test_run = get_test_run(dag)
