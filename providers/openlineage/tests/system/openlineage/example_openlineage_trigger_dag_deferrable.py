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
    - automatic injection of OL parent and root info to DAGRun conf
    - multiple levels of triggering
"""

from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

from system.openlineage.expected_events import get_expected_event_file_path
from system.openlineage.operator import OpenLineageTestOperator

DAG_ID = "openlineage_trigger_dag_deferrable"

with DAG(
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
        conf={"some_config": "value1"},
        poke_interval=5,
        deferrable=True,
    )

    check_events = OpenLineageTestOperator(
        task_id="check_events",
        file_path=get_expected_event_file_path(DAG_ID),
        allow_duplicate_events_regex="openlineage_trigger_dag_deferrable.trigger_dagrun.event.start",
    )

    trigger_dagrun >> check_events


with DAG(
    dag_id="openlineage_trigger_dag_deferrable_child__notrigger",
    start_date=datetime(2021, 1, 1),
    schedule=None,
    catchup=False,
    default_args={"retries": 0},
) as child_dag:
    trigger_dagrun2 = TriggerDagRunOperator(
        task_id="trigger_dagrun2",
        trigger_dag_id="openlineage_trigger_dag_deferrable_child2__notrigger",
        wait_for_completion=True,
        poke_interval=5,
    )


with DAG(
    dag_id="openlineage_trigger_dag_deferrable_child2__notrigger",
    start_date=datetime(2021, 1, 1),
    schedule=None,
    catchup=False,
    default_args={"retries": 0},
) as child_dag2:
    do_nothing_task = BashOperator(task_id="do_nothing_task", bash_command="sleep 10;")


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
