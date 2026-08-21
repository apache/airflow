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
DAG with edge labels on task dependencies.

It checks:
    - edge labels appear in AirflowJobFacet.tasks[task_id].downstream_task_edges
    - labeled edges are serialized as {target_id: {"label": "..."}}
    - unlabeled edges are serialized as {target_id: {}} (no label key)
    - multiple labeled edges from a single task are all captured
    - a task with mixed labeled and unlabeled outgoing edges captures both correctly
    - a task with no labels at all serializes all its edges as {}
"""

from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.utils.edgemodifier import Label

from system.openlineage.constants import DEFAULT_DAGRUN_TIMEOUT
from system.openlineage.expected_events import get_expected_event_file_path
from system.openlineage.operator import OpenLineageTestOperator

DAG_ID = "openlineage_edge_labels_dag"

with DAG(
    dagrun_timeout=DEFAULT_DAGRUN_TIMEOUT,
    dag_id=DAG_ID,
    start_date=datetime(2021, 1, 1),
    schedule=None,
    catchup=False,
    default_args={"retries": 0},
) as dag:
    # task_1: all outgoing edges are labeled (fan-out with labels)
    task_1 = BashOperator(task_id="task_1", bash_command="exit 0")
    task_2 = BashOperator(task_id="task_2", bash_command="exit 0")
    task_3 = BashOperator(task_id="task_3", bash_command="exit 0")
    # task_2: mixed — one labeled edge to task_4 and one unlabeled edge to task_5
    task_4 = BashOperator(task_id="task_4", bash_command="exit 0")
    task_5 = BashOperator(task_id="task_5", bash_command="exit 0")
    # task_5: no labels at all — both incoming and outgoing edges are unlabeled
    task_6 = BashOperator(task_id="task_6", bash_command="exit 0")

    check_events = OpenLineageTestOperator(
        task_id="check_events", file_path=get_expected_event_file_path(DAG_ID)
    )

    task_1 >> Label("success path") >> task_2
    task_1 >> Label("alternate path") >> task_3
    task_2 >> Label("follow-up") >> task_4
    task_2 >> task_5
    task_5 >> task_6
    [task_3, task_4, task_6] >> check_events


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: contributing-docs/testing/system_tests.rst)
test_run = get_test_run(dag)
