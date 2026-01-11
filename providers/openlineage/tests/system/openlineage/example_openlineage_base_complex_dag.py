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
Complex DAG without schedule, with multiple operators, task groups, dependencies etc.

It checks:
    - required keys
    - field formats and types
    - number of task events (one start, one complete)
    - if EmptyOperator will emit OL events with callback or outlet
    - if EmptyOperator without modification will not emit OL events
    - if CustomOperator without Extractor will emit OL events
    - task groups serialization without dependencies
    - additional task configuration attrs (owner, max_active_tis_per_dag etc.)
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from airflow import DAG
from airflow.models import Variable
from airflow.providers.common.compat.assets import Asset
from airflow.providers.common.compat.sdk import BaseOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator

try:
    from airflow.sdk import TaskGroup
except ImportError:
    from airflow.utils.task_group import TaskGroup  # type: ignore[no-redef]

from system.openlineage.expected_events import AIRFLOW_VERSION, get_expected_event_file_path
from system.openlineage.operator import OpenLineageTestOperator


def check_events_number_func():
    for event_type in ("start", "complete", "fail"):
        try:
            Variable.get(key=f"openlineage_base_complex_dag.task_0.event.{event_type}", deserialize_json=True)
        except Exception:
            pass
        else:
            raise ValueError("Expected no events for task `task_0`.")


def do_nothing():
    pass


class SomeCustomOperator(BashOperator):
    def __init__(self, **kwargs):
        # Just to test that these attrs are included in OL event
        self.deferrable = True
        self.external_dag_id = "external_dag_id"
        self.external_task_id = "external_task_id"
        super().__init__(**kwargs)


class CustomMappedOperator(BaseOperator):
    def __init__(self, value, **kwargs):
        super().__init__(**kwargs)
        self.value = value

    def execute(self, context):
        return self.value + 1


DAG_ID = "openlineage_base_complex_dag"

with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2021, 1, 1),
    schedule=None,
    catchup=False,
    description="OpenLineage complex DAG description",
    owner_links={"airflow": "https://airflow.apache.org/"},
    tags=["first", "second@", "with'quote", 'z"e'],
    default_args={"retries": 0},
) as dag:
    # task_0 will not emit any events, but the owner will be picked up and added to DAG
    task_0 = EmptyOperator(task_id="task_0", owner='owner"1')
    task_1 = BashOperator(
        task_id="task_1.id.with.dots",
        bash_command="exit 0;",
        owner="owner'2",
        execution_timeout=timedelta(seconds=456),
        doc_rst="RST doc",
    )
    task_2 = PythonOperator(
        task_id="task_2",
        python_callable=do_nothing,
        inlets=[Asset(uri="s3://bucket2/dir2/file2.txt"), Asset(uri="s3://bucket2/dir2/file3.txt")],
        max_retry_delay=42,
        doc="text doc",
        doc_md="should be skipped",
        doc_json="should be skipped",
        doc_yaml="should be skipped",
        doc_rst="should be skipped",
    )
    task_3 = EmptyOperator(
        task_id="task_3",
        outlets=[Asset(uri="s3://bucket/dir/file.txt")],
        doc_md="MD doc",
        doc_json="should be skipped",
        doc_yaml="should be skipped",
        doc_rst="should be skipped",
    )
    task_4 = SomeCustomOperator(
        task_id="task_4",
        bash_command="exit 0;",
        owner="owner3",
        max_active_tis_per_dag=7,
        max_active_tis_per_dagrun=2,
        doc_json="JSON doc",
        doc_yaml="should be skipped",
        doc_rst="should be skipped",
    )

    with TaskGroup("section_1", prefix_group_id=True) as tg:
        task_5 = CustomMappedOperator.partial(task_id="task_5", doc_md="md doc").expand(value=[1])
        with TaskGroup("section_2", parent_group=tg, tooltip="group_tooltip") as tg2:
            add_args: dict[str, Any] = {"sla": timedelta(seconds=123)} if AIRFLOW_VERSION.major == 2 else {}
            task_6 = EmptyOperator(
                task_id="task_6",
                on_success_callback=lambda x: print(1),
                doc_yaml="YAML doc",
                doc_rst="should be skipped",
                **add_args,
            )
            with TaskGroup("section_3", parent_group=tg2):
                task_7 = PythonOperator(task_id="task_7", python_callable=lambda: 1)

    check_events_number = PythonOperator(
        task_id="check_events_number", python_callable=check_events_number_func
    )

    check_events = OpenLineageTestOperator(
        task_id="check_events", file_path=get_expected_event_file_path(DAG_ID)
    )

    task_1 >> [task_2, task_7] >> check_events_number
    task_2 >> task_3 >> [task_4, task_5] >> task_6 >> check_events_number
    check_events_number >> check_events


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
