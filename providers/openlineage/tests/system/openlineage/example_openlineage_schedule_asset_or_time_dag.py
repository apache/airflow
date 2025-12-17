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
Simple DAG with asset or time schedule.

It checks:
    - schedule serialization
"""

from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.providers.common.compat.assets import Asset
from airflow.providers.standard.operators.bash import BashOperator
from airflow.timetables.trigger import CronTriggerTimetable

from system.openlineage.expected_events import AIRFLOW_VERSION, get_expected_event_file_path
from system.openlineage.operator import OpenLineageTestOperator

DAG_ID = "openlineage_schedule_asset_or_time_dag"

if AIRFLOW_VERSION.major == 3:
    from airflow.timetables.assets import AssetOrTimeSchedule

    schedule = AssetOrTimeSchedule(
        timetable=CronTriggerTimetable("21 13 29 2 4", timezone="UTC"),
        assets=(
            (Asset(uri="s3://bucket/file.txt", extra={"a": 1}) | Asset(uri="s3://bucket2/file.txt"))  # type: ignore[arg-type]
            & (Asset(uri="s3://bucket3/file.txt") | Asset(uri="s3://bucket4/file.txt", extra={"b": 2}))
        ),
    )
else:
    from airflow.timetables.datasets import DatasetOrTimeSchedule

    schedule = DatasetOrTimeSchedule(  # type: ignore[assignment]  # re-defining schedule, but it's for different AF
        timetable=CronTriggerTimetable("21 13 29 2 4", timezone="UTC"),
        datasets=(
            (Asset(uri="s3://bucket/file.txt", extra={"a": 1}) | Asset(uri="s3://bucket2/file.txt"))
            & (Asset(uri="s3://bucket3/file.txt") | Asset(uri="s3://bucket4/file.txt", extra={"b": 2}))
        ),
    )

with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2025, 5, 1),
    schedule=schedule,
    catchup=False,
    default_args={"retries": 0},
) as dag:
    do_nothing_task = BashOperator(task_id="do_nothing_task", bash_command="sleep 10;")

    check_events = OpenLineageTestOperator(
        task_id="check_events", file_path=get_expected_event_file_path(DAG_ID)
    )

    do_nothing_task >> check_events


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
