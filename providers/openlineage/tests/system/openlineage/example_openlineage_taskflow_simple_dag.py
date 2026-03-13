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
Simple DAG to verify that DAG defined with taskflow API emits OL events.

It checks:
    - required keys
    - field formats and types
    - number of task events (one start, one complete)
"""

from __future__ import annotations

from datetime import datetime

from system.openlineage.expected_events import get_expected_event_file_path
from system.openlineage.operator import OpenLineageTestOperator

try:
    from airflow.sdk import dag, task
except ImportError:  # Airflow 2
    from airflow.decorators import dag, task  # type: ignore[no-redef, attr-defined]


DAG_ID = "openlineage_taskflow_simple_dag"


@dag(schedule=None, start_date=datetime(2021, 1, 1), catchup=False, default_args={"retries": 0})
def openlineage_taskflow_simple_dag():
    @task
    def do_nothing_task(**context):
        return None

    do_nothing = do_nothing_task()

    check_events = OpenLineageTestOperator(
        task_id="check_events", file_path=get_expected_event_file_path(DAG_ID)
    )

    do_nothing >> check_events


openlineage_taskflow_simple_dag()


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
