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
DAG with a sensor in reschedule mode.

It checks:
    - the sensor emits one START (the START of later pokes is skipped) and one COMPLETE
    - taskInstance's `try_number` stays at 1 for both, as rescheduling does not start a new try
    - START and COMPLETE share the `runId` (the `runId` check of OpenLineageTestOperator is enabled by default)
    - every task event's identity, as `system.openlineage.expected_events` describes it, and each task in the Dag's job facet
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.sensors.time_delta import TimeDeltaSensor

from system.openlineage.constants import DEFAULT_DAGRUN_TIMEOUT
from system.openlineage.expected_events import get_expected_event_file_path
from system.openlineage.operator import OpenLineageTestOperator

DAG_ID = "openlineage_sensor_reschedule_dag"

with DAG(
    dagrun_timeout=DEFAULT_DAGRUN_TIMEOUT,
    dag_id=DAG_ID,
    start_date=datetime(2021, 1, 1),
    schedule=None,
    catchup=False,
    default_args={"retries": 0},
) as dag:
    # Timedelta is compared to the DAGRun start timestamp, which can occur long before a worker picks up the
    # task. We need to ensure the sensor is rescheduled at least once, so setting 180s.
    wait = TimeDeltaSensor(
        task_id="wait",
        delta=timedelta(seconds=int(os.getenv("SYSTEM_TESTS_OL_TIMEDELTA", "180"))),
        poke_interval=10,
        mode="reschedule",
    )

    check_events = OpenLineageTestOperator(
        task_id="check_events", file_path=get_expected_event_file_path(DAG_ID)
    )

    wait >> check_events


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: contributing-docs/testing/system_tests.rst)
test_run = get_test_run(dag)
