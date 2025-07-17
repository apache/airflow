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
from __future__ import annotations

from datetime import datetime

import pytest

from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import ShortCircuitOperator
from airflow.sdk import BaseSensorOperator

DEFAULT_DAG_RUN_ID = "test1"


class SucceedingSensor(BaseSensorOperator):
    def poke(self, context):
        return True


def test_sensor_included_in_ensure_tasks_output(dag_maker):
    from airflow.providers.standard.utils.skipmixin import _ensure_tasks
    from airflow.sdk import BaseOperator as SDKBaseOperator

    with dag_maker(
        dag_id="test_short_circuit_sensor_included",
        start_date=datetime(2024, 1, 1),
    ) as dag:
        short_circuit = ShortCircuitOperator(
            task_id="short_circuit",
            python_callable=lambda: False,
        )
        sensor = SucceedingSensor(task_id="sensor_task", poke_interval=5, timeout=10)
        downstream = EmptyOperator(task_id="regular_task")

        short_circuit >> [sensor, downstream]

    dag_maker.create_dagrun(run_id=DEFAULT_DAG_RUN_ID)

    downstream_nodes = dag.get_task("short_circuit").downstream_list
    task_list = _ensure_tasks(downstream_nodes)

    # Verify both sensor and regular task are included
    task_ids = [t.task_id for t in task_list]
    assert "sensor_task" in task_ids, "Sensor should be included in task list"
    assert "regular_task" in task_ids, "Regular task should be included in task list"
    assert len(task_list) == 2, "Both tasks should be included"

    # Check that sensor is a SDK-based BaseOperator
    sensor_in_list = next((t for t in task_list if t.task_id == "sensor_task"), None)
    assert sensor_in_list is not None, "Sensor task should be found in list"
    assert isinstance(sensor_in_list, SDKBaseOperator), "Sensor should be instance of SDK BaseOperator"


@pytest.mark.parametrize("deferrable", [False, True], ids=["poke_mode", "deferrable"])
def test_short_circuit_with_sensor_included_in_ensure_tasks(dag_maker, deferrable):
    from airflow.providers.standard.utils.skipmixin import _ensure_tasks
    from airflow.sdk import BaseOperator as SDKBaseOperator

    with dag_maker(
        dag_id="dag_short_circuit_with_sensor",
        start_date=datetime(2024, 1, 1),
    ) as dag:
        short_circuit = ShortCircuitOperator(
            task_id="short_circuit",
            python_callable=lambda: False,
        )

        sensor_task = S3KeySensor(
            task_id="sensor_task",
            bucket_name="dummy-bucket",
            bucket_key="dummy-key",
            aws_conn_id="aws_default",
            deferrable=deferrable,
            poke_interval=5,
            timeout=10,
            mode="reschedule" if deferrable else "poke",
            trigger_rule="none_skipped",
        )

        regular_task = EmptyOperator(task_id="regular_task")

        short_circuit >> [sensor_task, regular_task]

    dag_maker.create_dagrun(run_id="test_run")

    downstream_nodes = dag.get_task("short_circuit").downstream_list
    task_list = _ensure_tasks(downstream_nodes)

    # Verify both sensor and regular task are included
    task_ids = [t.task_id for t in task_list]
    assert "sensor_task" in task_ids, "Sensor should be included in task list"
    assert "regular_task" in task_ids, "Regular task should be included in task list"
    assert len(task_list) == 2, "Both tasks should be included"

    # Check that sensor is a SDK-based BaseOperator
    sensor_in_list = next((t for t in task_list if t.task_id == "sensor_task"), None)
    assert sensor_in_list is not None, "Sensor task should be found in list"
    assert isinstance(sensor_in_list, SDKBaseOperator), "Sensor should be instance of SDK BaseOperator"
