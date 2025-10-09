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

import datetime

import pendulum

from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.sensors.bash import BashSensor
from airflow.providers.standard.sensors.filesystem import FileSensor
from airflow.providers.standard.sensors.python import PythonSensor
from airflow.providers.standard.sensors.time import TimeSensor
from airflow.providers.standard.sensors.time_delta import TimeDeltaSensor
from airflow.providers.standard.sensors.weekday import DayOfWeekSensor
from airflow.providers.standard.utils.weekday import WeekDay
from airflow.sdk import DAG

try:
    from airflow.sdk import TriggerRule
except ImportError:
    # Compatibility for Airflow < 3.1
    from airflow.utils.trigger_rule import TriggerRule  # type: ignore[no-redef,attr-defined]


# [START example_callables]
def success_callable():
    return True


def failure_callable():
    return False


# [END example_callables]


with DAG(
    dag_id="example_sensors",
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["example"],
) as dag:
    # [START example_time_delta_sensor]
    t0 = TimeDeltaSensor(task_id="wait_some_seconds", delta=datetime.timedelta(seconds=2))
    # [END example_time_delta_sensor]

    # [START example_time_delta_sensor_async]
    t0a = TimeDeltaSensor(
        task_id="wait_some_seconds_async", delta=datetime.timedelta(seconds=2), deferrable=True
    )
    # [END example_time_delta_sensor_async]

    # [START example_time_sensors]
    t1 = TimeSensor(
        task_id="fire_immediately", target_time=datetime.datetime.now(tz=datetime.timezone.utc).time()
    )

    t2 = TimeSensor(
        task_id="timeout_after_second_date_in_the_future",
        timeout=1,
        soft_fail=True,
        target_time=(datetime.datetime.now(tz=datetime.timezone.utc) + datetime.timedelta(hours=1)).time(),
    )

    t1a = TimeSensor(
        task_id="fire_immediately_async",
        target_time=datetime.datetime.now(tz=datetime.timezone.utc).time(),
        deferrable=True,
    )

    t2a = TimeSensor(
        task_id="timeout_after_second_date_in_the_future_async",
        timeout=1,
        soft_fail=True,
        target_time=(datetime.datetime.now(tz=datetime.timezone.utc) + datetime.timedelta(hours=1)).time(),
        deferrable=True,
    )
    # [END example_time_sensors]

    # [START example_bash_sensors]
    t3 = BashSensor(task_id="Sensor_succeeds", bash_command="exit 0")

    t4 = BashSensor(task_id="Sensor_fails_after_3_seconds", timeout=3, soft_fail=True, bash_command="exit 1")
    # [END example_bash_sensors]

    t5 = BashOperator(task_id="remove_file", bash_command="rm -rf /tmp/temporary_file_for_testing")

    # [START example_file_sensor]
    t6 = FileSensor(task_id="wait_for_file", filepath="/tmp/temporary_file_for_testing")
    # [END example_file_sensor]

    # [START example_file_sensor_async]
    t7 = FileSensor(
        task_id="wait_for_file_async", filepath="/tmp/temporary_file_for_testing", deferrable=True
    )
    # [END example_file_sensor_async]

    t8 = BashOperator(
        task_id="create_file_after_3_seconds", bash_command="sleep 3; touch /tmp/temporary_file_for_testing"
    )

    # [START example_python_sensors]
    t9 = PythonSensor(task_id="success_sensor_python", python_callable=success_callable)

    t10 = PythonSensor(
        task_id="failure_timeout_sensor_python", timeout=3, soft_fail=True, python_callable=failure_callable
    )
    # [END example_python_sensors]

    # [START example_day_of_week_sensor]
    t11 = DayOfWeekSensor(
        task_id="week_day_sensor_failing_on_timeout", timeout=3, soft_fail=True, week_day=WeekDay.MONDAY
    )
    # [END example_day_of_week_sensor]

    tx = BashOperator(task_id="print_date_in_bash", bash_command="date")

    tx.trigger_rule = TriggerRule.NONE_FAILED
    [t0, t0a, t1, t1a, t2, t2a, t3, t4] >> tx
    t5 >> t6 >> t7 >> tx
    t8 >> tx
    [t9, t10] >> tx
    t11 >> tx
