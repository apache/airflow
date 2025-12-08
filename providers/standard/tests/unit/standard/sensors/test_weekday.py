#
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

from datetime import timedelta

import pytest

from airflow.models.dag import DAG
from airflow.providers.common.compat.sdk import AirflowSensorTimeout
from airflow.providers.standard.sensors.weekday import DayOfWeekSensor
from airflow.providers.standard.utils.weekday import WeekDay

from tests_common.test_utils import db
from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS, AIRFLOW_V_3_2_PLUS, timezone

if AIRFLOW_V_3_2_PLUS:
    from airflow.dag_processing.dagbag import DagBag
else:
    from airflow.models import DagBag  # type: ignore[attr-defined, no-redef]

pytestmark = pytest.mark.db_test


DEFAULT_DATE = timezone.datetime(2018, 12, 10)
WEEKDAY_DATE = timezone.datetime(2018, 12, 20)
WEEKEND_DATE = timezone.datetime(2018, 12, 22)
TEST_DAG_ID = "weekday_sensor_dag"
DEV_NULL = "/dev/null"
TEST_CASE_WEEKDAY_SENSOR_TRUE = {
    "with-string": "Thursday",
    "with-enum": WeekDay.THURSDAY,
    "with-enum-set": {WeekDay.THURSDAY},
    "with-enum-list": [WeekDay.THURSDAY],
    "with-enum-dict": {WeekDay.THURSDAY: "some_value"},
    "with-enum-set-2-items": {WeekDay.THURSDAY, WeekDay.FRIDAY},
    "with-enum-list-2-items": [WeekDay.THURSDAY, WeekDay.FRIDAY],
    "with-enum-dict-2-items": {WeekDay.THURSDAY: "some_value", WeekDay.FRIDAY: "some_value_2"},
    "with-string-set": {"Thursday"},
    "with-string-set-2-items": {"Thursday", "Friday"},
    "with-set-mix-types": {"Thursday", WeekDay.FRIDAY},
    "with-list-mix-types": ["Thursday", WeekDay.FRIDAY],
    "with-dict-mix-types": {"Thursday": "some_value", WeekDay.FRIDAY: "some_value_2"},
}


class TestDayOfWeekSensor:
    @staticmethod
    def clean_db():
        db.clear_db_runs()

    def setup_method(self):
        self.clean_db()
        self.dagbag = DagBag(dag_folder=DEV_NULL, include_examples=True)
        self.args = {"owner": "airflow", "start_date": DEFAULT_DATE}
        dag = DAG(TEST_DAG_ID, schedule=timedelta(days=1), default_args=self.args)
        self.dag = dag

    def teardwon_method(self):
        self.clean_db()

    @pytest.mark.parametrize(
        "week_day", TEST_CASE_WEEKDAY_SENSOR_TRUE.values(), ids=TEST_CASE_WEEKDAY_SENSOR_TRUE.keys()
    )
    def test_weekday_sensor_true(self, week_day):
        op = DayOfWeekSensor(
            task_id="weekday_sensor_check_true", week_day=week_day, use_task_logical_date=True, dag=self.dag
        )
        op.execute({"logical_date": WEEKDAY_DATE})
        assert op.week_day == week_day

    def test_weekday_sensor_false(self):
        op = DayOfWeekSensor(
            task_id="weekday_sensor_check_false",
            poke_interval=1,
            timeout=2,
            week_day="Tuesday",
            use_task_logical_date=True,
            dag=self.dag,
        )
        with pytest.raises(AirflowSensorTimeout):
            op.execute({"logical_date": WEEKDAY_DATE})

    def test_invalid_weekday_number(self):
        invalid_week_day = "Thsday"
        with pytest.raises(AttributeError, match=f'Invalid Week Day passed: "{invalid_week_day}"'):
            DayOfWeekSensor(
                task_id="weekday_sensor_invalid_weekday_num",
                week_day=invalid_week_day,
                use_task_logical_date=True,
                dag=self.dag,
            )

    def test_weekday_sensor_with_invalid_type(self):
        invalid_week_day = 5
        with pytest.raises(
            TypeError,
            match=f"Unsupported Type for week_day parameter: {type(invalid_week_day)}."
            "Input should be iterable type:"
            "str, set, list, dict or Weekday enum type",
        ):
            DayOfWeekSensor(
                task_id="weekday_sensor_check_true",
                week_day=invalid_week_day,
                use_task_logical_date=True,
                dag=self.dag,
            )

    def test_weekday_sensor_timeout_with_set(self):
        op = DayOfWeekSensor(
            task_id="weekday_sensor_check_false",
            poke_interval=1,
            timeout=2,
            week_day={WeekDay.MONDAY, WeekDay.TUESDAY},
            use_task_logical_date=True,
            dag=self.dag,
        )
        with pytest.raises(AirflowSensorTimeout):
            op.execute({"logical_date": WEEKDAY_DATE})

    @pytest.mark.skipif(not AIRFLOW_V_3_0_PLUS, reason="Skip on Airflow < 3.0")
    def test_weekday_sensor_should_use_run_after_when_logical_date_is_not_provided(self, dag_maker):
        with dag_maker(
            "test_weekday_sensor",
            schedule=None,
        ) as dag:
            op = DayOfWeekSensor(
                task_id="weekday_sensor_check_true",
                week_day={"Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"},
                use_task_logical_date=True,
                dag=dag,
            )
        dr = dag_maker.create_dagrun(
            run_id="manual_run",
            start_date=DEFAULT_DATE,
            logical_date=None,
            **{"run_after": timezone.utcnow()},
        )
        assert op.poke(context={"logical_date": None, "dag_run": dr}) is True
