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
from unittest.mock import Mock, patch

import pytest
from freezegun import freeze_time

from airflow.exceptions import AirflowException, AirflowRescheduleException, AirflowSensorTimeout
from airflow.models import TaskReschedule
from airflow.models.xcom import XCom
from airflow.operators.empty import EmptyOperator
from airflow.sensors.base import BaseSensorOperator, PokeReturnValue, poke_mode_only
from airflow.ti_deps.deps.ready_to_reschedule import ReadyToRescheduleDep
from airflow.utils import timezone
from airflow.utils.context import Context
from airflow.utils.state import State
from airflow.utils.timezone import datetime
from tests.test_utils import db

DEFAULT_DATE = datetime(2015, 1, 1)
TEST_DAG_ID = "unit_test_dag"
DUMMY_OP = "dummy_op"
SENSOR_OP = "sensor_op"
DEV_NULL = "dev/null"


class DummySensor(BaseSensorOperator):
    def __init__(self, return_value=False, **kwargs):
        super().__init__(**kwargs)
        self.return_value = return_value

    def poke(self, context: Context):
        return self.return_value


class DummySensorWithXcomValue(BaseSensorOperator):
    def __init__(self, return_value=False, xcom_value=None, **kwargs):
        super().__init__(**kwargs)
        self.xcom_value = xcom_value
        self.return_value = return_value

    def poke(self, context: Context):
        return PokeReturnValue(self.return_value, self.xcom_value)


class TestBaseSensor:
    @staticmethod
    def clean_db():
        db.clear_db_runs()
        db.clear_db_task_reschedule()
        db.clear_db_xcom()

    @pytest.fixture(autouse=True)
    def _auto_clean(self, dag_maker):
        """(auto use)"""
        self.clean_db()

        yield

        self.clean_db()

    @pytest.fixture
    def make_sensor(self, dag_maker):
        """Create a DummySensor and associated DagRun"""

        def _make_sensor(return_value, task_id=SENSOR_OP, **kwargs):
            poke_interval = "poke_interval"
            timeout = "timeout"

            if poke_interval not in kwargs:
                kwargs[poke_interval] = 0
            if timeout not in kwargs:
                kwargs[timeout] = 0

            with dag_maker(TEST_DAG_ID):
                if "xcom_value" in kwargs:
                    sensor = DummySensorWithXcomValue(task_id=task_id, return_value=return_value, **kwargs)
                else:
                    sensor = DummySensor(task_id=task_id, return_value=return_value, **kwargs)

                dummy_op = EmptyOperator(task_id=DUMMY_OP)
                sensor >> dummy_op
            return sensor, dag_maker.create_dagrun()

        return _make_sensor

    @classmethod
    def _run(cls, task, **kwargs):
        task.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True, **kwargs)

    def test_ok(self, make_sensor):
        sensor, dr = make_sensor(True)

        self._run(sensor)
        tis = dr.get_task_instances()
        assert len(tis) == 2
        for ti in tis:
            if ti.task_id == SENSOR_OP:
                assert ti.state == State.SUCCESS
            if ti.task_id == DUMMY_OP:
                assert ti.state == State.NONE

    def test_fail(self, make_sensor):
        sensor, dr = make_sensor(False)

        with pytest.raises(AirflowSensorTimeout):
            self._run(sensor)
        tis = dr.get_task_instances()
        assert len(tis) == 2
        for ti in tis:
            if ti.task_id == SENSOR_OP:
                assert ti.state == State.FAILED
            if ti.task_id == DUMMY_OP:
                assert ti.state == State.NONE

    def test_soft_fail(self, make_sensor):
        sensor, dr = make_sensor(False, soft_fail=True)

        self._run(sensor)
        tis = dr.get_task_instances()
        assert len(tis) == 2
        for ti in tis:
            if ti.task_id == SENSOR_OP:
                assert ti.state == State.SKIPPED
            if ti.task_id == DUMMY_OP:
                assert ti.state == State.NONE

    def test_soft_fail_with_retries(self, make_sensor):
        sensor, dr = make_sensor(
            return_value=False, soft_fail=True, retries=1, retry_delay=timedelta(milliseconds=1)
        )

        # first run times out and task instance is skipped
        self._run(sensor)
        tis = dr.get_task_instances()
        assert len(tis) == 2
        for ti in tis:
            if ti.task_id == SENSOR_OP:
                assert ti.state == State.SKIPPED
            if ti.task_id == DUMMY_OP:
                assert ti.state == State.NONE

    def test_ok_with_reschedule(self, make_sensor):
        sensor, dr = make_sensor(return_value=None, poke_interval=10, timeout=25, mode="reschedule")
        sensor.poke = Mock(side_effect=[False, False, True])

        # first poke returns False and task is re-scheduled
        date1 = timezone.utcnow()
        with freeze_time(date1):
            self._run(sensor)
        tis = dr.get_task_instances()
        assert len(tis) == 2
        for ti in tis:
            if ti.task_id == SENSOR_OP:
                # verify task is re-scheduled, i.e. state set to NONE
                assert ti.state == State.UP_FOR_RESCHEDULE
                # verify task start date is the initial one
                assert ti.start_date == date1
                # verify one row in task_reschedule table
                task_reschedules = TaskReschedule.find_for_task_instance(ti)
                assert len(task_reschedules) == 1
                assert task_reschedules[0].start_date == date1
                assert task_reschedules[0].reschedule_date == date1 + timedelta(seconds=sensor.poke_interval)
            if ti.task_id == DUMMY_OP:
                assert ti.state == State.NONE

        # second poke returns False and task is re-scheduled
        date2 = date1 + timedelta(seconds=sensor.poke_interval)
        with freeze_time(date2):
            self._run(sensor)
        tis = dr.get_task_instances()
        assert len(tis) == 2
        for ti in tis:
            if ti.task_id == SENSOR_OP:
                # verify task is re-scheduled, i.e. state set to NONE
                assert ti.state == State.UP_FOR_RESCHEDULE
                # verify task start date is the initial one
                assert ti.start_date == date1
                # verify two rows in task_reschedule table
                task_reschedules = TaskReschedule.find_for_task_instance(ti)
                assert len(task_reschedules) == 2
                assert task_reschedules[1].start_date == date2
                assert task_reschedules[1].reschedule_date == date2 + timedelta(seconds=sensor.poke_interval)
            if ti.task_id == DUMMY_OP:
                assert ti.state == State.NONE

        # third poke returns True and task succeeds
        date3 = date2 + timedelta(seconds=sensor.poke_interval)
        with freeze_time(date3):
            self._run(sensor)
        tis = dr.get_task_instances()
        assert len(tis) == 2
        for ti in tis:
            if ti.task_id == SENSOR_OP:
                assert ti.state == State.SUCCESS
                # verify task start date is the initial one
                assert ti.start_date == date1
            if ti.task_id == DUMMY_OP:
                assert ti.state == State.NONE

    def test_fail_with_reschedule(self, make_sensor):
        sensor, dr = make_sensor(return_value=False, poke_interval=10, timeout=5, mode="reschedule")

        # first poke returns False and task is re-scheduled
        date1 = timezone.utcnow()
        with freeze_time(date1):
            self._run(sensor)
        tis = dr.get_task_instances()
        assert len(tis) == 2
        for ti in tis:
            if ti.task_id == SENSOR_OP:
                assert ti.state == State.UP_FOR_RESCHEDULE
            if ti.task_id == DUMMY_OP:
                assert ti.state == State.NONE

        # second poke returns False, timeout occurs
        date2 = date1 + timedelta(seconds=sensor.poke_interval)
        with freeze_time(date2):
            with pytest.raises(AirflowSensorTimeout):
                self._run(sensor)
        tis = dr.get_task_instances()
        assert len(tis) == 2
        for ti in tis:
            if ti.task_id == SENSOR_OP:
                assert ti.state == State.FAILED
            if ti.task_id == DUMMY_OP:
                assert ti.state == State.NONE

    def test_soft_fail_with_reschedule(self, make_sensor):
        sensor, dr = make_sensor(
            return_value=False, poke_interval=10, timeout=5, soft_fail=True, mode="reschedule"
        )

        # first poke returns False and task is re-scheduled
        date1 = timezone.utcnow()
        with freeze_time(date1):
            self._run(sensor)
        tis = dr.get_task_instances()
        assert len(tis) == 2
        for ti in tis:
            if ti.task_id == SENSOR_OP:
                assert ti.state == State.UP_FOR_RESCHEDULE
            if ti.task_id == DUMMY_OP:
                assert ti.state == State.NONE

        # second poke returns False, timeout occurs
        date2 = date1 + timedelta(seconds=sensor.poke_interval)
        with freeze_time(date2):
            self._run(sensor)
        tis = dr.get_task_instances()
        assert len(tis) == 2
        for ti in tis:
            if ti.task_id == SENSOR_OP:
                assert ti.state == State.SKIPPED
            if ti.task_id == DUMMY_OP:
                assert ti.state == State.NONE

    def test_ok_with_reschedule_and_retry(self, make_sensor):
        sensor, dr = make_sensor(
            return_value=None,
            poke_interval=10,
            timeout=5,
            retries=1,
            retry_delay=timedelta(seconds=10),
            mode="reschedule",
        )
        sensor.poke = Mock(side_effect=[False, False, False, True])

        # first poke returns False and task is re-scheduled
        date1 = timezone.utcnow()
        with freeze_time(date1):
            self._run(sensor)
        tis = dr.get_task_instances()
        assert len(tis) == 2
        for ti in tis:
            if ti.task_id == SENSOR_OP:
                assert ti.state == State.UP_FOR_RESCHEDULE
                # verify one row in task_reschedule table
                task_reschedules = TaskReschedule.find_for_task_instance(ti)
                assert len(task_reschedules) == 1
                assert task_reschedules[0].start_date == date1
                assert task_reschedules[0].reschedule_date == date1 + timedelta(seconds=sensor.poke_interval)
                assert task_reschedules[0].try_number == 1
            if ti.task_id == DUMMY_OP:
                assert ti.state == State.NONE

        # second poke timesout and task instance is failed
        date2 = date1 + timedelta(seconds=sensor.poke_interval)
        with freeze_time(date2):
            with pytest.raises(AirflowSensorTimeout):
                self._run(sensor)
        tis = dr.get_task_instances()
        assert len(tis) == 2
        for ti in tis:
            if ti.task_id == SENSOR_OP:
                assert ti.state == State.FAILED
            if ti.task_id == DUMMY_OP:
                assert ti.state == State.NONE

        # Task is cleared
        sensor.clear()

        # third poke returns False and task is rescheduled again
        date3 = date2 + timedelta(seconds=sensor.poke_interval) + sensor.retry_delay
        with freeze_time(date3):
            self._run(sensor)
        tis = dr.get_task_instances()
        assert len(tis) == 2
        for ti in tis:
            if ti.task_id == SENSOR_OP:
                assert ti.state == State.UP_FOR_RESCHEDULE
                # verify one row in task_reschedule table
                task_reschedules = TaskReschedule.find_for_task_instance(ti)
                assert len(task_reschedules) == 1
                assert task_reschedules[0].start_date == date3
                assert task_reschedules[0].reschedule_date == date3 + timedelta(seconds=sensor.poke_interval)
                assert task_reschedules[0].try_number == 2
            if ti.task_id == DUMMY_OP:
                assert ti.state == State.NONE

        # fourth poke return True and task succeeds
        date4 = date3 + timedelta(seconds=sensor.poke_interval)
        with freeze_time(date4):
            self._run(sensor)
        tis = dr.get_task_instances()
        assert len(tis) == 2
        for ti in tis:
            if ti.task_id == SENSOR_OP:
                assert ti.state == State.SUCCESS
            if ti.task_id == DUMMY_OP:
                assert ti.state == State.NONE

    @pytest.mark.parametrize("mode", ["poke", "reschedule"])
    def test_should_include_ready_to_reschedule_dep(self, mode):
        sensor = DummySensor(task_id="a", return_value=True, mode=mode)
        deps = sensor.deps
        assert ReadyToRescheduleDep() in deps

    def test_invalid_mode(self):
        with pytest.raises(AirflowException):
            DummySensor(task_id="a", mode="foo")

    def test_ok_with_custom_reschedule_exception(self, make_sensor):
        sensor, dr = make_sensor(return_value=None, mode="reschedule")
        date1 = timezone.utcnow()
        date2 = date1 + timedelta(seconds=60)
        date3 = date1 + timedelta(seconds=120)
        sensor.poke = Mock(
            side_effect=[AirflowRescheduleException(date2), AirflowRescheduleException(date3), True]
        )

        # first poke returns False and task is re-scheduled
        with freeze_time(date1):
            self._run(sensor)
        tis = dr.get_task_instances()
        assert len(tis) == 2
        for ti in tis:
            if ti.task_id == SENSOR_OP:
                # verify task is re-scheduled, i.e. state set to NONE
                assert ti.state == State.UP_FOR_RESCHEDULE
                # verify one row in task_reschedule table
                task_reschedules = TaskReschedule.find_for_task_instance(ti)
                assert len(task_reschedules) == 1
                assert task_reschedules[0].start_date == date1
                assert task_reschedules[0].reschedule_date == date2
            if ti.task_id == DUMMY_OP:
                assert ti.state == State.NONE

        # second poke returns False and task is re-scheduled
        with freeze_time(date2):
            self._run(sensor)
        tis = dr.get_task_instances()
        assert len(tis) == 2
        for ti in tis:
            if ti.task_id == SENSOR_OP:
                # verify task is re-scheduled, i.e. state set to NONE
                assert ti.state == State.UP_FOR_RESCHEDULE
                # verify two rows in task_reschedule table
                task_reschedules = TaskReschedule.find_for_task_instance(ti)
                assert len(task_reschedules) == 2
                assert task_reschedules[1].start_date == date2
                assert task_reschedules[1].reschedule_date == date3
            if ti.task_id == DUMMY_OP:
                assert ti.state == State.NONE

        # third poke returns True and task succeeds
        with freeze_time(date3):
            self._run(sensor)
        tis = dr.get_task_instances()
        assert len(tis) == 2
        for ti in tis:
            if ti.task_id == SENSOR_OP:
                assert ti.state == State.SUCCESS
            if ti.task_id == DUMMY_OP:
                assert ti.state == State.NONE

    def test_reschedule_with_test_mode(self, make_sensor):
        sensor, dr = make_sensor(return_value=None, poke_interval=10, timeout=25, mode="reschedule")
        sensor.poke = Mock(side_effect=[False])

        # poke returns False and AirflowRescheduleException is raised
        date1 = timezone.utcnow()
        with freeze_time(date1):
            self._run(sensor, test_mode=True)
        tis = dr.get_task_instances()
        assert len(tis) == 2
        for ti in tis:
            if ti.task_id == SENSOR_OP:
                # in test mode state is not modified
                assert ti.state == State.NONE
                # in test mode no reschedule request is recorded
                task_reschedules = TaskReschedule.find_for_task_instance(ti)
                assert len(task_reschedules) == 0
            if ti.task_id == DUMMY_OP:
                assert ti.state == State.NONE

    def test_sensor_with_invalid_poke_interval(self):
        negative_poke_interval = -10
        non_number_poke_interval = "abcd"
        positive_poke_interval = 10
        with pytest.raises(AirflowException):
            DummySensor(
                task_id="test_sensor_task_1",
                return_value=None,
                poke_interval=negative_poke_interval,
                timeout=25,
            )

        with pytest.raises(AirflowException):
            DummySensor(
                task_id="test_sensor_task_2",
                return_value=None,
                poke_interval=non_number_poke_interval,
                timeout=25,
            )

        DummySensor(
            task_id="test_sensor_task_3", return_value=None, poke_interval=positive_poke_interval, timeout=25
        )

    def test_sensor_with_invalid_timeout(self):
        negative_timeout = -25
        non_number_timeout = "abcd"
        positive_timeout = 25
        with pytest.raises(AirflowException):
            DummySensor(
                task_id="test_sensor_task_1", return_value=None, poke_interval=10, timeout=negative_timeout
            )

        with pytest.raises(AirflowException):
            DummySensor(
                task_id="test_sensor_task_2", return_value=None, poke_interval=10, timeout=non_number_timeout
            )

        DummySensor(
            task_id="test_sensor_task_3", return_value=None, poke_interval=10, timeout=positive_timeout
        )

    def test_sensor_with_exponential_backoff_off(self):
        sensor = DummySensor(
            task_id=SENSOR_OP, return_value=None, poke_interval=5, timeout=60, exponential_backoff=False
        )

        started_at = timezone.utcnow() - timedelta(seconds=10)

        def run_duration():
            return (timezone.utcnow - started_at).total_seconds()

        assert sensor._get_next_poke_interval(started_at, run_duration, 1) == sensor.poke_interval
        assert sensor._get_next_poke_interval(started_at, run_duration, 2) == sensor.poke_interval

    def test_sensor_with_exponential_backoff_on(self):

        sensor = DummySensor(
            task_id=SENSOR_OP, return_value=None, poke_interval=5, timeout=60, exponential_backoff=True
        )

        with patch("airflow.utils.timezone.utcnow") as mock_utctime:
            mock_utctime.return_value = DEFAULT_DATE

            started_at = timezone.utcnow() - timedelta(seconds=10)

            def run_duration():
                return (timezone.utcnow - started_at).total_seconds()

            interval1 = sensor._get_next_poke_interval(started_at, run_duration, 1)
            interval2 = sensor._get_next_poke_interval(started_at, run_duration, 2)

            assert interval1 >= 0
            assert interval1 <= sensor.poke_interval
            assert interval2 >= sensor.poke_interval
            assert interval2 > interval1

    def test_sensor_with_exponential_backoff_on_and_max_wait(self):

        sensor = DummySensor(
            task_id=SENSOR_OP,
            return_value=None,
            poke_interval=10,
            timeout=60,
            exponential_backoff=True,
            max_wait=timedelta(seconds=30),
        )

        with patch("airflow.utils.timezone.utcnow") as mock_utctime:
            mock_utctime.return_value = DEFAULT_DATE

            started_at = timezone.utcnow() - timedelta(seconds=10)

            def run_duration():
                return (timezone.utcnow - started_at).total_seconds()

            for idx, expected in enumerate([2, 6, 13, 30, 30, 30, 30, 30]):
                assert sensor._get_next_poke_interval(started_at, run_duration, idx) == expected

    @pytest.mark.backend("mysql")
    def test_reschedule_poke_interval_too_long_on_mysql(self, make_sensor):
        with pytest.raises(AirflowException) as ctx:
            make_sensor(poke_interval=863998946, mode="reschedule", return_value="irrelevant")
        assert str(ctx.value) == (
            "Cannot set poke_interval to 863998946 seconds in reschedule mode "
            "since it will take reschedule time over MySQL's TIMESTAMP limit."
        )

    @pytest.mark.backend("mysql")
    def test_reschedule_date_too_late_on_mysql(self, make_sensor):
        sensor, _ = make_sensor(poke_interval=60 * 60 * 24, mode="reschedule", return_value=False)

        # A few hours until TIMESTAMP's limit, the next poke will take us over.
        with freeze_time(datetime(2038, 1, 19, tzinfo=timezone.utc)):
            with pytest.raises(AirflowSensorTimeout) as ctx:
                self._run(sensor)
        assert str(ctx.value) == (
            "Cannot reschedule DAG unit_test_dag to 2038-01-20T00:00:00+00:00 "
            "since it is over MySQL's TIMESTAMP storage limit."
        )

    def test_reschedule_and_retry_timeout(self, make_sensor):
        """
        Test mode="reschedule", retries and timeout configurations interact correctly.

        Given a sensor configured like this:

        poke_interval=5
        timeout=10
        retries=2
        retry_delay=timedelta(seconds=3)

        If the second poke raises RuntimeError, all other pokes return False, this is how it should
        behave:

        00:00 Returns False                try_number=1, max_tries=2, state=up_for_reschedule
        00:05 Raises RuntimeError          try_number=2, max_tries=2, state=up_for_retry
        00:08 Returns False                try_number=2, max_tries=2, state=up_for_reschedule
        00:13 Raises AirflowSensorTimeout  try_number=3, max_tries=2, state=failed

        And then the sensor is cleared at 00:19. It should behave like this:

        00:19 Returns False                try_number=3, max_tries=4, state=up_for_reschedule
        00:24 Returns False                try_number=3, max_tries=4, state=up_for_reschedule
        00:26 Returns False                try_number=3, max_tries=4, state=up_for_reschedule
        00:31 Raises AirflowSensorTimeout, try_number=4, max_tries=4, state=failed
        """
        sensor, dr = make_sensor(
            return_value=None,
            poke_interval=5,
            timeout=10,
            retries=2,
            retry_delay=timedelta(seconds=3),
            mode="reschedule",
        )

        sensor.poke = Mock(side_effect=[False, RuntimeError, False, False, False, False, False, False])

        def assert_ti_state(try_number, max_tries, state):
            tis = dr.get_task_instances()

            assert len(tis) == 2

            for ti in tis:
                if ti.task_id == SENSOR_OP:
                    assert ti.try_number == try_number
                    assert ti.max_tries == max_tries
                    assert ti.state == state
                    break
            else:
                self.fail("sensor not found")

        # first poke returns False and task is re-scheduled
        date1 = timezone.utcnow()
        with freeze_time(date1):
            self._run(sensor)
        assert_ti_state(1, 2, State.UP_FOR_RESCHEDULE)

        # second poke raises RuntimeError and task instance retries
        date2 = date1 + timedelta(seconds=sensor.poke_interval)
        with freeze_time(date2), pytest.raises(RuntimeError):
            self._run(sensor)
        assert_ti_state(2, 2, State.UP_FOR_RETRY)

        # third poke returns False and task is rescheduled again
        date3 = date2 + sensor.retry_delay + timedelta(seconds=1)
        with freeze_time(date3):
            self._run(sensor)
        assert_ti_state(2, 2, State.UP_FOR_RESCHEDULE)

        # fourth poke times out and raises AirflowSensorTimeout
        date4 = date3 + timedelta(seconds=sensor.poke_interval)
        with freeze_time(date4), pytest.raises(AirflowSensorTimeout):
            self._run(sensor)
        assert_ti_state(3, 2, State.FAILED)

        # Clear the failed sensor
        sensor.clear()

        date_i = date4 + timedelta(seconds=20)

        for _ in range(3):
            date_i += timedelta(seconds=sensor.poke_interval)
            with freeze_time(date_i):
                self._run(sensor)
            assert_ti_state(3, 4, State.UP_FOR_RESCHEDULE)

        # Last poke times out and raises AirflowSensorTimeout
        date8 = date_i + timedelta(seconds=sensor.poke_interval)
        with freeze_time(date8), pytest.raises(AirflowSensorTimeout):
            self._run(sensor)
        assert_ti_state(4, 4, State.FAILED)

    def test_sensor_with_xcom(self, make_sensor):
        xcom_value = "TestValue"
        sensor, dr = make_sensor(True, xcom_value=xcom_value)

        self._run(sensor)
        tis = dr.get_task_instances()
        assert len(tis) == 2
        for ti in tis:
            if ti.task_id == SENSOR_OP:
                assert ti.state == State.SUCCESS
            if ti.task_id == DUMMY_OP:
                assert ti.state == State.NONE
        actual_xcom_value = XCom.get_one(
            key="return_value", task_id=SENSOR_OP, dag_id=dr.dag_id, run_id=dr.run_id
        )
        assert actual_xcom_value == xcom_value

    def test_sensor_with_xcom_fails(self, make_sensor):
        xcom_value = "TestValue"
        sensor, dr = make_sensor(False, xcom_value=xcom_value)

        with pytest.raises(AirflowSensorTimeout):
            self._run(sensor)
        tis = dr.get_task_instances()
        assert len(tis) == 2
        for ti in tis:
            if ti.task_id == SENSOR_OP:
                assert ti.state == State.FAILED
            if ti.task_id == DUMMY_OP:
                assert ti.state == State.NONE
        actual_xcom_value = XCom.get_one(
            key="return_value", task_id=SENSOR_OP, dag_id=dr.dag_id, run_id=dr.run_id
        )
        assert actual_xcom_value is None


@poke_mode_only
class DummyPokeOnlySensor(BaseSensorOperator):
    def __init__(self, poke_changes_mode=False, **kwargs):
        self.mode = kwargs["mode"]
        super().__init__(**kwargs)
        self.poke_changes_mode = poke_changes_mode
        self.return_value = True

    def poke(self, context: Context):
        if self.poke_changes_mode:
            self.change_mode("reschedule")
        return self.return_value

    def change_mode(self, mode):
        self.mode = mode


class TestPokeModeOnly:
    def test_poke_mode_only_allows_poke_mode(self):
        try:
            sensor = DummyPokeOnlySensor(task_id="foo", mode="poke", poke_changes_mode=False)
        except ValueError:
            self.fail("__init__ failed with mode='poke'.")
        try:
            sensor.poke({})
        except ValueError:
            self.fail("poke failed without changing mode from 'poke'.")
        try:
            sensor.change_mode("poke")
        except ValueError:
            self.fail("class method failed without changing mode from 'poke'.")

    def test_poke_mode_only_bad_class_method(self):
        sensor = DummyPokeOnlySensor(task_id="foo", mode="poke", poke_changes_mode=False)
        with pytest.raises(ValueError):
            sensor.change_mode("reschedule")

    def test_poke_mode_only_bad_init(self):
        with pytest.raises(ValueError):
            DummyPokeOnlySensor(task_id="foo", mode="reschedule", poke_changes_mode=False)

    def test_poke_mode_only_bad_poke(self):
        sensor = DummyPokeOnlySensor(task_id="foo", mode="poke", poke_changes_mode=True)
        with pytest.raises(ValueError):
            sensor.poke({})
