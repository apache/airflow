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
from typing import TYPE_CHECKING
from unittest.mock import Mock

import pytest
import time_machine

from airflow.models.trigger import TriggerFailureReason
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import TaskInstanceState, timezone
from airflow.sdk.bases.sensor import BaseSensorOperator, PokeReturnValue, poke_mode_only
from airflow.sdk.definitions.dag import DAG
from airflow.sdk.exceptions import (
    AirflowException,
    AirflowFailException,
    AirflowRescheduleException,
    AirflowSensorTimeout,
    AirflowSkipException,
    AirflowTaskTimeout,
)
from airflow.sdk.execution_time.comms import RescheduleTask, TaskRescheduleStartDate
from airflow.sdk.timezone import datetime

if TYPE_CHECKING:
    from airflow.sdk.definitions.context import Context

DEFAULT_DATE = datetime(2015, 1, 1)
TEST_DAG_ID = "unit_test_dag"
DUMMY_OP = "dummy_op"
SENSOR_OP = "sensor_op"


class DummySensor(BaseSensorOperator):
    def __init__(self, return_value: bool | None = False, **kwargs):
        super().__init__(**kwargs)
        self.return_value = return_value

    def poke(self, context: Context):
        return self.return_value


class DummyAsyncSensor(BaseSensorOperator):
    def __init__(self, return_value=False, **kwargs):
        super().__init__(**kwargs)
        self.return_value = return_value

    def execute_complete(self, context, event=None):
        raise AirflowException("Should be skipped")


class DummySensorWithXcomValue(BaseSensorOperator):
    def __init__(self, return_value=False, xcom_value=None, **kwargs):
        super().__init__(**kwargs)
        self.xcom_value = xcom_value
        self.return_value = return_value

    def poke(self, context: Context):
        return PokeReturnValue(self.return_value, self.xcom_value)


class TestBaseSensor:
    @pytest.fixture
    def make_sensor(self):
        """Create a DummySensor"""

        def _make_sensor(return_value, task_id=SENSOR_OP, **kwargs):
            poke_interval = "poke_interval"
            timeout = "timeout"

            if poke_interval not in kwargs:
                kwargs[poke_interval] = 0
            if timeout not in kwargs:
                kwargs[timeout] = 0

            with DAG(TEST_DAG_ID):
                if "xcom_value" in kwargs:
                    sensor = DummySensorWithXcomValue(task_id=task_id, return_value=return_value, **kwargs)
                else:
                    sensor = DummySensor(task_id=task_id, return_value=return_value, **kwargs)

                dummy_op = EmptyOperator(task_id=DUMMY_OP)
                sensor >> dummy_op
            return sensor

        return _make_sensor

    @classmethod
    def _run(cls, task, context=None):
        if context is None:
            context = {}
        return task.execute(context)

    def test_ok(self, make_sensor):
        sensor = make_sensor(True)
        self._run(sensor)

    def test_fail(self, make_sensor):
        sensor = make_sensor(False)

        with pytest.raises(AirflowSensorTimeout):
            self._run(sensor)

    def test_soft_fail(self, make_sensor):
        sensor = make_sensor(False, soft_fail=True)

        with pytest.raises(AirflowSkipException):
            self._run(sensor)

    @pytest.mark.parametrize(
        "exception_cls",
        (ValueError,),
    )
    def test_soft_fail_with_exception(self, make_sensor, exception_cls):
        sensor = make_sensor(False, soft_fail=True)
        sensor.poke = Mock(side_effect=[exception_cls(None)])
        with pytest.raises(ValueError, match="None"):
            self._run(sensor)

    @pytest.mark.parametrize(
        "exception_cls",
        (
            AirflowSensorTimeout,
            AirflowTaskTimeout,
            AirflowFailException,
        ),
    )
    def test_soft_fail_with_skip_exception(self, make_sensor, exception_cls):
        sensor = make_sensor(False, soft_fail=True)
        sensor.poke = Mock(side_effect=[exception_cls(None)])

        with pytest.raises(AirflowSkipException):
            self._run(sensor)

    @pytest.mark.parametrize(
        "exception_cls",
        (AirflowSensorTimeout, AirflowTaskTimeout, AirflowFailException, Exception),
    )
    def test_never_fail_with_skip_exception(self, make_sensor, exception_cls):
        sensor = make_sensor(False, never_fail=True)
        sensor.poke = Mock(side_effect=[exception_cls(None)])

        with pytest.raises(AirflowSkipException):
            self._run(sensor)

    def test_ok_with_reschedule(self, run_task, make_sensor, time_machine):
        sensor = make_sensor(return_value=None, poke_interval=10, timeout=25, mode="reschedule")
        sensor.poke = Mock(side_effect=[False, False, True])

        # first poke returns False and task is re-scheduled
        date1 = timezone.utcnow()
        time_machine.move_to(date1, tick=False)

        state, msg, _ = run_task(task=sensor)

        assert state == TaskInstanceState.UP_FOR_RESCHEDULE
        assert msg.reschedule_date == date1 + timedelta(seconds=sensor.poke_interval)

        # second poke returns False and task is re-scheduled
        time_machine.coordinates.shift(sensor.poke_interval)
        date2 = date1 + timedelta(seconds=sensor.poke_interval)
        state, msg, _ = run_task(task=sensor)

        assert state == TaskInstanceState.UP_FOR_RESCHEDULE
        assert msg.reschedule_date == date2 + timedelta(seconds=sensor.poke_interval)

        # third poke returns True and task succeeds
        time_machine.coordinates.shift(sensor.poke_interval)
        state, _, _ = run_task(task=sensor)

        assert state == TaskInstanceState.SUCCESS

    def test_fail_with_reschedule(self, run_task, make_sensor, time_machine, mock_supervisor_comms):
        sensor = make_sensor(return_value=False, poke_interval=10, timeout=5, mode="reschedule")

        # first poke returns False and task is re-scheduled
        date1 = timezone.utcnow()
        time_machine.move_to(date1, tick=False)

        state, msg, _ = run_task(task=sensor)

        assert state == TaskInstanceState.UP_FOR_RESCHEDULE
        assert msg.reschedule_date == date1 + timedelta(seconds=sensor.poke_interval)

        # second poke returns False, timeout occurs
        time_machine.coordinates.shift(sensor.poke_interval)

        # Mocking values from DB/API-server
        mock_supervisor_comms.send.return_value = TaskRescheduleStartDate(start_date=date1)
        state, msg, error = run_task(task=sensor, context_update={"task_reschedule_count": 1})

        assert state == TaskInstanceState.FAILED
        assert isinstance(error, AirflowSensorTimeout)

    def test_soft_fail_with_reschedule(self, run_task, make_sensor, time_machine, mock_supervisor_comms):
        sensor = make_sensor(
            return_value=False, poke_interval=10, timeout=5, soft_fail=True, mode="reschedule"
        )

        # first poke returns False and task is re-scheduled
        date1 = timezone.utcnow()
        time_machine.move_to(date1, tick=False)

        state, msg, _ = run_task(task=sensor)
        assert state == TaskInstanceState.UP_FOR_RESCHEDULE

        # second poke returns False, timeout occurs
        time_machine.coordinates.shift(sensor.poke_interval)

        # Mocking values from DB/API-server
        mock_supervisor_comms.send.return_value = TaskRescheduleStartDate(start_date=date1)
        state, msg, _ = run_task(task=sensor, context_update={"task_reschedule_count": 1})
        assert state == TaskInstanceState.SKIPPED

    def test_ok_with_reschedule_and_exponential_backoff(
        self, run_task, make_sensor, time_machine, mock_supervisor_comms
    ):
        sensor = make_sensor(
            return_value=None,
            poke_interval=10,
            timeout=36000,
            mode="reschedule",
            exponential_backoff=True,
        )

        false_count = 10
        sensor.poke = Mock(side_effect=[False] * false_count + [True])

        task_start_date = timezone.utcnow()

        time_machine.move_to(task_start_date, tick=False)
        curr_date = task_start_date

        def run_duration():
            return (timezone.utcnow() - task_start_date).total_seconds()

        new_interval = 0
        mock_supervisor_comms.send.return_value = TaskRescheduleStartDate(start_date=task_start_date)

        # loop poke returns false
        for _poke_count in range(1, false_count + 1):
            curr_date = curr_date + timedelta(seconds=new_interval)
            time_machine.coordinates.shift(new_interval)
            state, msg, _ = run_task(sensor, context_update={"task_reschedule_count": _poke_count})
            assert state == TaskInstanceState.UP_FOR_RESCHEDULE
            old_interval = new_interval
            new_interval = sensor._get_next_poke_interval(task_start_date, run_duration, _poke_count)
            assert old_interval < new_interval  # actual test
            assert msg.reschedule_date == curr_date + timedelta(seconds=new_interval)

        # last poke returns True and task succeeds
        curr_date = curr_date + timedelta(seconds=new_interval)
        time_machine.coordinates.shift(new_interval)

        state, msg, _ = run_task(sensor, context_update={"task_reschedule_count": false_count + 1})
        assert state == TaskInstanceState.SUCCESS

    def test_invalid_mode(self):
        with pytest.raises(ValueError, match="The mode must be one of"):
            DummySensor(task_id="a", mode="foo")

    def test_ok_with_custom_reschedule_exception(self, make_sensor, run_task):
        sensor = make_sensor(return_value=None, mode="reschedule")
        date1 = timezone.utcnow()
        date2 = date1 + timedelta(seconds=60)
        date3 = date1 + timedelta(seconds=120)
        sensor.poke = Mock(
            side_effect=[AirflowRescheduleException(date2), AirflowRescheduleException(date3), True]
        )

        # first poke returns False and task is re-scheduled
        with time_machine.travel(date1, tick=False):
            state, msg, error = run_task(sensor)

        assert state == TaskInstanceState.UP_FOR_RESCHEDULE
        assert isinstance(msg, RescheduleTask)
        assert msg.reschedule_date == date2

        # second poke returns False and task is re-scheduled
        with time_machine.travel(date2, tick=False):
            state, msg, error = run_task(sensor)

        assert state == TaskInstanceState.UP_FOR_RESCHEDULE
        assert isinstance(msg, RescheduleTask)
        assert msg.reschedule_date == date3

        # third poke returns True and task succeeds
        with time_machine.travel(date3, tick=False):
            state, _, _ = run_task(sensor)
        assert state == TaskInstanceState.SUCCESS

    def test_sensor_with_invalid_poke_interval(self):
        negative_poke_interval = -10
        non_number_poke_interval = "abcd"
        positive_poke_interval = 10
        with pytest.raises(
            ValueError, match="Operator arg `poke_interval` must be timedelta object or a non-negative number"
        ):
            DummySensor(
                task_id="test_sensor_task_1",
                return_value=None,
                poke_interval=negative_poke_interval,
                timeout=25,
            )

        with pytest.raises(
            ValueError, match="Operator arg `poke_interval` must be timedelta object or a non-negative number"
        ):
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
        with pytest.raises(
            ValueError, match="Operator arg `timeout` must be timedelta object or a non-negative number"
        ):
            DummySensor(
                task_id="test_sensor_task_1", return_value=None, poke_interval=10, timeout=negative_timeout
            )

        with pytest.raises(
            ValueError, match="Operator arg `timeout` must be timedelta object or a non-negative number"
        ):
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
            return (timezone.utcnow() - started_at).total_seconds()

        assert sensor._get_next_poke_interval(started_at, run_duration, 1) == sensor.poke_interval
        assert sensor._get_next_poke_interval(started_at, run_duration, 2) == sensor.poke_interval

    def test_sensor_with_exponential_backoff_on(self):
        sensor = DummySensor(
            task_id=SENSOR_OP, return_value=None, poke_interval=5, timeout=60, exponential_backoff=True
        )

        with time_machine.travel(DEFAULT_DATE):
            started_at = timezone.utcnow() - timedelta(seconds=10)

            def run_duration():
                return (timezone.utcnow() - started_at).total_seconds()

            interval1 = sensor._get_next_poke_interval(started_at, run_duration, 1)
            interval2 = sensor._get_next_poke_interval(started_at, run_duration, 2)

            assert interval1 >= 0
            assert interval1 <= sensor.poke_interval
            assert interval2 >= sensor.poke_interval
            assert interval2 > interval1

    @pytest.mark.parametrize("poke_interval", [0, 0.1, 0.9, 1, 2, 3])
    def test_sensor_with_exponential_backoff_on_and_small_poke_interval(self, poke_interval):
        """Test that sensor works correctly when poke_interval is small and exponential_backoff is on"""
        sensor = DummySensor(
            task_id=SENSOR_OP,
            return_value=None,
            poke_interval=poke_interval,
            timeout=60,
            exponential_backoff=True,
        )

        with time_machine.travel(DEFAULT_DATE):
            started_at = timezone.utcnow() - timedelta(seconds=10)

            def run_duration():
                return (timezone.utcnow() - started_at).total_seconds()

            intervals = [
                sensor._get_next_poke_interval(started_at, run_duration, retry_number)
                for retry_number in range(1, 10)
            ]

            for interval1, interval2 in zip(intervals, intervals[1:]):
                # intervals should be increasing or equals
                assert interval1 <= interval2
            if poke_interval > 0:
                # check if the intervals are increasing after some retries when poke_interval > 0
                assert intervals[0] < intervals[-1]
            else:
                # check if the intervals are equal after some retries when poke_interval == 0
                assert intervals[0] == intervals[-1]

    def test_sensor_with_exponential_backoff_on_and_max_wait(self):
        sensor = DummySensor(
            task_id=SENSOR_OP,
            return_value=None,
            poke_interval=10,
            timeout=60,
            exponential_backoff=True,
            max_wait=timedelta(seconds=30),
        )

        with time_machine.travel(DEFAULT_DATE):
            started_at = timezone.utcnow() - timedelta(seconds=10)

            def run_duration():
                return (timezone.utcnow() - started_at).total_seconds()

            for idx, expected in enumerate([2, 6, 13, 30, 30, 30, 30, 30]):
                assert sensor._get_next_poke_interval(started_at, run_duration, idx) == expected

    def test_reschedule_and_retry_timeout(self, mock_supervisor_comms, make_sensor, time_machine, run_task):
        """
        Test mode="reschedule", retries and timeout configurations interact correctly.

        Given a sensor configured like this:
        - poke_interval=5
        - timeout=10
        - retries=2
        - retry_delay=timedelta(seconds=3)

        The test verifies two phases:

        Phase 1: Initial execution until failure
        00:00 Returns False                try_number=1, max_tries=2, state=up_for_reschedule
        00:05 Raises RuntimeError          try_number=2, max_tries=2, state=up_for_retry
        00:08 Returns False                try_number=2, max_tries=2, state=up_for_reschedule
        00:13 Raises AirflowSensorTimeout  try_number=3, max_tries=2, state=failed

        Phase 2: After clearing the failed sensor
        00:19 Returns False                try_number=3, max_tries=4, state=up_for_reschedule
        00:24 Returns False                try_number=3, max_tries=4, state=up_for_reschedule
        00:26 Returns False                try_number=3, max_tries=4, state=up_for_reschedule
        00:31 Raises AirflowSensorTimeout  try_number=4, max_tries=4, state=failed
        """
        # Setup sensor with test configuration
        sensor = make_sensor(
            return_value=None,
            poke_interval=5,
            timeout=10,
            retries=2,
            retry_delay=timedelta(seconds=3),
            mode="reschedule",
        )

        # Configure poke behavior for both phases
        sensor.poke = Mock(
            side_effect=[
                # Phase 1
                False,  # Initial poke
                RuntimeError,  # Second poke raises error
                False,  # Third poke after retry
                False,  # Fourth poke times out
                # Phase 2 (after clearing)
                False,  # First poke after clear
                False,  # Second poke after clear
                False,  # Third poke after clear
                False,  # Final poke times out
            ]
        )

        # To store the state across runs
        test_state = {
            "task_reschedule_count": 0,
            "current_time": timezone.datetime(2025, 1, 1),
            "try_number": 1,
            "max_tries": sensor.retries,  # Initial max_tries
            "first_reschedule_date": None,  # Track the first reschedule date
        }

        def _run_task():
            """
            Helper function to run the sensor task with consistent state management.

            This function:
            1. Freezes current time using timemachine
            2. Configures the supervisor comms mock to return the appropriate TR start date
            3. Runs the task with the current state (try_number, max_tries, task_reschedule_count etc)
            4. Updates the state dictionary

            We use this helper to ensure consistent state management across all task runs
            and to avoid duplicating the setup/teardown code for each run.
            """
            time_machine.move_to(test_state["current_time"], tick=False)

            # For timeout calculation, we need to use the first reschedule date
            # This ensures the timeout is calculated from the start of the task
            if test_state["first_reschedule_date"] is None:
                mock_supervisor_comms.send.return_value = TaskRescheduleStartDate(start_date=None)
            else:
                mock_supervisor_comms.send.return_value = TaskRescheduleStartDate(
                    start_date=test_state["first_reschedule_date"]
                )

            state, msg, error = run_task(
                task=sensor,
                try_number=test_state["try_number"],
                max_tries=test_state["max_tries"],
                context_update={"task_reschedule_count": test_state["task_reschedule_count"]},
            )

            if state == TaskInstanceState.UP_FOR_RESCHEDULE:
                test_state["task_reschedule_count"] += 1
                # Only set first_reschedule_date on the first successful reschedule
                if test_state["first_reschedule_date"] is None:
                    test_state["first_reschedule_date"] = test_state["current_time"]
            elif state == TaskInstanceState.UP_FOR_RETRY:
                test_state["try_number"] += 1
            return state, msg, error

        # Phase 1: Initial execution until failure
        # First poke - should reschedule
        state, _, _ = _run_task()
        assert state == TaskInstanceState.UP_FOR_RESCHEDULE

        # Second poke - should raise RuntimeError and retry
        test_state["current_time"] += timedelta(seconds=sensor.poke_interval)
        state, _, error = _run_task()
        assert state == TaskInstanceState.UP_FOR_RETRY
        assert isinstance(error, RuntimeError)

        # Third poke - should reschedule again
        test_state["current_time"] += sensor.retry_delay + timedelta(seconds=1)
        state, _, _ = _run_task()
        assert state == TaskInstanceState.UP_FOR_RESCHEDULE

        # Fourth poke - should timeout
        test_state["current_time"] += timedelta(seconds=sensor.poke_interval)
        state, _, error = _run_task()
        assert isinstance(error, AirflowSensorTimeout)
        assert state == TaskInstanceState.FAILED

        # Phase 2: After clearing the failed sensor
        # Reset supervisor comms to return None, simulating a fresh start after clearing
        test_state["first_reschedule_date"] = None
        test_state["max_tries"] = 4  # Original max_tries (2) + retries (2)
        test_state["current_time"] += timedelta(seconds=20)

        # Test three reschedules after clearing
        for _ in range(3):
            test_state["current_time"] += timedelta(seconds=sensor.poke_interval)
            state, _, _ = _run_task()
            assert state == TaskInstanceState.UP_FOR_RESCHEDULE

        # Final poke - should timeout
        test_state["current_time"] += timedelta(seconds=sensor.poke_interval)
        state, _, error = _run_task()
        assert isinstance(error, AirflowSensorTimeout)
        assert state == TaskInstanceState.FAILED

    def test_sensor_with_xcom(self, make_sensor):
        xcom_value = "TestValue"
        sensor = make_sensor(True, xcom_value=xcom_value)

        assert self._run(sensor) == xcom_value

    def test_sensor_with_xcom_fails(self, make_sensor):
        xcom_value = "TestValue"
        sensor = make_sensor(False, xcom_value=xcom_value)

        with pytest.raises(AirflowSensorTimeout):
            assert self._run(sensor) == xcom_value is None

    def test_resume_execution(self):
        op = BaseSensorOperator(task_id="hi")
        with pytest.raises(AirflowSensorTimeout):
            op.resume_execution(
                next_method="__fail__",
                next_kwargs={"error": TriggerFailureReason.TRIGGER_TIMEOUT},
                context={},
            )

    @pytest.mark.parametrize("mode", ["poke", "reschedule"])
    @pytest.mark.parametrize("retries", [0, 1])
    def test_sensor_timeout(self, mode, retries, run_task):
        """
        Test that AirflowSensorTimeout does not cause sensor to retry.
        """
        from airflow.providers.standard.sensors.python import PythonSensor

        def timeout():
            raise AirflowSensorTimeout

        task = PythonSensor(
            task_id="test_raise_sensor_timeout",
            python_callable=timeout,
            retries=retries,
            mode=mode,
        )

        state, _, error = run_task(task=task, dag_id=f"test_sensor_timeout_{mode}_{retries}")

        assert isinstance(error, AirflowSensorTimeout)
        assert state == TaskInstanceState.FAILED


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
        with pytest.raises(ValueError, match="Cannot set mode to 'reschedule'. Only 'poke' is acceptable"):
            sensor.change_mode("reschedule")

    def test_poke_mode_only_bad_init(self):
        with pytest.raises(ValueError, match="Cannot set mode to 'reschedule'. Only 'poke' is acceptable"):
            DummyPokeOnlySensor(task_id="foo", mode="reschedule", poke_changes_mode=False)

    def test_poke_mode_only_bad_poke(self):
        sensor = DummyPokeOnlySensor(task_id="foo", mode="poke", poke_changes_mode=True)
        with pytest.raises(ValueError, match="Cannot set mode to 'reschedule'. Only 'poke' is acceptable"):
            sensor.poke({})


class TestAsyncSensor:
    @pytest.mark.parametrize(
        ("soft_fail", "expected_exception"),
        [
            (True, AirflowSkipException),
            (False, AirflowException),
        ],
    )
    def test_fail_after_resuming_deferred_sensor(self, soft_fail, expected_exception):
        async_sensor = DummyAsyncSensor(task_id="dummy_async_sensor", soft_fail=soft_fail)
        with pytest.raises(expected_exception):
            async_sensor.resume_execution("execute_complete", None, {})
