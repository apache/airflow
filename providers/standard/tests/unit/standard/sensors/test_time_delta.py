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
from typing import Any

import pendulum
import pytest
import time_machine

from airflow.exceptions import AirflowProviderDeprecationWarning
from airflow.models.dag import DAG
from airflow.providers.common.compat.sdk import TaskDeferred
from airflow.providers.standard.sensors.time_delta import (
    TimeDeltaSensor,
    TimeDeltaSensorAsync,
    WaitSensor,
)
from airflow.providers.standard.triggers.temporal import DateTimeTrigger
from airflow.utils.types import DagRunType

from tests_common.test_utils import db
from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS, AIRFLOW_V_3_2_PLUS, timezone

if AIRFLOW_V_3_2_PLUS:
    from airflow.dag_processing.dagbag import DagBag
else:
    from airflow.models.dagbag import DagBag  # type: ignore[attr-defined, no-redef]

pytestmark = pytest.mark.db_test

DEFAULT_DATE = timezone.datetime(2015, 1, 1)
DEV_NULL = "/dev/null"
TEST_DAG_ID = "unit_tests"

if AIRFLOW_V_3_0_PLUS:
    DEFER_PATH = "airflow.sdk.BaseOperator.defer"
else:
    DEFER_PATH = "airflow.models.baseoperator.BaseOperator.defer"


@pytest.fixture(autouse=True)
def clear_db():
    db.clear_db_dags()
    db.clear_db_runs()


class TestTimedeltaSensor:
    def setup_method(self):
        self.dagbag = DagBag(dag_folder=DEV_NULL, include_examples=False)
        self.dag = DAG(TEST_DAG_ID, schedule=timedelta(days=1), start_date=DEFAULT_DATE)

    def test_timedelta_sensor(self, mocker):
        op = TimeDeltaSensor(task_id="timedelta_sensor_check", delta=timedelta(seconds=2), dag=self.dag)
        op.execute({"dag_run": mocker.MagicMock(run_after=DEFAULT_DATE), "data_interval_end": DEFAULT_DATE})


@pytest.mark.parametrize(
    ("run_after", "interval_end"),
    [
        (timezone.utcnow() + timedelta(days=1), timezone.utcnow() + timedelta(days=2)),
        (timezone.utcnow() + timedelta(days=1), None),
    ],
)
def test_timedelta_sensor_run_after_vs_interval(run_after, interval_end, dag_maker, session):
    """Interval end should be used as base time when present else run_after"""
    if not AIRFLOW_V_3_0_PLUS and not interval_end:
        pytest.skip("not applicable")

    context = {}
    if interval_end:
        context["data_interval_end"] = interval_end
    delta = timedelta(seconds=1)
    with dag_maker():
        op = TimeDeltaSensor(task_id="wait_sensor_check", delta=delta, mode="reschedule")

    kwargs = {}
    if AIRFLOW_V_3_0_PLUS:
        from airflow.utils.types import DagRunTriggeredByType

        kwargs.update(triggered_by=DagRunTriggeredByType.TEST, run_after=run_after)
    dr = dag_maker.create_dagrun(
        run_id="abcrhroceuh",
        run_type=DagRunType.MANUAL,
        state=None,
        session=session,
        **kwargs,
    )
    ti = dr.task_instances[0]
    context.update(dag_run=dr, ti=ti)
    expected = interval_end or run_after
    actual = op._derive_base_time(context)
    assert actual == expected


@pytest.mark.parametrize(
    ("run_after", "interval_end"),
    [
        (timezone.utcnow() + timedelta(days=1), timezone.utcnow() + timedelta(days=2)),
        (timezone.utcnow() + timedelta(days=1), None),
    ],
)
def test_timedelta_sensor_deferrable_run_after_vs_interval(run_after, interval_end, dag_maker):
    """Test that TimeDeltaSensor defers correctly when flag is enabled."""
    if not AIRFLOW_V_3_0_PLUS and not interval_end:
        pytest.skip("not applicable")

    context: dict[str, Any] = {}
    if interval_end:
        context["data_interval_end"] = interval_end

    with dag_maker():
        kwargs = {}
        if AIRFLOW_V_3_0_PLUS:
            from airflow.utils.types import DagRunTriggeredByType

            kwargs.update(triggered_by=DagRunTriggeredByType.TEST, run_after=run_after)

        delta = timedelta(minutes=5)
        sensor = TimeDeltaSensor(
            task_id="timedelta_sensor_deferrable",
            delta=delta,
            deferrable=True,  # <-- the feature under test
        )

    dr = dag_maker.create_dagrun(
        run_id="abcrhroceuh",
        run_type=DagRunType.MANUAL,
        state=None,
        **kwargs,
    )
    context.update(dag_run=dr)

    expected_base = interval_end or run_after
    expected_fire_time = expected_base + delta

    with pytest.raises(TaskDeferred) as td:
        sensor.execute(context)

    # The sensor should defer once with a DateTimeTrigger
    trigger = td.value.trigger
    assert isinstance(trigger, DateTimeTrigger)
    assert trigger.moment == expected_fire_time


class TestTimeDeltaSensorAsync:
    def setup_method(self):
        self.dagbag = DagBag(dag_folder=DEV_NULL, include_examples=True)
        self.args = {"owner": "airflow", "start_date": DEFAULT_DATE}
        self.dag = DAG(TEST_DAG_ID, schedule=timedelta(days=1), default_args=self.args)

    @pytest.mark.parametrize(
        "should_defer",
        [False, True],
    )
    def test_timedelta_sensor(self, mocker, should_defer):
        defer_mock = mocker.patch(DEFER_PATH)
        delta = timedelta(hours=1)
        with pytest.warns(AirflowProviderDeprecationWarning):
            op = TimeDeltaSensorAsync(task_id="timedelta_sensor_check", delta=delta, dag=self.dag)
        if should_defer:
            data_interval_end = pendulum.now("UTC").add(hours=1)
        else:
            data_interval_end = pendulum.now("UTC").replace(microsecond=0, second=0, minute=0).add(hours=-1)
        op.execute({"data_interval_end": data_interval_end})
        if should_defer:
            defer_mock.assert_called_once()
        else:
            defer_mock.assert_not_called()

    @pytest.mark.parametrize(
        "should_defer",
        [False, True],
    )
    def test_wait_sensor(self, mocker, should_defer):
        defer_mock = mocker.patch(DEFER_PATH)
        sleep_mock = mocker.patch("airflow.providers.standard.sensors.time_delta.sleep")
        wait_time = timedelta(seconds=30)
        op = WaitSensor(
            task_id="wait_sensor_check", time_to_wait=wait_time, dag=self.dag, deferrable=should_defer
        )
        with time_machine.travel(pendulum.datetime(year=2024, month=8, day=1, tz="UTC"), tick=False):
            op.execute({})
            if should_defer:
                defer_mock.assert_called_once()
            else:
                defer_mock.assert_not_called()
                sleep_mock.assert_called_once_with(30)

    @pytest.mark.parametrize(
        ("run_after", "interval_end"),
        [
            (timezone.utcnow() + timedelta(days=1), timezone.utcnow() + timedelta(days=2)),
            (timezone.utcnow() + timedelta(days=1), None),
        ],
    )
    def test_timedelta_sensor_async_run_after_vs_interval(self, run_after, interval_end, dag_maker):
        """Interval end should be used as base time when present else run_after"""
        if not AIRFLOW_V_3_0_PLUS and not interval_end:
            pytest.skip("not applicable")

        context = {}
        if interval_end:
            context["data_interval_end"] = interval_end
        with dag_maker() as dag:
            ...
        kwargs = {}
        if AIRFLOW_V_3_0_PLUS:
            from airflow.utils.types import DagRunTriggeredByType

            kwargs.update(triggered_by=DagRunTriggeredByType.TEST, run_after=run_after)

        dr = dag_maker.create_dagrun(
            run_id="abcrhroceuh",
            run_type=DagRunType.MANUAL,
            state=None,
            **kwargs,
        )
        context.update(dag_run=dr)
        delta = timedelta(seconds=1)
        with pytest.warns(AirflowProviderDeprecationWarning):
            op = TimeDeltaSensorAsync(task_id="wait_sensor_check", delta=delta, dag=dag)
        base_time = interval_end or run_after
        expected_time = base_time + delta
        with pytest.raises(TaskDeferred) as caught:
            op.execute(context)

        assert caught.value.trigger.moment == expected_time
