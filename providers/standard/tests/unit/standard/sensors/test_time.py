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

from datetime import datetime, time

import pendulum
import pytest
import time_machine
from pendulum.tz.timezone import FixedTimezone

from airflow.exceptions import AirflowProviderDeprecationWarning
from airflow.models.dag import DAG
from airflow.providers.common.compat.sdk import TaskDeferred
from airflow.providers.standard.sensors.time import TimeSensor, TimeSensorAsync
from airflow.providers.standard.triggers.temporal import DateTimeTrigger, TimeOfDayTrigger
from airflow.triggers.base import StartTriggerArgs

from tests_common.test_utils.compat import timezone

DEFAULT_TIMEZONE = pendulum.timezone("Asia/Singapore")  # UTC+08:00
DEFAULT_DATE_WO_TZ = datetime(2015, 1, 1)
DEFAULT_DATE_WITH_TZ = datetime(2015, 1, 1, tzinfo=DEFAULT_TIMEZONE)


class TestTimeSensor:
    @pytest.mark.parametrize(
        ("tzinfo", "start_date", "target_time", "expected"),
        [
            (timezone.utc, DEFAULT_DATE_WO_TZ, time(10, 0), True),
            (timezone.utc, DEFAULT_DATE_WITH_TZ, time(16, 0), True),
            (timezone.utc, DEFAULT_DATE_WITH_TZ, time(23, 0), False),
            (DEFAULT_TIMEZONE, DEFAULT_DATE_WO_TZ, time(23, 0), False),
        ],
    )
    @time_machine.travel(timezone.datetime(2020, 1, 1, 13, 0).replace(tzinfo=timezone.utc))
    def test_timezone(self, tzinfo, start_date, target_time, expected, monkeypatch):
        monkeypatch.setattr("airflow.settings.TIMEZONE", tzinfo)
        dag = DAG("test_timezone", schedule=None, default_args={"start_date": start_date})
        op = TimeSensor(task_id="test", target_time=target_time, dag=dag)
        assert op.poke(None) == expected

    def test_target_time_aware_dag_timezone(self):
        # This behavior should be the same for both deferrable and non-deferrable
        with DAG("test_target_time_aware", schedule=None, start_date=datetime(2020, 1, 1, 13, 0)):
            aware_time = time(0, 1).replace(tzinfo=DEFAULT_TIMEZONE)
            op = TimeSensor(task_id="test", target_time=aware_time)
            assert op.target_datetime.tzinfo == timezone.utc

    @pytest.mark.parametrize(
        ("current_datetime", "server_timezone"),
        [
            ("2025-01-26 22:00:00", "UTC"),
            ("2025-01-27 07:00:00", "Asia/Seoul"),  # UTC+09:00
        ],
    )
    def test_target_date_aware_dag_timezone(self, current_datetime, server_timezone):
        travel_time = pendulum.parse(current_datetime, tz=pendulum.timezone(server_timezone))
        user_timezone = pendulum.timezone("Asia/Seoul")
        expected_target_datetime = pendulum.datetime(2025, 1, 26, 22, 0, 0, tz="UTC")

        with time_machine.travel(travel_time, tick=False):
            with DAG(
                "test_target_date_aware",
                schedule=None,
                start_date=datetime(2025, 1, 27, 7, tzinfo=user_timezone),
            ):
                aware_datetime = pendulum.datetime(2025, 1, 27, 7).replace(tzinfo=user_timezone)
                op = TimeSensor(task_id="test", target_time=aware_datetime.time())

                # In the old logic, if server's timezone is UTC op.target_datetime could be incorrectly.
                # For example, it might be set to `2025-01-25 22:00:00 UTC`, not `2025-01-26 22:00:00 UTC`.
                # This issue stems from using datetime.today() in an environment where the server timezone differs from the user's timezone
                # The local date '2025-01-26' is combined with the target time `07:00:00`,
                # resulting in `2025-01-26 07:00:00` in local time.
                # When this is converted to UTC by convert_to_utc, it becomes `2025-01-25 22:00:00 UTC`.
                assert op.target_datetime == expected_target_datetime

    def test_target_time_naive_dag_timezone(self):
        # Again, this behavior should be the same for both deferrable and non-deferrable
        with DAG(
            dag_id="test_target_time_naive_dag_timezone",
            schedule=None,
            start_date=datetime(2020, 1, 1, 23, 0, tzinfo=DEFAULT_TIMEZONE),
        ):
            op = TimeSensor(task_id="test", target_time=time(9, 0))

            # Since the DEFAULT_TIMEZONE is UTC+8:00, then hour 9 should be converted to hour 1
            assert op.target_datetime.time() == pendulum.time(1, 0)
            assert op.target_datetime.tzinfo == timezone.utc

    @time_machine.travel("2020-07-07 00:00:00", tick=False)
    def test_task_is_deferred(self):
        with DAG(
            dag_id="test_task_is_deferred",
            schedule=None,
            start_date=datetime(2020, 1, 1, 13, 0),
        ):
            op = TimeSensor(task_id="test", target_time=time(10, 0), deferrable=True)

        assert op.target_datetime.utcoffset() is not None

        with pytest.raises(TaskDeferred) as exc_info:
            op.execute({})

        assert isinstance(exc_info.value.trigger, DateTimeTrigger)
        assert exc_info.value.trigger.moment == pendulum.datetime(2020, 7, 7, 10)
        assert exc_info.value.kwargs is None
        assert exc_info.value.method_name == "execute_complete"

    def test_execute_complete_accepts_event(self):
        """Ensure execute_complete supports the 'event' kwarg when deferrable=True."""
        with DAG(
            dag_id="test_execute_complete_accepts_event",
            schedule=None,
            start_date=datetime(2020, 1, 1),
        ):
            op = TimeSensor(task_id="test", target_time=time(10, 0), deferrable=True)

        try:
            op.execute_complete(context={}, event={"status": "success"})
        except TypeError as e:
            pytest.fail(f"TypeError raised: {e}")

    # --- E1: target already passed today → succeed immediately (not next day) ---
    @time_machine.travel("2020-07-07 15:00:00", tick=False)
    def test_target_already_passed_today_succeeds(self):
        with DAG("e1", schedule=None, start_date=datetime(2020, 1, 1)):
            op = TimeSensor(task_id="t", target_time=time(10, 0))
        assert op.poke({}) is True
        # Still today's date, not tomorrow
        assert op.target_datetime.date() == pendulum.date(2020, 7, 7)

    # --- E2: cache moment for life of attempt (no midnight drift) ---
    def test_target_datetime_cached_across_midnight(self):
        with time_machine.travel("2020-07-07 23:59:00", tick=False):
            with DAG("e2", schedule=None, start_date=datetime(2020, 1, 1)):
                op = TimeSensor(task_id="t", target_time=time(10, 0))
            first = op.target_datetime
        with time_machine.travel("2020-07-08 00:01:00", tick=False):
            second = op.target_datetime
        assert first == second
        assert first.date() == pendulum.date(2020, 7, 7)

    # --- E3: spring-forward gap → shift forward ---
    @time_machine.travel("2024-03-10 12:00:00", tick=False)
    def test_dst_spring_forward_shifts_forward(self):
        ny = pendulum.timezone("America/New_York")
        with DAG("e3", schedule=None, start_date=datetime(2024, 1, 1, tzinfo=ny)):
            op = TimeSensor(task_id="t", target_time=time(2, 30))
        # 02:30 does not exist; pendulum shifts to 03:30 EDT = 07:30 UTC
        assert op.target_datetime == pendulum.datetime(2024, 3, 10, 7, 30, tz="UTC")

    # --- E4: fall-back ambiguous → fold=0 (first occurrence) ---
    @time_machine.travel("2024-11-03 12:00:00", tick=False)
    def test_dst_fall_back_uses_fold_zero(self):
        ny = pendulum.timezone("America/New_York")
        with DAG("e4", schedule=None, start_date=datetime(2024, 1, 1, tzinfo=ny)):
            op = TimeSensor(task_id="t", target_time=time(1, 30))
        # fold=0 → EDT (UTC-4) → 01:30-04:00 = 05:30 UTC
        assert op.target_datetime == pendulum.datetime(2024, 11, 3, 5, 30, tz="UTC")

    # --- E5: no Dag context → UTC fallback, no crash ---
    @time_machine.travel("2020-07-07 08:00:00", tick=False)
    def test_no_dag_context_falls_back_to_utc(self):
        op = TimeSensor(task_id="t", target_time=time(10, 0))
        assert op.target_datetime == pendulum.datetime(2020, 7, 7, 10, 0, tz="UTC")
        assert op.poke({}) is False

    # --- E6: TimeSensorAsync inherits the fix ---
    @time_machine.travel("2020-07-07 00:00:00", tick=False)
    def test_time_sensor_async_inherits_lazy_resolution(self):
        with pytest.warns(AirflowProviderDeprecationWarning, match="TimeSensorAsync is deprecated"):
            with DAG("e6", schedule=None, start_date=datetime(2020, 1, 1)):
                op = TimeSensorAsync(task_id="t", target_time=time(10, 0))
        assert op.deferrable is True
        with pytest.raises(TaskDeferred) as exc_info:
            op.execute({})
        assert isinstance(exc_info.value.trigger, DateTimeTrigger)
        assert exc_info.value.trigger.moment == pendulum.datetime(2020, 7, 7, 10)

    # --- E7: FixedTimezone and named timezone both serialize round-trip ---
    def test_fixed_and_named_timezone_in_start_trigger_args(self):
        fixed = FixedTimezone(19800)  # +05:30
        with DAG("e7_fixed", schedule=None, start_date=datetime(2020, 1, 1, tzinfo=fixed)):
            op_fixed = TimeSensor(
                task_id="fixed",
                target_time=time(10, 30),
                start_from_trigger=True,
                end_from_trigger=True,
            )
        assert op_fixed.start_trigger_args.trigger_kwargs["timezone"] == 19800
        assert op_fixed.start_trigger_args.trigger_kwargs["target_time"] == "10:30:00"
        assert op_fixed.start_trigger_args.trigger_kwargs["end_from_trigger"] is True

        named = pendulum.timezone("Asia/Kolkata")
        with DAG("e7_named", schedule=None, start_date=datetime(2020, 1, 1, tzinfo=named)):
            op_named = TimeSensor(
                task_id="named",
                target_time=time(10, 30),
                start_from_trigger=True,
            )
        assert op_named.start_trigger_args.trigger_kwargs["timezone"] == "Asia/Kolkata"

        # Round-trip via TimeOfDayTrigger
        for op in (op_fixed, op_named):
            kwargs = op.start_trigger_args.trigger_kwargs
            trigger = TimeOfDayTrigger(**kwargs)
            classpath, ser = trigger.serialize()
            assert classpath.endswith("TimeOfDayTrigger")
            restored = TimeOfDayTrigger(**ser)
            assert restored.target_time == kwargs["target_time"]
            assert restored.timezone == kwargs["timezone"]

    # --- E8: start_trigger_args is per-instance, not shared class mutation ---
    def test_start_trigger_args_not_shared_across_instances(self):
        with DAG("e8", schedule=None, start_date=datetime(2020, 1, 1)):
            op_a = TimeSensor(task_id="a", target_time=time(9, 0), start_from_trigger=True)
            op_b = TimeSensor(task_id="b", target_time=time(17, 30), start_from_trigger=True)

        assert op_a.start_trigger_args is not op_b.start_trigger_args
        assert op_a.start_trigger_args is not TimeSensor.start_trigger_args
        assert op_b.start_trigger_args is not TimeSensor.start_trigger_args
        assert op_a.start_trigger_args.trigger_kwargs["target_time"] == "09:00:00"
        assert op_b.start_trigger_args.trigger_kwargs["target_time"] == "17:30:00"
        # Class template unchanged
        assert TimeSensor.start_trigger_args.trigger_kwargs["target_time"] == "00:00:00"

    # --- E9 HEADLINE: serialization stable across datetime.now() values ---
    def test_start_from_trigger_keeps_serialized_dag_hash_stable(self):
        from airflow.serialization.serialized_objects import LazyDeserializedDAG

        def build_hash() -> str:
            with DAG(
                dag_id="test_start_from_trigger_hash_stable",
                schedule=None,
                start_date=datetime(2020, 1, 1),
            ) as dag:
                TimeSensor(
                    task_id="test",
                    target_time=time(10, 0),
                    start_from_trigger=True,
                    end_from_trigger=True,
                )
            return LazyDeserializedDAG.from_dag(dag).hash

        with time_machine.travel("2025-01-01 00:00:00", tick=False):
            first_hash = build_hash()
        with time_machine.travel("2025-06-15 12:34:56", tick=False):
            second_hash = build_hash()

        assert first_hash == second_hash

    def test_start_from_trigger_serialized_dict_identical_across_parses(self):
        """SerializedDAG payload must be byte-identical when now() differs (E9)."""
        from airflow.serialization.serialized_objects import LazyDeserializedDAG

        def build_data():
            with DAG(
                dag_id="test_start_from_trigger_dict_stable",
                schedule=None,
                start_date=datetime(2020, 1, 1),
            ) as dag:
                TimeSensor(
                    task_id="test",
                    target_time=time(10, 0),
                    start_from_trigger=True,
                )
            return LazyDeserializedDAG.from_dag(dag).data

        with time_machine.travel("2025-01-01 00:00:00", tick=False):
            first = build_data()
        with time_machine.travel("2025-12-31 23:59:59", tick=False):
            second = build_data()

        assert first == second

    # --- E10: end_from_trigger propagates into trigger kwargs (both paths) ---
    @time_machine.travel("2020-07-07 00:00:00", tick=False)
    def test_end_from_trigger_propagates(self):
        with DAG("e10", schedule=None, start_date=datetime(2020, 1, 1)):
            op = TimeSensor(
                task_id="t",
                target_time=time(10, 0),
                start_from_trigger=True,
                end_from_trigger=True,
                deferrable=True,
            )
        assert op.start_trigger_args.trigger_kwargs["end_from_trigger"] is True

        with pytest.raises(TaskDeferred) as exc_info:
            op.execute({})
        assert exc_info.value.trigger.end_from_trigger is True

    def test_start_from_trigger_uses_time_of_day_trigger(self):
        with DAG("sft", schedule=None, start_date=datetime(2020, 1, 1)):
            op = TimeSensor(task_id="t", target_time=time(10, 0), start_from_trigger=True)
        assert op.start_from_trigger is True
        assert isinstance(op.start_trigger_args, StartTriggerArgs)
        assert op.start_trigger_args.trigger_cls.endswith("TimeOfDayTrigger")
        assert "moment" not in op.start_trigger_args.trigger_kwargs

    def test_start_from_trigger_still_in_signature(self):
        import inspect

        sig = inspect.signature(TimeSensor.__init__)
        assert "start_from_trigger" in sig.parameters
        assert sig.parameters["start_from_trigger"].default is False
