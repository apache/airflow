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
Tests for :class:`AssetEventSensor`.

Most tests are database-backed (``db_test``): they create real ``AssetEvent`` rows and exercise the
whole fetch path (real ``InletEventsAccessor`` -> DB-backed ``SUPERVISOR_COMMS`` -> real
execution-API SQL), rather than patching ``_fetch_asset_events``. The DB harness lives in the
``conftest.py`` one directory up (``db_supervisor_comms`` / ``asset_event_rows`` fixtures).
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import pytest

from airflow.providers.common.compat.sdk import Asset, AssetAlias, PokeReturnValue, TaskDeferred
from airflow.providers.standard.exceptions import UnexpectedAssetEventTriggerEventError
from airflow.providers.standard.sensors.asset import AssetEventSensor
from airflow.providers.standard.triggers.asset import AssetEventTrigger

from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.version_compat import AIRFLOW_V_3_4_PLUS

pytestmark = pytest.mark.skipif(not AIRFLOW_V_3_4_PLUS, reason="AssetEventSensor requires Airflow 3.4+")

FETCH_PATH = "airflow.providers.standard.sensors.asset._fetch_asset_events"

ASSET_NAME = "my_asset"
ASSET_URI = "s3://bucket/key"
ALIAS_NAME = "my_alias"


def _ts(day: int) -> datetime:
    return datetime(2024, 1, day, tzinfo=timezone.utc)


# top-level (importable) process_result callables, so they also work in deferrable mode
def only_us_partitions(events: list[Any]) -> list[Any]:
    """Keep only events whose partition key targets the ``us`` region."""
    return [event for event in events if (event.partition_key or "").startswith("us|")]


def dedup_by_partition_key(events: list[Any]) -> list[Any]:
    """Keep the first event seen for each distinct partition key (order preserved)."""
    seen: set[str | None] = set()
    out: list[Any] = []
    for event in events:
        if event.partition_key in seen:
            continue
        seen.add(event.partition_key)
        out.append(event)
    return out


class _Processor:
    """A callable holder whose bound method cannot be serialized to an import path."""

    def run(self, events: list[Any]) -> list[Any]:
        return events


def _partition_keys(result: Any) -> list[str | None]:
    """Extract partition keys from a poke result's (serialized) xcom value."""
    assert isinstance(result, PokeReturnValue)
    return [event["partition_key"] for event in (result.xcom_value or [])]


class TestInit:
    """Pure construction/validation logic (no DB access)."""

    def test_requires_a_target(self):
        with pytest.raises(ValueError, match="One of `asset`, `name`, `uri`, or `alias_name`"):
            AssetEventSensor(task_id="s")

    def test_target_from_asset(self):
        sensor = AssetEventSensor(task_id="s", asset=Asset(name="my_asset", uri="s3://bucket/key"))
        assert sensor.name == "my_asset"
        assert sensor.uri == "s3://bucket/key"
        assert sensor.alias_name is None

    def test_target_from_asset_alias(self):
        sensor = AssetEventSensor(task_id="s", asset=AssetAlias(name="my_alias"))
        assert sensor.alias_name == "my_alias"
        assert sensor.name is None
        assert sensor.uri is None

    def test_target_rejects_bad_type(self):
        with pytest.raises(TypeError, match="must be an Asset or AssetAlias"):
            AssetEventSensor(task_id="s", asset=object())  # type: ignore[arg-type]

    def test_rejects_invalid_expected_count(self):
        with pytest.raises(ValueError, match="expected_count"):
            AssetEventSensor(task_id="s", name=ASSET_NAME, expected_count=-2)

    def test_requires_airflow_3_4(self, mocker):
        # The version guard is dead code in CI (the module is skipped below 3.4), so patch the flag.
        mocker.patch("airflow.providers.standard.sensors.asset.AIRFLOW_V_3_4_PLUS", False)
        with pytest.raises(RuntimeError, match="requires Apache Airflow 3.4"):
            AssetEventSensor(task_id="s", name=ASSET_NAME)

    def test_template_fields(self):
        assert set(AssetEventSensor.template_fields) >= {
            "name",
            "uri",
            "alias_name",
            "partition_key",
            "partition_key_regexp_pattern",
            "extra",
            "after",
            "before",
        }

    def test_deferrable_default_from_config(self):
        # ``deferrable``'s default is bound to ``[operators] default_deferrable`` at import time, so
        # the module has to be reloaded under the patched config to observe the new default.
        import importlib

        from airflow.providers.standard.sensors import asset as asset_module

        try:
            with conf_vars({("operators", "default_deferrable"): "True"}):
                importlib.reload(asset_module)
                sensor = asset_module.AssetEventSensor(task_id="s", name=ASSET_NAME)
                assert sensor.deferrable is True
        finally:
            importlib.reload(asset_module)  # restore the default-config version for other tests

    def test_non_deferrable_execute_delegates_to_base(self, mocker):
        base_execute = mocker.patch(
            "airflow.providers.standard.sensors.asset.BaseSensorOperator.execute",
            return_value="delegated",
        )
        sensor = AssetEventSensor(task_id="s", name=ASSET_NAME, deferrable=False)
        assert sensor.execute({"k": "v"}) == "delegated"
        base_execute.assert_called_once()

    def test_execute_complete_success(self):
        sensor = AssetEventSensor(task_id="s", name="my_asset")
        events = [{"id": 1, "partition_key": "a"}]
        assert sensor.execute_complete({}, {"status": "success", "events": events}) == events

    def test_execute_complete_failure(self):
        sensor = AssetEventSensor(task_id="s", name="my_asset")
        with pytest.raises(UnexpectedAssetEventTriggerEventError, match="unexpected trigger event"):
            sensor.execute_complete({}, {"status": "boom"})

    def test_execute_complete_none_event(self):
        sensor = AssetEventSensor(task_id="s", name="my_asset")
        with pytest.raises(UnexpectedAssetEventTriggerEventError, match="unexpected trigger event"):
            sensor.execute_complete({})

    def test_execute_complete_missing_status(self):
        sensor = AssetEventSensor(task_id="s", name="my_asset")
        with pytest.raises(UnexpectedAssetEventTriggerEventError, match="unexpected trigger event"):
            sensor.execute_complete({}, {"events": []})


@pytest.mark.db_test
class TestPoke:
    """poke() against real ``AssetEvent`` rows (no patching of the fetch)."""

    def test_returns_all_events(self, asset_event_rows, db_supervisor_comms):
        sensor = AssetEventSensor(task_id="s", name=ASSET_NAME)  # expected_count=-1 (at least one)
        result = sensor.poke({})
        assert bool(result) is True
        assert _partition_keys(result) == [
            "us|2024-01-01",
            "us|2024-01-02",
            "eu|2024-01-01",
            "us|2024-01-01",
            None,
        ]

    def test_no_match_is_not_done(self, asset_event_rows, db_supervisor_comms):
        sensor = AssetEventSensor(task_id="s", name="does_not_exist")
        result = sensor.poke({})
        assert bool(result) is False
        assert result.xcom_value is None

    def test_exact_partition_key_filter(self, asset_event_rows, db_supervisor_comms):
        sensor = AssetEventSensor(task_id="s", name=ASSET_NAME, partition_key="us|2024-01-01")
        assert _partition_keys(sensor.poke({})) == ["us|2024-01-01", "us|2024-01-01"]

    def test_extra_filter(self, asset_event_rows, db_supervisor_comms):
        sensor = AssetEventSensor(task_id="s", name=ASSET_NAME, extra={"region": "eu"})
        assert _partition_keys(sensor.poke({})) == ["eu|2024-01-01"]

    def test_time_range_limit_and_descending(self, asset_event_rows, db_supervisor_comms):
        sensor = AssetEventSensor(
            task_id="s", name=ASSET_NAME, after=_ts(2), before=_ts(4), ascending=False, limit=2
        )
        # events in [day2, day4] are ids 2,3,4; descending -> 4,3; limited to 2
        assert _partition_keys(sensor.poke({})) == ["us|2024-01-01", "eu|2024-01-01"]

    @pytest.mark.parametrize(("expected_count", "done"), [(5, True), (4, False), (1, False)])
    def test_exact_expected_count(self, asset_event_rows, db_supervisor_comms, expected_count, done):
        sensor = AssetEventSensor(task_id="s", name=ASSET_NAME, expected_count=expected_count)
        assert bool(sensor.poke({})) is done

    def test_by_uri(self, asset_event_rows, db_supervisor_comms):
        sensor = AssetEventSensor(task_id="s", uri=ASSET_URI)
        assert len(sensor.poke({}).xcom_value) == 5

    def test_by_alias(self, asset_event_rows, db_supervisor_comms):
        sensor = AssetEventSensor(task_id="s", asset=AssetAlias(name=ALIAS_NAME))
        assert len(sensor.poke({}).xcom_value) == 5

    def test_expected_count_zero_matches_none(self, asset_event_rows, db_supervisor_comms):
        # expected_count=0 means "exactly zero events"; a filter matching nothing satisfies it.
        sensor = AssetEventSensor(
            task_id="s", name=ASSET_NAME, partition_key="does-not-exist", expected_count=0
        )
        result = sensor.poke({})
        assert bool(result) is True
        assert result.xcom_value == []

    def test_limit_below_exact_expected_count_never_satisfied(self, asset_event_rows, db_supervisor_comms):
        # limit caps the DB result below the exact expected_count, so the condition can never be met.
        sensor = AssetEventSensor(task_id="s", name=ASSET_NAME, limit=1, expected_count=3)
        assert bool(sensor.poke({})) is False

    def test_combined_filters(self, asset_event_rows, db_supervisor_comms):
        # partition_key AND extra AND time-range together, resolved by real SQL -> only id=1.
        sensor = AssetEventSensor(
            task_id="s",
            name=ASSET_NAME,
            partition_key="us|2024-01-01",
            extra={"region": "us"},
            after=_ts(1),
            before=_ts(1),
        )
        assert _partition_keys(sensor.poke({})) == ["us|2024-01-01"]

    def test_string_datetime_bounds(self, asset_event_rows, db_supervisor_comms):
        # after/before accept ISO strings (coerced to datetime by the comms message model).
        sensor = AssetEventSensor(
            task_id="s",
            name=ASSET_NAME,
            after="2024-01-02T00:00:00+00:00",
            before="2024-01-03T00:00:00+00:00",
        )
        assert _partition_keys(sensor.poke({})) == ["us|2024-01-02", "eu|2024-01-01"]


@pytest.mark.db_test
class TestProcessResult:
    """process_result (filter / dedup) applied to real DB events before the count check."""

    def test_filters_out_events(self, asset_event_rows, db_supervisor_comms):
        sensor = AssetEventSensor(
            task_id="s", name=ASSET_NAME, process_result=only_us_partitions, expected_count=3
        )
        result = sensor.poke({})
        assert bool(result) is True
        assert _partition_keys(result) == ["us|2024-01-01", "us|2024-01-02", "us|2024-01-01"]

    def test_dedup(self, asset_event_rows, db_supervisor_comms):
        sensor = AssetEventSensor(
            task_id="s", name=ASSET_NAME, process_result=dedup_by_partition_key, expected_count=4
        )
        result = sensor.poke({})
        assert bool(result) is True
        assert _partition_keys(result) == ["us|2024-01-01", "us|2024-01-02", "eu|2024-01-01", None]

    def test_by_import_path(self, asset_event_rows, db_supervisor_comms):
        sensor = AssetEventSensor(
            task_id="s",
            name=ASSET_NAME,
            process_result=f"{__name__}.only_us_partitions",
            expected_count=3,
        )
        assert bool(sensor.poke({})) is True

    def test_reduces_below_expected(self, asset_event_rows, db_supervisor_comms):
        # only 3 "us" events remain after filtering, so an exact expectation of 5 is not met
        sensor = AssetEventSensor(
            task_id="s", name=ASSET_NAME, process_result=only_us_partitions, expected_count=5
        )
        assert bool(sensor.poke({})) is False

    def test_returns_empty_list_not_done(self, asset_event_rows, db_supervisor_comms):
        sensor = AssetEventSensor(task_id="s", name=ASSET_NAME, process_result=lambda events: [])
        result = sensor.poke({})  # expected_count=-1 -> "at least one" -> not satisfied by []
        assert bool(result) is False
        assert result.xcom_value is None

    def test_returns_empty_list_with_expected_zero(self, asset_event_rows, db_supervisor_comms):
        sensor = AssetEventSensor(
            task_id="s", name=ASSET_NAME, process_result=lambda events: [], expected_count=0
        )
        result = sensor.poke({})
        assert bool(result) is True
        assert result.xcom_value == []

    def test_returns_more_events_than_fetched(self, asset_event_rows, db_supervisor_comms):
        sensor = AssetEventSensor(
            task_id="s", name=ASSET_NAME, process_result=lambda events: events + events, expected_count=10
        )
        result = sensor.poke({})
        assert bool(result) is True
        assert len(result.xcom_value) == 10

    def test_exception_propagates(self, asset_event_rows, db_supervisor_comms):
        def boom(events):
            raise RuntimeError("process_result exploded")

        sensor = AssetEventSensor(task_id="s", name=ASSET_NAME, process_result=boom)
        with pytest.raises(RuntimeError, match="process_result exploded"):
            sensor.poke({})


@pytest.mark.db_test
class TestDeferrable:
    def test_short_circuits_when_already_satisfied(self, asset_event_rows, db_supervisor_comms):
        # Initial poke already satisfies expected_count, so execute() returns without deferring.
        sensor = AssetEventSensor(
            task_id="s",
            name=ASSET_NAME,
            process_result=only_us_partitions,
            expected_count=3,
            deferrable=True,
        )
        events = sensor.execute({})
        assert [event["partition_key"] for event in events] == [
            "us|2024-01-01",
            "us|2024-01-02",
            "us|2024-01-01",
        ]

    def test_defers_and_forwards_process_result_path(self, asset_event_rows, db_supervisor_comms):
        # Ask for more events than exist so the initial poke fails and the sensor defers.
        sensor = AssetEventSensor(
            task_id="s",
            name=ASSET_NAME,
            partition_key="us|2024-01-01",
            process_result=only_us_partitions,
            expected_count=99,
            deferrable=True,
        )
        with pytest.raises(TaskDeferred) as exc:
            sensor.execute({})
        trigger = exc.value.trigger
        assert isinstance(trigger, AssetEventTrigger)
        assert trigger.name == ASSET_NAME
        assert trigger.partition_key == "us|2024-01-01"
        assert trigger.expected_count == 99
        assert trigger.process_result_path.endswith("test_asset.only_us_partitions")

    def test_rejects_non_importable_callable(self, asset_event_rows, db_supervisor_comms):
        # A lambda cannot be serialized to an import path for the trigger, so deferring must fail.
        sensor = AssetEventSensor(
            task_id="s",
            name=ASSET_NAME,
            expected_count=99,  # force a defer past the initial poke
            process_result=lambda events: events,
            deferrable=True,
        )
        with pytest.raises(ValueError, match="top-level importable function"):
            sensor.execute({})

    def test_rejects_bound_method_callable(self, asset_event_rows, db_supervisor_comms):
        # A bound method resolves to ``module.Class.method`` which import_string cannot import;
        # this must fail at defer time (not later in the triggerer).
        sensor = AssetEventSensor(
            task_id="s",
            name=ASSET_NAME,
            expected_count=99,
            process_result=_Processor().run,
            deferrable=True,
        )
        with pytest.raises(ValueError, match="not importable"):
            sensor.execute({})

    def test_forwards_poke_interval_to_trigger(self, asset_event_rows, db_supervisor_comms):
        sensor = AssetEventSensor(
            task_id="s", name=ASSET_NAME, expected_count=99, deferrable=True, poke_interval=42
        )
        with pytest.raises(TaskDeferred) as exc:
            sensor.execute({})
        assert exc.value.trigger.poke_interval == 42


class TestDeferrableFilterForwarding:
    """The regexp filter cannot run on SQLite, so verify filter -> trigger forwarding with a mock.

    This is the one place we still patch the fetch: it lets us assert that *all* filters (including
    ``partition_key_regexp_pattern``) are forwarded to the trigger without executing a regexp query
    that the local SQLite backend does not support.
    """

    def test_forwards_all_filters_to_trigger(self, mocker):
        mocker.patch(FETCH_PATH, return_value=[])
        sensor = AssetEventSensor(
            task_id="s",
            name=ASSET_NAME,
            uri=ASSET_URI,
            after=_ts(1),
            before=_ts(9),
            ascending=False,
            limit=7,
            partition_key="us|2024-01-01",
            partition_key_regexp_pattern="us.*",
            extra={"region": "us"},
            expected_count=3,
            deferrable=True,
        )
        with pytest.raises(TaskDeferred) as exc:
            sensor.execute({})
        trigger = exc.value.trigger
        assert isinstance(trigger, AssetEventTrigger)
        assert (trigger.name, trigger.uri) == (ASSET_NAME, ASSET_URI)
        assert (trigger.after, trigger.before) == (_ts(1), _ts(9))
        assert trigger.ascending is False
        assert trigger.limit == 7
        assert trigger.partition_key == "us|2024-01-01"
        assert trigger.partition_key_regexp_pattern == "us.*"
        assert trigger.extra == {"region": "us"}
        assert trigger.expected_count == 3
