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

from decimal import Decimal

import pytest
from pydantic_ai.usage import RunUsage

from airflow.providers.common.ai.utils.usage_budget import (
    USAGE_BUDGET_KEY,
    TaskStateStoreUsageBudget,
    copy_run_usage,
    dump_run_usage,
    load_run_usage,
    subtract_run_usage,
)

from tests_common.test_utils.version_compat import AIRFLOW_V_3_3_PLUS


class TestDumpLoadRunUsage:
    def test_round_trips_all_fields_including_decimal_cost(self):
        usage = RunUsage(
            requests=3,
            tool_calls=2,
            input_tokens=10,
            cache_write_tokens=1,
            cache_read_tokens=2,
            input_audio_tokens=3,
            cache_audio_read_tokens=4,
            output_tokens=5,
            output_audio_tokens=6,
            details={"reasoning": 7},
            cost=Decimal("0.123456789"),
        )

        loaded = load_run_usage(dump_run_usage(usage), key=USAGE_BUDGET_KEY)

        assert loaded == usage
        assert isinstance(loaded.cost, Decimal)

    def test_none_cost_round_trips_as_none(self):
        loaded = load_run_usage(dump_run_usage(RunUsage(cost=None)), key=USAGE_BUDGET_KEY)
        assert loaded.cost is None

    def test_unknown_keys_in_raw_are_ignored(self):
        """A record written by a newer version of this module still loads today."""
        raw = dump_run_usage(RunUsage(requests=1))
        raw["a_future_field"] = "some-value"

        loaded = load_run_usage(raw, key=USAGE_BUDGET_KEY)

        assert loaded == RunUsage(requests=1)

    @pytest.mark.parametrize(
        ("raw", "match"),
        [
            pytest.param("not-a-dict", "not a dict", id="non-dict"),
            pytest.param({"cost": "not-a-number"}, "not a valid number", id="cost-not-numeric"),
            pytest.param({"requests": "3"}, "not an int", id="count-field-not-int"),
            pytest.param({"requests": True}, "not an int", id="count-field-is-bool"),
            pytest.param({"details": "not-a-dict"}, "not a dict", id="details-not-dict"),
        ],
    )
    def test_malformed_shapes_raise_valueerror_naming_the_key(self, raw, match):
        with pytest.raises(ValueError, match=match) as exc_info:
            load_run_usage(raw, key=USAGE_BUDGET_KEY)
        assert USAGE_BUDGET_KEY in str(exc_info.value)


class TestCopyRunUsage:
    def test_copy_is_independent_of_the_original(self):
        """copy.copy shares the `details` dict, which `incr()` mutates in place -- a
        shallow copy would let a later increment of the original leak into the copy."""
        original = RunUsage(requests=1, details={"reasoning": 1})

        copied = copy_run_usage(original)
        original.incr(RunUsage(requests=1, details={"reasoning": 1}))

        assert copied == RunUsage(requests=1, details={"reasoning": 1})
        assert original == RunUsage(requests=2, details={"reasoning": 2})


class TestSubtractRunUsage:
    def test_cost_none_on_both_sides_stays_none(self):
        result = subtract_run_usage(RunUsage(cost=None), RunUsage(cost=None))
        assert result.cost is None

    def test_cost_known_produces_numeric_delta(self):
        result = subtract_run_usage(RunUsage(cost=Decimal("0.30")), RunUsage(cost=Decimal("0.10")))
        assert result.cost == Decimal("0.20")

    def test_cost_unchanged_produces_zero_not_none(self):
        """Unlike RunUsage.__sub__, which returns None for an unchanged cost -- indistinguishable
        from "unknown" -- this must return a numeric 0 so a reported delta is never confused with
        "cost unavailable"."""
        result = subtract_run_usage(RunUsage(cost=Decimal("0.10")), RunUsage(cost=Decimal("0.10")))
        assert result.cost == 0

    def test_fields_subtract_field_by_field(self):
        total = RunUsage(requests=5, tool_calls=3, input_tokens=100, output_tokens=50)
        base = RunUsage(requests=2, tool_calls=1, input_tokens=40, output_tokens=10)

        result = subtract_run_usage(total, base)

        assert result.requests == 3
        assert result.tool_calls == 2
        assert result.input_tokens == 60
        assert result.output_tokens == 40

    def test_details_subtract_per_key_including_keys_on_only_one_side(self):
        total = RunUsage(details={"reasoning": 10, "only_total": 5})
        base = RunUsage(details={"reasoning": 4, "only_base": 2})

        result = subtract_run_usage(total, base)

        assert result.details == {"reasoning": 6, "only_total": 5, "only_base": -2}


class FakeTaskStateStore:
    """In-memory stand-in for ``context['task_state_store']``, with retention recorded."""

    def __init__(self) -> None:
        self.store: dict = {}
        self.retentions: dict = {}

    def get(self, key, default=None):
        return self.store.get(key, default)

    def set(self, key, value, *, retention=None):
        self.store[key] = value
        self.retentions[key] = retention

    def delete(self, key):
        del self.store[key]


class RaisingTaskStateStore:
    """Every method raises -- for the best-effort save()/clear() canaries."""

    def get(self, key, default=None):
        raise RuntimeError("store down")

    def set(self, key, value, *, retention=None):
        raise RuntimeError("store down")

    def delete(self, key):
        raise RuntimeError("store down")


@pytest.mark.skipif(not AIRFLOW_V_3_3_PLUS, reason="task state store needs Airflow >= 3.3")
class TestTaskStateStoreUsageBudgetLoad:
    def test_absent_key_returns_fresh_usage(self):
        budget = TaskStateStoreUsageBudget(FakeTaskStateStore(), max_tries=1)
        assert budget.load() == RunUsage()

    def test_same_max_tries_returns_loaded_usage(self):
        """A retry never changes ``ti.max_tries`` (``handle_failure`` does not touch it),
        so a record written under the same ``max_tries`` is carried forward."""
        store = FakeTaskStateStore()
        TaskStateStoreUsageBudget(store, max_tries=1).save(RunUsage(requests=3, cost=Decimal("0.30")))

        budget = TaskStateStoreUsageBudget(store, max_tries=1)

        assert budget.load() == RunUsage(requests=3, cost=Decimal("0.30"))

    def test_different_max_tries_starts_from_zero(self):
        """``clear_task_instances`` bumps ``ti.max_tries`` on a clear
        (``airflow-core/src/airflow/models/taskinstance.py``); a stored record from
        before that clear must not carry its spend into the new cycle."""
        store = FakeTaskStateStore()
        TaskStateStoreUsageBudget(store, max_tries=1).save(RunUsage(requests=3, cost=Decimal("0.30")))

        budget = TaskStateStoreUsageBudget(store, max_tries=2)

        assert budget.load() == RunUsage()

    def test_raw_missing_usage_envelope_raises_valueerror(self):
        store = FakeTaskStateStore()
        store.store[USAGE_BUDGET_KEY] = {"version": 1, "max_tries": 1}  # no "usage" key

        with pytest.raises(ValueError, match=USAGE_BUDGET_KEY):
            TaskStateStoreUsageBudget(store, max_tries=1).load()

    def test_raw_not_a_dict_raises_valueerror(self):
        store = FakeTaskStateStore()
        store.store[USAGE_BUDGET_KEY] = "not-a-dict"

        with pytest.raises(ValueError, match=USAGE_BUDGET_KEY):
            TaskStateStoreUsageBudget(store, max_tries=1).load()


@pytest.mark.skipif(not AIRFLOW_V_3_3_PLUS, reason="task state store needs Airflow >= 3.3")
class TestTaskStateStoreUsageBudgetSave:
    def test_writes_envelope_with_version_and_max_tries_at_never_expire(self):
        from airflow.sdk.execution_time.context import NEVER_EXPIRE

        store = FakeTaskStateStore()
        TaskStateStoreUsageBudget(store, max_tries=2).save(RunUsage(requests=1, cost=Decimal("0.10")))

        assert store.store[USAGE_BUDGET_KEY] == {
            "version": 1,
            "max_tries": 2,
            "usage": dump_run_usage(RunUsage(requests=1, cost=Decimal("0.10"))),
        }
        assert store.retentions[USAGE_BUDGET_KEY] == NEVER_EXPIRE

    def test_a_failed_write_is_swallowed_not_raised(self):
        """Canary: removing the try/except around ``self._store.set`` in
        ``TaskStateStoreUsageBudget.save`` turns this red with ``RuntimeError``."""
        TaskStateStoreUsageBudget(RaisingTaskStateStore(), max_tries=1).save(RunUsage())


@pytest.mark.skipif(not AIRFLOW_V_3_3_PLUS, reason="task state store needs Airflow >= 3.3")
class TestTaskStateStoreUsageBudgetClear:
    def test_deletes_the_key(self):
        store = FakeTaskStateStore()
        store.store[USAGE_BUDGET_KEY] = {"version": 1, "max_tries": 1, "usage": dump_run_usage(RunUsage())}

        TaskStateStoreUsageBudget(store, max_tries=1).clear()

        assert USAGE_BUDGET_KEY not in store.store

    def test_a_failed_delete_is_swallowed_not_raised(self):
        """Canary: removing the try/except around ``self._store.delete`` in
        ``TaskStateStoreUsageBudget.clear`` turns this red with ``RuntimeError``."""
        TaskStateStoreUsageBudget(RaisingTaskStateStore(), max_tries=1).clear()
