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

import unittest.mock

import pytest

from airflow.jobs.scheduler_job_runner import SchedulerJobRunner
from airflow.partition_mappers.base import RollupMapper
from airflow.partition_mappers.temporal import StartOfHourMapper
from airflow.partition_mappers.wait_policy import MinimumCount, WaitForAll, WaitPolicy
from airflow.partition_mappers.window import HourWindow
from airflow.serialization.decoders import decode_partition_mapper
from airflow.serialization.encoders import encode_partition_mapper
from airflow.serialization.enums import Encoding
from airflow.serialization.helpers import WaitPolicyNotSupported


def _mapper(**kwargs):
    """Build a RollupMapper with a fixed upstream/window so tests focus on wait_policy."""
    return RollupMapper(upstream_mapper=StartOfHourMapper(), window=HourWindow(), **kwargs)


class TestPolicyConstruction:
    def test_minimum_count_zero_rejected(self):
        with pytest.raises(ValueError, match="MinimumCount\\(0\\) is degenerate"):
            MinimumCount(0)

    def test_minimum_count_positive(self):
        assert MinimumCount(5).n == 5

    def test_minimum_count_negative(self):
        assert MinimumCount(-3).n == -3

    def test_wait_for_all_is_stateless(self):
        a = WaitForAll()
        b = WaitForAll()
        assert type(a) is WaitForAll
        assert type(b) is WaitForAll
        assert a == b
        assert hash(a) == hash(b)

    def test_default_wait_policy_is_wait_for_all_instance(self):
        mapper = _mapper()
        assert isinstance(mapper.wait_policy, WaitForAll)


class TestPolicySemantics:
    @pytest.mark.parametrize(
        ("matched", "expected", "fires"),
        [
            (0, 0, True),
            (5, 5, True),
            (4, 5, False),
        ],
    )
    def test_wait_for_all_is_satisfied(self, matched, expected, fires):
        assert WaitForAll().is_satisfied(matched, expected) is fires

    @pytest.mark.parametrize(
        ("matched", "expected", "fires"),
        [
            (5, 60, True),  # at cap
            (4, 60, False),  # below cap
            (5, 5, True),  # window equal to threshold
            (5, 4, True),  # window smaller than threshold — pure method returns True
        ],
    )
    def test_minimum_count_positive_is_satisfied(self, matched, expected, fires):
        assert MinimumCount(5).is_satisfied(matched, expected) is fires

    @pytest.mark.parametrize(
        ("matched", "expected", "fires"),
        [
            (57, 60, True),  # exactly at threshold (= 60 + -3 = 57)
            (56, 60, False),  # one below threshold
            (0, 0, True),  # clamp: max(0, 0 + -3) = 0, 0 >= 0
        ],
    )
    def test_minimum_count_negative_is_satisfied(self, matched, expected, fires):
        assert MinimumCount(-3).is_satisfied(matched, expected) is fires

    @pytest.mark.parametrize(
        ("policy", "expected", "unreachable"),
        [
            (WaitForAll(), 0, False),
            (WaitForAll(), 60, False),
            (MinimumCount(5), 5, False),  # at cap — still reachable
            (MinimumCount(5), 4, True),  # over cap — unreachable
            (MinimumCount(-3), 0, False),
            (MinimumCount(-3), 60, False),
        ],
    )
    def test_is_unreachable(self, policy, expected, unreachable):
        assert policy.is_unreachable(expected) is unreachable


class TestRepr:
    def test_wait_for_all_repr(self):
        assert repr(WaitForAll()) == "WaitForAll()"

    def test_minimum_count_repr(self):
        assert repr(MinimumCount(5)) == "MinimumCount(n=5)"
        assert repr(MinimumCount(-3)) == "MinimumCount(n=-3)"


class TestSerializeRoundTrip:
    @pytest.mark.parametrize(
        "policy",
        [
            pytest.param(WaitForAll(), id="wait-for-all"),
            pytest.param(MinimumCount(5), id="minimum-count-5"),
            pytest.param(MinimumCount(-3), id="minimum-count-negative-3"),
        ],
    )
    def test_round_trip(self, policy):
        mapper = _mapper(wait_policy=policy)
        restored = decode_partition_mapper(encode_partition_mapper(mapper))
        assert isinstance(restored, RollupMapper)
        assert restored.wait_policy == policy

    def test_default_policy_wire_shape(self):
        encoded = encode_partition_mapper(_mapper())[Encoding.VAR]
        wp = encoded["wait_policy"]
        assert wp[Encoding.TYPE].endswith("WaitForAll")
        assert wp[Encoding.VAR] == {}
        assert "allow_missing" not in encoded
        assert "minimum_count" not in encoded

    def test_non_builtin_wait_policy_rejected(self):
        class Custom(WaitPolicy):
            pass

        mapper = _mapper(wait_policy=Custom())
        with pytest.raises(WaitPolicyNotSupported):
            encode_partition_mapper(mapper)

    def test_round_trip_fast_path_uses_core_wait_for_all(self):
        """After deserialization the wait_policy is a core-side WaitForAll."""
        mapper = _mapper(wait_policy=WaitForAll())
        restored = decode_partition_mapper(encode_partition_mapper(mapper))
        assert isinstance(restored, RollupMapper)
        assert isinstance(restored.wait_policy, WaitForAll)


class TestSchedulerDispatch:
    """Drive _check_rollup_asset_status directly with synthetic counts."""

    @pytest.fixture
    def runner(self):
        # ``__new__`` deliberately bypasses ``__init__``. The instance returned
        # has *no* ``self.log``, ``self.executors``, ``self._partition_unreachable_seen``,
        # or anything else ``SchedulerJobRunner.__init__`` would set. This
        # enforces — structurally, not just by convention — that
        # ``_check_rollup_asset_status`` stays a pure function of ``(mapper,
        # expected_count, matched_count)``. The method runs in the scheduler
        # hot path on every tick for every (apdr, asset) pair, so any
        # ``self.X`` side effect (logging, db access, anything I/O) would
        # degrade throughput silently. Audit logging and error visibility live
        # one level up in ``_resolve_asset_partition_status``, deduped per
        # ``(dag_id, name, uri)`` — that is the correct layer.
        #
        # DO NOT change this to a normal constructor to "fix" a future
        # ``AttributeError``. If you need state inside the dispatcher,
        # reconsider whether the side effect belongs in the dispatcher or in
        # the caller.
        return SchedulerJobRunner.__new__(SchedulerJobRunner)

    @pytest.mark.parametrize(
        ("policy", "expected_count", "matched_count", "fires"),
        [
            # WaitForAll — only when every key arrived
            pytest.param(WaitForAll(), 60, 60, True, id="wait-all-complete"),
            pytest.param(WaitForAll(), 60, 59, False, id="wait-all-one-missing"),
            # Empty window: 0 expected keys, 0 matched. WaitForAll treats
            # ``matched == expected`` as satisfied even when both are zero.
            pytest.param(WaitForAll(), 0, 0, True, id="wait-all-empty-window"),
            # MinimumCount(5) — fire when >=5 keys arrived
            pytest.param(MinimumCount(5), 60, 5, True, id="minimum-count-5-exactly"),
            pytest.param(MinimumCount(5), 60, 4, False, id="minimum-count-5-short"),
            # Empty window with MinimumCount(5): ``0 >= 5`` → does not fire.
            pytest.param(MinimumCount(5), 0, 0, False, id="minimum-count-5-empty-window"),
            # MinimumCount(-3) on window 60: fire when at most 3 missing
            pytest.param(MinimumCount(-3), 60, 57, True, id="minimum-count-neg3-exactly"),
            pytest.param(MinimumCount(-3), 60, 56, False, id="minimum-count-neg3-short"),
            # Empty window with MinimumCount(-3): clamp max(0, 0-3)=0, 0>=0 → fires.
            pytest.param(MinimumCount(-3), 0, 0, True, id="minimum-count-neg3-empty-window"),
        ],
    )
    def test_dispatch(self, runner, policy, expected_count, matched_count, fires):
        result = runner._check_rollup_asset_status(
            mapper=_mapper(wait_policy=policy),
            expected_count=expected_count,
            matched_count=matched_count,
        )
        assert result is fires


class TestUnreachableWarning:
    """
    Drive _resolve_asset_partition_status with a permanently-unreachable policy.

    MinimumCount(61) on HourWindow() is unreachable because the window only
    ever produces 60 upstream keys.  The scheduler must warn once per
    (target_dag_id, name, uri) tuple and return False on every call.
    """

    _PARTITION_KEY = "2024-01-01T00"
    _TARGET_DAG_ID = "my_dag"
    _NAME = "my_asset"
    _URI = "s3://bucket/key"
    _ASSET_ID = 1

    # MinimumCount(61) on HourWindow() (60 keys) — permanently unreachable.
    _UNREACHABLE_MAPPER = RollupMapper(
        upstream_mapper=StartOfHourMapper(),
        window=HourWindow(),
        wait_policy=MinimumCount(61),
    )

    @pytest.fixture
    def runner(self):
        """
        Bare SchedulerJobRunner instance with only the attributes that
        _resolve_asset_partition_status touches: _log, _partition_unreachable_seen.
        ``__new__`` bypasses __init__ so no DB connections or executor setup occurs.
        """
        r = SchedulerJobRunner.__new__(SchedulerJobRunner)
        r._log = unittest.mock.MagicMock()
        r._partition_unreachable_seen = set()
        return r

    def _call(self, runner):
        """
        Invoke _resolve_asset_partition_status with a synthetic APDR and
        timetable.  The timetable always returns the fixed unreachable mapper;
        the APDR carries the fixed partition key and target dag_id.
        """
        apdr = unittest.mock.MagicMock()
        apdr.partition_key = self._PARTITION_KEY
        apdr.target_dag_id = self._TARGET_DAG_ID

        timetable = unittest.mock.MagicMock()
        timetable.get_partition_mapper.return_value = self._UNREACHABLE_MAPPER

        session = unittest.mock.MagicMock()

        return runner._resolve_asset_partition_status(
            session=session,
            asset_id=self._ASSET_ID,
            name=self._NAME,
            uri=self._URI,
            apdr=apdr,
            timetable=timetable,
            actual_by_asset={},
        )

    def test_unreachable_policy_logs_warning_once(self, runner):
        result = self._call(runner)

        assert result is False

        expected_warning_call = unittest.mock.call(
            "Wait policy %r is unreachable for asset (name=%r, uri=%r) on Dag %r "
            "given the window's cardinality %d; downstream Dag run is permanently "
            "unreachable.",
            MinimumCount(61),
            self._NAME,
            self._URI,
            self._TARGET_DAG_ID,
            60,
        )
        assert runner._log.warning.mock_calls == [expected_warning_call]
        assert (self._TARGET_DAG_ID, self._NAME, self._URI) in runner._partition_unreachable_seen

    def test_unreachable_policy_dedups_warning_across_calls(self, runner):
        result1 = self._call(runner)
        result2 = self._call(runner)

        assert result1 is False
        assert result2 is False
        # Warning fired exactly once — second call was suppressed by dedup set.
        assert len(runner._log.warning.mock_calls) == 1
