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
from airflow.partition_mappers.wait_policy import MinimumCount, PartitionSatisfaction, WaitForAll, WaitPolicy
from airflow.partition_mappers.window import HourWindow
from airflow.serialization.decoders import decode_partition_mapper
from airflow.serialization.encoders import encode_partition_mapper, encode_wait_policy
from airflow.serialization.enums import Encoding
from airflow.serialization.helpers import WaitPolicyNotSupported


@pytest.fixture
def make_mapper():
    """Build a RollupMapper with a fixed upstream/window so tests focus on wait_policy."""

    def _make(**kwargs):
        return RollupMapper(upstream_mapper=StartOfHourMapper(), window=HourWindow(), **kwargs)

    return _make


class TestPolicyConstruction:
    def test_minimum_count_zero_rejected(self):
        with pytest.raises(ValueError, match="MinimumCount\\(0\\) is degenerate"):
            MinimumCount(0)

    @pytest.mark.parametrize("n", [5, -3])
    def test_minimum_count_stores_n(self, n):
        assert MinimumCount(n).n == n

    def test_default_wait_policy_is_wait_for_all_instance(self, make_mapper):
        mapper = make_mapper()
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


class TestSerializeRoundTrip:
    @pytest.mark.parametrize(
        "policy",
        [
            pytest.param(WaitForAll(), id="wait-for-all"),
            pytest.param(MinimumCount(5), id="minimum-count-5"),
            pytest.param(MinimumCount(-3), id="minimum-count-negative-3"),
        ],
    )
    def test_round_trip(self, policy, make_mapper):
        mapper = make_mapper(wait_policy=policy)
        restored = decode_partition_mapper(encode_partition_mapper(mapper))
        assert isinstance(restored, RollupMapper)
        assert restored.wait_policy == policy

    def test_default_policy_wire_shape(self, make_mapper):
        encoded = encode_partition_mapper(make_mapper())[Encoding.VAR]
        wp = encoded["wait_policy"]
        assert wp[Encoding.TYPE].endswith("WaitForAll")
        assert wp[Encoding.VAR] == {}
        assert "allow_missing" not in encoded
        assert "minimum_count" not in encoded

    def test_non_builtin_wait_policy_rejected(self, make_mapper):
        class Custom(WaitPolicy):
            pass

        mapper = make_mapper(wait_policy=Custom())
        with pytest.raises(WaitPolicyNotSupported):
            encode_partition_mapper(mapper)

    # Guard: the singledispatch default must delegate to policy.serialize(). An earlier
    # draft returned a bare `{}` here, so re-encoding a core-class instance (what
    # decode_wait_policy produces) silently dropped the payload (e.g. MinimumCount.n).
    @pytest.mark.parametrize(
        ("policy", "expected_var"),
        [
            pytest.param(MinimumCount(3), {"n": 3}, id="minimum-count-positive"),
            pytest.param(MinimumCount(-2), {"n": -2}, id="minimum-count-negative"),
            pytest.param(WaitForAll(), {}, id="wait-for-all"),
        ],
    )
    def test_core_wait_policy_re_encode_preserves_wire_shape(self, policy, expected_var):
        assert encode_wait_policy(policy)[Encoding.VAR] == expected_var

    def test_round_trip_fast_path_uses_core_wait_for_all(self, make_mapper):
        """After deserialization the wait_policy is a core-side WaitForAll."""
        mapper = make_mapper(wait_policy=WaitForAll())
        restored = decode_partition_mapper(encode_partition_mapper(mapper))
        assert isinstance(restored, RollupMapper)
        assert isinstance(restored.wait_policy, WaitForAll)


class TestWaitForAllKeySemantics:
    """Verify WaitForAll.is_satisfied_by_keys short-circuits on the first missing key."""

    @pytest.mark.parametrize(
        ("matched", "expected", "fires"),
        [
            # Full subset: all expected keys present.
            ({"a", "b", "c"}, {"a", "b", "c"}, True),
            # Missing one key.
            ({"a"}, {"a", "b"}, False),
            # Both sets empty (vacuously satisfied).
            (set(), set(), True),
            # matched is a strict superset (extra keys do not prevent firing).
            ({"a", "b", "c"}, {"a", "b"}, True),
        ],
    )
    def test_wait_for_all_key_semantics(self, matched, expected, fires):
        assert WaitForAll().is_satisfied_by_keys(matched=matched, expected=expected).satisfied is fires

    def test_minimum_count_uses_base_default(self):
        """MinimumCount inherits the base default: set → count → is_satisfied."""
        expected2 = {str(i) for i in range(60)}
        assert (
            MinimumCount(5)
            .is_satisfied_by_keys(matched={str(i) for i in range(5)}, expected=expected2)
            .satisfied
            is True
        )
        assert (
            MinimumCount(5)
            .is_satisfied_by_keys(matched={str(i) for i in range(4)}, expected=expected2)
            .satisfied
            is False
        )


class TestBaseDelegationEquivalence:
    """
    For every (matched, expected) pair: is_satisfied_by_keys must equal
    is_satisfied(len(matched & expected), len(expected)) — the base-default contract.
    WaitForAll overrides the method but must preserve the same observable result.
    """

    @pytest.mark.parametrize(
        ("policy", "matched", "expected"),
        [
            # WaitForAll — full match, partial match, empty, superset.
            pytest.param(WaitForAll(), {"a", "b"}, {"a", "b"}, id="wfa-full"),
            pytest.param(WaitForAll(), {"a"}, {"a", "b"}, id="wfa-partial"),
            pytest.param(WaitForAll(), set(), set(), id="wfa-empty"),
            pytest.param(WaitForAll(), {"a", "b", "c"}, {"a", "b"}, id="wfa-superset"),
            # MinimumCount(5): at-cap (5 matched out of 60 expected) and over-cap (4 matched).
            pytest.param(
                MinimumCount(5),
                {str(i) for i in range(5)},
                {str(i) for i in range(60)},
                id="mc5-at-cap",
            ),
            pytest.param(
                MinimumCount(5),
                {str(i) for i in range(4)},
                {str(i) for i in range(60)},
                id="mc5-over-cap",
            ),
            # MinimumCount(-3): at-cap (57 matched out of 60 expected) and over-cap (56 matched).
            pytest.param(
                MinimumCount(-3),
                {str(i) for i in range(57)},
                {str(i) for i in range(60)},
                id="mc-neg3-at-cap",
            ),
            pytest.param(
                MinimumCount(-3),
                {str(i) for i in range(56)},
                {str(i) for i in range(60)},
                id="mc-neg3-over-cap",
            ),
        ],
    )
    def test_key_method_equals_count_method(self, policy, matched, expected):
        key_result = policy.is_satisfied_by_keys(matched=matched, expected=expected)
        count_result = policy.is_satisfied(matched=len(matched & expected), expected=len(expected))
        assert key_result.satisfied is count_result


class TestSchedulerDispatch:
    """
    Drive is_satisfied_by_keys directly with synthetic key sets.

    Calling the method directly on the policy object — without constructing a
    SchedulerJobRunner — enforces structurally that is_satisfied_by_keys is a
    pure function of (matched, expected): if the implementation reaches for any
    scheduler-side state it will raise AttributeError here. The method runs in
    the scheduler hot path on every tick for every (apdr, asset) pair, so any
    self.X side effect (logging, db access, anything I/O) would degrade
    throughput silently. Audit logging and error visibility live one level up in
    _resolve_asset_partition_status, deduped per (dag_id, name, uri).
    """

    @pytest.mark.parametrize(
        ("policy", "expected_keys", "matched_keys", "fires"),
        [
            # WaitForAll — only when every key arrived.
            pytest.param(
                WaitForAll(),
                {str(i) for i in range(60)},
                {str(i) for i in range(60)},
                True,
                id="wait-all-complete",
            ),
            pytest.param(
                WaitForAll(),
                {str(i) for i in range(60)},
                {str(i) for i in range(59)},
                False,
                id="wait-all-one-missing",
            ),
            # Empty window: 0 expected keys. WaitForAll vacuously satisfied.
            pytest.param(WaitForAll(), set(), set(), True, id="wait-all-empty-window"),
            # MinimumCount(5) — fire when >=5 expected keys are matched.
            pytest.param(
                MinimumCount(5),
                {str(i) for i in range(60)},
                {str(i) for i in range(5)},
                True,
                id="minimum-count-5-exactly",
            ),
            pytest.param(
                MinimumCount(5),
                {str(i) for i in range(60)},
                {str(i) for i in range(4)},
                False,
                id="minimum-count-5-short",
            ),
            # Empty window with MinimumCount(5): 0 >= 5 → does not fire.
            pytest.param(MinimumCount(5), set(), set(), False, id="minimum-count-5-empty-window"),
            # MinimumCount(-3) on window 60: fire when at most 3 missing.
            pytest.param(
                MinimumCount(-3),
                {str(i) for i in range(60)},
                {str(i) for i in range(57)},
                True,
                id="minimum-count-neg3-exactly",
            ),
            pytest.param(
                MinimumCount(-3),
                {str(i) for i in range(60)},
                {str(i) for i in range(56)},
                False,
                id="minimum-count-neg3-short",
            ),
            # Empty window with MinimumCount(-3): clamp max(0, 0-3)=0, 0>=0 → fires.
            pytest.param(MinimumCount(-3), set(), set(), True, id="minimum-count-neg3-empty-window"),
        ],
    )
    def test_dispatch(self, policy, expected_keys, matched_keys, fires):
        result = policy.is_satisfied_by_keys(matched=matched_keys, expected=expected_keys)
        assert result.satisfied is fires


class TestPartitionSatisfactionStructure:
    """
    Pin the PartitionSatisfaction return structure of is_satisfied_by_keys.

    Covers three states (success / partial / unreachable) for both WaitForAll
    and MinimumCount, verifying both satisfied and unreachable fields together.
    """

    @pytest.mark.parametrize(
        ("policy", "matched_keys", "expected_keys", "satisfied", "unreachable"),
        [
            # WaitForAll — all expected keys present.
            pytest.param(
                WaitForAll(),
                {"a", "b", "c"},
                {"a", "b", "c"},
                True,
                False,
                id="wfa-success",
            ),
            # WaitForAll — one key missing (partial).
            pytest.param(
                WaitForAll(),
                {"a"},
                {"a", "b"},
                False,
                False,
                id="wfa-partial",
            ),
            # WaitForAll — unreachable is always False.
            pytest.param(
                WaitForAll(),
                set(),
                {str(i) for i in range(5)},
                False,
                False,
                id="wfa-unreachable-always-false",
            ),
            # MinimumCount — threshold met (success).
            pytest.param(
                MinimumCount(5),
                {str(i) for i in range(5)},
                {str(i) for i in range(60)},
                True,
                False,
                id="mc5-success",
            ),
            # MinimumCount — threshold not met, still reachable (partial).
            pytest.param(
                MinimumCount(5),
                {str(i) for i in range(4)},
                {str(i) for i in range(60)},
                False,
                False,
                id="mc5-partial",
            ),
            # MinimumCount — n > expected → permanently unreachable.
            pytest.param(
                MinimumCount(5),
                set(),
                {str(i) for i in range(4)},
                False,
                True,
                id="mc5-unreachable",
            ),
        ],
    )
    def test_is_satisfied_by_keys_structure(
        self, policy, matched_keys, expected_keys, satisfied, unreachable
    ):
        result = policy.is_satisfied_by_keys(matched=matched_keys, expected=expected_keys)
        assert result.satisfied is satisfied
        assert result.unreachable is unreachable
        if unreachable:
            assert result.unreachable_reason is not None
            assert str(len(expected_keys)) in result.unreachable_reason
            assert repr(policy) in result.unreachable_reason
        else:
            assert result.unreachable_reason is None


class TestPartitionSatisfactionInvariant:
    """
    PartitionSatisfaction enforces the cross-field invariant at construction:
    unreachable_reason is non-None if and only if unreachable is True.
    """

    @pytest.mark.parametrize(
        ("satisfied", "unreachable", "unreachable_reason", "match"),
        [
            pytest.param(
                False, True, None, "unreachable_reason must be set", id="unreachable-true-reason-none"
            ),
            pytest.param(
                True, False, "x", "unreachable_reason must be None", id="unreachable-false-reason-set"
            ),
        ],
    )
    def test_invalid_cross_field_combinations_raise(self, satisfied, unreachable, unreachable_reason, match):
        with pytest.raises(ValueError, match=match):
            PartitionSatisfaction(
                satisfied=satisfied,
                unreachable=unreachable,
                unreachable_reason=unreachable_reason,
            )


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
            "Rollup asset (name=%r, uri=%r) on Dag %r is permanently unreachable: %s",
            self._NAME,
            self._URI,
            self._TARGET_DAG_ID,
            f"wait policy {MinimumCount(61)!r} can never be satisfied given the window's cardinality 60",
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

    def test_warn_unreachable_asset_partition_dedup(self, runner):
        """_warn_unreachable_asset_partition called twice with the same key logs only once."""
        apdr = unittest.mock.MagicMock()
        apdr.target_dag_id = self._TARGET_DAG_ID

        runner._warn_unreachable_asset_partition(
            apdr=apdr, name=self._NAME, uri=self._URI, reason="some reason"
        )
        runner._warn_unreachable_asset_partition(
            apdr=apdr, name=self._NAME, uri=self._URI, reason="some reason"
        )

        assert len(runner._log.warning.mock_calls) == 1
        assert (self._TARGET_DAG_ID, self._NAME, self._URI) in runner._partition_unreachable_seen
