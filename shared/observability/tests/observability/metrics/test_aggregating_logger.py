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

import datetime
from unittest import mock

import pytest

from airflow_shared.observability.metrics.aggregating_logger import (
    MAX_METRICS,
    MAX_TIMING_VALUES,
    AggregatedMetrics,
    AggregatingLogger,
)

EMPTY = AggregatedMetrics(counters={}, gauges={}, timings={})


class TestAggregatingLogger:
    def test_counters_sum_their_increments(self):
        backend = AggregatingLogger()
        backend.incr("ti_successes")  # 1
        backend.incr("ti_successes", 3)  # 1 + 3 = 4
        backend.decr("ti_successes", 2)  # 4 - 2 = 2

        assert backend.drain().counters == {("ti_successes", ()): 2}

    def test_tags_make_separate_keys_regardless_of_order_and_type(self):
        backend = AggregatingLogger()
        backend.incr("ti.finish", tags={"dag_id": "dag", "try_number": 1})
        backend.incr("ti.finish", tags={"try_number": "1", "dag_id": "dag"})
        backend.incr("ti.finish", tags={"dag_id": "other"})

        assert backend.drain().counters == {
            ("ti.finish", (("dag_id", "dag"), ("try_number", "1"))): 2,
            ("ti.finish", (("dag_id", "other"),)): 1,
        }

    @pytest.mark.parametrize(
        ("params", "final_params"),
        [
            pytest.param([(5, False), (7, False)], (7, False), id="last_absolute_value_wins"),
            pytest.param([(5, True), (7, True)], (12, True), id="deltas_fold_and_stay_a_delta"),
            pytest.param(
                [(5, False), (7, True)], (12, False), id="delta_on_an_absolute_value_stays_absolute"
            ),
            pytest.param([(5, True), (7, False)], (7, False), id="absolute_value_replaces_a_delta"),
        ],
    )
    def test_gauges_keep_one_value_per_key(self, params, final_params):
        backend = AggregatingLogger()
        for value, delta in params:
            backend.gauge("pool.open_slots", value, delta=delta)

        assert backend.drain().gauges == {("pool.open_slots", ()): final_params}

    def test_timings_keep_every_observation_in_milliseconds(self):
        backend = AggregatingLogger()
        backend.timing("task.duration", 12)
        backend.timing("task.duration", datetime.timedelta(seconds=1.5))
        backend.timing("task.duration", None)

        assert backend.drain().timings == {("task.duration", ()): [12.0, 1500.0]}

    def test_timer_records_its_duration(self):
        backend = AggregatingLogger()
        with backend.timer("task.duration", tags={"dag_id": "dag"}) as timer:
            pass

        assert backend.drain().timings == {("task.duration", (("dag_id", "dag"),)): [timer.duration]}

    @pytest.mark.parametrize(
        ("stat", "send"),
        [
            pytest.param(None, True, id="no_name"),
            pytest.param("task.duration", False, id="send_false"),
        ],
    )
    def test_timer_records_nothing(self, stat: str, send: bool):
        backend = AggregatingLogger()
        timer = backend.timer(stat).start()
        timer.stop(send=send)

        assert timer.duration is not None
        assert backend.drain() == EMPTY

    @pytest.mark.parametrize(
        ("rate", "kept"),
        [
            pytest.param(0.4, False, id="below_the_sample_is_skipped"),
            pytest.param(0.6, True, id="above_the_sample_is_kept"),
            pytest.param(1, True, id="full_rate_is_never_sampled"),
        ],
    )
    @mock.patch("airflow_shared.observability.metrics.aggregating_logger.random.random", return_value=0.5)
    def test_rate_samples_counters_and_gauges(self, _random, rate: int | float, kept):
        backend = AggregatingLogger()
        backend.incr("ti_successes", rate=rate)
        backend.gauge("pool.open_slots", 1, rate=rate)

        drained = backend.drain()
        assert (bool(drained.counters), bool(drained.gauges)) == (kept, kept)

    def test_drain_hands_over_everything_and_starts_over(self):
        backend = AggregatingLogger()
        backend.incr("ti_successes")
        backend.gauge("pool.open_slots", 3)
        backend.timing("task.duration", 12)

        assert backend.drain() == AggregatedMetrics(
            counters={("ti_successes", ()): 1},
            gauges={("pool.open_slots", ()): (3, False)},
            timings={("task.duration", ()): [12.0]},
        )
        assert backend.drain() == EMPTY

    def test_large_batch_callback_fires_at_the_metric_bound(self):
        on_large_batch = mock.Mock()
        backend = AggregatingLogger(on_large_batch=on_large_batch)
        for i in range(MAX_METRICS - 1):
            backend.incr(f"metric_{i}")
        on_large_batch.assert_not_called()

        backend.gauge("one_more", 1)

        on_large_batch.assert_called_once_with()

    def test_large_batch_callback_fires_at_the_timing_bound(self):
        on_large_batch = mock.Mock()
        backend = AggregatingLogger(on_large_batch=on_large_batch)
        for _ in range(MAX_TIMING_VALUES - 1):
            backend.timing("task.duration", 1)
        on_large_batch.assert_not_called()

        backend.timing("task.duration", 1)

        on_large_batch.assert_called_once_with()

    @pytest.mark.execution_timeout(10)
    def test_large_batch_callback_can_drain(self):
        batches = []
        backend = AggregatingLogger(on_large_batch=lambda: batches.append(backend.drain()))

        for i in range(MAX_METRICS):
            backend.incr(f"metric_{i}")

        assert len(batches) == 1
        assert len(batches[0].counters) == MAX_METRICS
        assert backend.drain() == EMPTY

    def test_large_batch_without_callback_keeps_accumulating(self):
        backend = AggregatingLogger()

        for i in range(MAX_METRICS + 1):
            backend.incr(f"metric_{i}")

        assert len(backend.drain().counters) == MAX_METRICS + 1
