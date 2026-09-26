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
# ruff: noqa: S101
from __future__ import annotations

from unittest import mock

import pytest

from dev.dag_parsing_poc import run_scheduler_parsing as experiment


def test_summaries_distinguish_step_latency_from_scheduler_latency_and_counts():
    samples = [
        {"name": name, "value": value, "kind": kind}
        for name, values, kind in (
            ("scheduler.parsing_step_duration", [1, 3, 5], "ms"),
            ("scheduler.scheduler_loop_duration", [100, 200, 300], "ms"),
            ("scheduler.parsing_step_deferred", [1, 1, 1], "c"),
            ("unrelated", [999], "ms"),
        )
        for value in values
    ]
    result = experiment.summarize_metrics(samples)
    assert result["scheduler.parsing_step_duration"]["median_ms"] == 3
    assert result["scheduler.scheduler_loop_duration"]["max_ms"] == 300
    assert result["scheduler.parsing_step_deferred"] == {"count": 3}
    assert "unrelated" not in result


@mock.patch.object(experiment.time, "sleep", autospec=True)
@mock.patch.object(experiment.time, "monotonic", autospec=True, return_value=1)
@mock.patch.object(experiment, "get_state", autospec=True)
@pytest.mark.parametrize("failure", [None, "heartbeat", "completed_tasks", "metrics"])
def test_phase_requires_both_progress_and_callback_measurements(get_state, monotonic, sleep, failure):
    before = {"heartbeat": "before", "completed_tasks": 1}
    after = {"heartbeat": "after", "completed_tasks": 2}
    if failure in before:
        after[failure] = before[failure]
    get_state.side_effect = [before, after]
    metrics = mock.create_autospec(experiment.MetricsCapture, instance=True)
    metrics.samples = (
        []
        if failure == "metrics"
        else [{"at": 2, "name": "scheduler.parsing_step_duration", "value": 2, "kind": "ms"}]
    )
    if failure is not None:
        with pytest.raises(RuntimeError, match="Missing|stopped making progress"):
            experiment.record_phase("test", mock.sentinel.database, metrics, 10)
    else:
        phase = experiment.record_phase("test", mock.sentinel.database, metrics, 10)
        assert phase["after"] == after
        assert phase["metrics"]["scheduler.parsing_step_duration"]["samples"] == 1
