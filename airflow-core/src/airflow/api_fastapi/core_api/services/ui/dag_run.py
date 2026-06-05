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

import statistics
from collections import Counter

from airflow.api_fastapi.core_api.datamodels.ui.dag_runs import DurationStats


def compute_duration_stats(durations: list[float]) -> DurationStats | None:
    """
    Compute duration statistics from a list of completed DAG run durations (in seconds).

    Returns None when the list is empty (no completed runs exist yet).
    Mode is computed on second-rounded values to avoid float precision noise; returns None
    when every run has a unique duration.
    Percentiles use linear interpolation between adjacent sorted values.
    """
    if not durations:
        return None

    sorted_d = sorted(durations)

    counts = Counter(round(d) for d in sorted_d)
    max_count = max(counts.values())
    mode_val: float | None = (
        float(min(k for k, v in counts.items() if v == max_count)) if max_count > 1 else None
    )

    def _percentile(p: float) -> float:
        idx = (len(sorted_d) - 1) * p / 100
        lo = int(idx)
        hi = min(lo + 1, len(sorted_d) - 1)
        return sorted_d[lo] + (sorted_d[hi] - sorted_d[lo]) * (idx - lo)

    return DurationStats(
        mean=round(statistics.mean(sorted_d), 3),
        mode=round(mode_val, 3) if mode_val is not None else None,
        p50=round(_percentile(50), 3),
        p90=round(_percentile(90), 3),
        p95=round(_percentile(95), 3),
        p99=round(_percentile(99), 3),
    )
