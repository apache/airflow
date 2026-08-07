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

from airflow.api_fastapi.core_api.services.ui.dag_run import compute_duration_stats


@pytest.mark.parametrize("cast", [float, Decimal], ids=["float", "decimal"])
def test_compute_duration_stats_handles_float_and_decimal(cast):
    """Durations arrive as Decimal on Postgres 14+ (EXTRACT(epoch ...) returns numeric) and as
    float/int on other backends; the stats must compute for either without raising TypeError."""
    durations = [cast(v) for v in (10, 20, 20, 30, 40)]

    stats = compute_duration_stats(durations)

    assert stats is not None
    assert stats.mean == 24.0
    assert stats.mode == 20.0
    assert stats.p50 == 20.0
    assert stats.p90 == 36.0
    assert stats.p95 == 38.0
    assert stats.p99 == 39.6
