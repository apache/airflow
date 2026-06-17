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
"""Tests for the triggerer/deadline callback context-construction layers (PR #66608).

Covers ``TriggerRunner._build_context_from_dag_run_data`` ``_deadline`` handling
and SDK callback path resolution for partial / lambda / bound-method callables.
"""

from __future__ import annotations

from airflow.jobs.triggerer_job_runner import TriggerRunner

# Base valid dag_run_data accepted by DRDataModel.
_BASE_DAG_RUN_DATA: dict = {
    "dag_id": "example_dag",
    "run_id": "manual__2024-01-01",
    "logical_date": "2024-01-01T00:00:00+00:00",
    "data_interval_start": None,
    "data_interval_end": None,
    "run_after": "2024-01-01T00:00:00+00:00",
    "start_date": "2024-01-01T00:01:00+00:00",
    "end_date": None,
    "run_type": "manual",
    "state": "running",
    "conf": {},
    "consumed_asset_events": [],
    "partition_key": None,
}


# ---------------------------------------------------------------------------
# _build_context_from_dag_run_data with the _deadline key variants
# ---------------------------------------------------------------------------
class TestDeadlineKey:
    def test_deadline_popped_before_model_construction(self):
        """_deadline must be popped so it does not trip extra='forbid'."""
        data = {**_BASE_DAG_RUN_DATA, "_deadline": {"id": "x", "deadline_time": "t"}}
        ctx = TriggerRunner._build_context_from_dag_run_data(data)
        assert ctx["deadline"] == {"id": "x", "deadline_time": "t"}
        # the input dict was mutated by pop - acceptable, but verify no leak into model
        assert "_deadline" not in ctx["dag_run"].model_dump()
