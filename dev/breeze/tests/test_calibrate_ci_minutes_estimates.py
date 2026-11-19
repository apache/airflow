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

import importlib.util
import json

import pytest

from airflow_breeze.utils.path_utils import AIRFLOW_ROOT_PATH

# The script is standalone (run with `uv run`), so it is loaded from its path.
SCRIPT_PATH = AIRFLOW_ROOT_PATH / "dev" / "calibrate_ci_minutes_estimates.py"
_spec = importlib.util.spec_from_file_location("calibrate_ci_minutes_estimates", SCRIPT_PATH)
assert _spec is not None
assert _spec.loader is not None
calibrate = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(calibrate)

JOBS = [
    {
        "name": "Basic tests / Static checks",
        "conclusion": "success",
        "started_at": "2026-10-22T10:00:00Z",
        "completed_at": "2026-10-22T10:06:00Z",
    },
    {
        "name": "MyPy providers checks",
        "conclusion": "success",
        "started_at": "2026-10-22T10:00:00Z",
        "completed_at": "2026-10-22T10:08:00Z",
    },
    {
        "name": "TypeScript SDK docs",
        "conclusion": "success",
        "started_at": "2026-10-22T10:00:00Z",
        "completed_at": "2026-10-22T10:01:00Z",
    },
]


def _cache_run(cache_dir, run_id, *, runner_type='["ubuntu-22.04"]', all_versions="false"):
    (cache_dir / f"{run_id}.jobs.json").write_text(json.dumps(JOBS))
    (cache_dir / f"{run_id}.build-info.log").write_text(
        "2026-10-22T10:00:00Z full-tests-needed = false\n"
        f"2026-10-22T10:00:00Z runner-type = {runner_type}\n"
        f"2026-10-22T10:00:00Z all-versions = {all_versions}\n"
    )


@pytest.mark.parametrize(
    ("runner_type", "all_versions", "counted"),
    [
        pytest.param('["ubuntu-22.04"]', "false", True, id="public runners, default versions"),
        pytest.param(
            "runs-on=1/family=m7a/image=ubuntu22-full-x64/volume=150gb/cpu={0}/ram={1}",
            "false",
            False,
            id="self-hosted runners",
        ),
        pytest.param('["ubuntu-22.04"]', "true", False, id="all versions"),
    ],
)
def test_build_rows_counts_only_the_runs_the_estimate_models(tmp_path, runner_type, all_versions, counted):
    _cache_run(tmp_path, 1, runner_type=runner_type, all_versions=all_versions)

    rows = calibrate.build_rows([1], tmp_path)

    assert len(rows) == int(counted)


def test_build_rows_groups_ts_sdk_docs_separately(tmp_path):
    _cache_run(tmp_path, 1)

    (row,) = calibrate.build_rows([1], tmp_path)

    assert row["minutes"]["ts-sdk-docs"] == 1
    assert "ts-sdk" not in row["minutes"]


def test_build_rows_groups_provider_mypy_separately(tmp_path):
    _cache_run(tmp_path, 1)

    (row,) = calibrate.build_rows([1], tmp_path)

    assert row["minutes"]["mypy-providers"] == 8
    assert "providers" not in row["minutes"]
