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
import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest
import time_machine

MODULE_PATH = (
    Path(__file__).resolve().parents[4] / "scripts" / "ci" / "testing" / "compute_remaining_test_timeout.py"
)


@pytest.fixture
def timeout_module():
    module_name = "test_compute_remaining_test_timeout_module"
    sys.modules.pop(module_name, None)
    spec = importlib.util.spec_from_file_location(module_name, MODULE_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize(
    ("job_timeout_seconds", "elapsed_seconds", "expected_seconds"),
    [
        pytest.param(65 * 60, 8 * 60, 3120, id="image-pull-only"),
        pytest.param(65 * 60, 25 * 60, 2100, id="image-pull-and-slow-migration-tests"),
        pytest.param(65 * 60, 55 * 60, 600, id="floor-applies-when-budget-nearly-gone"),
        pytest.param(65 * 60, 70 * 60, 600, id="floor-applies-when-budget-overrun"),
    ],
)
def test_compute_remaining_test_timeout(
    timeout_module, job_timeout_seconds, elapsed_seconds, expected_seconds
):
    assert (
        timeout_module.compute_remaining_test_timeout(job_timeout_seconds, elapsed_seconds)
        == expected_seconds
    )


@time_machine.travel("2026-08-29 12:10:00+00:00", tick=False)
def test_main_derives_the_budget_left_from_the_job_start_time(timeout_module, monkeypatch, capsys):
    job_start_epoch = int(datetime(2026, 8, 29, 12, 0, tzinfo=timezone.utc).timestamp())
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "compute_remaining_test_timeout.py",
            "--job-timeout-minutes",
            "65",
            "--job-start-epoch",
            str(job_start_epoch),
        ],
    )

    timeout_module.main()

    assert int(capsys.readouterr().out) == (65 - 10 - 5) * 60
