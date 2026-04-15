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
import subprocess
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

MODULE_PATH = Path(__file__).resolve().parents[3] / "scripts" / "ci" / "slack_notification_state.py"


@pytest.fixture
def slack_state_module():
    module_name = "test_slack_notification_state_module"
    sys.modules.pop(module_name, None)
    spec = importlib.util.spec_from_file_location(module_name, MODULE_PATH)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class TestDownloadPreviousState:
    def test_uses_explicit_get_method(self, slack_state_module):
        """`gh api` defaults to POST when -f is passed; we must force GET to avoid 404."""
        completed = subprocess.CompletedProcess(args=[], returncode=0, stdout="null", stderr="")
        with patch.object(subprocess, "run", return_value=completed) as mock_run:
            slack_state_module.download_previous_state("artifact", "apache/airflow")
        args = mock_run.call_args[0][0]
        assert "--method" in args
        method_index = args.index("--method")
        assert args[method_index + 1] == "GET"


class TestDetermineAction:
    def test_no_failures_no_prev_state(self, slack_state_module):
        assert slack_state_module.determine_action([], None) == "skip"

    def test_no_failures_no_prev_failures(self, slack_state_module):
        prev = {"failures": [], "last_notified": "2025-01-01T00:00:00+00:00"}
        assert slack_state_module.determine_action([], prev) == "skip"

    def test_recovery_when_clear_after_failures(self, slack_state_module):
        prev = {"failures": ["job-a"], "last_notified": "2025-01-01T00:00:00+00:00"}
        assert slack_state_module.determine_action([], prev) == "notify_recovery"

    def test_notify_new_on_first_failure(self, slack_state_module):
        assert slack_state_module.determine_action(["job-a"], None) == "notify_new"

    def test_notify_new_on_changed_failures(self, slack_state_module):
        prev = {"failures": ["job-a"], "last_notified": "2025-01-01T00:00:00+00:00"}
        assert slack_state_module.determine_action(["job-b"], prev) == "notify_new"
