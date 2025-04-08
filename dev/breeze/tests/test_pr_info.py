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

import os
from pathlib import Path
from unittest import mock

from airflow_breeze.commands.ci_commands import workflow_info

TEST_PR_INFO_DIR = Path(__file__).parent / "test_pr_info_files"


def test_pr_info():
    with mock.patch.dict(os.environ, {"AIRFLOW_SELF_HOSTED_RUNNER": ""}):
        json_string = (TEST_PR_INFO_DIR / "pr_github_context.json").read_text()
        wi = workflow_info(json_string)
        assert wi.pull_request_labels == [
            "area:providers",
            "area:dev-tools",
            "area:logging",
            "kind:documentation",
        ]
        assert wi.target_repo == "apache/airflow"
        assert wi.head_repo == "test/airflow"
        assert wi.event_name == "pull_request"
        assert wi.pr_number == 26004
        assert wi.get_runs_on() == '["ubuntu-22.04"]'
        assert wi.is_canary_run() == "false"
        assert wi.run_coverage() == "false"


def test_push_info():
    with mock.patch.dict(os.environ, {"AIRFLOW_SELF_HOSTED_RUNNER": ""}):
        json_string = (TEST_PR_INFO_DIR / "push_github_context.json").read_text()
        wi = workflow_info(json_string)
        assert wi.pull_request_labels == []
        assert wi.target_repo == "apache/airflow"
        assert wi.head_repo == "apache/airflow"
        assert wi.event_name == "push"
        assert wi.pr_number is None
        assert wi.get_runs_on() == '["ubuntu-22.04"]'
        assert wi.is_canary_run() == "true"
        assert wi.run_coverage() == "true"


def test_schedule():
    with mock.patch.dict(os.environ, {"AIRFLOW_SELF_HOSTED_RUNNER": ""}):
        json_string = (TEST_PR_INFO_DIR / "schedule_github_context.json").read_text()
        wi = workflow_info(json_string)
        assert wi.pull_request_labels == []
        assert wi.target_repo == "apache/airflow"
        assert wi.head_repo == "apache/airflow"
        assert wi.event_name == "schedule"
        assert wi.pr_number is None
        assert wi.get_runs_on() == '["ubuntu-22.04"]'
        assert wi.is_canary_run() == "true"
        assert wi.run_coverage() == "false"


def test_runs_on_self_hosted():
    with mock.patch.dict(os.environ, {"AIRFLOW_SELF_HOSTED_RUNNER": "true"}):
        json_string = (TEST_PR_INFO_DIR / "simple_pr.json").read_text()
        wi = workflow_info(json_string)
        assert wi.pull_request_labels == ["another"]
        assert wi.target_repo == "apache/airflow"
        assert wi.head_repo == "apache/airflow"
        assert wi.event_name == "pull_request"
        assert wi.pr_number == 1234
        # TODO: fix it when we fix self-hosted runners
        assert wi.get_runs_on() == '["ubuntu-22.04"]'
        # assert wi.get_runs_on() == '["self-hosted", "Linux", "X64"]'
        assert wi.is_canary_run() == "false"
        assert wi.run_coverage() == "false"


def test_runs_on_forced_public_runner():
    with mock.patch.dict(os.environ, {"AIRFLOW_SELF_HOSTED_RUNNER": "true"}):
        json_string = (TEST_PR_INFO_DIR / "self_hosted_forced_pr.json").read_text()
        wi = workflow_info(json_string)
        assert wi.pull_request_labels == ["use public runners", "another"]
        assert wi.target_repo == "apache/airflow"
        assert wi.head_repo == "apache/airflow"
        assert wi.event_name == "pull_request"
        assert wi.pr_number == 1234
        assert wi.get_runs_on() == '["ubuntu-22.04"]'
        assert wi.is_canary_run() == "false"
        assert wi.run_coverage() == "false"


def test_runs_on_simple_pr_other_repo():
    with mock.patch.dict(os.environ, {"AIRFLOW_SELF_HOSTED_RUNNER": ""}):
        json_string = (TEST_PR_INFO_DIR / "simple_pr_different_repo.json").read_text()
        wi = workflow_info(json_string)
        assert wi.pull_request_labels == ["another"]
        assert wi.target_repo == "apache/airflow"
        assert wi.head_repo == "test/airflow"
        assert wi.event_name == "pull_request"
        assert wi.pr_number == 1234
        assert wi.get_runs_on() == '["ubuntu-22.04"]'
        assert wi.is_canary_run() == "false"
        assert wi.run_coverage() == "false"


def test_runs_on_push_other_branch():
    with mock.patch.dict(os.environ, {"AIRFLOW_SELF_HOSTED_RUNNER": "true"}):
        json_string = (TEST_PR_INFO_DIR / "push_other_branch.json").read_text()
        wi = workflow_info(json_string)
        assert wi.pull_request_labels == []
        assert wi.target_repo == "apache/airflow"
        assert wi.head_repo == "apache/airflow"
        assert wi.event_name == "push"
        assert wi.pr_number is None
        # TODO: revert it when we fix self-hosted runners
        assert wi.get_runs_on() == '["ubuntu-22.04"]'
        # assert wi.get_runs_on() == '["self-hosted", "Linux", "X64"]'
        assert wi.is_canary_run() == "false"
        assert wi.run_coverage() == "false"


def test_runs_on_push_v_test_branch():
    with mock.patch.dict(os.environ, {"AIRFLOW_SELF_HOSTED_RUNNER": "true"}):
        json_string = (TEST_PR_INFO_DIR / "push_v_test_branch.json").read_text()
        wi = workflow_info(json_string)
        assert wi.pull_request_labels == []
        assert wi.target_repo == "apache/airflow"
        assert wi.head_repo == "apache/airflow"
        assert wi.event_name == "push"
        assert wi.pr_number is None
        # TODO: revert it when we fix self-hosted runners
        assert wi.get_runs_on() == '["ubuntu-22.04"]'
        # assert wi.get_runs_on() == '["self-hosted", "Linux", "X64"]'
        assert wi.is_canary_run() == "true"
        assert wi.run_coverage() == "false"
