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
"""Tests for the pr_workflows module."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

from airflow_breeze.utils.pr_workflows import (
    find_pending_workflow_runs,
    find_workflow_runs_by_status,
    has_in_progress_workflows,
)


class TestFindWorkflowRunsByStatus:
    @patch("requests.get")
    def test_returns_matching_runs(self, mock_get):
        mock_get.return_value = MagicMock(
            status_code=200,
            json=lambda: {
                "workflow_runs": [
                    {"id": 1, "head_sha": "abc123", "status": "completed"},
                    {"id": 2, "head_sha": "abc123", "status": "completed"},
                ]
            },
        )
        runs = find_workflow_runs_by_status("token", "apache/airflow", "abc123", "completed")
        assert len(runs) == 2

    @patch("requests.get")
    def test_api_failure_returns_empty(self, mock_get):
        mock_get.return_value = MagicMock(status_code=500)
        runs = find_workflow_runs_by_status("token", "apache/airflow", "abc123", "completed")
        assert runs == []


class TestFindPendingWorkflowRuns:
    @patch("airflow_breeze.utils.pr_workflows.find_workflow_runs_by_status")
    def test_delegates_to_find_with_action_required_status(self, mock_find):
        mock_find.return_value = [{"id": 1, "conclusion": "action_required"}]
        pending = find_pending_workflow_runs("token", "apache/airflow", "abc123")
        mock_find.assert_called_once_with("token", "apache/airflow", "abc123", "action_required")
        assert len(pending) == 1
        assert pending[0]["id"] == 1


class TestHasInProgressWorkflows:
    @patch("airflow_breeze.utils.pr_workflows.find_workflow_runs_by_status")
    def test_returns_true_when_in_progress(self, mock_find):
        mock_find.return_value = [{"id": 1}]
        assert has_in_progress_workflows("token", "apache/airflow", "abc123") is True

    @patch("airflow_breeze.utils.pr_workflows.find_workflow_runs_by_status")
    def test_returns_false_when_none(self, mock_find):
        mock_find.return_value = []
        assert has_in_progress_workflows("token", "apache/airflow", "abc123") is False
