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

from unittest.mock import MagicMock, PropertyMock, patch

import pytest

from airflow.providers.amazon.aws.hooks.sagemaker_unified_studio_notebook import (
    SageMakerUnifiedStudioNotebookHook,
)

DOMAIN_ID = "dzd_example"
PROJECT_ID = "proj_example"
NOTEBOOK_ID = "notebook_123"
NOTEBOOK_RUN_ID = "run_456"

HOOK_MODULE = "airflow.providers.amazon.aws.hooks.sagemaker_unified_studio_notebook"


class TestSageMakerUnifiedStudioNotebookHook:
    @pytest.fixture(autouse=True)
    def setup(self):
        self.mock_client = MagicMock()
        with patch.object(SageMakerUnifiedStudioNotebookHook, "conn", new_callable=PropertyMock) as mock_conn:
            mock_conn.return_value = self.mock_client
            self.hook = SageMakerUnifiedStudioNotebookHook(aws_conn_id=None)
            yield

    # --- __init__ tests ---

    def test_inherits_aws_base_hook(self):
        """Hook extends AwsBaseHook with client_type='datazone'."""
        from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook

        assert isinstance(self.hook, AwsBaseHook)
        assert self.hook.client_type == "datazone"

    # --- start_notebook_run ---

    def test_start_notebook_run_minimal(self):
        """Start run with only required params."""
        self.mock_client.start_notebook_run.return_value = {"notebookRunId": NOTEBOOK_RUN_ID}

        result = self.hook.start_notebook_run(
            notebook_id=NOTEBOOK_ID, domain_id=DOMAIN_ID, project_id=PROJECT_ID
        )

        assert result == {"notebookRunId": NOTEBOOK_RUN_ID}
        call_kwargs = self.mock_client.start_notebook_run.call_args[1]
        assert call_kwargs["domain_id"] == DOMAIN_ID
        assert call_kwargs["project_id"] == PROJECT_ID
        assert call_kwargs["notebook_id"] == NOTEBOOK_ID
        assert "client_token" in call_kwargs
        # Optional params should not be present
        assert "notebook_parameters" not in call_kwargs
        assert "compute_configuration" not in call_kwargs
        assert "timeout_configuration" not in call_kwargs
        assert "trigger_source" not in call_kwargs

    def test_start_notebook_run_all_params(self):
        """Start run with all optional params provided."""
        self.mock_client.start_notebook_run.return_value = {"notebookRunId": NOTEBOOK_RUN_ID}

        result = self.hook.start_notebook_run(
            notebook_id=NOTEBOOK_ID,
            domain_id=DOMAIN_ID,
            project_id=PROJECT_ID,
            client_token="my-token",
            notebook_parameters={"param1": "value1"},
            compute_configuration={"instance_type": "ml.m5.large"},
            timeout_configuration={"run_timeout_in_minutes": 120},
            workflow_name="my_dag",
        )

        assert result == {"notebookRunId": NOTEBOOK_RUN_ID}
        call_kwargs = self.mock_client.start_notebook_run.call_args[1]
        assert call_kwargs["client_token"] == "my-token"
        assert call_kwargs["notebook_parameters"] == {"param1": "value1"}
        assert call_kwargs["compute_configuration"] == {"instance_type": "ml.m5.large"}
        assert call_kwargs["timeout_configuration"] == {"run_timeout_in_minutes": 120}
        assert call_kwargs["trigger_source"] == {"type": "workflow", "workflow_name": "my_dag"}

    def test_start_notebook_run_auto_generates_client_token(self):
        """client_token is auto-generated as a UUID when not provided."""
        self.mock_client.start_notebook_run.return_value = {}

        self.hook.start_notebook_run(notebook_id=NOTEBOOK_ID, domain_id=DOMAIN_ID, project_id=PROJECT_ID)

        call_kwargs = self.mock_client.start_notebook_run.call_args[1]
        token = call_kwargs["client_token"]
        # UUID4 format: 8-4-4-4-12 hex chars
        assert len(token) == 36
        assert token.count("-") == 4

    # --- get_notebook_run ---

    def test_get_notebook_run(self):
        """get_notebook_run passes correct params to the client."""
        expected = {"status": "SUCCEEDED", "notebookRunId": NOTEBOOK_RUN_ID}
        self.mock_client.get_notebook_run.return_value = expected

        result = self.hook.get_notebook_run(NOTEBOOK_RUN_ID, domain_id=DOMAIN_ID)

        assert result == expected
        self.mock_client.get_notebook_run.assert_called_once_with(
            domain_id=DOMAIN_ID,
            notebook_run_id=NOTEBOOK_RUN_ID,
        )

    # --- _handle_state ---

    def test_handle_state_in_progress(self):
        """In-progress states return None."""
        for status in ("QUEUED", "STARTING", "RUNNING", "STOPPING"):
            result = self.hook._handle_state(NOTEBOOK_RUN_ID, status, "")
            assert result is None

    def test_handle_state_succeeded(self):
        """SUCCEEDED state returns success dict."""
        result = self.hook._handle_state(NOTEBOOK_RUN_ID, "SUCCEEDED", "")
        assert result == {"State": "SUCCEEDED", "NotebookRunId": NOTEBOOK_RUN_ID}

    def test_handle_state_stopped(self):
        """STOPPED is a finished state and returns success dict."""
        result = self.hook._handle_state(NOTEBOOK_RUN_ID, "STOPPED", "")
        assert result == {"State": "STOPPED", "NotebookRunId": NOTEBOOK_RUN_ID}

    def test_handle_state_failed_with_error_message(self):
        """FAILED state with error message raises RuntimeError with that message."""
        with pytest.raises(RuntimeError, match="Something went wrong"):
            self.hook._handle_state(NOTEBOOK_RUN_ID, "FAILED", "Something went wrong")

    def test_handle_state_failed_empty_error_message(self):
        """FAILED state with empty error message raises RuntimeError with status info."""
        with pytest.raises(RuntimeError, match=f"Exiting notebook run {NOTEBOOK_RUN_ID}. State: FAILED"):
            self.hook._handle_state(NOTEBOOK_RUN_ID, "FAILED", "")

    def test_handle_state_unexpected_status(self):
        """Unexpected status raises RuntimeError."""
        with pytest.raises(RuntimeError, match=f"Exiting notebook run {NOTEBOOK_RUN_ID}. State: UNKNOWN"):
            self.hook._handle_state(NOTEBOOK_RUN_ID, "UNKNOWN", "")

    # --- wait_for_notebook_run ---

    @patch(f"{HOOK_MODULE}.time.sleep")
    def test_wait_for_notebook_run_immediate_success(self, mock_sleep):
        """Run completes on first poll."""
        self.mock_client.get_notebook_run.return_value = {"status": "SUCCEEDED"}

        result = self.hook.wait_for_notebook_run(NOTEBOOK_RUN_ID, domain_id=DOMAIN_ID, waiter_delay=5)

        assert result == {"State": "SUCCEEDED", "NotebookRunId": NOTEBOOK_RUN_ID}
        mock_sleep.assert_called_once_with(5)

    @patch(f"{HOOK_MODULE}.time.sleep")
    def test_wait_for_notebook_run_polls_then_succeeds(self, mock_sleep):
        """Run is in progress for a few polls then completes."""
        self.mock_client.get_notebook_run.side_effect = [
            {"status": "QUEUED"},
            {"status": "STARTING"},
            {"status": "RUNNING"},
            {"status": "SUCCEEDED"},
        ]

        result = self.hook.wait_for_notebook_run(NOTEBOOK_RUN_ID, domain_id=DOMAIN_ID, waiter_delay=5)

        assert result == {"State": "SUCCEEDED", "NotebookRunId": NOTEBOOK_RUN_ID}
        assert mock_sleep.call_count == 4

    @patch(f"{HOOK_MODULE}.time.sleep")
    def test_wait_for_notebook_run_fails(self, mock_sleep):
        """Run fails with an error message."""
        self.mock_client.get_notebook_run.return_value = {
            "status": "FAILED",
            "errorMessage": "Notebook crashed",
        }

        with pytest.raises(RuntimeError, match="Notebook crashed"):
            self.hook.wait_for_notebook_run(NOTEBOOK_RUN_ID, domain_id=DOMAIN_ID, waiter_delay=5)

    @patch(f"{HOOK_MODULE}.time.sleep")
    def test_wait_for_notebook_run_timeout(self, mock_sleep):
        """Run times out after max attempts."""
        self.mock_client.get_notebook_run.return_value = {"status": "RUNNING"}

        with pytest.raises(RuntimeError, match="Execution timed out"):
            self.hook.wait_for_notebook_run(
                NOTEBOOK_RUN_ID,
                domain_id=DOMAIN_ID,
                waiter_delay=5,
                timeout_configuration={"run_timeout_in_minutes": 1},
            )

        assert self.mock_client.get_notebook_run.call_count == 12

    @patch(f"{HOOK_MODULE}.time.sleep")
    def test_wait_for_notebook_run_default_timeout(self, mock_sleep):
        """Default waiter_max_attempts derived from 12-hour timeout."""
        self.mock_client.get_notebook_run.return_value = {"status": "SUCCEEDED"}

        result = self.hook.wait_for_notebook_run(NOTEBOOK_RUN_ID, domain_id=DOMAIN_ID, waiter_delay=10)

        assert result == {"State": "SUCCEEDED", "NotebookRunId": NOTEBOOK_RUN_ID}

    @patch(f"{HOOK_MODULE}.time.sleep")
    def test_wait_for_notebook_run_empty_timeout_configuration(self, mock_sleep):
        """Empty timeout_configuration falls back to default 12-hour timeout."""
        self.mock_client.get_notebook_run.return_value = {"status": "SUCCEEDED"}

        result = self.hook.wait_for_notebook_run(
            NOTEBOOK_RUN_ID, domain_id=DOMAIN_ID, waiter_delay=10, timeout_configuration={}
        )

        assert result == {"State": "SUCCEEDED", "NotebookRunId": NOTEBOOK_RUN_ID}

    # --- _validate_api_availability ---

    def test_validate_api_availability_missing_start_notebook_run(self):
        """Raises RuntimeError when start_notebook_run is not available on the client."""
        mock_client = MagicMock(spec=[])  # no APIs available
        with patch.object(SageMakerUnifiedStudioNotebookHook, "conn", new_callable=PropertyMock) as mock_conn:
            mock_conn.return_value = mock_client
            hook = SageMakerUnifiedStudioNotebookHook(aws_conn_id=None)
            with pytest.raises(RuntimeError, match="start_notebook_run.*not available"):
                hook._validate_api_availability()

    def test_validate_api_availability_missing_get_notebook_run(self):
        """Raises RuntimeError when get_notebook_run is not available on the client."""
        mock_client = MagicMock(spec=["start_notebook_run"])
        with patch.object(SageMakerUnifiedStudioNotebookHook, "conn", new_callable=PropertyMock) as mock_conn:
            mock_conn.return_value = mock_client
            hook = SageMakerUnifiedStudioNotebookHook(aws_conn_id=None)
            with pytest.raises(RuntimeError, match="get_notebook_run.*not available"):
                hook._validate_api_availability()

    def test_validate_api_availability_passes_when_apis_present(self):
        """No exception when both required APIs are present on the client."""
        # self.mock_client is a MagicMock which has all attributes by default
        self.hook._validate_api_availability()  # Should not raise
