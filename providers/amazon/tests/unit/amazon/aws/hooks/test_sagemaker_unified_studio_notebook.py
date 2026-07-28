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

import json
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
            notebook_identifier=NOTEBOOK_ID, domain_identifier=DOMAIN_ID, owning_project_identifier=PROJECT_ID
        )

        assert result == {"notebookRunId": NOTEBOOK_RUN_ID}
        call_kwargs = self.mock_client.start_notebook_run.call_args[1]
        assert call_kwargs["domainIdentifier"] == DOMAIN_ID
        assert call_kwargs["owningProjectIdentifier"] == PROJECT_ID
        assert call_kwargs["notebookIdentifier"] == NOTEBOOK_ID
        assert "clientToken" in call_kwargs
        # Optional params should not be present
        assert "parameters" not in call_kwargs
        assert "computeConfiguration" not in call_kwargs
        assert "timeoutConfiguration" not in call_kwargs
        assert "triggerSource" not in call_kwargs

    def test_start_notebook_run_all_params(self):
        """Start run with all optional params provided."""
        self.mock_client.start_notebook_run.return_value = {"notebookRunId": NOTEBOOK_RUN_ID}

        result = self.hook.start_notebook_run(
            notebook_identifier=NOTEBOOK_ID,
            domain_identifier=DOMAIN_ID,
            owning_project_identifier=PROJECT_ID,
            client_token="my-token",
            notebook_parameters={"param1": "value1"},
            compute_configuration={"instanceType": "ml.m5.large"},
            timeout_configuration={"runTimeoutInMinutes": 120},
            workflow_name="my_dag",
        )

        assert result == {"notebookRunId": NOTEBOOK_RUN_ID}
        call_kwargs = self.mock_client.start_notebook_run.call_args[1]
        assert call_kwargs["clientToken"] == "my-token"
        assert call_kwargs["parameters"] == {"param1": "value1"}
        assert call_kwargs["computeConfiguration"] == {"instanceType": "ml.m5.large"}
        assert call_kwargs["timeoutConfiguration"] == {"runTimeoutInMinutes": 120}
        assert call_kwargs["triggerSource"] == {"type": "WORKFLOW", "name": "my_dag"}

    def test_start_notebook_run_auto_generates_client_token(self):
        """client_token is auto-generated as a UUID when not provided."""
        self.mock_client.start_notebook_run.return_value = {}

        self.hook.start_notebook_run(
            notebook_identifier=NOTEBOOK_ID, domain_identifier=DOMAIN_ID, owning_project_identifier=PROJECT_ID
        )

        call_kwargs = self.mock_client.start_notebook_run.call_args[1]
        token = call_kwargs["clientToken"]
        # UUID4 format: 8-4-4-4-12 hex chars
        assert len(token) == 36
        assert token.count("-") == 4

    # --- get_notebook_run ---

    def test_get_notebook_run(self):
        """get_notebook_run passes correct params to the client."""
        expected = {"status": "SUCCEEDED", "notebookRunId": NOTEBOOK_RUN_ID}
        self.mock_client.get_notebook_run.return_value = expected

        result = self.hook.get_notebook_run(NOTEBOOK_RUN_ID, domain_identifier=DOMAIN_ID)

        assert result == expected
        self.mock_client.get_notebook_run.assert_called_once_with(
            domainIdentifier=DOMAIN_ID,
            identifier=NOTEBOOK_RUN_ID,
        )

    # --- _handle_status ---

    def test_handle_status_in_progress(self):
        """In-progress statuses return None."""
        for status in ("QUEUED", "STARTING", "RUNNING", "STOPPING"):
            result = self.hook._handle_status(NOTEBOOK_RUN_ID, status, "")
            assert result is None

    def test_handle_status_succeeded(self):
        """SUCCEEDED status returns success dict."""
        result = self.hook._handle_status(NOTEBOOK_RUN_ID, "SUCCEEDED", "")
        assert result == {"Status": "SUCCEEDED", "NotebookRunId": NOTEBOOK_RUN_ID}

    def test_handle_status_stopped(self):
        """STOPPED is a terminal non-success status and raises RuntimeError."""
        with pytest.raises(RuntimeError):
            self.hook._handle_status(NOTEBOOK_RUN_ID, "STOPPED", "")

    def test_handle_status_failed_with_error_message(self):
        """FAILED status with error message raises RuntimeError with that message."""
        with pytest.raises(RuntimeError, match="Something went wrong"):
            self.hook._handle_status(NOTEBOOK_RUN_ID, "FAILED", "Something went wrong")

    def test_handle_status_failed_empty_error_message(self):
        """FAILED status with empty error message raises RuntimeError with status info."""
        with pytest.raises(RuntimeError, match=f"Exiting notebook run {NOTEBOOK_RUN_ID}. Status: FAILED"):
            self.hook._handle_status(NOTEBOOK_RUN_ID, "FAILED", "")

    def test_handle_status_unexpected(self):
        """Unexpected status raises RuntimeError."""
        with pytest.raises(RuntimeError, match=f"Exiting notebook run {NOTEBOOK_RUN_ID}. Status: UNKNOWN"):
            self.hook._handle_status(NOTEBOOK_RUN_ID, "UNKNOWN", "")

    # --- wait_for_notebook_run ---

    @patch(f"{HOOK_MODULE}.time.sleep")
    def test_wait_for_notebook_run_immediate_success(self, mock_sleep):
        """Run completes on first poll."""
        self.mock_client.get_notebook_run.return_value = {"status": "SUCCEEDED"}

        result = self.hook.wait_for_notebook_run(NOTEBOOK_RUN_ID, domain_identifier=DOMAIN_ID, waiter_delay=5)

        assert result == {"Status": "SUCCEEDED", "NotebookRunId": NOTEBOOK_RUN_ID}
        mock_sleep.assert_not_called()

    @patch(f"{HOOK_MODULE}.time.sleep")
    def test_wait_for_notebook_run_polls_then_succeeds(self, mock_sleep):
        """Run is in progress for a few polls then completes."""
        self.mock_client.get_notebook_run.side_effect = [
            {"status": "QUEUED"},
            {"status": "STARTING"},
            {"status": "RUNNING"},
            {"status": "SUCCEEDED"},
        ]

        result = self.hook.wait_for_notebook_run(NOTEBOOK_RUN_ID, domain_identifier=DOMAIN_ID, waiter_delay=5)

        assert result == {"Status": "SUCCEEDED", "NotebookRunId": NOTEBOOK_RUN_ID}
        assert mock_sleep.call_count == 3

    @patch(f"{HOOK_MODULE}.time.sleep")
    def test_wait_for_notebook_run_fails(self, mock_sleep):
        """Run fails with an error message."""
        self.mock_client.get_notebook_run.return_value = {
            "status": "FAILED",
            "errorMessage": "Notebook crashed",
        }

        with pytest.raises(RuntimeError, match="Notebook crashed"):
            self.hook.wait_for_notebook_run(NOTEBOOK_RUN_ID, domain_identifier=DOMAIN_ID, waiter_delay=5)

    @patch(f"{HOOK_MODULE}.time.sleep")
    def test_wait_for_notebook_run_timeout(self, mock_sleep):
        """Run times out after max attempts."""
        self.mock_client.get_notebook_run.return_value = {"status": "RUNNING"}

        with pytest.raises(RuntimeError, match="Execution timed out"):
            self.hook.wait_for_notebook_run(
                NOTEBOOK_RUN_ID,
                domain_identifier=DOMAIN_ID,
                waiter_delay=5,
                timeout_configuration={"runTimeoutInMinutes": 1},
            )

        assert self.mock_client.get_notebook_run.call_count == 12

    @patch(f"{HOOK_MODULE}.time.sleep")
    def test_wait_for_notebook_run_default_timeout(self, mock_sleep):
        """Default waiter_max_attempts derived from 12-hour timeout."""
        self.mock_client.get_notebook_run.return_value = {"status": "SUCCEEDED"}

        result = self.hook.wait_for_notebook_run(
            NOTEBOOK_RUN_ID, domain_identifier=DOMAIN_ID, waiter_delay=10
        )

        assert result == {"Status": "SUCCEEDED", "NotebookRunId": NOTEBOOK_RUN_ID}

    @patch(f"{HOOK_MODULE}.time.sleep")
    def test_wait_for_notebook_run_empty_timeout_configuration(self, mock_sleep):
        """Empty timeout_configuration falls back to default 12-hour timeout."""
        self.mock_client.get_notebook_run.return_value = {"status": "SUCCEEDED"}

        result = self.hook.wait_for_notebook_run(
            NOTEBOOK_RUN_ID, domain_identifier=DOMAIN_ID, waiter_delay=10, timeout_configuration={}
        )

        assert result == {"Status": "SUCCEEDED", "NotebookRunId": NOTEBOOK_RUN_ID}

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

    def test_start_notebook_run_raises_when_api_unavailable(self):
        """start_notebook_run fails fast when the API is not available on the client."""
        mock_client = MagicMock(spec=[])
        with patch.object(SageMakerUnifiedStudioNotebookHook, "conn", new_callable=PropertyMock) as mock_conn:
            mock_conn.return_value = mock_client
            hook = SageMakerUnifiedStudioNotebookHook(aws_conn_id=None)
            with pytest.raises(RuntimeError, match="not available"):
                hook.start_notebook_run(
                    notebook_identifier=NOTEBOOK_ID,
                    domain_identifier=DOMAIN_ID,
                    owning_project_identifier=PROJECT_ID,
                )

    def test_get_notebook_run_raises_when_api_unavailable(self):
        """get_notebook_run fails fast when the API is not available on the client."""
        mock_client = MagicMock(spec=[])
        with patch.object(SageMakerUnifiedStudioNotebookHook, "conn", new_callable=PropertyMock) as mock_conn:
            mock_conn.return_value = mock_client
            hook = SageMakerUnifiedStudioNotebookHook(aws_conn_id=None)
            with pytest.raises(RuntimeError, match="not available"):
                hook.get_notebook_run(NOTEBOOK_RUN_ID, domain_identifier=DOMAIN_ID)

    # --- get_project_s3_path ---

    def _stub_tooling_blueprint_lookup(
        self,
        environments: list[dict] | None = None,
        tooling_lite_environments: list[dict] | None = None,
    ) -> None:
        """Set up list_environment_blueprints + list_environments for the tooling lookup.

        Returns the Tooling blueprint with id ``bp-tooling`` and lists ``environments``
        for it. If ``tooling_lite_environments`` is provided, also returns a
        ToolingLite blueprint (``bp-tooling-lite``) and lists those for it.
        """
        environments = environments or []

        def list_envs(**kwargs):
            blueprint_id = kwargs.get("environmentBlueprintIdentifier")
            if blueprint_id == "bp-tooling":
                return {"items": environments}
            if blueprint_id == "bp-tooling-lite":
                return {"items": tooling_lite_environments or []}
            return {"items": []}

        def list_blueprints(**kwargs):
            name = kwargs.get("name")
            if name == "Tooling":
                return {"items": [{"id": "bp-tooling", "name": "Tooling"}]}
            if name == "ToolingLite":
                return {"items": [{"id": "bp-tooling-lite", "name": "ToolingLite"}]}
            return {"items": []}

        self.mock_client.list_environment_blueprints.side_effect = list_blueprints
        self.mock_client.list_environments.side_effect = list_envs

    def test_get_project_s3_path_uses_default_tooling_environment(self):
        """Resolves bucket name from the default tooling environment's s3BucketPath."""
        env_id = "env-tooling-1"
        bucket = "my-byor-bucket"
        self._stub_tooling_blueprint_lookup(
            environments=[
                {"id": "env-other", "name": "Tooling", "deploymentOrder": 5},
                {"id": env_id, "name": "Tooling", "deploymentOrder": 1},
            ]
        )
        self.mock_client.get_environment.return_value = {
            "id": env_id,
            "provisionedResources": [
                {"name": "userRoleArn", "value": "arn:aws:iam::123:role/foo"},
                {"name": "s3BucketPath", "value": f"s3://{bucket}/dzd_x/{PROJECT_ID}/dev"},
            ],
        }

        result = self.hook.get_project_s3_path(DOMAIN_ID, PROJECT_ID)

        assert result == (bucket, f"dzd_x/{PROJECT_ID}/dev")
        self.mock_client.list_environment_blueprints.assert_any_call(
            domainIdentifier=DOMAIN_ID,
            managed=True,
            name="Tooling",
        )
        self.mock_client.list_environments.assert_called_once_with(
            domainIdentifier=DOMAIN_ID,
            projectIdentifier=PROJECT_ID,
            environmentBlueprintIdentifier="bp-tooling",
        )
        self.mock_client.get_environment.assert_called_once_with(
            domainIdentifier=DOMAIN_ID,
            identifier=env_id,
        )

    def test_get_project_s3_path_picks_lowest_deployment_order(self):
        """Picks the env with the lowest non-null deploymentOrder, ignoring None."""
        env_id = "env-tooling-default"
        bucket = "default-bucket"
        self._stub_tooling_blueprint_lookup(
            environments=[
                {"id": "env-no-order", "name": "Tooling", "deploymentOrder": None},
                {"id": "env-other", "name": "Tooling", "deploymentOrder": 9},
                {"id": env_id, "name": "Tooling", "deploymentOrder": 1},
            ]
        )
        self.mock_client.get_environment.return_value = {
            "id": env_id,
            "provisionedResources": [{"name": "s3BucketPath", "value": f"s3://{bucket}/p"}],
        }

        result = self.hook.get_project_s3_path(DOMAIN_ID, PROJECT_ID)

        assert result == (bucket, "p")
        self.mock_client.get_environment.assert_called_once_with(
            domainIdentifier=DOMAIN_ID,
            identifier=env_id,
        )

    def test_get_project_s3_path_falls_back_to_tooling_lite(self):
        """Falls back to ToolingLite when the Tooling blueprint has no envs."""
        env_id = "env-lite-1"
        bucket = "lite-bucket"
        self._stub_tooling_blueprint_lookup(
            environments=[],
            tooling_lite_environments=[{"id": env_id, "name": "ToolingLite", "deploymentOrder": 1}],
        )
        self.mock_client.get_environment.return_value = {
            "id": env_id,
            "provisionedResources": [{"name": "s3BucketPath", "value": f"s3://{bucket}/p"}],
        }

        result = self.hook.get_project_s3_path(DOMAIN_ID, PROJECT_ID)

        assert result == (bucket, "p")
        # Both blueprint lookups happened.
        assert self.mock_client.list_environment_blueprints.call_count == 2
        self.mock_client.get_environment.assert_called_once_with(
            domainIdentifier=DOMAIN_ID,
            identifier=env_id,
        )

    def test_get_project_s3_path_falls_back_to_first_when_no_deployment_order(self):
        """When envs exist but none has deploymentOrder, returns the first env."""
        env_id = "env-tooling-1"
        bucket = "first-bucket"
        self._stub_tooling_blueprint_lookup(
            environments=[
                {"id": env_id, "name": "AmazonSagemakerEnvironmentConfig-x"},
                {"id": "env-tooling-2", "name": "AmazonSagemakerEnvironmentConfig-y"},
            ]
        )
        self.mock_client.get_environment.return_value = {
            "id": env_id,
            "provisionedResources": [{"name": "s3BucketPath", "value": f"s3://{bucket}/p"}],
        }

        result = self.hook.get_project_s3_path(DOMAIN_ID, PROJECT_ID)

        assert result == (bucket, "p")
        self.mock_client.get_environment.assert_called_once_with(
            domainIdentifier=DOMAIN_ID,
            identifier=env_id,
        )

    def test_get_project_s3_path_raises_when_no_environments_for_either_blueprint(self):
        """Raises RuntimeError when neither Tooling nor ToolingLite has any envs."""
        self._stub_tooling_blueprint_lookup(environments=[], tooling_lite_environments=[])
        with pytest.raises(RuntimeError, match="No default Tooling or ToolingLite environment found"):
            self.hook.get_project_s3_path(DOMAIN_ID, PROJECT_ID)

    def test_get_project_s3_path_raises_when_blueprint_missing(self):
        """Raises RuntimeError when the Tooling blueprint is not registered in the domain."""
        self.mock_client.list_environment_blueprints.return_value = {"items": []}
        with pytest.raises(RuntimeError, match="Tooling environment blueprint not found"):
            self.hook.get_project_s3_path(DOMAIN_ID, PROJECT_ID)

    def test_get_project_s3_path_raises_when_resource_missing(self):
        """Raises RuntimeError when s3BucketPath is not in provisionedResources."""
        self._stub_tooling_blueprint_lookup(
            environments=[{"id": "env-1", "name": "Tooling", "deploymentOrder": 1}]
        )
        self.mock_client.get_environment.return_value = {
            "id": "env-1",
            "provisionedResources": [{"name": "userRoleArn", "value": "arn:aws:iam::123:role/foo"}],
        }
        with pytest.raises(RuntimeError, match="s3BucketPath provisioned resource not found"):
            self.hook.get_project_s3_path(DOMAIN_ID, PROJECT_ID)

    def test_get_project_s3_path_raises_when_resource_value_empty(self):
        """Raises RuntimeError when s3BucketPath is present but empty."""
        self._stub_tooling_blueprint_lookup(
            environments=[{"id": "env-1", "name": "Tooling", "deploymentOrder": 1}]
        )
        self.mock_client.get_environment.return_value = {
            "id": "env-1",
            "provisionedResources": [{"name": "s3BucketPath", "value": ""}],
        }
        with pytest.raises(RuntimeError, match="s3BucketPath provisioned resource is empty"):
            self.hook.get_project_s3_path(DOMAIN_ID, PROJECT_ID)

    def test_get_project_s3_path_raises_on_malformed_uri(self):
        """Raises RuntimeError when s3BucketPath has an unexpected format."""
        self._stub_tooling_blueprint_lookup(
            environments=[{"id": "env-1", "name": "Tooling", "deploymentOrder": 1}]
        )
        self.mock_client.get_environment.return_value = {
            "id": "env-1",
            "provisionedResources": [{"name": "s3BucketPath", "value": "not-an-s3-uri"}],
        }
        with pytest.raises(RuntimeError, match="unexpected format"):
            self.hook.get_project_s3_path(DOMAIN_ID, PROJECT_ID)

    def test_get_project_s3_path_wraps_client_error(self):
        """Wraps boto ClientError from DataZone APIs in RuntimeError."""
        from botocore.exceptions import ClientError

        error_response = {"Error": {"Code": "AccessDenied", "Message": "no access"}}
        self.mock_client.list_environment_blueprints.side_effect = ClientError(
            error_response, "ListEnvironmentBlueprints"
        )
        with pytest.raises(RuntimeError, match="Failed to resolve default tooling environment"):
            self.hook.get_project_s3_path(DOMAIN_ID, PROJECT_ID)

    # --- get_notebook_outputs ---

    def _stub_project_bucket(self, bucket: str = "test-bucket") -> None:
        """Configure the mock client to resolve the project bucket to ``bucket``."""
        self._stub_tooling_blueprint_lookup(
            environments=[{"id": "env-1", "name": "Tooling", "deploymentOrder": 1}]
        )
        self.mock_client.get_environment.return_value = {
            "id": "env-1",
            "provisionedResources": [
                {"name": "s3BucketPath", "value": f"s3://{bucket}/dzd_x/{PROJECT_ID}/dev"},
            ],
        }

    def test_get_notebook_outputs_success(self):
        """Reads and parses JSON outputs from S3."""
        outputs = {"name": "Alice", "age": 42}
        bucket = "test-bucket"
        self._stub_project_bucket(bucket)

        with patch(f"{HOOK_MODULE}.S3Hook") as mock_s3_hook_cls:
            mock_s3_hook_cls.return_value.read_key.return_value = json.dumps(outputs)
            result = self.hook.get_notebook_outputs(
                notebook_identifier=NOTEBOOK_ID,
                notebook_run_id=NOTEBOOK_RUN_ID,
                domain_identifier=DOMAIN_ID,
                owning_project_identifier=PROJECT_ID,
            )

        assert result == outputs
        expected_key = f"dzd_x/{PROJECT_ID}/dev/.sys/notebooks/{NOTEBOOK_ID}/runs/{NOTEBOOK_RUN_ID}/notebook_outputs.json"
        mock_s3_hook_cls.return_value.read_key.assert_called_once_with(key=expected_key, bucket_name=bucket)

    def test_get_notebook_outputs_iam_mode_no_prefix(self):
        """IAM-mode projects (s3BucketPath is bucket-only) read from the bucket root."""
        outputs = {"name": "Alice"}
        bucket = "iam-mode-bucket"
        # Tooling env returns s3BucketPath without a path component.
        self._stub_tooling_blueprint_lookup(
            environments=[{"id": "env-1", "name": "Tooling", "deploymentOrder": 1}]
        )
        self.mock_client.get_environment.return_value = {
            "id": "env-1",
            "provisionedResources": [{"name": "s3BucketPath", "value": f"s3://{bucket}"}],
        }

        with patch(f"{HOOK_MODULE}.S3Hook") as mock_s3_hook_cls:
            mock_s3_hook_cls.return_value.read_key.return_value = json.dumps(outputs)
            result = self.hook.get_notebook_outputs(
                notebook_identifier=NOTEBOOK_ID,
                notebook_run_id=NOTEBOOK_RUN_ID,
                domain_identifier=DOMAIN_ID,
                owning_project_identifier=PROJECT_ID,
            )

        assert result == outputs
        expected_key = f".sys/notebooks/{NOTEBOOK_ID}/runs/{NOTEBOOK_RUN_ID}/notebook_outputs.json"
        mock_s3_hook_cls.return_value.read_key.assert_called_once_with(key=expected_key, bucket_name=bucket)

    def test_get_notebook_outputs_no_such_key(self):
        """Returns empty dict when the outputs file does not exist in S3."""
        from botocore.exceptions import ClientError

        error_response = {"Error": {"Code": "NoSuchKey", "Message": "Not found"}}
        self._stub_project_bucket()

        with patch(f"{HOOK_MODULE}.S3Hook") as mock_s3_hook_cls:
            mock_s3_hook_cls.return_value.read_key.side_effect = ClientError(error_response, "GetObject")
            result = self.hook.get_notebook_outputs(
                notebook_identifier=NOTEBOOK_ID,
                notebook_run_id=NOTEBOOK_RUN_ID,
                domain_identifier=DOMAIN_ID,
                owning_project_identifier=PROJECT_ID,
            )

        assert result == {}

    def test_get_notebook_outputs_404_error(self):
        """Returns empty dict when S3 returns a 404 HTTP error code."""
        from botocore.exceptions import ClientError

        error_response = {"Error": {"Code": "404", "Message": "Not Found"}}
        self._stub_project_bucket()

        with patch(f"{HOOK_MODULE}.S3Hook") as mock_s3_hook_cls:
            mock_s3_hook_cls.return_value.read_key.side_effect = ClientError(error_response, "HeadObject")
            result = self.hook.get_notebook_outputs(
                notebook_identifier=NOTEBOOK_ID,
                notebook_run_id=NOTEBOOK_RUN_ID,
                domain_identifier=DOMAIN_ID,
                owning_project_identifier=PROJECT_ID,
            )

        assert result == {}

    def test_get_notebook_outputs_invalid_json(self):
        """Returns empty dict when S3 file contains invalid JSON."""
        self._stub_project_bucket()

        with patch(f"{HOOK_MODULE}.S3Hook") as mock_s3_hook_cls:
            mock_s3_hook_cls.return_value.read_key.return_value = "not valid json {{{"
            result = self.hook.get_notebook_outputs(
                notebook_identifier=NOTEBOOK_ID,
                notebook_run_id=NOTEBOOK_RUN_ID,
                domain_identifier=DOMAIN_ID,
                owning_project_identifier=PROJECT_ID,
            )

        assert result == {}

    def test_get_notebook_outputs_non_dict_json(self):
        """Returns empty dict when S3 file contains valid JSON but not a dict."""
        self._stub_project_bucket()

        with patch(f"{HOOK_MODULE}.S3Hook") as mock_s3_hook_cls:
            mock_s3_hook_cls.return_value.read_key.return_value = json.dumps(["a", "b"])
            result = self.hook.get_notebook_outputs(
                notebook_identifier=NOTEBOOK_ID,
                notebook_run_id=NOTEBOOK_RUN_ID,
                domain_identifier=DOMAIN_ID,
                owning_project_identifier=PROJECT_ID,
            )

        assert result == {}

    def test_get_notebook_outputs_unexpected_exception(self):
        """Returns empty dict on unexpected S3 errors."""
        self._stub_project_bucket()

        with patch(f"{HOOK_MODULE}.S3Hook") as mock_s3_hook_cls:
            mock_s3_hook_cls.return_value.read_key.side_effect = ConnectionError("Network issue")
            result = self.hook.get_notebook_outputs(
                notebook_identifier=NOTEBOOK_ID,
                notebook_run_id=NOTEBOOK_RUN_ID,
                domain_identifier=DOMAIN_ID,
                owning_project_identifier=PROJECT_ID,
            )

        assert result == {}

    def test_get_notebook_outputs_empty_dict(self):
        """Returns empty dict when S3 file contains an empty JSON object."""
        self._stub_project_bucket()

        with patch(f"{HOOK_MODULE}.S3Hook") as mock_s3_hook_cls:
            mock_s3_hook_cls.return_value.read_key.return_value = json.dumps({})
            result = self.hook.get_notebook_outputs(
                notebook_identifier=NOTEBOOK_ID,
                notebook_run_id=NOTEBOOK_RUN_ID,
                domain_identifier=DOMAIN_ID,
                owning_project_identifier=PROJECT_ID,
            )

        assert result == {}

    def test_get_notebook_outputs_returns_empty_when_bucket_resolution_fails(self):
        """Returns empty dict when DataZone APIs fail to resolve the project bucket."""
        from botocore.exceptions import ClientError

        error_response = {"Error": {"Code": "AccessDenied", "Message": "no access"}}
        self.mock_client.list_environments.side_effect = ClientError(error_response, "ListEnvironments")

        with patch(f"{HOOK_MODULE}.S3Hook") as mock_s3_hook_cls:
            result = self.hook.get_notebook_outputs(
                notebook_identifier=NOTEBOOK_ID,
                notebook_run_id=NOTEBOOK_RUN_ID,
                domain_identifier=DOMAIN_ID,
                owning_project_identifier=PROJECT_ID,
            )

        assert result == {}
        # S3Hook is not even instantiated when bucket cannot be resolved.
        mock_s3_hook_cls.assert_not_called()
