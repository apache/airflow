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

from airflow.providers.amazon.aws.operators.sagemaker_unified_studio_notebook import (
    SageMakerUnifiedStudioNotebookOperator,
)
from airflow.providers.amazon.aws.triggers.sagemaker_unified_studio_notebook import (
    SageMakerUnifiedStudioNotebookTrigger,
)
from airflow.providers.common.compat.sdk import AirflowException, TaskDeferred

TASK_ID = "test_notebook_run"
NOTEBOOK_ID = "nb-1234567890"
DOMAIN_ID = "dzd_example"
PROJECT_ID = "proj_example"
DAG_ID = "test_dag"
NOTEBOOK_RUN_ID = "run_abc123"
NOTEBOOK_OUTPUT_PREFIX = "NOTEBOOK_OUTPUT"

HOOK_PATH = (
    "airflow.providers.amazon.aws.operators.sagemaker_unified_studio_notebook"
    ".SageMakerUnifiedStudioNotebookOperator.hook"
)


def _make_context(dag_id=DAG_ID):
    """Build a minimal mock context with a dag that has a dag_id."""
    dag = MagicMock()
    dag.dag_id = dag_id
    ti = MagicMock()
    return {"dag": dag, "ti": ti}


class TestSageMakerUnifiedStudioNotebookOperator:
    def test_init_defaults(self):
        op = SageMakerUnifiedStudioNotebookOperator(
            task_id=TASK_ID,
            notebook_identifier=NOTEBOOK_ID,
            domain_identifier=DOMAIN_ID,
            owning_project_identifier=PROJECT_ID,
        )
        assert op.notebook_identifier == NOTEBOOK_ID
        assert op.domain_identifier == DOMAIN_ID
        assert op.owning_project_identifier == PROJECT_ID
        assert op.client_token is None
        assert op.notebook_parameters is None
        assert op.compute_configuration is None
        assert op.timeout_configuration is None
        assert op.wait_for_completion is True
        assert op.waiter_delay == 10
        assert op.deferrable is False

    def test_init_all_params(self):
        op = SageMakerUnifiedStudioNotebookOperator(
            task_id=TASK_ID,
            notebook_identifier=NOTEBOOK_ID,
            domain_identifier=DOMAIN_ID,
            owning_project_identifier=PROJECT_ID,
            client_token="tok-123",
            notebook_parameters={"key": "val"},
            compute_configuration={"instanceType": "ml.m5.large"},
            timeout_configuration={"runTimeoutInMinutes": 60},
            wait_for_completion=False,
            waiter_delay=30,
            deferrable=True,
        )
        assert op.client_token == "tok-123"
        assert op.notebook_parameters == {"key": "val"}
        assert op.compute_configuration == {"instanceType": "ml.m5.large"}
        assert op.timeout_configuration == {"runTimeoutInMinutes": 60}
        assert op.wait_for_completion is False
        assert op.waiter_delay == 30
        assert op.deferrable is True

    # --- hook property ---

    def test_hook_property(self):
        op = SageMakerUnifiedStudioNotebookOperator(
            task_id=TASK_ID,
            notebook_identifier=NOTEBOOK_ID,
            domain_identifier=DOMAIN_ID,
            owning_project_identifier=PROJECT_ID,
            waiter_delay=15,
            timeout_configuration={"runTimeoutInMinutes": 120},
            aws_conn_id=None,
        )
        from airflow.providers.amazon.aws.hooks.sagemaker_unified_studio_notebook import (
            SageMakerUnifiedStudioNotebookHook,
        )

        assert isinstance(op.hook, SageMakerUnifiedStudioNotebookHook)
        assert op.hook.client_type == "datazone"

    # --- execute success ---

    @patch(HOOK_PATH, new_callable=PropertyMock)
    def test_execute_success(self, mock_hook_prop):
        mock_hook = MagicMock()
        mock_hook_prop.return_value = mock_hook
        mock_hook.start_notebook_run.return_value = {"id": NOTEBOOK_RUN_ID}
        mock_hook.wait_for_notebook_run.return_value = {
            "Status": "SUCCEEDED",
            "NotebookRunId": NOTEBOOK_RUN_ID,
        }
        mock_hook.get_notebook_outputs.return_value = {}

        op = SageMakerUnifiedStudioNotebookOperator(
            task_id=TASK_ID,
            notebook_identifier=NOTEBOOK_ID,
            domain_identifier=DOMAIN_ID,
            owning_project_identifier=PROJECT_ID,
            client_token="my-token",
            notebook_parameters={"p1": "v1"},
            compute_configuration={"instanceType": "ml.m5.large"},
            timeout_configuration={"runTimeoutInMinutes": 60},
        )

        result = op.execute(_make_context())

        assert result == {"notebook_run_id": NOTEBOOK_RUN_ID}
        mock_hook.start_notebook_run.assert_called_once_with(
            notebook_identifier=NOTEBOOK_ID,
            domain_identifier=DOMAIN_ID,
            owning_project_identifier=PROJECT_ID,
            client_token="my-token",
            notebook_parameters={"p1": "v1"},
            compute_configuration={"instanceType": "ml.m5.large"},
            timeout_configuration={"runTimeoutInMinutes": 60},
            workflow_name=DAG_ID,
        )
        mock_hook.wait_for_notebook_run.assert_called_once_with(
            NOTEBOOK_RUN_ID,
            domain_identifier=DOMAIN_ID,
            waiter_delay=10,
            timeout_configuration={"runTimeoutInMinutes": 60},
        )

    @patch(HOOK_PATH, new_callable=PropertyMock)
    def test_execute_passes_dag_id_as_workflow_name(self, mock_hook_prop):
        mock_hook = MagicMock()
        mock_hook_prop.return_value = mock_hook
        mock_hook.start_notebook_run.return_value = {"id": NOTEBOOK_RUN_ID}
        mock_hook.wait_for_notebook_run.return_value = {}
        mock_hook.get_notebook_outputs.return_value = {}

        op = SageMakerUnifiedStudioNotebookOperator(
            task_id=TASK_ID,
            notebook_identifier=NOTEBOOK_ID,
            domain_identifier=DOMAIN_ID,
            owning_project_identifier=PROJECT_ID,
        )
        op.execute(_make_context(dag_id="my_custom_dag"))

        call_kwargs = mock_hook.start_notebook_run.call_args[1]
        assert call_kwargs["workflow_name"] == "my_custom_dag"

    # --- execute propagates hook failures ---

    @patch(HOOK_PATH, new_callable=PropertyMock)
    def test_execute_propagates_start_failure(self, mock_hook_prop):
        mock_hook = MagicMock()
        mock_hook_prop.return_value = mock_hook
        mock_hook.start_notebook_run.side_effect = RuntimeError("API unavailable")

        op = SageMakerUnifiedStudioNotebookOperator(
            task_id=TASK_ID,
            notebook_identifier=NOTEBOOK_ID,
            domain_identifier=DOMAIN_ID,
            owning_project_identifier=PROJECT_ID,
        )
        with pytest.raises(RuntimeError, match="API unavailable"):
            op.execute(_make_context())

    @patch(HOOK_PATH, new_callable=PropertyMock)
    def test_execute_propagates_wait_failure(self, mock_hook_prop):
        mock_hook = MagicMock()
        mock_hook_prop.return_value = mock_hook
        mock_hook.start_notebook_run.return_value = {"id": NOTEBOOK_RUN_ID}
        mock_hook.wait_for_notebook_run.side_effect = RuntimeError("Notebook crashed")

        op = SageMakerUnifiedStudioNotebookOperator(
            task_id=TASK_ID,
            notebook_identifier=NOTEBOOK_ID,
            domain_identifier=DOMAIN_ID,
            owning_project_identifier=PROJECT_ID,
        )
        with pytest.raises(RuntimeError, match="Notebook crashed"):
            op.execute(_make_context())

    # --- execute with minimal params (no optionals) ---

    @patch(HOOK_PATH, new_callable=PropertyMock)
    def test_execute_minimal_params(self, mock_hook_prop):
        """Execute with only required params passes None for all optionals."""
        mock_hook = MagicMock()
        mock_hook_prop.return_value = mock_hook
        mock_hook.start_notebook_run.return_value = {"id": NOTEBOOK_RUN_ID}
        mock_hook.wait_for_notebook_run.return_value = {}
        mock_hook.get_notebook_outputs.return_value = {}

        op = SageMakerUnifiedStudioNotebookOperator(
            task_id=TASK_ID,
            notebook_identifier=NOTEBOOK_ID,
            domain_identifier=DOMAIN_ID,
            owning_project_identifier=PROJECT_ID,
        )
        result = op.execute(_make_context())

        assert result == {"notebook_run_id": NOTEBOOK_RUN_ID}
        call_kwargs = mock_hook.start_notebook_run.call_args[1]
        assert call_kwargs["client_token"] is None
        assert call_kwargs["notebook_parameters"] is None
        assert call_kwargs["compute_configuration"] is None
        assert call_kwargs["timeout_configuration"] is None

    # --- wait_for_completion=False ---

    @patch(HOOK_PATH, new_callable=PropertyMock)
    def test_execute_no_wait(self, mock_hook_prop):
        """When wait_for_completion=False, execute returns immediately without polling."""
        mock_hook = MagicMock()
        mock_hook_prop.return_value = mock_hook
        mock_hook.start_notebook_run.return_value = {"id": NOTEBOOK_RUN_ID}

        op = SageMakerUnifiedStudioNotebookOperator(
            task_id=TASK_ID,
            notebook_identifier=NOTEBOOK_ID,
            domain_identifier=DOMAIN_ID,
            owning_project_identifier=PROJECT_ID,
            wait_for_completion=False,
        )
        result = op.execute(_make_context())

        assert result == {"notebook_run_id": NOTEBOOK_RUN_ID}
        mock_hook.start_notebook_run.assert_called_once()
        mock_hook.wait_for_notebook_run.assert_not_called()
        mock_hook.get_notebook_outputs.assert_not_called()

    # --- deferrable mode ---

    @patch(HOOK_PATH, new_callable=PropertyMock)
    def test_execute_deferrable(self, mock_hook_prop):
        """When deferrable=True, execute defers to the trigger instead of polling."""
        mock_hook = MagicMock()
        mock_hook_prop.return_value = mock_hook
        mock_hook.start_notebook_run.return_value = {"id": NOTEBOOK_RUN_ID}

        op = SageMakerUnifiedStudioNotebookOperator(
            task_id=TASK_ID,
            notebook_identifier=NOTEBOOK_ID,
            domain_identifier=DOMAIN_ID,
            owning_project_identifier=PROJECT_ID,
            deferrable=True,
            waiter_delay=20,
            timeout_configuration={"runTimeoutInMinutes": 120},
        )

        with pytest.raises(TaskDeferred) as exc_info:
            op.execute(_make_context())

        trigger = exc_info.value.trigger
        assert isinstance(trigger, SageMakerUnifiedStudioNotebookTrigger)
        assert trigger.notebook_run_id == NOTEBOOK_RUN_ID
        assert trigger.domain_identifier == DOMAIN_ID
        assert trigger.owning_project_identifier == PROJECT_ID
        assert trigger.waiter_delay == 20
        assert trigger.timeout_configuration == {"runTimeoutInMinutes": 120}
        assert exc_info.value.method_name == "execute_complete"
        mock_hook.wait_for_notebook_run.assert_not_called()

    @patch(HOOK_PATH, new_callable=PropertyMock)
    def test_execute_deferrable_overrides_wait_for_completion(self, mock_hook_prop):
        """Deferrable takes precedence over wait_for_completion=False."""
        mock_hook = MagicMock()
        mock_hook_prop.return_value = mock_hook
        mock_hook.start_notebook_run.return_value = {"id": NOTEBOOK_RUN_ID}

        op = SageMakerUnifiedStudioNotebookOperator(
            task_id=TASK_ID,
            notebook_identifier=NOTEBOOK_ID,
            domain_identifier=DOMAIN_ID,
            owning_project_identifier=PROJECT_ID,
            deferrable=True,
            wait_for_completion=False,
        )

        with pytest.raises(TaskDeferred):
            op.execute(_make_context())

        mock_hook.wait_for_notebook_run.assert_not_called()

    # --- execute_complete ---

    @patch(HOOK_PATH, new_callable=PropertyMock)
    def test_execute_complete_success(self, mock_hook_prop):
        mock_hook = MagicMock()
        mock_hook_prop.return_value = mock_hook
        mock_hook.get_notebook_outputs.return_value = {}

        op = SageMakerUnifiedStudioNotebookOperator(
            task_id=TASK_ID,
            notebook_identifier=NOTEBOOK_ID,
            domain_identifier=DOMAIN_ID,
            owning_project_identifier=PROJECT_ID,
        )
        event = {"status": "success", "notebook_run_id": NOTEBOOK_RUN_ID}
        result = op.execute_complete(context=_make_context(), event=event)
        assert result == {"notebook_run_id": NOTEBOOK_RUN_ID}

    def test_execute_complete_failure(self):
        op = SageMakerUnifiedStudioNotebookOperator(
            task_id=TASK_ID,
            notebook_identifier=NOTEBOOK_ID,
            domain_identifier=DOMAIN_ID,
            owning_project_identifier=PROJECT_ID,
        )
        event = {"status": "FAILED", "notebook_run_id": NOTEBOOK_RUN_ID, "message": "OOM"}
        with pytest.raises(RuntimeError, match="Notebook run did not succeed"):
            op.execute_complete(context=_make_context(), event=event)

    def test_execute_complete_none_event(self):
        op = SageMakerUnifiedStudioNotebookOperator(
            task_id=TASK_ID,
            notebook_identifier=NOTEBOOK_ID,
            domain_identifier=DOMAIN_ID,
            owning_project_identifier=PROJECT_ID,
        )
        with pytest.raises(AirflowException, match="event is None"):
            op.execute_complete(context=_make_context(), event=None)

    # --- notebook outputs / xcom push ---

    @patch(HOOK_PATH, new_callable=PropertyMock)
    def test_execute_pushes_notebook_outputs_to_xcom(self, mock_hook_prop):
        """When notebook outputs exist, each key-value pair is pushed to xcom with NOTEBOOK_OUTPUT prefix."""
        mock_hook = MagicMock()
        mock_hook_prop.return_value = mock_hook
        mock_hook.start_notebook_run.return_value = {"id": NOTEBOOK_RUN_ID}
        mock_hook.wait_for_notebook_run.return_value = {
            "Status": "SUCCEEDED",
            "NotebookRunId": NOTEBOOK_RUN_ID,
        }
        mock_hook.get_notebook_outputs.return_value = {"name": "Alice", "age": 42}

        op = SageMakerUnifiedStudioNotebookOperator(
            task_id=TASK_ID,
            notebook_identifier=NOTEBOOK_ID,
            domain_identifier=DOMAIN_ID,
            owning_project_identifier=PROJECT_ID,
        )
        context = _make_context()
        result = op.execute(context)

        assert result == {
            "notebook_run_id": NOTEBOOK_RUN_ID,
            f"{NOTEBOOK_OUTPUT_PREFIX}.name": "Alice",
            f"{NOTEBOOK_OUTPUT_PREFIX}.age": 42,
        }
        mock_hook.get_notebook_outputs.assert_called_once_with(
            notebook_identifier=NOTEBOOK_ID,
            notebook_run_id=NOTEBOOK_RUN_ID,
            owning_project_identifier=PROJECT_ID,
        )
        context["ti"].xcom_push.assert_any_call(key="notebook_run_id", value=NOTEBOOK_RUN_ID)
        context["ti"].xcom_push.assert_any_call(key=f"{NOTEBOOK_OUTPUT_PREFIX}.name", value="Alice")
        context["ti"].xcom_push.assert_any_call(key=f"{NOTEBOOK_OUTPUT_PREFIX}.age", value=42)
        assert (
            context["ti"].xcom_push.call_count == 4
        )  # sagemaker_unified_studio + notebook_run_id + 2 outputs

    @patch(HOOK_PATH, new_callable=PropertyMock)
    def test_execute_no_outputs_does_not_push_xcom(self, mock_hook_prop):
        """When no notebook outputs exist, only notebook_run_id is pushed to xcom."""
        mock_hook = MagicMock()
        mock_hook_prop.return_value = mock_hook
        mock_hook.start_notebook_run.return_value = {"id": NOTEBOOK_RUN_ID}
        mock_hook.wait_for_notebook_run.return_value = {}
        mock_hook.get_notebook_outputs.return_value = {}

        op = SageMakerUnifiedStudioNotebookOperator(
            task_id=TASK_ID,
            notebook_identifier=NOTEBOOK_ID,
            domain_identifier=DOMAIN_ID,
            owning_project_identifier=PROJECT_ID,
        )
        context = _make_context()
        result = op.execute(context)

        assert result == {"notebook_run_id": NOTEBOOK_RUN_ID}
        context["ti"].xcom_push.assert_any_call(key="notebook_run_id", value=NOTEBOOK_RUN_ID)
        # sagemaker_unified_studio link is also pushed by persist()
        assert context["ti"].xcom_push.call_count == 2

    @patch(HOOK_PATH, new_callable=PropertyMock)
    def test_execute_no_wait_skips_outputs(self, mock_hook_prop):
        """When wait_for_completion=False, notebook outputs are not read."""
        mock_hook = MagicMock()
        mock_hook_prop.return_value = mock_hook
        mock_hook.start_notebook_run.return_value = {"id": NOTEBOOK_RUN_ID}

        op = SageMakerUnifiedStudioNotebookOperator(
            task_id=TASK_ID,
            notebook_identifier=NOTEBOOK_ID,
            domain_identifier=DOMAIN_ID,
            owning_project_identifier=PROJECT_ID,
            wait_for_completion=False,
        )
        context = _make_context()
        result = op.execute(context)

        assert result == {"notebook_run_id": NOTEBOOK_RUN_ID}
        mock_hook.get_notebook_outputs.assert_not_called()

    @patch(HOOK_PATH, new_callable=PropertyMock)
    def test_execute_complete_pushes_notebook_outputs_to_xcom(self, mock_hook_prop):
        """execute_complete reads outputs from S3 and pushes to xcom with NOTEBOOK_OUTPUT prefix."""
        mock_hook = MagicMock()
        mock_hook_prop.return_value = mock_hook
        mock_hook.get_notebook_outputs.return_value = {"result": "success_value"}

        op = SageMakerUnifiedStudioNotebookOperator(
            task_id=TASK_ID,
            notebook_identifier=NOTEBOOK_ID,
            domain_identifier=DOMAIN_ID,
            owning_project_identifier=PROJECT_ID,
        )
        context = _make_context()
        event = {"status": "success", "notebook_run_id": NOTEBOOK_RUN_ID}
        result = op.execute_complete(context=context, event=event)

        assert result == {
            "notebook_run_id": NOTEBOOK_RUN_ID,
            f"{NOTEBOOK_OUTPUT_PREFIX}.result": "success_value",
        }
        context["ti"].xcom_push.assert_any_call(key="notebook_run_id", value=NOTEBOOK_RUN_ID)
        context["ti"].xcom_push.assert_any_call(key=f"{NOTEBOOK_OUTPUT_PREFIX}.result", value="success_value")

    @patch(HOOK_PATH, new_callable=PropertyMock)
    def test_execute_complete_no_outputs(self, mock_hook_prop):
        """execute_complete with no outputs returns only notebook_run_id and pushes it to xcom."""
        mock_hook = MagicMock()
        mock_hook_prop.return_value = mock_hook
        mock_hook.get_notebook_outputs.return_value = {}

        op = SageMakerUnifiedStudioNotebookOperator(
            task_id=TASK_ID,
            notebook_identifier=NOTEBOOK_ID,
            domain_identifier=DOMAIN_ID,
            owning_project_identifier=PROJECT_ID,
        )
        context = _make_context()
        event = {"status": "success", "notebook_run_id": NOTEBOOK_RUN_ID}
        result = op.execute_complete(context=context, event=event)

        assert result == {"notebook_run_id": NOTEBOOK_RUN_ID}
        context["ti"].xcom_push.assert_called_once_with(key="notebook_run_id", value=NOTEBOOK_RUN_ID)
