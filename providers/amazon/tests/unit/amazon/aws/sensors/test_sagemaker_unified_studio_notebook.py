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

from airflow.providers.amazon.aws.sensors.sagemaker_unified_studio_notebook import (
    SageMakerUnifiedStudioNotebookSensor,
)
from airflow.providers.common.compat.sdk import Context

DOMAIN_ID = "dzd_example"
PROJECT_ID = "proj_example"
NOTEBOOK_ID = "nb-1234567890"
NOTEBOOK_RUN_ID = "nr-1234567890"
NOTEBOOK_OUTPUT_PREFIX = "NOTEBOOK_OUTPUT"

HOOK_PATH = (
    "airflow.providers.amazon.aws.sensors.sagemaker_unified_studio_notebook"
    ".SageMakerUnifiedStudioNotebookSensor.hook"
)


class TestSageMakerUnifiedStudioNotebookSensor:
    def test_init(self):
        sensor = SageMakerUnifiedStudioNotebookSensor(
            task_id="test_task",
            domain_identifier=DOMAIN_ID,
            owning_project_identifier=PROJECT_ID,
            notebook_run_id=NOTEBOOK_RUN_ID,
            notebook_identifier=NOTEBOOK_ID,
        )
        assert sensor.domain_identifier == DOMAIN_ID
        assert sensor.owning_project_identifier == PROJECT_ID
        assert sensor.notebook_run_id == NOTEBOOK_RUN_ID
        assert sensor.notebook_identifier == NOTEBOOK_ID
        assert sensor.endpoint_url is None

    def test_init_with_endpoint_url(self):
        sensor = SageMakerUnifiedStudioNotebookSensor(
            task_id="test_task",
            domain_identifier=DOMAIN_ID,
            owning_project_identifier=PROJECT_ID,
            notebook_run_id=NOTEBOOK_RUN_ID,
            notebook_identifier=NOTEBOOK_ID,
            endpoint_url="https://custom.endpoint.example.com",
        )
        assert sensor.endpoint_url == "https://custom.endpoint.example.com"

    def test_hook_property(self):
        from airflow.providers.amazon.aws.hooks.sagemaker_unified_studio_notebook import (
            SageMakerUnifiedStudioNotebookHook,
        )

        sensor = SageMakerUnifiedStudioNotebookSensor(
            task_id="test_task",
            domain_identifier=DOMAIN_ID,
            owning_project_identifier=PROJECT_ID,
            notebook_run_id=NOTEBOOK_RUN_ID,
            notebook_identifier=NOTEBOOK_ID,
            aws_conn_id=None,
        )
        assert isinstance(sensor.hook, SageMakerUnifiedStudioNotebookHook)
        assert sensor.hook.client_type == "datazone"

    @patch(HOOK_PATH, new_callable=PropertyMock)
    def test_poke_success_state(self, mock_hook_prop):
        mock_hook_instance = MagicMock()
        mock_hook_prop.return_value = mock_hook_instance
        mock_hook_instance.get_notebook_run.return_value = {"status": "SUCCEEDED"}

        sensor = SageMakerUnifiedStudioNotebookSensor(
            task_id="test_task",
            domain_identifier=DOMAIN_ID,
            owning_project_identifier=PROJECT_ID,
            notebook_run_id=NOTEBOOK_RUN_ID,
            notebook_identifier=NOTEBOOK_ID,
        )

        result = sensor.poke(context=MagicMock(spec=Context))
        assert result is True
        mock_hook_instance.get_notebook_run.assert_called_once_with(
            NOTEBOOK_RUN_ID, domain_identifier=DOMAIN_ID
        )

    @pytest.mark.parametrize("status", ["QUEUED", "STARTING", "RUNNING", "STOPPING"])
    @patch(HOOK_PATH, new_callable=PropertyMock)
    def test_poke_in_progress_states(self, mock_hook_prop, status):
        mock_hook_instance = MagicMock()
        mock_hook_prop.return_value = mock_hook_instance
        mock_hook_instance.get_notebook_run.return_value = {"status": status}

        sensor = SageMakerUnifiedStudioNotebookSensor(
            task_id="test_task",
            domain_identifier=DOMAIN_ID,
            owning_project_identifier=PROJECT_ID,
            notebook_run_id=NOTEBOOK_RUN_ID,
            notebook_identifier=NOTEBOOK_ID,
        )

        result = sensor.poke(context=MagicMock(spec=Context))
        assert result is False
        mock_hook_instance.get_notebook_run.assert_called_once_with(
            NOTEBOOK_RUN_ID, domain_identifier=DOMAIN_ID
        )

    @patch(HOOK_PATH, new_callable=PropertyMock)
    def test_poke_failed_state(self, mock_hook_prop):
        mock_hook_instance = MagicMock()
        mock_hook_prop.return_value = mock_hook_instance
        mock_hook_instance.get_notebook_run.return_value = {"status": "FAILED"}

        sensor = SageMakerUnifiedStudioNotebookSensor(
            task_id="test_task",
            domain_identifier=DOMAIN_ID,
            owning_project_identifier=PROJECT_ID,
            notebook_run_id=NOTEBOOK_RUN_ID,
            notebook_identifier=NOTEBOOK_ID,
        )

        with pytest.raises(RuntimeError, match=f"Exiting notebook run {NOTEBOOK_RUN_ID}. State: FAILED"):
            sensor.poke(context=MagicMock(spec=Context))

        mock_hook_instance.get_notebook_run.assert_called_once_with(
            NOTEBOOK_RUN_ID, domain_identifier=DOMAIN_ID
        )

    @patch(HOOK_PATH, new_callable=PropertyMock)
    def test_poke_stopped_state(self, mock_hook_prop):
        mock_hook_instance = MagicMock()
        mock_hook_prop.return_value = mock_hook_instance
        mock_hook_instance.get_notebook_run.return_value = {"status": "STOPPED"}

        sensor = SageMakerUnifiedStudioNotebookSensor(
            task_id="test_task",
            domain_identifier=DOMAIN_ID,
            owning_project_identifier=PROJECT_ID,
            notebook_run_id=NOTEBOOK_RUN_ID,
            notebook_identifier=NOTEBOOK_ID,
        )

        with pytest.raises(RuntimeError, match=f"Exiting notebook run {NOTEBOOK_RUN_ID}. State: STOPPED"):
            sensor.poke(context=MagicMock(spec=Context))

    @patch(HOOK_PATH, new_callable=PropertyMock)
    def test_poke_unexpected_state(self, mock_hook_prop):
        mock_hook_instance = MagicMock()
        mock_hook_prop.return_value = mock_hook_instance
        mock_hook_instance.get_notebook_run.return_value = {"status": "UNKNOWN_STATE"}

        sensor = SageMakerUnifiedStudioNotebookSensor(
            task_id="test_task",
            domain_identifier=DOMAIN_ID,
            owning_project_identifier=PROJECT_ID,
            notebook_run_id=NOTEBOOK_RUN_ID,
            notebook_identifier=NOTEBOOK_ID,
        )

        with pytest.raises(
            RuntimeError, match=f"Exiting notebook run {NOTEBOOK_RUN_ID}. State: UNKNOWN_STATE"
        ):
            sensor.poke(context=MagicMock(spec=Context))

    @patch(HOOK_PATH, new_callable=PropertyMock)
    def test_poke_empty_status(self, mock_hook_prop):
        mock_hook_instance = MagicMock()
        mock_hook_prop.return_value = mock_hook_instance
        mock_hook_instance.get_notebook_run.return_value = {}

        sensor = SageMakerUnifiedStudioNotebookSensor(
            task_id="test_task",
            domain_identifier=DOMAIN_ID,
            owning_project_identifier=PROJECT_ID,
            notebook_run_id=NOTEBOOK_RUN_ID,
            notebook_identifier=NOTEBOOK_ID,
        )

        with pytest.raises(RuntimeError, match=f"Exiting notebook run {NOTEBOOK_RUN_ID}. State: "):
            sensor.poke(context=MagicMock(spec=Context))

    # --- execute with notebook outputs ---

    @patch(HOOK_PATH, new_callable=PropertyMock)
    @patch.object(SageMakerUnifiedStudioNotebookSensor, "poke", return_value=True)
    def test_execute_calls_poke_and_reads_outputs(self, mock_poke, mock_hook_prop):
        mock_hook = MagicMock()
        mock_hook_prop.return_value = mock_hook
        mock_hook.get_notebook_outputs.return_value = {"name": "Alice"}

        sensor = SageMakerUnifiedStudioNotebookSensor(
            task_id="test_task",
            domain_identifier=DOMAIN_ID,
            owning_project_identifier=PROJECT_ID,
            notebook_run_id=NOTEBOOK_RUN_ID,
            notebook_identifier=NOTEBOOK_ID,
        )

        context = MagicMock(spec=Context)
        context.__getitem__ = MagicMock(return_value=MagicMock())
        sensor.execute(context=context)

        mock_poke.assert_called_once_with(context)
        mock_hook.get_notebook_outputs.assert_called_once_with(
            notebook_identifier=NOTEBOOK_ID,
            notebook_run_id=NOTEBOOK_RUN_ID,
            owning_project_identifier=PROJECT_ID,
        )
        context["ti"].xcom_push.assert_called_once_with(key=f"{NOTEBOOK_OUTPUT_PREFIX}.name", value="Alice")

    @patch(HOOK_PATH, new_callable=PropertyMock)
    @patch.object(SageMakerUnifiedStudioNotebookSensor, "poke", return_value=True)
    def test_execute_no_outputs_does_not_push_xcom(self, mock_poke, mock_hook_prop):
        mock_hook = MagicMock()
        mock_hook_prop.return_value = mock_hook
        mock_hook.get_notebook_outputs.return_value = {}

        sensor = SageMakerUnifiedStudioNotebookSensor(
            task_id="test_task",
            domain_identifier=DOMAIN_ID,
            owning_project_identifier=PROJECT_ID,
            notebook_run_id=NOTEBOOK_RUN_ID,
            notebook_identifier=NOTEBOOK_ID,
        )

        context = MagicMock(spec=Context)
        context.__getitem__ = MagicMock(return_value=MagicMock())
        sensor.execute(context=context)

        mock_hook.get_notebook_outputs.assert_called_once()
        context["ti"].xcom_push.assert_not_called()

    @patch(HOOK_PATH, new_callable=PropertyMock)
    @patch.object(SageMakerUnifiedStudioNotebookSensor, "poke", return_value=True)
    def test_execute_multiple_outputs_pushed_to_xcom(self, mock_poke, mock_hook_prop):
        mock_hook = MagicMock()
        mock_hook_prop.return_value = mock_hook
        mock_hook.get_notebook_outputs.return_value = {"name": "Alice", "age": 42, "dept": "Engineering"}

        sensor = SageMakerUnifiedStudioNotebookSensor(
            task_id="test_task",
            domain_identifier=DOMAIN_ID,
            owning_project_identifier=PROJECT_ID,
            notebook_run_id=NOTEBOOK_RUN_ID,
            notebook_identifier=NOTEBOOK_ID,
        )

        context = MagicMock(spec=Context)
        context.__getitem__ = MagicMock(return_value=MagicMock())
        sensor.execute(context=context)

        assert context["ti"].xcom_push.call_count == 3
        context["ti"].xcom_push.assert_any_call(key=f"{NOTEBOOK_OUTPUT_PREFIX}.name", value="Alice")
        context["ti"].xcom_push.assert_any_call(key=f"{NOTEBOOK_OUTPUT_PREFIX}.age", value=42)
        context["ti"].xcom_push.assert_any_call(key=f"{NOTEBOOK_OUTPUT_PREFIX}.dept", value="Engineering")
