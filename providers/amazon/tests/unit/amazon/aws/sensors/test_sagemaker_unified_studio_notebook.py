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
NOTEBOOK_RUN_ID = "nr-1234567890"

HOOK_PATH = (
    "airflow.providers.amazon.aws.sensors.sagemaker_unified_studio_notebook"
    ".SageMakerUnifiedStudioNotebookSensor.hook"
)


class TestSageMakerUnifiedStudioNotebookSensor:
    def test_init(self):
        sensor = SageMakerUnifiedStudioNotebookSensor(
            task_id="test_task",
            domain_id=DOMAIN_ID,
            project_id=PROJECT_ID,
            notebook_run_id=NOTEBOOK_RUN_ID,
        )
        assert sensor.domain_id == DOMAIN_ID
        assert sensor.project_id == PROJECT_ID
        assert sensor.notebook_run_id == NOTEBOOK_RUN_ID
        assert sensor.success_states == ["SUCCEEDED"]
        assert sensor.in_progress_states == ["QUEUED", "STARTING", "RUNNING", "STOPPING"]

    def test_hook_property(self):
        from airflow.providers.amazon.aws.hooks.sagemaker_unified_studio_notebook import (
            SageMakerUnifiedStudioNotebookHook,
        )

        sensor = SageMakerUnifiedStudioNotebookSensor(
            task_id="test_task",
            domain_id=DOMAIN_ID,
            project_id=PROJECT_ID,
            notebook_run_id=NOTEBOOK_RUN_ID,
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
            domain_id=DOMAIN_ID,
            project_id=PROJECT_ID,
            notebook_run_id=NOTEBOOK_RUN_ID,
        )

        result = sensor.poke(context=MagicMock(spec=Context))
        assert result is True
        mock_hook_instance.get_notebook_run.assert_called_once_with(NOTEBOOK_RUN_ID, domain_id=DOMAIN_ID)

    @pytest.mark.parametrize("status", ["QUEUED", "STARTING", "RUNNING", "STOPPING"])
    @patch(HOOK_PATH, new_callable=PropertyMock)
    def test_poke_in_progress_states(self, mock_hook_prop, status):
        mock_hook_instance = MagicMock()
        mock_hook_prop.return_value = mock_hook_instance
        mock_hook_instance.get_notebook_run.return_value = {"status": status}

        sensor = SageMakerUnifiedStudioNotebookSensor(
            task_id="test_task",
            domain_id=DOMAIN_ID,
            project_id=PROJECT_ID,
            notebook_run_id=NOTEBOOK_RUN_ID,
        )

        result = sensor.poke(context=MagicMock(spec=Context))
        assert result is False
        mock_hook_instance.get_notebook_run.assert_called_once_with(NOTEBOOK_RUN_ID, domain_id=DOMAIN_ID)

    @patch(HOOK_PATH, new_callable=PropertyMock)
    def test_poke_failed_state(self, mock_hook_prop):
        mock_hook_instance = MagicMock()
        mock_hook_prop.return_value = mock_hook_instance
        mock_hook_instance.get_notebook_run.return_value = {"status": "FAILED"}

        sensor = SageMakerUnifiedStudioNotebookSensor(
            task_id="test_task",
            domain_id=DOMAIN_ID,
            project_id=PROJECT_ID,
            notebook_run_id=NOTEBOOK_RUN_ID,
        )

        with pytest.raises(RuntimeError, match=f"Exiting notebook run {NOTEBOOK_RUN_ID}. State: FAILED"):
            sensor.poke(context=MagicMock(spec=Context))

        mock_hook_instance.get_notebook_run.assert_called_once_with(NOTEBOOK_RUN_ID, domain_id=DOMAIN_ID)

    @patch(HOOK_PATH, new_callable=PropertyMock)
    def test_poke_stopped_state(self, mock_hook_prop):
        mock_hook_instance = MagicMock()
        mock_hook_prop.return_value = mock_hook_instance
        mock_hook_instance.get_notebook_run.return_value = {"status": "STOPPED"}

        sensor = SageMakerUnifiedStudioNotebookSensor(
            task_id="test_task",
            domain_id=DOMAIN_ID,
            project_id=PROJECT_ID,
            notebook_run_id=NOTEBOOK_RUN_ID,
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
            domain_id=DOMAIN_ID,
            project_id=PROJECT_ID,
            notebook_run_id=NOTEBOOK_RUN_ID,
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
            domain_id=DOMAIN_ID,
            project_id=PROJECT_ID,
            notebook_run_id=NOTEBOOK_RUN_ID,
        )

        with pytest.raises(RuntimeError, match=f"Exiting notebook run {NOTEBOOK_RUN_ID}. State: "):
            sensor.poke(context=MagicMock(spec=Context))

    @patch.object(SageMakerUnifiedStudioNotebookSensor, "poke", return_value=True)
    def test_execute_calls_poke(self, mock_poke):
        sensor = SageMakerUnifiedStudioNotebookSensor(
            task_id="test_task",
            domain_id=DOMAIN_ID,
            project_id=PROJECT_ID,
            notebook_run_id=NOTEBOOK_RUN_ID,
        )

        context = MagicMock(spec=Context)
        sensor.execute(context=context)

        mock_poke.assert_called_once_with(context)
