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

from unittest import mock

from google.cloud import aiplatform

from airflow.providers.google.cloud.operators.vertex_ai.experiment_service import (
    CreateExperimentOperator,
    CreateExperimentRunOperator,
    DeleteExperimentOperator,
    DeleteExperimentRunOperator,
    ListExperimentRunsOperator,
    UpdateExperimentRunStateOperator,
)

VERTEX_AI_PATH = "airflow.providers.google.cloud.operators.vertex_ai.experiment_service.{}"

TASK_ID = "test_task_id"
GCP_PROJECT = "test-project"
GCP_LOCATION = "test-location"
GCP_CONN_ID = "test-conn"
IMPERSONATION_CHAIN = ["ACCOUNT_1", "ACCOUNT_2", "ACCOUNT_3"]

TEST_EXPERIMENT_NAME = "test_experiment_name"
TEST_EXPERIMENT_RUN_NAME = "test_experiment_run_name"
TEST_EXPERIMENT_DESCRIPTION = "test_description"
TEST_TARGET_STATE = aiplatform.gapic.Execution.State.COMPLETE
TEST_TENSORBOARD = None
TEST_DELETE_BACKING_TENSORBOARD_RUNS = True


class TestVertexAICreateExperimentOperator:
    @mock.patch(VERTEX_AI_PATH.format("ExperimentHook"))
    def test_execute(self, mock_hook):
        op = CreateExperimentOperator(
            task_id=TASK_ID,
            project_id=GCP_PROJECT,
            location=GCP_LOCATION,
            experiment_name=TEST_EXPERIMENT_NAME,
            experiment_description=TEST_EXPERIMENT_DESCRIPTION,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        op.execute(context={"ti": mock.MagicMock()})
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.create_experiment.assert_called_once_with(
            project_id=GCP_PROJECT,
            location=GCP_LOCATION,
            experiment_name=TEST_EXPERIMENT_NAME,
            experiment_description=TEST_EXPERIMENT_DESCRIPTION,
            experiment_tensorboard=TEST_TENSORBOARD,
        )


class TestVertexAIDeleteExperimentOperator:
    @mock.patch(VERTEX_AI_PATH.format("ExperimentHook"))
    def test_execute(self, mock_hook):
        op = DeleteExperimentOperator(
            task_id=TASK_ID,
            project_id=GCP_PROJECT,
            location=GCP_LOCATION,
            experiment_name=TEST_EXPERIMENT_NAME,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            delete_backing_tensorboard_runs=TEST_DELETE_BACKING_TENSORBOARD_RUNS,
        )
        op.execute(context={"ti": mock.MagicMock()})
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.delete_experiment.assert_called_once_with(
            project_id=GCP_PROJECT,
            location=GCP_LOCATION,
            experiment_name=TEST_EXPERIMENT_NAME,
            delete_backing_tensorboard_runs=TEST_DELETE_BACKING_TENSORBOARD_RUNS,
        )


class TestVertexAICreateExperimentRunOperator:
    @mock.patch(VERTEX_AI_PATH.format("ExperimentRunHook"))
    def test_execute(self, mock_hook):
        op = CreateExperimentRunOperator(
            task_id=TASK_ID,
            project_id=GCP_PROJECT,
            location=GCP_LOCATION,
            experiment_name=TEST_EXPERIMENT_NAME,
            experiment_run_name=TEST_EXPERIMENT_RUN_NAME,
            experiment_run_tensorboard=TEST_TENSORBOARD,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        op.execute(context={"ti": mock.MagicMock()})
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.create_experiment_run.assert_called_once_with(
            project_id=GCP_PROJECT,
            location=GCP_LOCATION,
            experiment_name=TEST_EXPERIMENT_NAME,
            experiment_run_name=TEST_EXPERIMENT_RUN_NAME,
            experiment_run_tensorboard=TEST_TENSORBOARD,
            run_after_creation=False,
        )


class TestVertexAIListExperimentRunsOperator:
    @mock.patch(VERTEX_AI_PATH.format("ExperimentRunHook"))
    def test_execute(self, mock_hook):
        op = ListExperimentRunsOperator(
            task_id=TASK_ID,
            project_id=GCP_PROJECT,
            location=GCP_LOCATION,
            experiment_name=TEST_EXPERIMENT_NAME,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        op.execute(context={"ti": mock.MagicMock()})
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.list_experiment_runs.assert_called_once_with(
            project_id=GCP_PROJECT,
            location=GCP_LOCATION,
            experiment_name=TEST_EXPERIMENT_NAME,
        )


class TestVertexAIUpdateExperimentRunStateOperator:
    @mock.patch(VERTEX_AI_PATH.format("ExperimentRunHook"))
    def test_execute(self, mock_hook):
        op = UpdateExperimentRunStateOperator(
            task_id=TASK_ID,
            project_id=GCP_PROJECT,
            location=GCP_LOCATION,
            experiment_name=TEST_EXPERIMENT_NAME,
            experiment_run_name=TEST_EXPERIMENT_RUN_NAME,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            new_state=TEST_TARGET_STATE,
        )
        op.execute(context={"ti": mock.MagicMock()})
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.update_experiment_run_state.assert_called_once_with(
            project_id=GCP_PROJECT,
            location=GCP_LOCATION,
            experiment_name=TEST_EXPERIMENT_NAME,
            experiment_run_name=TEST_EXPERIMENT_RUN_NAME,
            new_state=TEST_TARGET_STATE,
        )


class TestVertexAIDeleteExperimentRunOperator:
    @mock.patch(VERTEX_AI_PATH.format("ExperimentRunHook"))
    def test_execute(self, mock_hook):
        op = DeleteExperimentRunOperator(
            task_id=TASK_ID,
            project_id=GCP_PROJECT,
            location=GCP_LOCATION,
            experiment_name=TEST_EXPERIMENT_NAME,
            experiment_run_name=TEST_EXPERIMENT_RUN_NAME,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        op.execute(context={"ti": mock.MagicMock()})
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.delete_experiment_run.assert_called_once_with(
            project_id=GCP_PROJECT,
            location=GCP_LOCATION,
            experiment_name=TEST_EXPERIMENT_NAME,
            experiment_run_name=TEST_EXPERIMENT_RUN_NAME,
        )
