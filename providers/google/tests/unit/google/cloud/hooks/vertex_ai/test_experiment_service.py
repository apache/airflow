#
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

from airflow.providers.google.cloud.hooks.vertex_ai.experiment_service import (
    ExperimentHook,
    ExperimentRunHook,
)

from unit.google.cloud.utils.base_gcp_mock import (
    mock_base_gcp_hook_default_project_id,
    mock_base_gcp_hook_no_default_project_id,
)

TEST_GCP_CONN_ID: str = "test-gcp-conn-id"
TEST_REGION: str = "test-region"
TEST_PROJECT_ID: str = "test-project-id"
TEST_EXPERIMENT_NAME = "test_experiment_name"
TEST_EXPERIMENT_RUN_NAME = "test_experiment_run_name"
TEST_EXPERIMENT_DESCRIPTION = "test_description"
TEST_TARGET_STATE = aiplatform.gapic.Execution.State.COMPLETE
TEST_TENSORBOARD = False
TEST_DELETE_BACKING_TENSORBOARD_RUNS = False

BASE_STRING = "airflow.providers.google.common.hooks.base_google.{}"
EXPERIMENT_SERVICE_STRING = "airflow.providers.google.cloud.hooks.vertex_ai.experiment_service.{}"


class TestExperimentWithDefaultProjectIdHook:
    def setup_method(self):
        with mock.patch(
            BASE_STRING.format("GoogleBaseHook.__init__"), new=mock_base_gcp_hook_default_project_id
        ):
            self.hook = ExperimentHook(gcp_conn_id=TEST_GCP_CONN_ID)

    @mock.patch(EXPERIMENT_SERVICE_STRING.format("aiplatform.init"))
    def test_create_experiment(self, mock_init) -> None:
        self.hook.create_experiment(
            project_id=TEST_PROJECT_ID,
            location=TEST_REGION,
            experiment_name=TEST_EXPERIMENT_NAME,
            experiment_description=TEST_EXPERIMENT_DESCRIPTION,
            experiment_tensorboard=TEST_TENSORBOARD,
        )
        mock_init.assert_called_with(
            project=TEST_PROJECT_ID,
            location=TEST_REGION,
            experiment=TEST_EXPERIMENT_NAME,
            experiment_description=TEST_EXPERIMENT_DESCRIPTION,
            experiment_tensorboard=TEST_TENSORBOARD,
        )

    @mock.patch(EXPERIMENT_SERVICE_STRING.format("aiplatform.Experiment"))
    def test_delete_experiment(self, mock_experiment) -> None:
        self.hook.delete_experiment(
            project_id=TEST_PROJECT_ID,
            location=TEST_REGION,
            experiment_name=TEST_EXPERIMENT_NAME,
            delete_backing_tensorboard_runs=TEST_DELETE_BACKING_TENSORBOARD_RUNS,
        )
        mock_experiment.assert_called_with(
            project=TEST_PROJECT_ID,
            location=TEST_REGION,
            experiment_name=TEST_EXPERIMENT_NAME,
        )
        mock_experiment.return_value.delete.assert_called_with(
            delete_backing_tensorboard_runs=TEST_DELETE_BACKING_TENSORBOARD_RUNS
        )


class TestExperimentWithoutDefaultProjectIdHook:
    def setup_method(self):
        with mock.patch(
            BASE_STRING.format("GoogleBaseHook.__init__"), new=mock_base_gcp_hook_no_default_project_id
        ):
            self.hook = ExperimentHook(gcp_conn_id=TEST_GCP_CONN_ID)

    @mock.patch(EXPERIMENT_SERVICE_STRING.format("aiplatform.init"))
    def test_create_experiment(self, mock_init) -> None:
        self.hook.create_experiment(
            project_id=TEST_PROJECT_ID,
            location=TEST_REGION,
            experiment_name=TEST_EXPERIMENT_NAME,
            experiment_description=TEST_EXPERIMENT_DESCRIPTION,
            experiment_tensorboard=TEST_TENSORBOARD,
        )
        mock_init.assert_called_with(
            project=TEST_PROJECT_ID,
            location=TEST_REGION,
            experiment=TEST_EXPERIMENT_NAME,
            experiment_description=TEST_EXPERIMENT_DESCRIPTION,
            experiment_tensorboard=TEST_TENSORBOARD,
        )

    @mock.patch(EXPERIMENT_SERVICE_STRING.format("aiplatform.Experiment"))
    def test_delete_experiment(self, mock_experiment) -> None:
        self.hook.delete_experiment(
            project_id=TEST_PROJECT_ID,
            location=TEST_REGION,
            experiment_name=TEST_EXPERIMENT_NAME,
            delete_backing_tensorboard_runs=TEST_DELETE_BACKING_TENSORBOARD_RUNS,
        )
        mock_experiment.assert_called_with(
            project=TEST_PROJECT_ID,
            location=TEST_REGION,
            experiment_name=TEST_EXPERIMENT_NAME,
        )
        mock_experiment.return_value.delete.assert_called_with(
            delete_backing_tensorboard_runs=TEST_DELETE_BACKING_TENSORBOARD_RUNS
        )


class TestExperimentRunWithDefaultProjectIdHook:
    def setup_method(self):
        with mock.patch(
            BASE_STRING.format("GoogleBaseHook.__init__"), new=mock_base_gcp_hook_default_project_id
        ):
            self.hook = ExperimentRunHook(gcp_conn_id=TEST_GCP_CONN_ID)

    @mock.patch(EXPERIMENT_SERVICE_STRING.format("aiplatform.ExperimentRun"))
    def test_create_experiment_run(self, mock_experiment_run) -> None:
        self.hook.create_experiment_run(
            project_id=TEST_PROJECT_ID,
            location=TEST_REGION,
            experiment_name=TEST_EXPERIMENT_NAME,
            experiment_run_name=TEST_EXPERIMENT_RUN_NAME,
            experiment_run_tensorboard=TEST_TENSORBOARD,
        )
        mock_experiment_run.create.assert_called_with(
            project=TEST_PROJECT_ID,
            location=TEST_REGION,
            experiment=TEST_EXPERIMENT_NAME,
            run_name=TEST_EXPERIMENT_RUN_NAME,
            state=aiplatform.gapic.Execution.State.NEW,
            tensorboard=TEST_TENSORBOARD,
        )

    @mock.patch(EXPERIMENT_SERVICE_STRING.format("aiplatform.ExperimentRun"))
    def test_delete_experiment_run(self, mock_experiment_run) -> None:
        self.hook.delete_experiment_run(
            project_id=TEST_PROJECT_ID,
            location=TEST_REGION,
            experiment_name=TEST_EXPERIMENT_NAME,
            experiment_run_name=TEST_EXPERIMENT_RUN_NAME,
        )
        mock_experiment_run.assert_called_with(
            project=TEST_PROJECT_ID,
            location=TEST_REGION,
            experiment=TEST_EXPERIMENT_NAME,
            run_name=TEST_EXPERIMENT_RUN_NAME,
        )
        mock_experiment_run.return_value.delete.assert_called_with(
            delete_backing_tensorboard_run=TEST_DELETE_BACKING_TENSORBOARD_RUNS
        )


class TestExperimentRunWithoutDefaultProjectIdHook:
    def setup_method(self):
        with mock.patch(
            BASE_STRING.format("GoogleBaseHook.__init__"), new=mock_base_gcp_hook_no_default_project_id
        ):
            self.hook = ExperimentRunHook(gcp_conn_id=TEST_GCP_CONN_ID)

    @mock.patch(EXPERIMENT_SERVICE_STRING.format("aiplatform.ExperimentRun"))
    def test_create_experiment_run(self, mock_experiment_run) -> None:
        self.hook.create_experiment_run(
            project_id=TEST_PROJECT_ID,
            location=TEST_REGION,
            experiment_name=TEST_EXPERIMENT_NAME,
            experiment_run_name=TEST_EXPERIMENT_RUN_NAME,
            experiment_run_tensorboard=TEST_TENSORBOARD,
        )
        mock_experiment_run.create.assert_called_with(
            project=TEST_PROJECT_ID,
            location=TEST_REGION,
            experiment=TEST_EXPERIMENT_NAME,
            run_name=TEST_EXPERIMENT_RUN_NAME,
            state=aiplatform.gapic.Execution.State.NEW,
            tensorboard=TEST_TENSORBOARD,
        )

    @mock.patch(EXPERIMENT_SERVICE_STRING.format("aiplatform.ExperimentRun"))
    def test_delete_experiment_run(self, mock_experiment_run) -> None:
        self.hook.delete_experiment_run(
            project_id=TEST_PROJECT_ID,
            location=TEST_REGION,
            experiment_name=TEST_EXPERIMENT_NAME,
            experiment_run_name=TEST_EXPERIMENT_RUN_NAME,
        )
        mock_experiment_run.assert_called_with(
            project=TEST_PROJECT_ID,
            location=TEST_REGION,
            experiment=TEST_EXPERIMENT_NAME,
            run_name=TEST_EXPERIMENT_RUN_NAME,
        )
        mock_experiment_run.return_value.delete.assert_called_with(
            delete_backing_tensorboard_run=TEST_DELETE_BACKING_TENSORBOARD_RUNS
        )
