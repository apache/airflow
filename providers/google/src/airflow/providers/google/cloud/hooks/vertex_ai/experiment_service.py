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

from google.cloud import aiplatform
from google.cloud.aiplatform.compat.types import execution_v1 as gca_execution

from airflow.providers.google.common.hooks.base_google import PROVIDE_PROJECT_ID, GoogleBaseHook


class ExperimentHook(GoogleBaseHook):
    """Use the Vertex AI SDK for Python to manage your experiments."""

    @GoogleBaseHook.fallback_to_default_project_id
    def create_experiment(
        self,
        experiment_name: str,
        location: str,
        experiment_description: str = "",
        project_id: str = PROVIDE_PROJECT_ID,
        experiment_tensorboard: str | None = None,
    ):
        """
        Create an experiment and, optionally, associate a Vertex AI TensorBoard instance using the Vertex AI SDK for Python.

        :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
        :param location: Required. The ID of the Google Cloud location that the service belongs to.
        :param experiment_name: Required. The name of the evaluation experiment.
        :param experiment_description: Optional. Description of the evaluation experiment.
        :param experiment_tensorboard: Optional. The Vertex TensorBoard instance to use as a backing
            TensorBoard for the provided experiment. If no TensorBoard is provided, a default Tensorboard
            instance is created and used by this experiment.
        """
        aiplatform.init(
            experiment=experiment_name,
            experiment_description=experiment_description,
            experiment_tensorboard=experiment_tensorboard if experiment_tensorboard else False,
            project=project_id,
            location=location,
        )
        self.log.info("Created experiment with name: %s", experiment_name)

    @GoogleBaseHook.fallback_to_default_project_id
    def delete_experiment(
        self,
        experiment_name: str,
        location: str,
        project_id: str = PROVIDE_PROJECT_ID,
        delete_backing_tensorboard_runs: bool = False,
    ) -> None:
        """
        Delete an experiment.

        Deleting an experiment deletes that experiment and all experiment runs associated with the experiment.
        The Vertex AI TensorBoard experiment associated with the experiment is not deleted.

        :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
        :param location: Required. The ID of the Google Cloud location that the service belongs to.
        :param experiment_name: Required. The name of the evaluation experiment.
        :param delete_backing_tensorboard_runs: Optional. If True will also delete the Vertex AI TensorBoard
            runs associated with the experiment runs under this experiment that we used to store time series
            metrics.
        """
        experiment = aiplatform.Experiment(
            experiment_name=experiment_name, project=project_id, location=location
        )

        experiment.delete(delete_backing_tensorboard_runs=delete_backing_tensorboard_runs)


class ExperimentRunHook(GoogleBaseHook):
    """Use the Vertex AI SDK for Python to create and manage your experiment runs."""

    @GoogleBaseHook.fallback_to_default_project_id
    def create_experiment_run(
        self,
        experiment_run_name: str,
        experiment_name: str,
        location: str,
        project_id: str = PROVIDE_PROJECT_ID,
        experiment_run_tensorboard: str | None = None,
        run_after_creation: bool = False,
    ) -> None:
        """
        Create experiment run for the experiment.

        :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
        :param location: Required. The ID of the Google Cloud location that the service belongs to.
        :param experiment_name: Required. The name of the evaluation experiment.
        :param experiment_run_name: Required. The specific run name or ID for this experiment.
        :param experiment_run_tensorboard: Optional. A backing TensorBoard resource to enable and store time
            series metrics logged to this experiment run.
        :param run_after_creation: Optional. Responsible for state after creation of experiment run.
            If true experiment run will be created with state RUNNING.
        """
        experiment_run_state = (
            gca_execution.Execution.State.NEW
            if not run_after_creation
            else gca_execution.Execution.State.RUNNING
        )
        experiment_run = aiplatform.ExperimentRun.create(
            run_name=experiment_run_name,
            experiment=experiment_name,
            project=project_id,
            location=location,
            state=experiment_run_state,
            tensorboard=experiment_run_tensorboard,
        )
        self.log.info(
            "Created experiment run with name: %s and status: %s",
            experiment_run.name,
            experiment_run.state,
        )

    @GoogleBaseHook.fallback_to_default_project_id
    def list_experiment_runs(
        self,
        experiment_name: str,
        location: str,
        project_id: str = PROVIDE_PROJECT_ID,
    ) -> list[aiplatform.ExperimentRun]:
        """
        List experiment run for the experiment.

        :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
        :param location: Required. The ID of the Google Cloud location that the service belongs to.
        :param experiment_name: Required. The name of the evaluation experiment.
        """
        experiment_runs = aiplatform.ExperimentRun.list(
            experiment=experiment_name,
            project=project_id,
            location=location,
        )
        return experiment_runs

    @GoogleBaseHook.fallback_to_default_project_id
    def update_experiment_run_state(
        self,
        experiment_run_name: str,
        experiment_name: str,
        location: str,
        new_state: gca_execution.Execution.State,
        project_id: str = PROVIDE_PROJECT_ID,
    ) -> None:
        """
        Update state of the experiment run.

        :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
        :param location: Required. The ID of the Google Cloud location that the service belongs to.
        :param experiment_name: Required. The name of the evaluation experiment.
        :param experiment_run_name: Required. The specific run name or ID for this experiment.
        :param new_state: Required. New state of the experiment run.
        """
        experiment_run = aiplatform.ExperimentRun(
            run_name=experiment_run_name,
            experiment=experiment_name,
            project=project_id,
            location=location,
        )
        self.log.info("State of the %s before update is: %s", experiment_run.name, experiment_run.state)

        experiment_run.update_state(new_state)

    @GoogleBaseHook.fallback_to_default_project_id
    def delete_experiment_run(
        self,
        experiment_run_name: str,
        experiment_name: str,
        location: str,
        project_id: str = PROVIDE_PROJECT_ID,
        delete_backing_tensorboard_run: bool = False,
    ) -> None:
        """
        Delete experiment run from the experiment.

        :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
        :param location: Required. The ID of the Google Cloud location that the service belongs to.
        :param experiment_name: Required. The name of the evaluation experiment.
        :param experiment_run_name: Required. The specific run name or ID for this experiment.
        :param delete_backing_tensorboard_run: Whether to delete the backing Vertex AI TensorBoard run
            that stores time series metrics for this run.
        """
        self.log.info("Next experiment run will be deleted: %s", experiment_run_name)
        experiment_run = aiplatform.ExperimentRun(
            run_name=experiment_run_name, experiment=experiment_name, project=project_id, location=location
        )
        experiment_run.delete(delete_backing_tensorboard_run=delete_backing_tensorboard_run)
