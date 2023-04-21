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
"""This module contains a Google Cloud Vertex AI hook."""
from __future__ import annotations

from typing import Sequence

from google.api_core.client_options import ClientOptions
from google.api_core.gapic_v1.method import DEFAULT, _MethodDefault
from google.api_core.operation import Operation
from google.api_core.retry import Retry
from google.cloud.aiplatform import (
    CustomContainerTrainingJob,
    CustomPythonPackageTrainingJob,
    CustomTrainingJob,
    datasets,
    models,
)
from google.cloud.aiplatform_v1 import JobServiceClient, PipelineServiceClient
from google.cloud.aiplatform_v1.services.job_service.pagers import ListCustomJobsPager
from google.cloud.aiplatform_v1.services.pipeline_service.pagers import (
    ListPipelineJobsPager,
    ListTrainingPipelinesPager,
)
from google.cloud.aiplatform_v1.types import CustomJob, PipelineJob, TrainingPipeline

from airflow import AirflowException
from airflow.providers.google.common.consts import CLIENT_INFO
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook


class CustomJobHook(GoogleBaseHook):
    """Hook for Google Cloud Vertex AI Custom Job APIs."""

    def __init__(
        self,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        if kwargs.get("delegate_to") is not None:
            raise RuntimeError(
                "The `delegate_to` parameter has been deprecated before and finally removed in this version"
                " of Google Provider. You MUST convert it to `impersonate_chain`"
            )
        super().__init__(
            gcp_conn_id=gcp_conn_id,
            impersonation_chain=impersonation_chain,
        )
        self._job: None | (
            CustomContainerTrainingJob | CustomPythonPackageTrainingJob | CustomTrainingJob
        ) = None

    def get_pipeline_service_client(
        self,
        region: str | None = None,
    ) -> PipelineServiceClient:
        """Returns PipelineServiceClient."""
        if region and region != "global":
            client_options = ClientOptions(api_endpoint=f"{region}-aiplatform.googleapis.com:443")
        else:
            client_options = ClientOptions()
        return PipelineServiceClient(
            credentials=self.get_credentials(), client_info=CLIENT_INFO, client_options=client_options
        )

    def get_job_service_client(
        self,
        region: str | None = None,
    ) -> JobServiceClient:
        """Returns JobServiceClient"""
        if region and region != "global":
            client_options = ClientOptions(api_endpoint=f"{region}-aiplatform.googleapis.com:443")
        else:
            client_options = ClientOptions()

        return JobServiceClient(
            credentials=self.get_credentials(), client_info=CLIENT_INFO, client_options=client_options
        )

    def get_custom_container_training_job(
        self,
        display_name: str,
        container_uri: str,
        command: Sequence[str] = [],
        model_serving_container_image_uri: str | None = None,
        model_serving_container_predict_route: str | None = None,
        model_serving_container_health_route: str | None = None,
        model_serving_container_command: Sequence[str] | None = None,
        model_serving_container_args: Sequence[str] | None = None,
        model_serving_container_environment_variables: dict[str, str] | None = None,
        model_serving_container_ports: Sequence[int] | None = None,
        model_description: str | None = None,
        model_instance_schema_uri: str | None = None,
        model_parameters_schema_uri: str | None = None,
        model_prediction_schema_uri: str | None = None,
        project: str | None = None,
        location: str | None = None,
        labels: dict[str, str] | None = None,
        training_encryption_spec_key_name: str | None = None,
        model_encryption_spec_key_name: str | None = None,
        staging_bucket: str | None = None,
    ) -> CustomContainerTrainingJob:
        """Returns CustomContainerTrainingJob object"""
        return CustomContainerTrainingJob(
            display_name=display_name,
            container_uri=container_uri,
            command=command,
            model_serving_container_image_uri=model_serving_container_image_uri,
            model_serving_container_predict_route=model_serving_container_predict_route,
            model_serving_container_health_route=model_serving_container_health_route,
            model_serving_container_command=model_serving_container_command,
            model_serving_container_args=model_serving_container_args,
            model_serving_container_environment_variables=model_serving_container_environment_variables,
            model_serving_container_ports=model_serving_container_ports,
            model_description=model_description,
            model_instance_schema_uri=model_instance_schema_uri,
            model_parameters_schema_uri=model_parameters_schema_uri,
            model_prediction_schema_uri=model_prediction_schema_uri,
            project=project,
            location=location,
            credentials=self.get_credentials(),
            labels=labels,
            training_encryption_spec_key_name=training_encryption_spec_key_name,
            model_encryption_spec_key_name=model_encryption_spec_key_name,
            staging_bucket=staging_bucket,
        )

    def get_custom_python_package_training_job(
        self,
        display_name: str,
        python_package_gcs_uri: str,
        python_module_name: str,
        container_uri: str,
        model_serving_container_image_uri: str | None = None,
        model_serving_container_predict_route: str | None = None,
        model_serving_container_health_route: str | None = None,
        model_serving_container_command: Sequence[str] | None = None,
        model_serving_container_args: Sequence[str] | None = None,
        model_serving_container_environment_variables: dict[str, str] | None = None,
        model_serving_container_ports: Sequence[int] | None = None,
        model_description: str | None = None,
        model_instance_schema_uri: str | None = None,
        model_parameters_schema_uri: str | None = None,
        model_prediction_schema_uri: str | None = None,
        project: str | None = None,
        location: str | None = None,
        labels: dict[str, str] | None = None,
        training_encryption_spec_key_name: str | None = None,
        model_encryption_spec_key_name: str | None = None,
        staging_bucket: str | None = None,
    ):
        """Returns CustomPythonPackageTrainingJob object"""
        return CustomPythonPackageTrainingJob(
            display_name=display_name,
            container_uri=container_uri,
            python_package_gcs_uri=python_package_gcs_uri,
            python_module_name=python_module_name,
            model_serving_container_image_uri=model_serving_container_image_uri,
            model_serving_container_predict_route=model_serving_container_predict_route,
            model_serving_container_health_route=model_serving_container_health_route,
            model_serving_container_command=model_serving_container_command,
            model_serving_container_args=model_serving_container_args,
            model_serving_container_environment_variables=model_serving_container_environment_variables,
            model_serving_container_ports=model_serving_container_ports,
            model_description=model_description,
            model_instance_schema_uri=model_instance_schema_uri,
            model_parameters_schema_uri=model_parameters_schema_uri,
            model_prediction_schema_uri=model_prediction_schema_uri,
            project=project,
            location=location,
            credentials=self.get_credentials(),
            labels=labels,
            training_encryption_spec_key_name=training_encryption_spec_key_name,
            model_encryption_spec_key_name=model_encryption_spec_key_name,
            staging_bucket=staging_bucket,
        )

    def get_custom_training_job(
        self,
        display_name: str,
        script_path: str,
        container_uri: str,
        requirements: Sequence[str] | None = None,
        model_serving_container_image_uri: str | None = None,
        model_serving_container_predict_route: str | None = None,
        model_serving_container_health_route: str | None = None,
        model_serving_container_command: Sequence[str] | None = None,
        model_serving_container_args: Sequence[str] | None = None,
        model_serving_container_environment_variables: dict[str, str] | None = None,
        model_serving_container_ports: Sequence[int] | None = None,
        model_description: str | None = None,
        model_instance_schema_uri: str | None = None,
        model_parameters_schema_uri: str | None = None,
        model_prediction_schema_uri: str | None = None,
        project: str | None = None,
        location: str | None = None,
        labels: dict[str, str] | None = None,
        training_encryption_spec_key_name: str | None = None,
        model_encryption_spec_key_name: str | None = None,
        staging_bucket: str | None = None,
    ):
        """Returns CustomTrainingJob object"""
        return CustomTrainingJob(
            display_name=display_name,
            script_path=script_path,
            container_uri=container_uri,
            requirements=requirements,
            model_serving_container_image_uri=model_serving_container_image_uri,
            model_serving_container_predict_route=model_serving_container_predict_route,
            model_serving_container_health_route=model_serving_container_health_route,
            model_serving_container_command=model_serving_container_command,
            model_serving_container_args=model_serving_container_args,
            model_serving_container_environment_variables=model_serving_container_environment_variables,
            model_serving_container_ports=model_serving_container_ports,
            model_description=model_description,
            model_instance_schema_uri=model_instance_schema_uri,
            model_parameters_schema_uri=model_parameters_schema_uri,
            model_prediction_schema_uri=model_prediction_schema_uri,
            project=project,
            location=location,
            credentials=self.get_credentials(),
            labels=labels,
            training_encryption_spec_key_name=training_encryption_spec_key_name,
            model_encryption_spec_key_name=model_encryption_spec_key_name,
            staging_bucket=staging_bucket,
        )

    @staticmethod
    def extract_model_id(obj: dict) -> str:
        """Returns unique id of the Model."""
        return obj["name"].rpartition("/")[-1]

    @staticmethod
    def extract_training_id(resource_name: str) -> str:
        """Returns unique id of the Training pipeline."""
        return resource_name.rpartition("/")[-1]

    @staticmethod
    def extract_custom_job_id(custom_job_name: str) -> str:
        """Returns unique id of the Custom Job pipeline."""
        return custom_job_name.rpartition("/")[-1]

    def wait_for_operation(self, operation: Operation, timeout: float | None = None):
        """Waits for long-lasting operation to complete."""
        try:
            return operation.result(timeout=timeout)
        except Exception:
            error = operation.exception(timeout=timeout)
            raise AirflowException(error)

    def cancel_job(self) -> None:
        """Cancel Job for training pipeline"""
        if self._job:
            self._job.cancel()

    def _run_job(
        self,
        job: (CustomTrainingJob | CustomContainerTrainingJob | CustomPythonPackageTrainingJob),
        dataset: None
        | (
            datasets.ImageDataset | datasets.TabularDataset | datasets.TextDataset | datasets.VideoDataset
        ) = None,
        annotation_schema_uri: str | None = None,
        model_display_name: str | None = None,
        model_labels: dict[str, str] | None = None,
        base_output_dir: str | None = None,
        service_account: str | None = None,
        network: str | None = None,
        bigquery_destination: str | None = None,
        args: list[str | float | int] | None = None,
        environment_variables: dict[str, str] | None = None,
        replica_count: int = 1,
        machine_type: str = "n1-standard-4",
        accelerator_type: str = "ACCELERATOR_TYPE_UNSPECIFIED",
        accelerator_count: int = 0,
        boot_disk_type: str = "pd-ssd",
        boot_disk_size_gb: int = 100,
        training_fraction_split: float | None = None,
        validation_fraction_split: float | None = None,
        test_fraction_split: float | None = None,
        training_filter_split: str | None = None,
        validation_filter_split: str | None = None,
        test_filter_split: str | None = None,
        predefined_split_column_name: str | None = None,
        timestamp_split_column_name: str | None = None,
        tensorboard: str | None = None,
        sync=True,
    ) -> tuple[models.Model | None, str, str]:
        """Run Job for training pipeline"""
        model = job.run(
            dataset=dataset,
            annotation_schema_uri=annotation_schema_uri,
            model_display_name=model_display_name,
            model_labels=model_labels,
            base_output_dir=base_output_dir,
            service_account=service_account,
            network=network,
            bigquery_destination=bigquery_destination,
            args=args,
            environment_variables=environment_variables,
            replica_count=replica_count,
            machine_type=machine_type,
            accelerator_type=accelerator_type,
            accelerator_count=accelerator_count,
            boot_disk_type=boot_disk_type,
            boot_disk_size_gb=boot_disk_size_gb,
            training_fraction_split=training_fraction_split,
            validation_fraction_split=validation_fraction_split,
            test_fraction_split=test_fraction_split,
            training_filter_split=training_filter_split,
            validation_filter_split=validation_filter_split,
            test_filter_split=test_filter_split,
            predefined_split_column_name=predefined_split_column_name,
            timestamp_split_column_name=timestamp_split_column_name,
            tensorboard=tensorboard,
            sync=sync,
        )
        training_id = self.extract_training_id(job.resource_name)
        custom_job_id = self.extract_custom_job_id(
            job.gca_resource.training_task_metadata.get("backingCustomJob")
        )
        if model:
            model.wait()
        else:
            self.log.warning(
                "Training did not produce a Managed Model returning None. Training Pipeline is not "
                "configured to upload a Model. Create the Training Pipeline with "
                "model_serving_container_image_uri and model_display_name passed in. "
                "Ensure that your training script saves to model to os.environ['AIP_MODEL_DIR']."
            )
        return model, training_id, custom_job_id

    @GoogleBaseHook.fallback_to_default_project_id
    def cancel_pipeline_job(
        self,
        project_id: str,
        region: str,
        pipeline_job: str,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> None:
        """
        Cancels a PipelineJob. Starts asynchronous cancellation on the PipelineJob. The server makes a best
        effort to cancel the pipeline, but success is not guaranteed. Clients can use
        [PipelineService.GetPipelineJob][google.cloud.aiplatform.v1.PipelineService.GetPipelineJob] or other
        methods to check whether the cancellation succeeded or whether the pipeline completed despite
        cancellation. On successful cancellation, the PipelineJob is not deleted; instead it becomes a
        pipeline with a [PipelineJob.error][google.cloud.aiplatform.v1.PipelineJob.error] value with a
        [google.rpc.Status.code][google.rpc.Status.code] of 1, corresponding to ``Code.CANCELLED``, and
        [PipelineJob.state][google.cloud.aiplatform.v1.PipelineJob.state] is set to ``CANCELLED``.

        :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
        :param region: Required. The ID of the Google Cloud region that the service belongs to.
        :param pipeline_job: The name of the PipelineJob to cancel.
        :param retry: Designation of what errors, if any, should be retried.
        :param timeout: The timeout for this request.
        :param metadata: Strings which should be sent along with the request as metadata.
        """
        client = self.get_pipeline_service_client(region)
        name = client.pipeline_job_path(project_id, region, pipeline_job)

        client.cancel_pipeline_job(
            request={
                "name": name,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

    @GoogleBaseHook.fallback_to_default_project_id
    def cancel_training_pipeline(
        self,
        project_id: str,
        region: str,
        training_pipeline: str,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> None:
        """
        Cancels a TrainingPipeline. Starts asynchronous cancellation on the TrainingPipeline. The server makes
        a best effort to cancel the pipeline, but success is not guaranteed. Clients can use
        [PipelineService.GetTrainingPipeline][google.cloud.aiplatform.v1.PipelineService.GetTrainingPipeline]
        or other methods to check whether the cancellation succeeded or whether the pipeline completed despite
        cancellation. On successful cancellation, the TrainingPipeline is not deleted; instead it becomes a
        pipeline with a [TrainingPipeline.error][google.cloud.aiplatform.v1.TrainingPipeline.error] value with
        a [google.rpc.Status.code][google.rpc.Status.code] of 1, corresponding to ``Code.CANCELLED``, and
        [TrainingPipeline.state][google.cloud.aiplatform.v1.TrainingPipeline.state] is set to ``CANCELLED``.

        :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
        :param region: Required. The ID of the Google Cloud region that the service belongs to.
        :param training_pipeline: Required. The name of the TrainingPipeline to cancel.
        :param retry: Designation of what errors, if any, should be retried.
        :param timeout: The timeout for this request.
        :param metadata: Strings which should be sent along with the request as metadata.
        """
        client = self.get_pipeline_service_client(region)
        name = client.training_pipeline_path(project_id, region, training_pipeline)

        client.cancel_training_pipeline(
            request={
                "name": name,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

    @GoogleBaseHook.fallback_to_default_project_id
    def cancel_custom_job(
        self,
        project_id: str,
        region: str,
        custom_job: str,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> None:
        """
        Cancels a CustomJob. Starts asynchronous cancellation on the CustomJob. The server makes a best effort
        to cancel the job, but success is not guaranteed. Clients can use
        [JobService.GetCustomJob][google.cloud.aiplatform.v1.JobService.GetCustomJob] or other methods to
        check whether the cancellation succeeded or whether the job completed despite cancellation. On
        successful cancellation, the CustomJob is not deleted; instead it becomes a job with a
        [CustomJob.error][google.cloud.aiplatform.v1.CustomJob.error] value with a
        [google.rpc.Status.code][google.rpc.Status.code] of 1, corresponding to ``Code.CANCELLED``, and
        [CustomJob.state][google.cloud.aiplatform.v1.CustomJob.state] is set to ``CANCELLED``.

        :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
        :param region: Required. The ID of the Google Cloud region that the service belongs to.
        :param custom_job: Required. The name of the CustomJob to cancel.
        :param retry: Designation of what errors, if any, should be retried.
        :param timeout: The timeout for this request.
        :param metadata: Strings which should be sent along with the request as metadata.
        """
        client = self.get_job_service_client(region)
        name = JobServiceClient.custom_job_path(project_id, region, custom_job)

        client.cancel_custom_job(
            request={
                "name": name,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

    @GoogleBaseHook.fallback_to_default_project_id
    def create_pipeline_job(
        self,
        project_id: str,
        region: str,
        pipeline_job: PipelineJob,
        pipeline_job_id: str,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> PipelineJob:
        """
        Creates a PipelineJob. A PipelineJob will run immediately when created.

        :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
        :param region: Required. The ID of the Google Cloud region that the service belongs to.
        :param pipeline_job:  Required. The PipelineJob to create.
        :param pipeline_job_id:  The ID to use for the PipelineJob, which will become the final component of
            the PipelineJob name. If not provided, an ID will be automatically generated.

            This value should be less than 128 characters, and valid characters are /[a-z][0-9]-/.
        :param retry: Designation of what errors, if any, should be retried.
        :param timeout: The timeout for this request.
        :param metadata: Strings which should be sent along with the request as metadata.
        """
        client = self.get_pipeline_service_client(region)
        parent = client.common_location_path(project_id, region)

        result = client.create_pipeline_job(
            request={
                "parent": parent,
                "pipeline_job": pipeline_job,
                "pipeline_job_id": pipeline_job_id,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result

    @GoogleBaseHook.fallback_to_default_project_id
    def create_training_pipeline(
        self,
        project_id: str,
        region: str,
        training_pipeline: TrainingPipeline,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> TrainingPipeline:
        """
        Creates a TrainingPipeline. A created TrainingPipeline right away will be attempted to be run.

        :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
        :param region: Required. The ID of the Google Cloud region that the service belongs to.
        :param training_pipeline:  Required. The TrainingPipeline to create.
        :param retry: Designation of what errors, if any, should be retried.
        :param timeout: The timeout for this request.
        :param metadata: Strings which should be sent along with the request as metadata.
        """
        client = self.get_pipeline_service_client(region)
        parent = client.common_location_path(project_id, region)

        result = client.create_training_pipeline(
            request={
                "parent": parent,
                "training_pipeline": training_pipeline,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result

    @GoogleBaseHook.fallback_to_default_project_id
    def create_custom_job(
        self,
        project_id: str,
        region: str,
        custom_job: CustomJob,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> CustomJob:
        """
        Creates a CustomJob. A created CustomJob right away will be attempted to be run.

        :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
        :param region: Required. The ID of the Google Cloud region that the service belongs to.
        :param custom_job:  Required. The CustomJob to create. This corresponds to the ``custom_job`` field on
            the ``request`` instance; if ``request`` is provided, this should not be set.
        :param retry: Designation of what errors, if any, should be retried.
        :param timeout: The timeout for this request.
        :param metadata: Strings which should be sent along with the request as metadata.
        """
        client = self.get_job_service_client(region)
        parent = JobServiceClient.common_location_path(project_id, region)

        result = client.create_custom_job(
            request={
                "parent": parent,
                "custom_job": custom_job,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result

    @GoogleBaseHook.fallback_to_default_project_id
    def create_custom_container_training_job(
        self,
        project_id: str,
        region: str,
        display_name: str,
        container_uri: str,
        command: Sequence[str] = [],
        model_serving_container_image_uri: str | None = None,
        model_serving_container_predict_route: str | None = None,
        model_serving_container_health_route: str | None = None,
        model_serving_container_command: Sequence[str] | None = None,
        model_serving_container_args: Sequence[str] | None = None,
        model_serving_container_environment_variables: dict[str, str] | None = None,
        model_serving_container_ports: Sequence[int] | None = None,
        model_description: str | None = None,
        model_instance_schema_uri: str | None = None,
        model_parameters_schema_uri: str | None = None,
        model_prediction_schema_uri: str | None = None,
        labels: dict[str, str] | None = None,
        training_encryption_spec_key_name: str | None = None,
        model_encryption_spec_key_name: str | None = None,
        staging_bucket: str | None = None,
        # RUN
        dataset: None
        | (
            datasets.ImageDataset | datasets.TabularDataset | datasets.TextDataset | datasets.VideoDataset
        ) = None,
        annotation_schema_uri: str | None = None,
        model_display_name: str | None = None,
        model_labels: dict[str, str] | None = None,
        base_output_dir: str | None = None,
        service_account: str | None = None,
        network: str | None = None,
        bigquery_destination: str | None = None,
        args: list[str | float | int] | None = None,
        environment_variables: dict[str, str] | None = None,
        replica_count: int = 1,
        machine_type: str = "n1-standard-4",
        accelerator_type: str = "ACCELERATOR_TYPE_UNSPECIFIED",
        accelerator_count: int = 0,
        boot_disk_type: str = "pd-ssd",
        boot_disk_size_gb: int = 100,
        training_fraction_split: float | None = None,
        validation_fraction_split: float | None = None,
        test_fraction_split: float | None = None,
        training_filter_split: str | None = None,
        validation_filter_split: str | None = None,
        test_filter_split: str | None = None,
        predefined_split_column_name: str | None = None,
        timestamp_split_column_name: str | None = None,
        tensorboard: str | None = None,
        sync=True,
    ) -> tuple[models.Model | None, str, str]:
        """
        Create Custom Container Training Job

        :param display_name: Required. The user-defined name of this TrainingPipeline.
        :param command: The command to be invoked when the container is started.
            It overrides the entrypoint instruction in Dockerfile when provided
        :param container_uri: Required: Uri of the training container image in the GCR.
        :param model_serving_container_image_uri: If the training produces a managed Vertex AI Model, the URI
            of the Model serving container suitable for serving the model produced by the
            training script.
        :param model_serving_container_predict_route: If the training produces a managed Vertex AI Model, An
            HTTP path to send prediction requests to the container, and which must be supported
            by it. If not specified a default HTTP path will be used by Vertex AI.
        :param model_serving_container_health_route: If the training produces a managed Vertex AI Model, an
            HTTP path to send health check requests to the container, and which must be supported
            by it. If not specified a standard HTTP path will be used by AI Platform.
        :param model_serving_container_command: The command with which the container is run. Not executed
            within a shell. The Docker image's ENTRYPOINT is used if this is not provided.
            Variable references $(VAR_NAME) are expanded using the container's
            environment. If a variable cannot be resolved, the reference in the
            input string will be unchanged. The $(VAR_NAME) syntax can be escaped
            with a double $$, ie: $$(VAR_NAME). Escaped references will never be
            expanded, regardless of whether the variable exists or not.
        :param model_serving_container_args: The arguments to the command. The Docker image's CMD is used if
            this is not provided. Variable references $(VAR_NAME) are expanded using the
            container's environment. If a variable cannot be resolved, the reference
            in the input string will be unchanged. The $(VAR_NAME) syntax can be
            escaped with a double $$, ie: $$(VAR_NAME). Escaped references will
            never be expanded, regardless of whether the variable exists or not.
        :param model_serving_container_environment_variables: The environment variables that are to be
            present in the container. Should be a dictionary where keys are environment variable names
            and values are environment variable values for those names.
        :param model_serving_container_ports: Declaration of ports that are exposed by the container. This
            field is primarily informational, it gives Vertex AI information about the
            network connections the container uses. Listing or not a port here has
            no impact on whether the port is actually exposed, any port listening on
            the default "0.0.0.0" address inside a container will be accessible from
            the network.
        :param model_description: The description of the Model.
        :param model_instance_schema_uri: Optional. Points to a YAML file stored on Google Cloud
                Storage describing the format of a single instance, which
                are used in
                ``PredictRequest.instances``,
                ``ExplainRequest.instances``
                and
                ``BatchPredictionJob.input_config``.
                The schema is defined as an OpenAPI 3.0.2 `Schema
                Object <https://tinyurl.com/y538mdwt#schema-object>`__.
                AutoML Models always have this field populated by AI
                Platform. Note: The URI given on output will be immutable
                and probably different, including the URI scheme, than the
                one given on input. The output URI will point to a location
                where the user only has a read access.
        :param model_parameters_schema_uri: Optional. Points to a YAML file stored on Google Cloud
                Storage describing the parameters of prediction and
                explanation via
                ``PredictRequest.parameters``,
                ``ExplainRequest.parameters``
                and
                ``BatchPredictionJob.model_parameters``.
                The schema is defined as an OpenAPI 3.0.2 `Schema
                Object <https://tinyurl.com/y538mdwt#schema-object>`__.
                AutoML Models always have this field populated by AI
                Platform, if no parameters are supported it is set to an
                empty string. Note: The URI given on output will be
                immutable and probably different, including the URI scheme,
                than the one given on input. The output URI will point to a
                location where the user only has a read access.
        :param model_prediction_schema_uri: Optional. Points to a YAML file stored on Google Cloud
                Storage describing the format of a single prediction
                produced by this Model, which are returned via
                ``PredictResponse.predictions``,
                ``ExplainResponse.explanations``,
                and
                ``BatchPredictionJob.output_config``.
                The schema is defined as an OpenAPI 3.0.2 `Schema
                Object <https://tinyurl.com/y538mdwt#schema-object>`__.
                AutoML Models always have this field populated by AI
                Platform. Note: The URI given on output will be immutable
                and probably different, including the URI scheme, than the
                one given on input. The output URI will point to a location
                where the user only has a read access.
        :param project_id: Project to run training in.
        :param region: Location to run training in.
        :param labels: Optional. The labels with user-defined metadata to
                organize TrainingPipelines.
                Label keys and values can be no longer than 64
                characters, can only
                contain lowercase letters, numeric characters,
                underscores and dashes. International characters
                are allowed.
                See https://goo.gl/xmQnxf for more information
                and examples of labels.
        :param training_encryption_spec_key_name: Optional. The Cloud KMS resource identifier of the customer
                managed encryption key used to protect the training pipeline. Has the
                form:
                ``projects/my-project/locations/my-region/keyRings/my-kr/cryptoKeys/my-key``.
                The key needs to be in the same region as where the compute
                resource is created.

                If set, this TrainingPipeline will be secured by this key.

                Note: Model trained by this TrainingPipeline is also secured
                by this key if ``model_to_upload`` is not set separately.
        :param model_encryption_spec_key_name: Optional. The Cloud KMS resource identifier of the customer
                managed encryption key used to protect the model. Has the
                form:
                ``projects/my-project/locations/my-region/keyRings/my-kr/cryptoKeys/my-key``.
                The key needs to be in the same region as where the compute
                resource is created.

                If set, the trained Model will be secured by this key.
        :param staging_bucket: Bucket used to stage source and training artifacts.
        :param dataset: Vertex AI to fit this training against.
        :param annotation_schema_uri: Google Cloud Storage URI points to a YAML file describing
            annotation schema. The schema is defined as an OpenAPI 3.0.2
            [Schema Object]
            (https://github.com/OAI/OpenAPI-Specification/blob/main/versions/3.0.2.md#schema-object)

            Only Annotations that both match this schema and belong to
            DataItems not ignored by the split method are used in
            respectively training, validation or test role, depending on
            the role of the DataItem they are on.

            When used in conjunction with
            ``annotations_filter``,
            the Annotations used for training are filtered by both
            ``annotations_filter``
            and
            ``annotation_schema_uri``.
        :param model_display_name: If the script produces a managed Vertex AI Model. The display name of
                the Model. The name can be up to 128 characters long and can be consist
                of any UTF-8 characters.

                If not provided upon creation, the job's display_name is used.
        :param model_labels: Optional. The labels with user-defined metadata to
                organize your Models.
                Label keys and values can be no longer than 64
                characters, can only
                contain lowercase letters, numeric characters,
                underscores and dashes. International characters
                are allowed.
                See https://goo.gl/xmQnxf for more information
                and examples of labels.
        :param base_output_dir: GCS output directory of job. If not provided a timestamped directory in the
            staging directory will be used.

            Vertex AI sets the following environment variables when it runs your training code:

            -  AIP_MODEL_DIR: a Cloud Storage URI of a directory intended for saving model artifacts,
                i.e. <base_output_dir>/model/
            -  AIP_CHECKPOINT_DIR: a Cloud Storage URI of a directory intended for saving checkpoints,
                i.e. <base_output_dir>/checkpoints/
            -  AIP_TENSORBOARD_LOG_DIR: a Cloud Storage URI of a directory intended for saving TensorBoard
                logs, i.e. <base_output_dir>/logs/

        :param service_account: Specifies the service account for workload run-as account.
                Users submitting jobs must have act-as permission on this run-as account.
        :param network: The full name of the Compute Engine network to which the job
                should be peered.
                Private services access must already be configured for the network.
                If left unspecified, the job is not peered with any network.
        :param bigquery_destination: Provide this field if `dataset` is a BiqQuery dataset.
                The BigQuery project location where the training data is to
                be written to. In the given project a new dataset is created
                with name
                ``dataset_<dataset-id>_<annotation-type>_<timestamp-of-training-call>``
                where timestamp is in YYYY_MM_DDThh_mm_ss_sssZ format. All
                training input data will be written into that dataset. In
                the dataset three tables will be created, ``training``,
                ``validation`` and ``test``.

                -  AIP_DATA_FORMAT = "bigquery".
                -  AIP_TRAINING_DATA_URI ="bigquery_destination.dataset_*.training"
                -  AIP_VALIDATION_DATA_URI = "bigquery_destination.dataset_*.validation"
                -  AIP_TEST_DATA_URI = "bigquery_destination.dataset_*.test"
        :param args: Command line arguments to be passed to the Python script.
        :param environment_variables: Environment variables to be passed to the container.
                Should be a dictionary where keys are environment variable names
                and values are environment variable values for those names.
                At most 10 environment variables can be specified.
                The Name of the environment variable must be unique.
        :param replica_count: The number of worker replicas. If replica count = 1 then one chief
                replica will be provisioned. If replica_count > 1 the remainder will be
                provisioned as a worker replica pool.
        :param machine_type: The type of machine to use for training.
        :param accelerator_type: Hardware accelerator type. One of ACCELERATOR_TYPE_UNSPECIFIED,
                NVIDIA_TESLA_K80, NVIDIA_TESLA_P100, NVIDIA_TESLA_V100, NVIDIA_TESLA_P4,
                NVIDIA_TESLA_T4
        :param accelerator_count: The number of accelerators to attach to a worker replica.
        :param boot_disk_type: Type of the boot disk, default is `pd-ssd`.
                Valid values: `pd-ssd` (Persistent Disk Solid State Drive) or
                `pd-standard` (Persistent Disk Hard Disk Drive).
        :param boot_disk_size_gb: Size in GB of the boot disk, default is 100GB.
                boot disk size must be within the range of [100, 64000].
        :param training_fraction_split: Optional. The fraction of the input data that is to be used to train
                the Model. This is ignored if Dataset is not provided.
        :param validation_fraction_split: Optional. The fraction of the input data that is to be used to
            validate the Model. This is ignored if Dataset is not provided.
        :param test_fraction_split: Optional. The fraction of the input data that is to be used to evaluate
                the Model. This is ignored if Dataset is not provided.
        :param training_filter_split: Optional. A filter on DataItems of the Dataset. DataItems that match
                this filter are used to train the Model. A filter with same syntax
                as the one used in DatasetService.ListDataItems may be used. If a
                single DataItem is matched by more than one of the FilterSplit filters,
                then it is assigned to the first set that applies to it in the training,
                validation, test order. This is ignored if Dataset is not provided.
        :param validation_filter_split: Optional. A filter on DataItems of the Dataset. DataItems that match
                this filter are used to validate the Model. A filter with same syntax
                as the one used in DatasetService.ListDataItems may be used. If a
                single DataItem is matched by more than one of the FilterSplit filters,
                then it is assigned to the first set that applies to it in the training,
                validation, test order. This is ignored if Dataset is not provided.
        :param test_filter_split: Optional. A filter on DataItems of the Dataset. DataItems that match
                this filter are used to test the Model. A filter with same syntax
                as the one used in DatasetService.ListDataItems may be used. If a
                single DataItem is matched by more than one of the FilterSplit filters,
                then it is assigned to the first set that applies to it in the training,
                validation, test order. This is ignored if Dataset is not provided.
        :param predefined_split_column_name: Optional. The key is a name of one of the Dataset's data
                columns. The value of the key (either the label's value or
                value in the column) must be one of {``training``,
                ``validation``, ``test``}, and it defines to which set the
                given piece of data is assigned. If for a piece of data the
                key is not present or has an invalid value, that piece is
                ignored by the pipeline.

                Supported only for tabular and time series Datasets.
        :param timestamp_split_column_name: Optional. The key is a name of one of the Dataset's data
                columns. The value of the key values of the key (the values in
                the column) must be in RFC 3339 `date-time` format, where
                `time-offset` = `"Z"` (e.g. 1985-04-12T23:20:50.52Z). If for a
                piece of data the key is not present or has an invalid value,
                that piece is ignored by the pipeline.

                Supported only for tabular and time series Datasets.
        :param tensorboard: Optional. The name of a Vertex AI resource to which this CustomJob will upload
                logs. Format:
                ``projects/{project}/locations/{location}/tensorboards/{tensorboard}``
                For more information on configuring your service account please visit:
                https://cloud.google.com/vertex-ai/docs/experiments/tensorboard-training
        :param sync: Whether to execute the AI Platform job synchronously. If False, this method
                will be executed in concurrent Future and any downstream object will
                be immediately returned and synced when the Future has completed.
        """
        self._job = self.get_custom_container_training_job(
            project=project_id,
            location=region,
            display_name=display_name,
            container_uri=container_uri,
            command=command,
            model_serving_container_image_uri=model_serving_container_image_uri,
            model_serving_container_predict_route=model_serving_container_predict_route,
            model_serving_container_health_route=model_serving_container_health_route,
            model_serving_container_command=model_serving_container_command,
            model_serving_container_args=model_serving_container_args,
            model_serving_container_environment_variables=model_serving_container_environment_variables,
            model_serving_container_ports=model_serving_container_ports,
            model_description=model_description,
            model_instance_schema_uri=model_instance_schema_uri,
            model_parameters_schema_uri=model_parameters_schema_uri,
            model_prediction_schema_uri=model_prediction_schema_uri,
            labels=labels,
            training_encryption_spec_key_name=training_encryption_spec_key_name,
            model_encryption_spec_key_name=model_encryption_spec_key_name,
            staging_bucket=staging_bucket,
        )

        if not self._job:
            raise AirflowException("CustomJob was not created")

        model, training_id, custom_job_id = self._run_job(
            job=self._job,
            dataset=dataset,
            annotation_schema_uri=annotation_schema_uri,
            model_display_name=model_display_name,
            model_labels=model_labels,
            base_output_dir=base_output_dir,
            service_account=service_account,
            network=network,
            bigquery_destination=bigquery_destination,
            args=args,
            environment_variables=environment_variables,
            replica_count=replica_count,
            machine_type=machine_type,
            accelerator_type=accelerator_type,
            accelerator_count=accelerator_count,
            boot_disk_type=boot_disk_type,
            boot_disk_size_gb=boot_disk_size_gb,
            training_fraction_split=training_fraction_split,
            validation_fraction_split=validation_fraction_split,
            test_fraction_split=test_fraction_split,
            training_filter_split=training_filter_split,
            validation_filter_split=validation_filter_split,
            test_filter_split=test_filter_split,
            predefined_split_column_name=predefined_split_column_name,
            timestamp_split_column_name=timestamp_split_column_name,
            tensorboard=tensorboard,
            sync=sync,
        )

        return model, training_id, custom_job_id

    @GoogleBaseHook.fallback_to_default_project_id
    def create_custom_python_package_training_job(
        self,
        project_id: str,
        region: str,
        display_name: str,
        python_package_gcs_uri: str,
        python_module_name: str,
        container_uri: str,
        model_serving_container_image_uri: str | None = None,
        model_serving_container_predict_route: str | None = None,
        model_serving_container_health_route: str | None = None,
        model_serving_container_command: Sequence[str] | None = None,
        model_serving_container_args: Sequence[str] | None = None,
        model_serving_container_environment_variables: dict[str, str] | None = None,
        model_serving_container_ports: Sequence[int] | None = None,
        model_description: str | None = None,
        model_instance_schema_uri: str | None = None,
        model_parameters_schema_uri: str | None = None,
        model_prediction_schema_uri: str | None = None,
        labels: dict[str, str] | None = None,
        training_encryption_spec_key_name: str | None = None,
        model_encryption_spec_key_name: str | None = None,
        staging_bucket: str | None = None,
        # RUN
        dataset: None
        | (
            datasets.ImageDataset | datasets.TabularDataset | datasets.TextDataset | datasets.VideoDataset
        ) = None,
        annotation_schema_uri: str | None = None,
        model_display_name: str | None = None,
        model_labels: dict[str, str] | None = None,
        base_output_dir: str | None = None,
        service_account: str | None = None,
        network: str | None = None,
        bigquery_destination: str | None = None,
        args: list[str | float | int] | None = None,
        environment_variables: dict[str, str] | None = None,
        replica_count: int = 1,
        machine_type: str = "n1-standard-4",
        accelerator_type: str = "ACCELERATOR_TYPE_UNSPECIFIED",
        accelerator_count: int = 0,
        boot_disk_type: str = "pd-ssd",
        boot_disk_size_gb: int = 100,
        training_fraction_split: float | None = None,
        validation_fraction_split: float | None = None,
        test_fraction_split: float | None = None,
        training_filter_split: str | None = None,
        validation_filter_split: str | None = None,
        test_filter_split: str | None = None,
        predefined_split_column_name: str | None = None,
        timestamp_split_column_name: str | None = None,
        tensorboard: str | None = None,
        sync=True,
    ) -> tuple[models.Model | None, str, str]:
        """
        Create Custom Python Package Training Job

        :param display_name: Required. The user-defined name of this TrainingPipeline.
        :param python_package_gcs_uri: Required: GCS location of the training python package.
        :param python_module_name: Required: The module name of the training python package.
        :param container_uri: Required: Uri of the training container image in the GCR.
        :param model_serving_container_image_uri: If the training produces a managed Vertex AI Model, the URI
            of the Model serving container suitable for serving the model produced by the
            training script.
        :param model_serving_container_predict_route: If the training produces a managed Vertex AI Model, An
            HTTP path to send prediction requests to the container, and which must be supported
            by it. If not specified a default HTTP path will be used by Vertex AI.
        :param model_serving_container_health_route: If the training produces a managed Vertex AI Model, an
            HTTP path to send health check requests to the container, and which must be supported
            by it. If not specified a standard HTTP path will be used by AI Platform.
        :param model_serving_container_command: The command with which the container is run. Not executed
            within a shell. The Docker image's ENTRYPOINT is used if this is not provided.
            Variable references $(VAR_NAME) are expanded using the container's
            environment. If a variable cannot be resolved, the reference in the
            input string will be unchanged. The $(VAR_NAME) syntax can be escaped
            with a double $$, ie: $$(VAR_NAME). Escaped references will never be
            expanded, regardless of whether the variable exists or not.
        :param model_serving_container_args: The arguments to the command. The Docker image's CMD is used if
            this is not provided. Variable references $(VAR_NAME) are expanded using the
            container's environment. If a variable cannot be resolved, the reference
            in the input string will be unchanged. The $(VAR_NAME) syntax can be
            escaped with a double $$, ie: $$(VAR_NAME). Escaped references will
            never be expanded, regardless of whether the variable exists or not.
        :param model_serving_container_environment_variables: The environment variables that are to be
            present in the container. Should be a dictionary where keys are environment variable names
            and values are environment variable values for those names.
        :param model_serving_container_ports: Declaration of ports that are exposed by the container. This
            field is primarily informational, it gives Vertex AI information about the
            network connections the container uses. Listing or not a port here has
            no impact on whether the port is actually exposed, any port listening on
            the default "0.0.0.0" address inside a container will be accessible from
            the network.
        :param model_description: The description of the Model.
        :param model_instance_schema_uri: Optional. Points to a YAML file stored on Google Cloud
                Storage describing the format of a single instance, which
                are used in
                ``PredictRequest.instances``,
                ``ExplainRequest.instances``
                and
                ``BatchPredictionJob.input_config``.
                The schema is defined as an OpenAPI 3.0.2 `Schema
                Object <https://tinyurl.com/y538mdwt#schema-object>`__.
                AutoML Models always have this field populated by AI
                Platform. Note: The URI given on output will be immutable
                and probably different, including the URI scheme, than the
                one given on input. The output URI will point to a location
                where the user only has a read access.
        :param model_parameters_schema_uri: Optional. Points to a YAML file stored on Google Cloud
                Storage describing the parameters of prediction and
                explanation via
                ``PredictRequest.parameters``,
                ``ExplainRequest.parameters``
                and
                ``BatchPredictionJob.model_parameters``.
                The schema is defined as an OpenAPI 3.0.2 `Schema
                Object <https://tinyurl.com/y538mdwt#schema-object>`__.
                AutoML Models always have this field populated by AI
                Platform, if no parameters are supported it is set to an
                empty string. Note: The URI given on output will be
                immutable and probably different, including the URI scheme,
                than the one given on input. The output URI will point to a
                location where the user only has a read access.
        :param model_prediction_schema_uri: Optional. Points to a YAML file stored on Google Cloud
                Storage describing the format of a single prediction
                produced by this Model, which are returned via
                ``PredictResponse.predictions``,
                ``ExplainResponse.explanations``,
                and
                ``BatchPredictionJob.output_config``.
                The schema is defined as an OpenAPI 3.0.2 `Schema
                Object <https://tinyurl.com/y538mdwt#schema-object>`__.
                AutoML Models always have this field populated by AI
                Platform. Note: The URI given on output will be immutable
                and probably different, including the URI scheme, than the
                one given on input. The output URI will point to a location
                where the user only has a read access.
        :param project_id: Project to run training in.
        :param region: Location to run training in.
        :param labels: Optional. The labels with user-defined metadata to
                organize TrainingPipelines.
                Label keys and values can be no longer than 64
                characters, can only
                contain lowercase letters, numeric characters,
                underscores and dashes. International characters
                are allowed.
                See https://goo.gl/xmQnxf for more information
                and examples of labels.
        :param training_encryption_spec_key_name: Optional. The Cloud KMS resource identifier of the customer
                managed encryption key used to protect the training pipeline. Has the
                form:
                ``projects/my-project/locations/my-region/keyRings/my-kr/cryptoKeys/my-key``.
                The key needs to be in the same region as where the compute
                resource is created.

                If set, this TrainingPipeline will be secured by this key.

                Note: Model trained by this TrainingPipeline is also secured
                by this key if ``model_to_upload`` is not set separately.
        :param model_encryption_spec_key_name: Optional. The Cloud KMS resource identifier of the customer
                managed encryption key used to protect the model. Has the
                form:
                ``projects/my-project/locations/my-region/keyRings/my-kr/cryptoKeys/my-key``.
                The key needs to be in the same region as where the compute
                resource is created.

                If set, the trained Model will be secured by this key.
        :param staging_bucket: Bucket used to stage source and training artifacts.
        :param dataset: Vertex AI to fit this training against.
        :param annotation_schema_uri: Google Cloud Storage URI points to a YAML file describing
            annotation schema. The schema is defined as an OpenAPI 3.0.2
            [Schema Object]
            (https://github.com/OAI/OpenAPI-Specification/blob/main/versions/3.0.2.md#schema-object)

            Only Annotations that both match this schema and belong to
            DataItems not ignored by the split method are used in
            respectively training, validation or test role, depending on
            the role of the DataItem they are on.

            When used in conjunction with
            ``annotations_filter``,
            the Annotations used for training are filtered by both
            ``annotations_filter``
            and
            ``annotation_schema_uri``.
        :param model_display_name: If the script produces a managed Vertex AI Model. The display name of
                the Model. The name can be up to 128 characters long and can be consist
                of any UTF-8 characters.

                If not provided upon creation, the job's display_name is used.
        :param model_labels: Optional. The labels with user-defined metadata to
                organize your Models.
                Label keys and values can be no longer than 64
                characters, can only
                contain lowercase letters, numeric characters,
                underscores and dashes. International characters
                are allowed.
                See https://goo.gl/xmQnxf for more information
                and examples of labels.
        :param base_output_dir: GCS output directory of job. If not provided a timestamped directory in the
            staging directory will be used.

            Vertex AI sets the following environment variables when it runs your training code:

            -  AIP_MODEL_DIR: a Cloud Storage URI of a directory intended for saving model artifacts,
                i.e. <base_output_dir>/model/
            -  AIP_CHECKPOINT_DIR: a Cloud Storage URI of a directory intended for saving checkpoints,
                i.e. <base_output_dir>/checkpoints/
            -  AIP_TENSORBOARD_LOG_DIR: a Cloud Storage URI of a directory intended for saving TensorBoard
                logs, i.e. <base_output_dir>/logs/
        :param service_account: Specifies the service account for workload run-as account.
                Users submitting jobs must have act-as permission on this run-as account.
        :param network: The full name of the Compute Engine network to which the job
                should be peered.
                Private services access must already be configured for the network.
                If left unspecified, the job is not peered with any network.
        :param bigquery_destination: Provide this field if `dataset` is a BiqQuery dataset.
                The BigQuery project location where the training data is to
                be written to. In the given project a new dataset is created
                with name
                ``dataset_<dataset-id>_<annotation-type>_<timestamp-of-training-call>``
                where timestamp is in YYYY_MM_DDThh_mm_ss_sssZ format. All
                training input data will be written into that dataset. In
                the dataset three tables will be created, ``training``,
                ``validation`` and ``test``.

                -  AIP_DATA_FORMAT = "bigquery".
                -  AIP_TRAINING_DATA_URI ="bigquery_destination.dataset_*.training"
                -  AIP_VALIDATION_DATA_URI = "bigquery_destination.dataset_*.validation"
                -  AIP_TEST_DATA_URI = "bigquery_destination.dataset_*.test"
        :param args: Command line arguments to be passed to the Python script.
        :param environment_variables: Environment variables to be passed to the container.
                Should be a dictionary where keys are environment variable names
                and values are environment variable values for those names.
                At most 10 environment variables can be specified.
                The Name of the environment variable must be unique.
        :param replica_count: The number of worker replicas. If replica count = 1 then one chief
                replica will be provisioned. If replica_count > 1 the remainder will be
                provisioned as a worker replica pool.
        :param machine_type: The type of machine to use for training.
        :param accelerator_type: Hardware accelerator type. One of ACCELERATOR_TYPE_UNSPECIFIED,
                NVIDIA_TESLA_K80, NVIDIA_TESLA_P100, NVIDIA_TESLA_V100, NVIDIA_TESLA_P4,
                NVIDIA_TESLA_T4
        :param accelerator_count: The number of accelerators to attach to a worker replica.
        :param boot_disk_type: Type of the boot disk, default is `pd-ssd`.
                Valid values: `pd-ssd` (Persistent Disk Solid State Drive) or
                `pd-standard` (Persistent Disk Hard Disk Drive).
        :param boot_disk_size_gb: Size in GB of the boot disk, default is 100GB.
                boot disk size must be within the range of [100, 64000].
        :param training_fraction_split: Optional. The fraction of the input data that is to be used to train
                the Model. This is ignored if Dataset is not provided.
        :param validation_fraction_split: Optional. The fraction of the input data that is to be used to
            validate the Model. This is ignored if Dataset is not provided.
        :param test_fraction_split: Optional. The fraction of the input data that is to be used to evaluate
                the Model. This is ignored if Dataset is not provided.
        :param training_filter_split: Optional. A filter on DataItems of the Dataset. DataItems that match
                this filter are used to train the Model. A filter with same syntax
                as the one used in DatasetService.ListDataItems may be used. If a
                single DataItem is matched by more than one of the FilterSplit filters,
                then it is assigned to the first set that applies to it in the training,
                validation, test order. This is ignored if Dataset is not provided.
        :param validation_filter_split: Optional. A filter on DataItems of the Dataset. DataItems that match
                this filter are used to validate the Model. A filter with same syntax
                as the one used in DatasetService.ListDataItems may be used. If a
                single DataItem is matched by more than one of the FilterSplit filters,
                then it is assigned to the first set that applies to it in the training,
                validation, test order. This is ignored if Dataset is not provided.
        :param test_filter_split: Optional. A filter on DataItems of the Dataset. DataItems that match
                this filter are used to test the Model. A filter with same syntax
                as the one used in DatasetService.ListDataItems may be used. If a
                single DataItem is matched by more than one of the FilterSplit filters,
                then it is assigned to the first set that applies to it in the training,
                validation, test order. This is ignored if Dataset is not provided.
        :param predefined_split_column_name: Optional. The key is a name of one of the Dataset's data
                columns. The value of the key (either the label's value or
                value in the column) must be one of {``training``,
                ``validation``, ``test``}, and it defines to which set the
                given piece of data is assigned. If for a piece of data the
                key is not present or has an invalid value, that piece is
                ignored by the pipeline.

                Supported only for tabular and time series Datasets.
        :param timestamp_split_column_name: Optional. The key is a name of one of the Dataset's data
                columns. The value of the key values of the key (the values in
                the column) must be in RFC 3339 `date-time` format, where
                `time-offset` = `"Z"` (e.g. 1985-04-12T23:20:50.52Z). If for a
                piece of data the key is not present or has an invalid value,
                that piece is ignored by the pipeline.

                Supported only for tabular and time series Datasets.
        :param tensorboard: Optional. The name of a Vertex AI resource to which this CustomJob will upload
                logs. Format:
                ``projects/{project}/locations/{location}/tensorboards/{tensorboard}``
                For more information on configuring your service account please visit:
                https://cloud.google.com/vertex-ai/docs/experiments/tensorboard-training
        :param sync: Whether to execute the AI Platform job synchronously. If False, this method
                will be executed in concurrent Future and any downstream object will
                be immediately returned and synced when the Future has completed.
        """
        self._job = self.get_custom_python_package_training_job(
            project=project_id,
            location=region,
            display_name=display_name,
            python_package_gcs_uri=python_package_gcs_uri,
            python_module_name=python_module_name,
            container_uri=container_uri,
            model_serving_container_image_uri=model_serving_container_image_uri,
            model_serving_container_predict_route=model_serving_container_predict_route,
            model_serving_container_health_route=model_serving_container_health_route,
            model_serving_container_command=model_serving_container_command,
            model_serving_container_args=model_serving_container_args,
            model_serving_container_environment_variables=model_serving_container_environment_variables,
            model_serving_container_ports=model_serving_container_ports,
            model_description=model_description,
            model_instance_schema_uri=model_instance_schema_uri,
            model_parameters_schema_uri=model_parameters_schema_uri,
            model_prediction_schema_uri=model_prediction_schema_uri,
            labels=labels,
            training_encryption_spec_key_name=training_encryption_spec_key_name,
            model_encryption_spec_key_name=model_encryption_spec_key_name,
            staging_bucket=staging_bucket,
        )

        if not self._job:
            raise AirflowException("CustomJob was not created")

        model, training_id, custom_job_id = self._run_job(
            job=self._job,
            dataset=dataset,
            annotation_schema_uri=annotation_schema_uri,
            model_display_name=model_display_name,
            model_labels=model_labels,
            base_output_dir=base_output_dir,
            service_account=service_account,
            network=network,
            bigquery_destination=bigquery_destination,
            args=args,
            environment_variables=environment_variables,
            replica_count=replica_count,
            machine_type=machine_type,
            accelerator_type=accelerator_type,
            accelerator_count=accelerator_count,
            boot_disk_type=boot_disk_type,
            boot_disk_size_gb=boot_disk_size_gb,
            training_fraction_split=training_fraction_split,
            validation_fraction_split=validation_fraction_split,
            test_fraction_split=test_fraction_split,
            training_filter_split=training_filter_split,
            validation_filter_split=validation_filter_split,
            test_filter_split=test_filter_split,
            predefined_split_column_name=predefined_split_column_name,
            timestamp_split_column_name=timestamp_split_column_name,
            tensorboard=tensorboard,
            sync=sync,
        )

        return model, training_id, custom_job_id

    @GoogleBaseHook.fallback_to_default_project_id
    def create_custom_training_job(
        self,
        project_id: str,
        region: str,
        display_name: str,
        script_path: str,
        container_uri: str,
        requirements: Sequence[str] | None = None,
        model_serving_container_image_uri: str | None = None,
        model_serving_container_predict_route: str | None = None,
        model_serving_container_health_route: str | None = None,
        model_serving_container_command: Sequence[str] | None = None,
        model_serving_container_args: Sequence[str] | None = None,
        model_serving_container_environment_variables: dict[str, str] | None = None,
        model_serving_container_ports: Sequence[int] | None = None,
        model_description: str | None = None,
        model_instance_schema_uri: str | None = None,
        model_parameters_schema_uri: str | None = None,
        model_prediction_schema_uri: str | None = None,
        labels: dict[str, str] | None = None,
        training_encryption_spec_key_name: str | None = None,
        model_encryption_spec_key_name: str | None = None,
        staging_bucket: str | None = None,
        # RUN
        dataset: None
        | (
            datasets.ImageDataset | datasets.TabularDataset | datasets.TextDataset | datasets.VideoDataset
        ) = None,
        annotation_schema_uri: str | None = None,
        model_display_name: str | None = None,
        model_labels: dict[str, str] | None = None,
        base_output_dir: str | None = None,
        service_account: str | None = None,
        network: str | None = None,
        bigquery_destination: str | None = None,
        args: list[str | float | int] | None = None,
        environment_variables: dict[str, str] | None = None,
        replica_count: int = 1,
        machine_type: str = "n1-standard-4",
        accelerator_type: str = "ACCELERATOR_TYPE_UNSPECIFIED",
        accelerator_count: int = 0,
        boot_disk_type: str = "pd-ssd",
        boot_disk_size_gb: int = 100,
        training_fraction_split: float | None = None,
        validation_fraction_split: float | None = None,
        test_fraction_split: float | None = None,
        training_filter_split: str | None = None,
        validation_filter_split: str | None = None,
        test_filter_split: str | None = None,
        predefined_split_column_name: str | None = None,
        timestamp_split_column_name: str | None = None,
        tensorboard: str | None = None,
        sync=True,
    ) -> tuple[models.Model | None, str, str]:
        """
        Create Custom Training Job

        :param display_name: Required. The user-defined name of this TrainingPipeline.
        :param script_path: Required. Local path to training script.
        :param container_uri: Required: Uri of the training container image in the GCR.
        :param requirements: List of python packages dependencies of script.
        :param model_serving_container_image_uri: If the training produces a managed Vertex AI Model, the URI
            of the Model serving container suitable for serving the model produced by the
            training script.
        :param model_serving_container_predict_route: If the training produces a managed Vertex AI Model, An
            HTTP path to send prediction requests to the container, and which must be supported
            by it. If not specified a default HTTP path will be used by Vertex AI.
        :param model_serving_container_health_route: If the training produces a managed Vertex AI Model, an
            HTTP path to send health check requests to the container, and which must be supported
            by it. If not specified a standard HTTP path will be used by AI Platform.
        :param model_serving_container_command: The command with which the container is run. Not executed
            within a shell. The Docker image's ENTRYPOINT is used if this is not provided.
            Variable references $(VAR_NAME) are expanded using the container's
            environment. If a variable cannot be resolved, the reference in the
            input string will be unchanged. The $(VAR_NAME) syntax can be escaped
            with a double $$, ie: $$(VAR_NAME). Escaped references will never be
            expanded, regardless of whether the variable exists or not.
        :param model_serving_container_args: The arguments to the command. The Docker image's CMD is used if
            this is not provided. Variable references $(VAR_NAME) are expanded using the
            container's environment. If a variable cannot be resolved, the reference
            in the input string will be unchanged. The $(VAR_NAME) syntax can be
            escaped with a double $$, ie: $$(VAR_NAME). Escaped references will
            never be expanded, regardless of whether the variable exists or not.
        :param model_serving_container_environment_variables: The environment variables that are to be
            present in the container. Should be a dictionary where keys are environment variable names
            and values are environment variable values for those names.
        :param model_serving_container_ports: Declaration of ports that are exposed by the container. This
            field is primarily informational, it gives Vertex AI information about the
            network connections the container uses. Listing or not a port here has
            no impact on whether the port is actually exposed, any port listening on
            the default "0.0.0.0" address inside a container will be accessible from
            the network.
        :param model_description: The description of the Model.
        :param model_instance_schema_uri: Optional. Points to a YAML file stored on Google Cloud
                Storage describing the format of a single instance, which
                are used in
                ``PredictRequest.instances``,
                ``ExplainRequest.instances``
                and
                ``BatchPredictionJob.input_config``.
                The schema is defined as an OpenAPI 3.0.2 `Schema
                Object <https://tinyurl.com/y538mdwt#schema-object>`__.
                AutoML Models always have this field populated by AI
                Platform. Note: The URI given on output will be immutable
                and probably different, including the URI scheme, than the
                one given on input. The output URI will point to a location
                where the user only has a read access.
        :param model_parameters_schema_uri: Optional. Points to a YAML file stored on Google Cloud
                Storage describing the parameters of prediction and
                explanation via
                ``PredictRequest.parameters``,
                ``ExplainRequest.parameters``
                and
                ``BatchPredictionJob.model_parameters``.
                The schema is defined as an OpenAPI 3.0.2 `Schema
                Object <https://tinyurl.com/y538mdwt#schema-object>`__.
                AutoML Models always have this field populated by AI
                Platform, if no parameters are supported it is set to an
                empty string. Note: The URI given on output will be
                immutable and probably different, including the URI scheme,
                than the one given on input. The output URI will point to a
                location where the user only has a read access.
        :param model_prediction_schema_uri: Optional. Points to a YAML file stored on Google Cloud
                Storage describing the format of a single prediction
                produced by this Model, which are returned via
                ``PredictResponse.predictions``,
                ``ExplainResponse.explanations``,
                and
                ``BatchPredictionJob.output_config``.
                The schema is defined as an OpenAPI 3.0.2 `Schema
                Object <https://tinyurl.com/y538mdwt#schema-object>`__.
                AutoML Models always have this field populated by AI
                Platform. Note: The URI given on output will be immutable
                and probably different, including the URI scheme, than the
                one given on input. The output URI will point to a location
                where the user only has a read access.
        :param project_id: Project to run training in.
        :param region: Location to run training in.
        :param labels: Optional. The labels with user-defined metadata to
                organize TrainingPipelines.
                Label keys and values can be no longer than 64
                characters, can only
                contain lowercase letters, numeric characters,
                underscores and dashes. International characters
                are allowed.
                See https://goo.gl/xmQnxf for more information
                and examples of labels.
        :param training_encryption_spec_key_name: Optional. The Cloud KMS resource identifier of the customer
                managed encryption key used to protect the training pipeline. Has the
                form:
                ``projects/my-project/locations/my-region/keyRings/my-kr/cryptoKeys/my-key``.
                The key needs to be in the same region as where the compute
                resource is created.

                If set, this TrainingPipeline will be secured by this key.

                Note: Model trained by this TrainingPipeline is also secured
                by this key if ``model_to_upload`` is not set separately.
        :param model_encryption_spec_key_name: Optional. The Cloud KMS resource identifier of the customer
                managed encryption key used to protect the model. Has the
                form:
                ``projects/my-project/locations/my-region/keyRings/my-kr/cryptoKeys/my-key``.
                The key needs to be in the same region as where the compute
                resource is created.

                If set, the trained Model will be secured by this key.
        :param staging_bucket: Bucket used to stage source and training artifacts.
        :param dataset: Vertex AI to fit this training against.
        :param annotation_schema_uri: Google Cloud Storage URI points to a YAML file describing
            annotation schema. The schema is defined as an OpenAPI 3.0.2
            [Schema Object]
            (https://github.com/OAI/OpenAPI-Specification/blob/main/versions/3.0.2.md#schema-object)

            Only Annotations that both match this schema and belong to
            DataItems not ignored by the split method are used in
            respectively training, validation or test role, depending on
            the role of the DataItem they are on.

            When used in conjunction with
            ``annotations_filter``,
            the Annotations used for training are filtered by both
            ``annotations_filter``
            and
            ``annotation_schema_uri``.
        :param model_display_name: If the script produces a managed Vertex AI Model. The display name of
                the Model. The name can be up to 128 characters long and can be consist
                of any UTF-8 characters.

                If not provided upon creation, the job's display_name is used.
        :param model_labels: Optional. The labels with user-defined metadata to
                organize your Models.
                Label keys and values can be no longer than 64
                characters, can only
                contain lowercase letters, numeric characters,
                underscores and dashes. International characters
                are allowed.
                See https://goo.gl/xmQnxf for more information
                and examples of labels.
        :param base_output_dir: GCS output directory of job. If not provided a timestamped directory in the
            staging directory will be used.

            Vertex AI sets the following environment variables when it runs your training code:

            -  AIP_MODEL_DIR: a Cloud Storage URI of a directory intended for saving model artifacts,
                i.e. <base_output_dir>/model/
            -  AIP_CHECKPOINT_DIR: a Cloud Storage URI of a directory intended for saving checkpoints,
                i.e. <base_output_dir>/checkpoints/
            -  AIP_TENSORBOARD_LOG_DIR: a Cloud Storage URI of a directory intended for saving TensorBoard
                logs, i.e. <base_output_dir>/logs/
        :param service_account: Specifies the service account for workload run-as account.
                Users submitting jobs must have act-as permission on this run-as account.
        :param network: The full name of the Compute Engine network to which the job
                should be peered.
                Private services access must already be configured for the network.
                If left unspecified, the job is not peered with any network.
        :param bigquery_destination: Provide this field if `dataset` is a BiqQuery dataset.
                The BigQuery project location where the training data is to
                be written to. In the given project a new dataset is created
                with name
                ``dataset_<dataset-id>_<annotation-type>_<timestamp-of-training-call>``
                where timestamp is in YYYY_MM_DDThh_mm_ss_sssZ format. All
                training input data will be written into that dataset. In
                the dataset three tables will be created, ``training``,
                ``validation`` and ``test``.

                -  AIP_DATA_FORMAT = "bigquery".
                -  AIP_TRAINING_DATA_URI ="bigquery_destination.dataset_*.training"
                -  AIP_VALIDATION_DATA_URI = "bigquery_destination.dataset_*.validation"
                -  AIP_TEST_DATA_URI = "bigquery_destination.dataset_*.test"
        :param args: Command line arguments to be passed to the Python script.
        :param environment_variables: Environment variables to be passed to the container.
                Should be a dictionary where keys are environment variable names
                and values are environment variable values for those names.
                At most 10 environment variables can be specified.
                The Name of the environment variable must be unique.
        :param replica_count: The number of worker replicas. If replica count = 1 then one chief
                replica will be provisioned. If replica_count > 1 the remainder will be
                provisioned as a worker replica pool.
        :param machine_type: The type of machine to use for training.
        :param accelerator_type: Hardware accelerator type. One of ACCELERATOR_TYPE_UNSPECIFIED,
                NVIDIA_TESLA_K80, NVIDIA_TESLA_P100, NVIDIA_TESLA_V100, NVIDIA_TESLA_P4,
                NVIDIA_TESLA_T4
        :param accelerator_count: The number of accelerators to attach to a worker replica.
        :param boot_disk_type: Type of the boot disk, default is `pd-ssd`.
                Valid values: `pd-ssd` (Persistent Disk Solid State Drive) or
                `pd-standard` (Persistent Disk Hard Disk Drive).
        :param boot_disk_size_gb: Size in GB of the boot disk, default is 100GB.
                boot disk size must be within the range of [100, 64000].
        :param training_fraction_split: Optional. The fraction of the input data that is to be used to train
                the Model. This is ignored if Dataset is not provided.
        :param validation_fraction_split: Optional. The fraction of the input data that is to be used to
            validate the Model. This is ignored if Dataset is not provided.
        :param test_fraction_split: Optional. The fraction of the input data that is to be used to evaluate
                the Model. This is ignored if Dataset is not provided.
        :param training_filter_split: Optional. A filter on DataItems of the Dataset. DataItems that match
                this filter are used to train the Model. A filter with same syntax
                as the one used in DatasetService.ListDataItems may be used. If a
                single DataItem is matched by more than one of the FilterSplit filters,
                then it is assigned to the first set that applies to it in the training,
                validation, test order. This is ignored if Dataset is not provided.
        :param validation_filter_split: Optional. A filter on DataItems of the Dataset. DataItems that match
                this filter are used to validate the Model. A filter with same syntax
                as the one used in DatasetService.ListDataItems may be used. If a
                single DataItem is matched by more than one of the FilterSplit filters,
                then it is assigned to the first set that applies to it in the training,
                validation, test order. This is ignored if Dataset is not provided.
        :param test_filter_split: Optional. A filter on DataItems of the Dataset. DataItems that match
                this filter are used to test the Model. A filter with same syntax
                as the one used in DatasetService.ListDataItems may be used. If a
                single DataItem is matched by more than one of the FilterSplit filters,
                then it is assigned to the first set that applies to it in the training,
                validation, test order. This is ignored if Dataset is not provided.
        :param predefined_split_column_name: Optional. The key is a name of one of the Dataset's data
                columns. The value of the key (either the label's value or
                value in the column) must be one of {``training``,
                ``validation``, ``test``}, and it defines to which set the
                given piece of data is assigned. If for a piece of data the
                key is not present or has an invalid value, that piece is
                ignored by the pipeline.

                Supported only for tabular and time series Datasets.
        :param timestamp_split_column_name: Optional. The key is a name of one of the Dataset's data
                columns. The value of the key values of the key (the values in
                the column) must be in RFC 3339 `date-time` format, where
                `time-offset` = `"Z"` (e.g. 1985-04-12T23:20:50.52Z). If for a
                piece of data the key is not present or has an invalid value,
                that piece is ignored by the pipeline.

                Supported only for tabular and time series Datasets.
        :param tensorboard: Optional. The name of a Vertex AI resource to which this CustomJob will upload
                logs. Format:
                ``projects/{project}/locations/{location}/tensorboards/{tensorboard}``
                For more information on configuring your service account please visit:
                https://cloud.google.com/vertex-ai/docs/experiments/tensorboard-training
        :param sync: Whether to execute the AI Platform job synchronously. If False, this method
                will be executed in concurrent Future and any downstream object will
                be immediately returned and synced when the Future has completed.
        """
        self._job = self.get_custom_training_job(
            project=project_id,
            location=region,
            display_name=display_name,
            script_path=script_path,
            container_uri=container_uri,
            requirements=requirements,
            model_serving_container_image_uri=model_serving_container_image_uri,
            model_serving_container_predict_route=model_serving_container_predict_route,
            model_serving_container_health_route=model_serving_container_health_route,
            model_serving_container_command=model_serving_container_command,
            model_serving_container_args=model_serving_container_args,
            model_serving_container_environment_variables=model_serving_container_environment_variables,
            model_serving_container_ports=model_serving_container_ports,
            model_description=model_description,
            model_instance_schema_uri=model_instance_schema_uri,
            model_parameters_schema_uri=model_parameters_schema_uri,
            model_prediction_schema_uri=model_prediction_schema_uri,
            labels=labels,
            training_encryption_spec_key_name=training_encryption_spec_key_name,
            model_encryption_spec_key_name=model_encryption_spec_key_name,
            staging_bucket=staging_bucket,
        )

        if not self._job:
            raise AirflowException("CustomJob was not created")

        model, training_id, custom_job_id = self._run_job(
            job=self._job,
            dataset=dataset,
            annotation_schema_uri=annotation_schema_uri,
            model_display_name=model_display_name,
            model_labels=model_labels,
            base_output_dir=base_output_dir,
            service_account=service_account,
            network=network,
            bigquery_destination=bigquery_destination,
            args=args,
            environment_variables=environment_variables,
            replica_count=replica_count,
            machine_type=machine_type,
            accelerator_type=accelerator_type,
            accelerator_count=accelerator_count,
            boot_disk_type=boot_disk_type,
            boot_disk_size_gb=boot_disk_size_gb,
            training_fraction_split=training_fraction_split,
            validation_fraction_split=validation_fraction_split,
            test_fraction_split=test_fraction_split,
            training_filter_split=training_filter_split,
            validation_filter_split=validation_filter_split,
            test_filter_split=test_filter_split,
            predefined_split_column_name=predefined_split_column_name,
            timestamp_split_column_name=timestamp_split_column_name,
            tensorboard=tensorboard,
            sync=sync,
        )

        return model, training_id, custom_job_id

    @GoogleBaseHook.fallback_to_default_project_id
    def delete_pipeline_job(
        self,
        project_id: str,
        region: str,
        pipeline_job: str,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> Operation:
        """
        Deletes a PipelineJob.

        :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
        :param region: Required. The ID of the Google Cloud region that the service belongs to.
        :param pipeline_job: Required. The name of the PipelineJob resource to be deleted.
        :param retry: Designation of what errors, if any, should be retried.
        :param timeout: The timeout for this request.
        :param metadata: Strings which should be sent along with the request as metadata.
        """
        client = self.get_pipeline_service_client(region)
        name = client.pipeline_job_path(project_id, region, pipeline_job)

        result = client.delete_pipeline_job(
            request={
                "name": name,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result

    @GoogleBaseHook.fallback_to_default_project_id
    def delete_training_pipeline(
        self,
        project_id: str,
        region: str,
        training_pipeline: str,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> Operation:
        """
        Deletes a TrainingPipeline.

        :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
        :param region: Required. The ID of the Google Cloud region that the service belongs to.
        :param training_pipeline: Required. The name of the TrainingPipeline resource to be deleted.
        :param retry: Designation of what errors, if any, should be retried.
        :param timeout: The timeout for this request.
        :param metadata: Strings which should be sent along with the request as metadata.
        """
        client = self.get_pipeline_service_client(region)
        name = client.training_pipeline_path(project_id, region, training_pipeline)

        result = client.delete_training_pipeline(
            request={
                "name": name,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result

    @GoogleBaseHook.fallback_to_default_project_id
    def delete_custom_job(
        self,
        project_id: str,
        region: str,
        custom_job: str,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> Operation:
        """
        Deletes a CustomJob.

        :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
        :param region: Required. The ID of the Google Cloud region that the service belongs to.
        :param custom_job: Required. The name of the CustomJob to delete.
        :param retry: Designation of what errors, if any, should be retried.
        :param timeout: The timeout for this request.
        :param metadata: Strings which should be sent along with the request as metadata.
        """
        client = self.get_job_service_client(region)
        name = client.custom_job_path(project_id, region, custom_job)

        result = client.delete_custom_job(
            request={
                "name": name,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result

    @GoogleBaseHook.fallback_to_default_project_id
    def get_pipeline_job(
        self,
        project_id: str,
        region: str,
        pipeline_job: str,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> PipelineJob:
        """
        Gets a PipelineJob.

        :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
        :param region: Required. The ID of the Google Cloud region that the service belongs to.
        :param pipeline_job: Required. The name of the PipelineJob resource.
        :param retry: Designation of what errors, if any, should be retried.
        :param timeout: The timeout for this request.
        :param metadata: Strings which should be sent along with the request as metadata.
        """
        client = self.get_pipeline_service_client(region)
        name = client.pipeline_job_path(project_id, region, pipeline_job)

        result = client.get_pipeline_job(
            request={
                "name": name,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result

    @GoogleBaseHook.fallback_to_default_project_id
    def get_training_pipeline(
        self,
        project_id: str,
        region: str,
        training_pipeline: str,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> TrainingPipeline:
        """
        Gets a TrainingPipeline.

        :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
        :param region: Required. The ID of the Google Cloud region that the service belongs to.
        :param training_pipeline: Required. The name of the TrainingPipeline resource.
        :param retry: Designation of what errors, if any, should be retried.
        :param timeout: The timeout for this request.
        :param metadata: Strings which should be sent along with the request as metadata.
        """
        client = self.get_pipeline_service_client(region)
        name = client.training_pipeline_path(project_id, region, training_pipeline)

        result = client.get_training_pipeline(
            request={
                "name": name,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result

    @GoogleBaseHook.fallback_to_default_project_id
    def get_custom_job(
        self,
        project_id: str,
        region: str,
        custom_job: str,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> CustomJob:
        """
        Gets a CustomJob.

        :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
        :param region: Required. The ID of the Google Cloud region that the service belongs to.
        :param custom_job: Required. The name of the CustomJob to get.
        :param retry: Designation of what errors, if any, should be retried.
        :param timeout: The timeout for this request.
        :param metadata: Strings which should be sent along with the request as metadata.
        """
        client = self.get_job_service_client(region)
        name = JobServiceClient.custom_job_path(project_id, region, custom_job)

        result = client.get_custom_job(
            request={
                "name": name,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result

    @GoogleBaseHook.fallback_to_default_project_id
    def list_pipeline_jobs(
        self,
        project_id: str,
        region: str,
        page_size: int | None = None,
        page_token: str | None = None,
        filter: str | None = None,
        order_by: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> ListPipelineJobsPager:
        """
        Lists PipelineJobs in a Location.

        :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
        :param region: Required. The ID of the Google Cloud region that the service belongs to.
        :param filter: Optional. Lists the PipelineJobs that match the filter expression. The
            following fields are supported:

            -  ``pipeline_name``: Supports ``=`` and ``!=`` comparisons.
            -  ``display_name``: Supports ``=``, ``!=`` comparisons, and
               ``:`` wildcard.
            -  ``pipeline_job_user_id``: Supports ``=``, ``!=``
               comparisons, and ``:`` wildcard. for example, can check
               if pipeline's display_name contains *step* by doing
               display_name:"*step*"
            -  ``create_time``: Supports ``=``, ``!=``, ``<``, ``>``,
               ``<=``, and ``>=`` comparisons. Values must be in RFC
               3339 format.
            -  ``update_time``: Supports ``=``, ``!=``, ``<``, ``>``,
               ``<=``, and ``>=`` comparisons. Values must be in RFC
               3339 format.
            -  ``end_time``: Supports ``=``, ``!=``, ``<``, ``>``,
               ``<=``, and ``>=`` comparisons. Values must be in RFC
               3339 format.
            -  ``labels``: Supports key-value equality and key presence.

            Filter expressions can be combined together using logical
            operators (``AND`` & ``OR``). For example:
            ``pipeline_name="test" AND create_time>"2020-05-18T13:30:00Z"``.

            The syntax to define filter expression is based on
            https://google.aip.dev/160.
        :param page_size: Optional. The standard list page size.
        :param page_token: Optional. The standard list page token. Typically obtained via
            [ListPipelineJobsResponse.next_page_token][google.cloud.aiplatform.v1.ListPipelineJobsResponse.next_page_token]
            of the previous
            [PipelineService.ListPipelineJobs][google.cloud.aiplatform.v1.PipelineService.ListPipelineJobs]
            call.
        :param order_by: Optional. A comma-separated list of fields to order by. The default
            sort order is in ascending order. Use "desc" after a field
            name for descending. You can have multiple order_by fields
            provided e.g. "create_time desc, end_time", "end_time,
            start_time, update_time" For example, using "create_time
            desc, end_time" will order results by create time in
            descending order, and if there are multiple jobs having the
            same create time, order them by the end time in ascending
            order. if order_by is not specified, it will order by
            default order is create time in descending order. Supported
            fields:

            -  ``create_time``
            -  ``update_time``
            -  ``end_time``
            -  ``start_time``
        :param retry: Designation of what errors, if any, should be retried.
        :param timeout: The timeout for this request.
        :param metadata: Strings which should be sent along with the request as metadata.
        """
        client = self.get_pipeline_service_client(region)
        parent = client.common_location_path(project_id, region)

        result = client.list_pipeline_jobs(
            request={
                "parent": parent,
                "page_size": page_size,
                "page_token": page_token,
                "filter": filter,
                "order_by": order_by,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result

    @GoogleBaseHook.fallback_to_default_project_id
    def list_training_pipelines(
        self,
        project_id: str,
        region: str,
        page_size: int | None = None,
        page_token: str | None = None,
        filter: str | None = None,
        read_mask: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> ListTrainingPipelinesPager:
        """
        Lists TrainingPipelines in a Location.

        :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
        :param region: Required. The ID of the Google Cloud region that the service belongs to.
        :param filter: Optional. The standard list filter. Supported fields:

            -  ``display_name`` supports = and !=.

            -  ``state`` supports = and !=.

            Some examples of using the filter are:

            -  ``state="PIPELINE_STATE_SUCCEEDED" AND display_name="my_pipeline"``

            -  ``state="PIPELINE_STATE_RUNNING" OR display_name="my_pipeline"``

            -  ``NOT display_name="my_pipeline"``

            -  ``state="PIPELINE_STATE_FAILED"``
        :param page_size: Optional. The standard list page size.
        :param page_token: Optional. The standard list page token. Typically obtained via
            [ListTrainingPipelinesResponse.next_page_token][google.cloud.aiplatform.v1.ListTrainingPipelinesResponse.next_page_token]
            of the previous
            [PipelineService.ListTrainingPipelines][google.cloud.aiplatform.v1.PipelineService.ListTrainingPipelines]
            call.
        :param read_mask: Optional. Mask specifying which fields to read.
        :param retry: Designation of what errors, if any, should be retried.
        :param timeout: The timeout for this request.
        :param metadata: Strings which should be sent along with the request as metadata.
        """
        client = self.get_pipeline_service_client(region)
        parent = client.common_location_path(project_id, region)

        result = client.list_training_pipelines(
            request={
                "parent": parent,
                "page_size": page_size,
                "page_token": page_token,
                "filter": filter,
                "read_mask": read_mask,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result

    @GoogleBaseHook.fallback_to_default_project_id
    def list_custom_jobs(
        self,
        project_id: str,
        region: str,
        page_size: int | None,
        page_token: str | None,
        filter: str | None,
        read_mask: str | None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> ListCustomJobsPager:
        """
        Lists CustomJobs in a Location.

        :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
        :param region: Required. The ID of the Google Cloud region that the service belongs to.
        :param filter: Optional. The standard list filter. Supported fields:

            -  ``display_name`` supports = and !=.

            -  ``state`` supports = and !=.

            Some examples of using the filter are:

            -  ``state="PIPELINE_STATE_SUCCEEDED" AND display_name="my_pipeline"``

            -  ``state="PIPELINE_STATE_RUNNING" OR display_name="my_pipeline"``

            -  ``NOT display_name="my_pipeline"``

            -  ``state="PIPELINE_STATE_FAILED"``
        :param page_size: Optional. The standard list page size.
        :param page_token: Optional. The standard list page token. Typically obtained via
            [ListTrainingPipelinesResponse.next_page_token][google.cloud.aiplatform.v1.ListTrainingPipelinesResponse.next_page_token]
            of the previous
            [PipelineService.ListTrainingPipelines][google.cloud.aiplatform.v1.PipelineService.ListTrainingPipelines]
            call.
        :param read_mask: Optional. Mask specifying which fields to read.
        :param retry: Designation of what errors, if any, should be retried.
        :param timeout: The timeout for this request.
        :param metadata: Strings which should be sent along with the request as metadata.
        """
        client = self.get_job_service_client(region)
        parent = JobServiceClient.common_location_path(project_id, region)

        result = client.list_custom_jobs(
            request={
                "parent": parent,
                "page_size": page_size,
                "page_token": page_token,
                "filter": filter,
                "read_mask": read_mask,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result
