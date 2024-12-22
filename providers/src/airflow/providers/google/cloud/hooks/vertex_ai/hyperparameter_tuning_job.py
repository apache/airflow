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
"""
This module contains a Google Cloud Vertex AI hook.

.. spelling:word-list::

    JobServiceAsyncClient
"""

from __future__ import annotations

import asyncio
from collections.abc import Sequence
from typing import TYPE_CHECKING

from google.api_core.client_options import ClientOptions
from google.api_core.gapic_v1.method import DEFAULT, _MethodDefault
from google.cloud.aiplatform import CustomJob, HyperparameterTuningJob, gapic, hyperparameter_tuning
from google.cloud.aiplatform_v1 import JobServiceAsyncClient, JobServiceClient, JobState, types

from airflow.exceptions import AirflowException
from airflow.providers.google.common.consts import CLIENT_INFO
from airflow.providers.google.common.hooks.base_google import GoogleBaseAsyncHook, GoogleBaseHook

if TYPE_CHECKING:
    from google.api_core.operation import Operation
    from google.api_core.retry import AsyncRetry, Retry
    from google.cloud.aiplatform_v1.services.job_service.pagers import ListHyperparameterTuningJobsPager


class HyperparameterTuningJobHook(GoogleBaseHook):
    """Hook for Google Cloud Vertex AI Hyperparameter Tuning Job APIs."""

    def __init__(
        self,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(
            gcp_conn_id=gcp_conn_id,
            impersonation_chain=impersonation_chain,
            **kwargs,
        )
        self._hyperparameter_tuning_job: HyperparameterTuningJob | None = None

    def get_job_service_client(self, region: str | None = None) -> JobServiceClient:
        """Return JobServiceClient."""
        if region and region != "global":
            client_options = ClientOptions(api_endpoint=f"{region}-aiplatform.googleapis.com:443")
        else:
            client_options = ClientOptions()

        return JobServiceClient(
            credentials=self.get_credentials(), client_info=self.client_info, client_options=client_options
        )

    def get_hyperparameter_tuning_job_object(
        self,
        display_name: str,
        custom_job: CustomJob,
        metric_spec: dict[str, str],
        parameter_spec: dict[str, hyperparameter_tuning._ParameterSpec],
        max_trial_count: int,
        parallel_trial_count: int,
        max_failed_trial_count: int = 0,
        search_algorithm: str | None = None,
        measurement_selection: str | None = "best",
        project: str | None = None,
        location: str | None = None,
        labels: dict[str, str] | None = None,
        encryption_spec_key_name: str | None = None,
    ) -> HyperparameterTuningJob:
        """Return HyperparameterTuningJob object."""
        return HyperparameterTuningJob(
            display_name=display_name,
            custom_job=custom_job,
            metric_spec=metric_spec,
            parameter_spec=parameter_spec,
            max_trial_count=max_trial_count,
            parallel_trial_count=parallel_trial_count,
            max_failed_trial_count=max_failed_trial_count,
            search_algorithm=search_algorithm,
            measurement_selection=measurement_selection,
            project=project,
            location=location,
            credentials=self.get_credentials(),
            labels=labels,
            encryption_spec_key_name=encryption_spec_key_name,
        )

    def get_custom_job_object(
        self,
        display_name: str,
        worker_pool_specs: list[dict] | list[gapic.WorkerPoolSpec],
        base_output_dir: str | None = None,
        project: str | None = None,
        location: str | None = None,
        labels: dict[str, str] | None = None,
        encryption_spec_key_name: str | None = None,
        staging_bucket: str | None = None,
    ) -> CustomJob:
        """Return CustomJob object."""
        return CustomJob(
            display_name=display_name,
            worker_pool_specs=worker_pool_specs,
            base_output_dir=base_output_dir,
            project=project,
            location=location,
            credentials=self.get_credentials(),
            labels=labels,
            encryption_spec_key_name=encryption_spec_key_name,
            staging_bucket=staging_bucket,
        )

    @staticmethod
    def extract_hyperparameter_tuning_job_id(obj: dict) -> str:
        """Return unique id of the hyperparameter_tuning_job."""
        return obj["name"].rpartition("/")[-1]

    def wait_for_operation(self, operation: Operation, timeout: float | None = None):
        """Wait for long-lasting operation to complete."""
        try:
            return operation.result(timeout=timeout)
        except Exception:
            error = operation.exception(timeout=timeout)
            raise AirflowException(error)

    def cancel_hyperparameter_tuning_job(self) -> None:
        """Cancel HyperparameterTuningJob."""
        if self._hyperparameter_tuning_job:
            self._hyperparameter_tuning_job.cancel()

    @GoogleBaseHook.fallback_to_default_project_id
    def create_hyperparameter_tuning_job(
        self,
        project_id: str,
        region: str,
        display_name: str,
        metric_spec: dict[str, str],
        parameter_spec: dict[str, hyperparameter_tuning._ParameterSpec],
        max_trial_count: int,
        parallel_trial_count: int,
        # START: CustomJob param
        worker_pool_specs: list[dict] | list[gapic.WorkerPoolSpec],
        base_output_dir: str | None = None,
        custom_job_labels: dict[str, str] | None = None,
        custom_job_encryption_spec_key_name: str | None = None,
        staging_bucket: str | None = None,
        # END: CustomJob param
        max_failed_trial_count: int = 0,
        search_algorithm: str | None = None,
        measurement_selection: str | None = "best",
        hyperparameter_tuning_job_labels: dict[str, str] | None = None,
        hyperparameter_tuning_job_encryption_spec_key_name: str | None = None,
        # START: run param
        service_account: str | None = None,
        network: str | None = None,
        timeout: int | None = None,  # seconds
        restart_job_on_worker_restart: bool = False,
        enable_web_access: bool = False,
        tensorboard: str | None = None,
        sync: bool = True,
        # END: run param
        wait_job_completed: bool = True,
    ) -> HyperparameterTuningJob:
        """
        Create a HyperparameterTuningJob.

        :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
        :param region: Required. The ID of the Google Cloud region that the service belongs to.
        :param display_name: Required. The user-defined name of the HyperparameterTuningJob. The name can be
            up to 128 characters long and can be consist of any UTF-8 characters.
        :param metric_spec: Required. Dictionary representing metrics to optimize. The dictionary key is the
            metric_id, which is reported by your training job, and the dictionary value is the optimization
            goal of the metric('minimize' or 'maximize').
            example: metric_spec = {'loss': 'minimize', 'accuracy': 'maximize'}
        :param parameter_spec: Required. Dictionary representing parameters to optimize. The dictionary key
            is the metric_id, which is passed into your training job as a command line key word argument, and
            the dictionary value is the parameter specification of the metric.
        :param max_trial_count: Required. The desired total number of Trials.
        :param parallel_trial_count: Required. The desired number of Trials to run in parallel.
        :param worker_pool_specs: Required. The spec of the worker pools including machine type and Docker
            image. Can provided as a list of dictionaries or list of WorkerPoolSpec proto messages.
        :param base_output_dir: Optional. GCS output directory of job. If not provided a timestamped
            directory in the staging directory will be used.
        :param custom_job_labels: Optional. The labels with user-defined metadata to organize CustomJobs.
            Label keys and values can be no longer than 64 characters (Unicode codepoints), can only contain
            lowercase letters, numeric characters, underscores and dashes. International characters are
            allowed. See https://goo.gl/xmQnxf for more information and examples of labels.
        :param custom_job_encryption_spec_key_name: Optional.Customer-managed encryption key name for a
            CustomJob. If this is set, then all resources created by the CustomJob will be encrypted with the
            provided encryption key.
        :param staging_bucket: Optional. Bucket for produced custom job artifacts. Overrides staging_bucket
            set in aiplatform.init.
        :param max_failed_trial_count: Optional. The number of failed Trials that need to be seen before
            failing the HyperparameterTuningJob. If set to 0, Vertex AI decides how many Trials must fail
            before the whole job fails.
        :param search_algorithm: The search algorithm specified for the Study. Accepts one of the following:
            `None` - If you do not specify an algorithm, your job uses the default Vertex AI algorithm. The
            default algorithm applies Bayesian optimization to arrive at the optimal solution with a more
            effective search over the parameter space.

            'grid' - A simple grid search within the feasible space. This option is particularly useful if
            you want to specify a quantity of trials that is greater than the number of points in the
            feasible space. In such cases, if you do not specify a grid search, the Vertex AI default
            algorithm may generate duplicate suggestions. To use grid search, all parameter specs must be of
            type `IntegerParameterSpec`, `CategoricalParameterSpace`, or `DiscreteParameterSpec`.

            'random' - A simple random search within the feasible space.
        :param measurement_selection: This indicates which measurement to use if/when the service
            automatically selects the final measurement from previously reported intermediate measurements.
            Accepts: 'best', 'last'
            Choose this based on two considerations:
            A) Do you expect your measurements to monotonically improve? If so, choose 'last'. On the other
            hand, if you're in a situation where your system can "over-train" and you expect the performance
            to get better for a while but then start declining, choose 'best'.
            B) Are your measurements significantly noisy and/or irreproducible? If so, 'best' will tend to be
            over-optimistic, and it may be better to choose 'last'.
            If both or neither of (A) and (B) apply, it doesn't matter which selection type is chosen.
        :param hyperparameter_tuning_job_labels: Optional. The labels with user-defined metadata to organize
            HyperparameterTuningJobs. Label keys and values can be no longer than 64 characters (Unicode
            codepoints), can only contain lowercase letters, numeric characters, underscores and dashes.
            International characters are allowed. See https://goo.gl/xmQnxf for more information and examples
            of labels.
        :param hyperparameter_tuning_job_encryption_spec_key_name: Optional. Customer-managed encryption key
            options for a HyperparameterTuningJob. If this is set, then all resources created by the
            HyperparameterTuningJob will be encrypted with the provided encryption key.
        :param service_account: Optional. Specifies the service account for workload run-as account. Users
            submitting jobs must have act-as permission on this run-as account.
        :param network: Optional. The full name of the Compute Engine network to which the job should be
            peered. For example, projects/12345/global/networks/myVPC. Private services access must already
            be configured for the network. If left unspecified, the job is not peered with any network.
        :param timeout: The maximum job running time in seconds. The default is 7 days.
        :param restart_job_on_worker_restart: Restarts the entire CustomJob if a worker gets restarted. This
            feature can be used by distributed training jobs that are not resilient to workers leaving and
            joining a job.
        :param enable_web_access: Whether you want Vertex AI to enable interactive shell access to training
            containers. https://cloud.google.com/vertex-ai/docs/training/monitor-debug-interactive-shell
        :param tensorboard: Optional. The name of a Vertex AI
            [Tensorboard][google.cloud.aiplatform.v1beta1.Tensorboard] resource to which this CustomJob will
            upload Tensorboard logs. Format:
            ``projects/{project}/locations/{location}/tensorboards/{tensorboard}`` The training script should
            write Tensorboard to following Vertex AI environment variable: AIP_TENSORBOARD_LOG_DIR
            `service_account` is required with provided `tensorboard`. For more information on configuring
            your service account please visit:
            https://cloud.google.com/vertex-ai/docs/experiments/tensorboard-training
        :param sync: Whether to execute this method synchronously. If False, this method will unblock and it
            will be executed in a concurrent Future.
        :param wait_job_completed: Whether to wait for the job completed.
        """
        custom_job = self.get_custom_job_object(
            project=project_id,
            location=region,
            display_name=display_name,
            worker_pool_specs=worker_pool_specs,
            base_output_dir=base_output_dir,
            labels=custom_job_labels,
            encryption_spec_key_name=custom_job_encryption_spec_key_name,
            staging_bucket=staging_bucket,
        )

        self._hyperparameter_tuning_job = self.get_hyperparameter_tuning_job_object(
            project=project_id,
            location=region,
            display_name=display_name,
            custom_job=custom_job,
            metric_spec=metric_spec,
            parameter_spec=parameter_spec,
            max_trial_count=max_trial_count,
            parallel_trial_count=parallel_trial_count,
            max_failed_trial_count=max_failed_trial_count,
            search_algorithm=search_algorithm,
            measurement_selection=measurement_selection,
            labels=hyperparameter_tuning_job_labels,
            encryption_spec_key_name=hyperparameter_tuning_job_encryption_spec_key_name,
        )
        self._hyperparameter_tuning_job.run(
            service_account=service_account,
            network=network,
            timeout=timeout,  # seconds
            restart_job_on_worker_restart=restart_job_on_worker_restart,
            enable_web_access=enable_web_access,
            tensorboard=tensorboard,
            sync=sync,
        )

        if wait_job_completed:
            self._hyperparameter_tuning_job.wait()
        else:
            self._hyperparameter_tuning_job._wait_for_resource_creation()
        return self._hyperparameter_tuning_job

    @GoogleBaseHook.fallback_to_default_project_id
    def get_hyperparameter_tuning_job(
        self,
        project_id: str,
        region: str,
        hyperparameter_tuning_job: str,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> types.HyperparameterTuningJob:
        """
        Get a HyperparameterTuningJob.

        :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
        :param region: Required. The ID of the Google Cloud region that the service belongs to.
        :param hyperparameter_tuning_job: Required. The name of the HyperparameterTuningJob resource.
        :param retry: Designation of what errors, if any, should be retried.
        :param timeout: The timeout for this request.
        :param metadata: Strings which should be sent along with the request as metadata.
        """
        client = self.get_job_service_client(region)
        name = client.hyperparameter_tuning_job_path(project_id, region, hyperparameter_tuning_job)

        result = client.get_hyperparameter_tuning_job(
            request={
                "name": name,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result

    @GoogleBaseHook.fallback_to_default_project_id
    def list_hyperparameter_tuning_jobs(
        self,
        project_id: str,
        region: str,
        filter: str | None = None,
        page_size: int | None = None,
        page_token: str | None = None,
        read_mask: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> ListHyperparameterTuningJobsPager:
        """
        List HyperparameterTuningJobs in a Location.

        :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
        :param region: Required. The ID of the Google Cloud region that the service belongs to.
        :param filter: The standard list filter.
            Supported fields:
            -  ``display_name`` supports = and !=.
            -  ``state`` supports = and !=.
            -  ``model_display_name`` supports = and !=
            Some examples of using the filter are:
            -  ``state="JOB_STATE_SUCCEEDED" AND display_name="my_job"``
            -  ``state="JOB_STATE_RUNNING" OR display_name="my_job"``
            -  ``NOT display_name="my_job"``
            -  ``state="JOB_STATE_FAILED"``
        :param page_size: The standard list page size.
        :param page_token: The standard list page token.
        :param read_mask: Mask specifying which fields to read.
        :param retry: Designation of what errors, if any, should be retried.
        :param timeout: The timeout for this request.
        :param metadata: Strings which should be sent along with the request as metadata.
        """
        client = self.get_job_service_client(region)
        parent = client.common_location_path(project_id, region)

        result = client.list_hyperparameter_tuning_jobs(
            request={
                "parent": parent,
                "filter": filter,
                "page_size": page_size,
                "page_token": page_token,
                "read_mask": read_mask,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result

    @GoogleBaseHook.fallback_to_default_project_id
    def delete_hyperparameter_tuning_job(
        self,
        project_id: str,
        region: str,
        hyperparameter_tuning_job: str,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> Operation:
        """
        Delete a HyperparameterTuningJob.

        :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
        :param region: Required. The ID of the Google Cloud region that the service belongs to.
        :param hyperparameter_tuning_job: Required. The name of the HyperparameterTuningJob resource to be
            deleted.
        :param retry: Designation of what errors, if any, should be retried.
        :param timeout: The timeout for this request.
        :param metadata: Strings which should be sent along with the request as metadata.
        """
        client = self.get_job_service_client(region)
        name = client.hyperparameter_tuning_job_path(project_id, region, hyperparameter_tuning_job)

        result = client.delete_hyperparameter_tuning_job(
            request={
                "name": name,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result


class HyperparameterTuningJobAsyncHook(GoogleBaseAsyncHook):
    """Async hook for Google Cloud Vertex AI Hyperparameter Tuning Job APIs."""

    sync_hook_class = HyperparameterTuningJobHook

    def __init__(
        self,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ):
        super().__init__(
            gcp_conn_id=gcp_conn_id,
            impersonation_chain=impersonation_chain,
            **kwargs,
        )

    async def get_job_service_client(self, region: str | None = None) -> JobServiceAsyncClient:
        """
        Retrieve Vertex AI async client.

        :return: Google Cloud Vertex AI client object.
        """
        endpoint = f"{region}-aiplatform.googleapis.com:443" if region and region != "global" else None
        return JobServiceAsyncClient(
            credentials=(await self.get_sync_hook()).get_credentials(),
            client_info=CLIENT_INFO,
            client_options=ClientOptions(api_endpoint=endpoint),
        )

    async def get_hyperparameter_tuning_job(
        self,
        project_id: str,
        location: str,
        job_id: str,
        retry: AsyncRetry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> types.HyperparameterTuningJob:
        """
        Retrieve a hyperparameter tuning job.

        :param project_id: Required. The ID of the Google Cloud project that the job belongs to.
        :param location: Required. The ID of the Google Cloud region that the job belongs to.
        :param job_id: Required. The hyperparameter tuning job id.
        :param retry: Designation of what errors, if any, should be retried.
        :param timeout: The timeout for this request.
        :param metadata: Strings which should be sent along with the request as metadata.
        """
        client: JobServiceAsyncClient = await self.get_job_service_client(region=location)
        job_name = client.hyperparameter_tuning_job_path(project_id, location, job_id)

        result = await client.get_hyperparameter_tuning_job(
            request={
                "name": job_name,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

        return result

    async def wait_hyperparameter_tuning_job(
        self,
        project_id: str,
        location: str,
        job_id: str,
        retry: AsyncRetry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        poll_interval: int = 10,
    ) -> types.HyperparameterTuningJob:
        statuses_complete = {
            JobState.JOB_STATE_CANCELLED,
            JobState.JOB_STATE_FAILED,
            JobState.JOB_STATE_PAUSED,
            JobState.JOB_STATE_SUCCEEDED,
        }
        while True:
            try:
                self.log.info("Requesting hyperparameter tuning job with id %s", job_id)
                job: types.HyperparameterTuningJob = await self.get_hyperparameter_tuning_job(
                    project_id=project_id,
                    location=location,
                    job_id=job_id,
                    retry=retry,
                    timeout=timeout,
                    metadata=metadata,
                )
            except Exception as ex:
                self.log.exception("Exception occurred while requesting job %s", job_id)
                raise AirflowException(ex)

            self.log.info("Status of the hyperparameter tuning job %s is %s", job.name, job.state.name)
            if job.state in statuses_complete:
                return job

            self.log.info("Sleeping for %s seconds.", poll_interval)
            await asyncio.sleep(poll_interval)
