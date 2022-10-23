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
"""This module contains Google Vertex AI operators.

.. spelling::

    irreproducible
    codepoints
    Tensorboard
    aiplatform
    myVPC
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Optional, Sequence

from google.api_core.exceptions import NotFound
from google.api_core.gapic_v1.method import DEFAULT, _MethodDefault
from google.api_core.retry import Retry
from google.cloud.aiplatform import gapic, hyperparameter_tuning
from google.cloud.aiplatform_v1.types import HyperparameterTuningJob

from airflow.models import BaseOperator
from airflow.providers.google.cloud.hooks.vertex_ai.hyperparameter_tuning_job import (
    HyperparameterTuningJobHook,
)
from airflow.providers.google.cloud.links.vertex_ai import (
    VertexAIHyperparameterTuningJobListLink,
    VertexAITrainingLink,
)

if TYPE_CHECKING:
    from airflow.utils.context import Context


class CreateHyperparameterTuningJobOperator(BaseOperator):
    """
    Create Hyperparameter Tuning job

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
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
    :param delegate_to: The account to impersonate using domain-wide delegation of authority,
        if any. For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields = [
        "region",
        "project_id",
        "impersonation_chain",
    ]
    operator_extra_links = (VertexAITrainingLink(),)

    def __init__(
        self,
        *,
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
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: str | None = None,
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.region = region
        self.project_id = project_id
        self.display_name = display_name
        self.metric_spec = metric_spec
        self.parameter_spec = parameter_spec
        self.max_trial_count = max_trial_count
        self.parallel_trial_count = parallel_trial_count
        self.worker_pool_specs = worker_pool_specs
        self.base_output_dir = base_output_dir
        self.custom_job_labels = custom_job_labels
        self.custom_job_encryption_spec_key_name = custom_job_encryption_spec_key_name
        self.staging_bucket = staging_bucket
        self.max_failed_trial_count = max_failed_trial_count
        self.search_algorithm = search_algorithm
        self.measurement_selection = measurement_selection
        self.hyperparameter_tuning_job_labels = hyperparameter_tuning_job_labels
        self.hyperparameter_tuning_job_encryption_spec_key_name = (
            hyperparameter_tuning_job_encryption_spec_key_name
        )
        self.service_account = service_account
        self.network = network
        self.timeout = timeout
        self.restart_job_on_worker_restart = restart_job_on_worker_restart
        self.enable_web_access = enable_web_access
        self.tensorboard = tensorboard
        self.sync = sync
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.impersonation_chain = impersonation_chain
        self.hook = None  # type: Optional[HyperparameterTuningJobHook]

    def execute(self, context: Context):
        self.log.info("Creating Hyperparameter Tuning job")
        self.hook = HyperparameterTuningJobHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            impersonation_chain=self.impersonation_chain,
        )
        result = self.hook.create_hyperparameter_tuning_job(
            project_id=self.project_id,
            region=self.region,
            display_name=self.display_name,
            metric_spec=self.metric_spec,
            parameter_spec=self.parameter_spec,
            max_trial_count=self.max_trial_count,
            parallel_trial_count=self.parallel_trial_count,
            worker_pool_specs=self.worker_pool_specs,
            base_output_dir=self.base_output_dir,
            custom_job_labels=self.custom_job_labels,
            custom_job_encryption_spec_key_name=self.custom_job_encryption_spec_key_name,
            staging_bucket=self.staging_bucket,
            max_failed_trial_count=self.max_failed_trial_count,
            search_algorithm=self.search_algorithm,
            measurement_selection=self.measurement_selection,
            hyperparameter_tuning_job_labels=self.hyperparameter_tuning_job_labels,
            hyperparameter_tuning_job_encryption_spec_key_name=(
                self.hyperparameter_tuning_job_encryption_spec_key_name
            ),
            service_account=self.service_account,
            network=self.network,
            timeout=self.timeout,
            restart_job_on_worker_restart=self.restart_job_on_worker_restart,
            enable_web_access=self.enable_web_access,
            tensorboard=self.tensorboard,
            sync=self.sync,
        )

        hyperparameter_tuning_job = result.to_dict()
        hyperparameter_tuning_job_id = self.hook.extract_hyperparameter_tuning_job_id(
            hyperparameter_tuning_job
        )
        self.log.info("Hyperparameter Tuning job was created. Job id: %s", hyperparameter_tuning_job_id)

        self.xcom_push(context, key="hyperparameter_tuning_job_id", value=hyperparameter_tuning_job_id)
        VertexAITrainingLink.persist(
            context=context, task_instance=self, training_id=hyperparameter_tuning_job_id
        )
        return hyperparameter_tuning_job

    def on_kill(self) -> None:
        """
        Callback called when the operator is killed.
        Cancel any running job.
        """
        if self.hook:
            self.hook.cancel_hyperparameter_tuning_job()


class GetHyperparameterTuningJobOperator(BaseOperator):
    """
    Gets a HyperparameterTuningJob

    :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
    :param region: Required. The ID of the Google Cloud region that the service belongs to.
    :param hyperparameter_tuning_job_id: Required. The name of the HyperparameterTuningJob resource.
    :param retry: Designation of what errors, if any, should be retried.
    :param timeout: The timeout for this request.
    :param metadata: Strings which should be sent along with the request as metadata.
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
    :param delegate_to: The account to impersonate using domain-wide delegation of authority,
        if any. For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields = ("region", "hyperparameter_tuning_job_id", "project_id", "impersonation_chain")
    operator_extra_links = (VertexAITrainingLink(),)

    def __init__(
        self,
        *,
        region: str,
        project_id: str,
        hyperparameter_tuning_job_id: str,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: str | None = None,
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.region = region
        self.project_id = project_id
        self.hyperparameter_tuning_job_id = hyperparameter_tuning_job_id
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context):
        hook = HyperparameterTuningJobHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            impersonation_chain=self.impersonation_chain,
        )

        try:
            self.log.info("Get hyperparameter tuning job: %s", self.hyperparameter_tuning_job_id)
            result = hook.get_hyperparameter_tuning_job(
                project_id=self.project_id,
                region=self.region,
                hyperparameter_tuning_job=self.hyperparameter_tuning_job_id,
                retry=self.retry,
                timeout=self.timeout,
                metadata=self.metadata,
            )
            VertexAITrainingLink.persist(
                context=context, task_instance=self, training_id=self.hyperparameter_tuning_job_id
            )
            self.log.info("Hyperparameter tuning job was gotten.")
            return HyperparameterTuningJob.to_dict(result)
        except NotFound:
            self.log.info(
                "The Hyperparameter tuning job %s does not exist.", self.hyperparameter_tuning_job_id
            )


class DeleteHyperparameterTuningJobOperator(BaseOperator):
    """
    Deletes a HyperparameterTuningJob.

    :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
    :param region: Required. The ID of the Google Cloud region that the service belongs to.
    :param hyperparameter_tuning_job_id: Required. The name of the HyperparameterTuningJob resource to be
        deleted.
    :param retry: Designation of what errors, if any, should be retried.
    :param timeout: The timeout for this request.
    :param metadata: Strings which should be sent along with the request as metadata.
    """

    template_fields = ("region", "project_id", "hyperparameter_tuning_job_id", "impersonation_chain")

    def __init__(
        self,
        *,
        hyperparameter_tuning_job_id: str,
        region: str,
        project_id: str,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: str | None = None,
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.hyperparameter_tuning_job_id = hyperparameter_tuning_job_id
        self.region = region
        self.project_id = project_id
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context):
        hook = HyperparameterTuningJobHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            impersonation_chain=self.impersonation_chain,
        )
        try:
            self.log.info("Deleting Hyperparameter Tuning job: %s", self.hyperparameter_tuning_job_id)
            operation = hook.delete_hyperparameter_tuning_job(
                region=self.region,
                project_id=self.project_id,
                hyperparameter_tuning_job=self.hyperparameter_tuning_job_id,
                retry=self.retry,
                timeout=self.timeout,
                metadata=self.metadata,
            )
            hook.wait_for_operation(timeout=self.timeout, operation=operation)
            self.log.info("Hyperparameter Tuning job was deleted.")
        except NotFound:
            self.log.info(
                "The Hyperparameter Tuning Job ID %s does not exist.", self.hyperparameter_tuning_job_id
            )


class ListHyperparameterTuningJobOperator(BaseOperator):
    """
    Lists HyperparameterTuningJobs in a Location.

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

    template_fields = [
        "region",
        "project_id",
        "impersonation_chain",
    ]
    operator_extra_links = (VertexAIHyperparameterTuningJobListLink(),)

    def __init__(
        self,
        *,
        region: str,
        project_id: str,
        page_size: int | None = None,
        page_token: str | None = None,
        filter: str | None = None,
        read_mask: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: str | None = None,
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.region = region
        self.project_id = project_id
        self.page_size = page_size
        self.page_token = page_token
        self.filter = filter
        self.read_mask = read_mask
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context):
        hook = HyperparameterTuningJobHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            impersonation_chain=self.impersonation_chain,
        )
        results = hook.list_hyperparameter_tuning_jobs(
            region=self.region,
            project_id=self.project_id,
            page_size=self.page_size,
            page_token=self.page_token,
            filter=self.filter,
            read_mask=self.read_mask,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        VertexAIHyperparameterTuningJobListLink.persist(context=context, task_instance=self)
        return [HyperparameterTuningJob.to_dict(result) for result in results]
