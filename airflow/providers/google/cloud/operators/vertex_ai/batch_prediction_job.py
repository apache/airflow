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

    jsonl
    codepoints
    aiplatform
    gapic
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Sequence

from google.api_core.exceptions import NotFound
from google.api_core.gapic_v1.method import DEFAULT, _MethodDefault
from google.api_core.retry import Retry
from google.cloud.aiplatform import Model, explain
from google.cloud.aiplatform_v1.types import BatchPredictionJob

from airflow.providers.google.cloud.hooks.vertex_ai.batch_prediction_job import BatchPredictionJobHook
from airflow.providers.google.cloud.links.vertex_ai import (
    VertexAIBatchPredictionJobLink,
    VertexAIBatchPredictionJobListLink,
)
from airflow.providers.google.cloud.operators.cloud_base import GoogleCloudBaseOperator

if TYPE_CHECKING:
    from airflow.utils.context import Context


class CreateBatchPredictionJobOperator(GoogleCloudBaseOperator):
    """
    Creates a BatchPredictionJob. A BatchPredictionJob once created will right away be attempted to start.

    :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
    :param region: Required. The ID of the Google Cloud region that the service belongs to.
    :param batch_prediction_job: Required. The BatchPredictionJob to create.
    :param job_display_name: Required. The user-defined name of the BatchPredictionJob. The name can be
        up to 128 characters long and can be consist of any UTF-8 characters.
    :param model_name: Required. A fully-qualified model resource name or model ID.
    :param instances_format: Required. The format in which instances are provided. Must be one of the
        formats listed in `Model.supported_input_storage_formats`. Default is "jsonl" when using
        `gcs_source`. If a `bigquery_source` is provided, this is overridden to "bigquery".
    :param predictions_format: Required. The format in which Vertex AI outputs the predictions, must be
        one of the formats specified in `Model.supported_output_storage_formats`. Default is "jsonl" when
        using `gcs_destination_prefix`. If a `bigquery_destination_prefix` is provided, this is
        overridden to "bigquery".
    :param gcs_source: Google Cloud Storage URI(-s) to your instances to run batch prediction on. They
        must match `instances_format`. May contain wildcards. For more information on wildcards, see
        https://cloud.google.com/storage/docs/gsutil/addlhelp/WildcardNames.
    :param bigquery_source: BigQuery URI to a table, up to 2000 characters long.
        For example: `bq://projectId.bqDatasetId.bqTableId`
    :param gcs_destination_prefix: The Google Cloud Storage location of the directory where the output is
        to be written to. In the given directory a new directory is created. Its name is
        ``prediction-<model-display-name>-<job-create-time>``, where timestamp is in
        YYYY-MM-DDThh:mm:ss.sssZ ISO-8601 format. Inside of it files ``predictions_0001.<extension>``,
        ``predictions_0002.<extension>``, ..., ``predictions_N.<extension>`` are created where
        ``<extension>`` depends on chosen ``predictions_format``, and N may equal 0001 and depends on the
        total number of successfully predicted instances. If the Model has both ``instance`` and
        ``prediction`` schemata defined then each such file contains predictions as per the
        ``predictions_format``. If prediction for any instance failed (partially or completely), then an
        additional ``errors_0001.<extension>``, ``errors_0002.<extension>``,..., ``errors_N.<extension>``
        files are created (N depends on total number of failed predictions). These files contain the
        failed instances, as per their schema, followed by an additional ``error`` field which as value
        has ```google.rpc.Status`` <Status>`__ containing only ``code`` and ``message`` fields.
    :param bigquery_destination_prefix: The BigQuery project location where the output is to be written
        to. In the given project a new dataset is created with name
        ``prediction_<model-display-name>_<job-create-time>`` where is made BigQuery-dataset-name
        compatible (for example, most special characters become underscores), and timestamp is in
        YYYY_MM_DDThh_mm_ss_sssZ "based on ISO-8601" format. In the dataset two tables will be created,
        ``predictions``, and ``errors``. If the Model has both ``instance`` and ``prediction`` schemata
        defined then the tables have columns as follows: The ``predictions`` table contains instances for
        which the prediction succeeded, it has columns as per a concatenation of the Model's instance and
        prediction schemata. The ``errors`` table contains rows for which the prediction has failed, it
        has instance columns, as per the instance schema, followed by a single "errors" column, which as
        values has ```google.rpc.Status`` <Status>`__ represented as a STRUCT, and containing only
        ``code`` and ``message``.
    :param model_parameters: The parameters that govern the predictions. The schema of the parameters may
        be specified via the Model's `parameters_schema_uri`.
    :param machine_type: The type of machine for running batch prediction on dedicated resources. Not
        specifying machine type will result in batch prediction job being run with automatic resources.
    :param accelerator_type: The type of accelerator(s) that may be attached to the machine as per
        `accelerator_count`. Only used if `machine_type` is set.
    :param accelerator_count: The number of accelerators to attach to the `machine_type`. Only used if
        `machine_type` is set.
    :param starting_replica_count: The number of machine replicas used at the start of the batch
        operation. If not set, Vertex AI decides starting number, not greater than `max_replica_count`.
        Only used if `machine_type` is set.
    :param max_replica_count: The maximum number of machine replicas the batch operation may be scaled
        to. Only used if `machine_type` is set. Default is 10.
    :param generate_explanation: Optional. Generate explanation along with the batch prediction results.
        This will cause the batch prediction output to include explanations based on the
        `prediction_format`:
        - `bigquery`: output includes a column named `explanation`. The value is a struct that conforms to
        the [aiplatform.gapic.Explanation] object.
        - `jsonl`: The JSON objects on each line include an additional entry keyed `explanation`. The value
        of the entry is a JSON object that conforms to the [aiplatform.gapic.Explanation] object.
        - `csv`: Generating explanations for CSV format is not supported.
    :param explanation_metadata: Optional. Explanation metadata configuration for this
        BatchPredictionJob. Can be specified only if `generate_explanation` is set to `True`.
        This value overrides the value of `Model.explanation_metadata`. All fields of
        `explanation_metadata` are optional in the request. If a field of the `explanation_metadata`
        object is not populated, the corresponding field of the `Model.explanation_metadata` object is
        inherited. For more details, see `Ref docs <http://tinyurl.com/1igh60kt>`
    :param explanation_parameters: Optional. Parameters to configure explaining for Model's predictions.
        Can be specified only if `generate_explanation` is set to `True`.
        This value overrides the value of `Model.explanation_parameters`. All fields of
        `explanation_parameters` are optional in the request. If a field of the `explanation_parameters`
        object is not populated, the corresponding field of the `Model.explanation_parameters` object is
        inherited. For more details, see `Ref docs <http://tinyurl.com/1an4zake>`
    :param labels: Optional. The labels with user-defined metadata to organize your BatchPredictionJobs.
        Label keys and values can be no longer than 64 characters (Unicode codepoints), can only contain
        lowercase letters, numeric characters, underscores and dashes. International characters are
        allowed. See https://goo.gl/xmQnxf for more information and examples of labels.
    :param encryption_spec_key_name: Optional. The Cloud KMS resource identifier of the customer managed
        encryption key used to protect the job. Has the form:
        ``projects/my-project/locations/my-region/keyRings/my-kr/cryptoKeys/my-key``. The key needs to be
        in the same region as where the compute resource is created.
        If this is set, then all resources created by the BatchPredictionJob will be encrypted with the
        provided encryption key.
        Overrides encryption_spec_key_name set in aiplatform.init.
    :param sync: Whether to execute this method synchronously. If False, this method will be executed in
        concurrent Future and any downstream object will be immediately returned and synced when the
        Future has completed.
    :param retry: Designation of what errors, if any, should be retried.
    :param timeout: The timeout for this request.
    :param metadata: Strings which should be sent along with the request as metadata.
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields = ("region", "project_id", "model_name", "impersonation_chain")
    operator_extra_links = (VertexAIBatchPredictionJobLink(),)

    def __init__(
        self,
        *,
        region: str,
        project_id: str,
        job_display_name: str,
        model_name: str | Model,
        instances_format: str = "jsonl",
        predictions_format: str = "jsonl",
        gcs_source: str | Sequence[str] | None = None,
        bigquery_source: str | None = None,
        gcs_destination_prefix: str | None = None,
        bigquery_destination_prefix: str | None = None,
        model_parameters: dict | None = None,
        machine_type: str | None = None,
        accelerator_type: str | None = None,
        accelerator_count: int | None = None,
        starting_replica_count: int | None = None,
        max_replica_count: int | None = None,
        generate_explanation: bool | None = False,
        explanation_metadata: explain.ExplanationMetadata | None = None,
        explanation_parameters: explain.ExplanationParameters | None = None,
        labels: dict[str, str] | None = None,
        encryption_spec_key_name: str | None = None,
        sync: bool = True,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.region = region
        self.project_id = project_id
        self.job_display_name = job_display_name
        self.model_name = model_name
        self.instances_format = instances_format
        self.predictions_format = predictions_format
        self.gcs_source = gcs_source
        self.bigquery_source = bigquery_source
        self.gcs_destination_prefix = gcs_destination_prefix
        self.bigquery_destination_prefix = bigquery_destination_prefix
        self.model_parameters = model_parameters
        self.machine_type = machine_type
        self.accelerator_type = accelerator_type
        self.accelerator_count = accelerator_count
        self.starting_replica_count = starting_replica_count
        self.max_replica_count = max_replica_count
        self.generate_explanation = generate_explanation
        self.explanation_metadata = explanation_metadata
        self.explanation_parameters = explanation_parameters
        self.labels = labels
        self.encryption_spec_key_name = encryption_spec_key_name
        self.sync = sync
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain
        self.hook: BatchPredictionJobHook | None = None

    def execute(self, context: Context):
        self.log.info("Creating Batch prediction job")
        self.hook = BatchPredictionJobHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        result = self.hook.create_batch_prediction_job(
            region=self.region,
            project_id=self.project_id,
            job_display_name=self.job_display_name,
            model_name=self.model_name,
            instances_format=self.instances_format,
            predictions_format=self.predictions_format,
            gcs_source=self.gcs_source,
            bigquery_source=self.bigquery_source,
            gcs_destination_prefix=self.gcs_destination_prefix,
            bigquery_destination_prefix=self.bigquery_destination_prefix,
            model_parameters=self.model_parameters,
            machine_type=self.machine_type,
            accelerator_type=self.accelerator_type,
            accelerator_count=self.accelerator_count,
            starting_replica_count=self.starting_replica_count,
            max_replica_count=self.max_replica_count,
            generate_explanation=self.generate_explanation,
            explanation_metadata=self.explanation_metadata,
            explanation_parameters=self.explanation_parameters,
            labels=self.labels,
            encryption_spec_key_name=self.encryption_spec_key_name,
            sync=self.sync,
        )

        batch_prediction_job = result.to_dict()
        batch_prediction_job_id = self.hook.extract_batch_prediction_job_id(batch_prediction_job)
        self.log.info("Batch prediction job was created. Job id: %s", batch_prediction_job_id)

        self.xcom_push(context, key="batch_prediction_job_id", value=batch_prediction_job_id)
        VertexAIBatchPredictionJobLink.persist(
            context=context, task_instance=self, batch_prediction_job_id=batch_prediction_job_id
        )
        return batch_prediction_job

    def on_kill(self) -> None:
        """
        Callback called when the operator is killed.
        Cancel any running job.
        """
        if self.hook:
            self.hook.cancel_batch_prediction_job()


class DeleteBatchPredictionJobOperator(GoogleCloudBaseOperator):
    """
    Deletes a BatchPredictionJob. Can only be called on jobs that already finished.

    :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
    :param region: Required. The ID of the Google Cloud region that the service belongs to.
    :param batch_prediction_job_id: The ID of the BatchPredictionJob resource to be deleted.
    :param retry: Designation of what errors, if any, should be retried.
    :param timeout: The timeout for this request.
    :param metadata: Strings which should be sent along with the request as metadata.
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields = ("region", "project_id", "batch_prediction_job_id", "impersonation_chain")

    def __init__(
        self,
        *,
        region: str,
        project_id: str,
        batch_prediction_job_id: str,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.region = region
        self.project_id = project_id
        self.batch_prediction_job_id = batch_prediction_job_id
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context):
        hook = BatchPredictionJobHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )

        try:
            self.log.info("Deleting batch prediction job: %s", self.batch_prediction_job_id)
            operation = hook.delete_batch_prediction_job(
                project_id=self.project_id,
                region=self.region,
                batch_prediction_job=self.batch_prediction_job_id,
                retry=self.retry,
                timeout=self.timeout,
                metadata=self.metadata,
            )
            hook.wait_for_operation(timeout=self.timeout, operation=operation)
            self.log.info("Batch prediction job was deleted.")
        except NotFound:
            self.log.info("The Batch prediction job %s does not exist.", self.batch_prediction_job_id)


class GetBatchPredictionJobOperator(GoogleCloudBaseOperator):
    """
    Gets a BatchPredictionJob

    :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
    :param region: Required. The ID of the Google Cloud region that the service belongs to.
    :param batch_prediction_job: Required. The name of the BatchPredictionJob resource.
    :param retry: Designation of what errors, if any, should be retried.
    :param timeout: The timeout for this request.
    :param metadata: Strings which should be sent along with the request as metadata.
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields = ("region", "project_id", "impersonation_chain")
    operator_extra_links = (VertexAIBatchPredictionJobLink(),)

    def __init__(
        self,
        *,
        region: str,
        project_id: str,
        batch_prediction_job: str,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.region = region
        self.project_id = project_id
        self.batch_prediction_job = batch_prediction_job
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context):
        hook = BatchPredictionJobHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )

        try:
            self.log.info("Get batch prediction job: %s", self.batch_prediction_job)
            result = hook.get_batch_prediction_job(
                project_id=self.project_id,
                region=self.region,
                batch_prediction_job=self.batch_prediction_job,
                retry=self.retry,
                timeout=self.timeout,
                metadata=self.metadata,
            )
            self.log.info("Batch prediction job was gotten.")
            VertexAIBatchPredictionJobLink.persist(
                context=context, task_instance=self, batch_prediction_job_id=self.batch_prediction_job
            )
            return BatchPredictionJob.to_dict(result)
        except NotFound:
            self.log.info("The Batch prediction job %s does not exist.", self.batch_prediction_job)


class ListBatchPredictionJobsOperator(GoogleCloudBaseOperator):
    """
    Lists BatchPredictionJobs in a Location.

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
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields = ("region", "project_id", "impersonation_chain")
    operator_extra_links = (VertexAIBatchPredictionJobListLink(),)

    def __init__(
        self,
        *,
        region: str,
        project_id: str,
        filter: str | None = None,
        page_size: int | None = None,
        page_token: str | None = None,
        read_mask: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.region = region
        self.project_id = project_id
        self.filter = filter
        self.page_size = page_size
        self.page_token = page_token
        self.read_mask = read_mask
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context):
        hook = BatchPredictionJobHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        results = hook.list_batch_prediction_jobs(
            project_id=self.project_id,
            region=self.region,
            filter=self.filter,
            page_size=self.page_size,
            page_token=self.page_token,
            read_mask=self.read_mask,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        VertexAIBatchPredictionJobListLink.persist(context=context, task_instance=self)
        return [BatchPredictionJob.to_dict(result) for result in results]
