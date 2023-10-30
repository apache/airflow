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

from typing import TYPE_CHECKING, Sequence

from google.api_core.client_options import ClientOptions
from google.api_core.gapic_v1.method import DEFAULT, _MethodDefault
from google.cloud.aiplatform import BatchPredictionJob, Model, explain
from google.cloud.aiplatform_v1 import JobServiceClient

from airflow.exceptions import AirflowException
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook

if TYPE_CHECKING:
    from google.api_core.operation import Operation
    from google.api_core.retry import Retry
    from google.cloud.aiplatform_v1.services.job_service.pagers import ListBatchPredictionJobsPager


class BatchPredictionJobHook(GoogleBaseHook):
    """Hook for Google Cloud Vertex AI Batch Prediction Job APIs."""

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
        self._batch_prediction_job: BatchPredictionJob | None = None

    def get_job_service_client(self, region: str | None = None) -> JobServiceClient:
        """Returns JobServiceClient."""
        if region and region != "global":
            client_options = ClientOptions(api_endpoint=f"{region}-aiplatform.googleapis.com:443")
        else:
            client_options = ClientOptions()

        return JobServiceClient(
            credentials=self.get_credentials(), client_info=self.client_info, client_options=client_options
        )

    def wait_for_operation(self, operation: Operation, timeout: float | None = None):
        """Waits for long-lasting operation to complete."""
        try:
            return operation.result(timeout=timeout)
        except Exception:
            error = operation.exception(timeout=timeout)
            raise AirflowException(error)

    @staticmethod
    def extract_batch_prediction_job_id(obj: dict) -> str:
        """Returns unique id of the batch_prediction_job."""
        return obj["name"].rpartition("/")[-1]

    def cancel_batch_prediction_job(self) -> None:
        """Cancel BatchPredictionJob."""
        if self._batch_prediction_job:
            self._batch_prediction_job.cancel()

    @GoogleBaseHook.fallback_to_default_project_id
    def create_batch_prediction_job(
        self,
        project_id: str,
        region: str,
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
        create_request_timeout: float | None = None,
        batch_size: int | None = None,
    ) -> BatchPredictionJob:
        """
        Create a batch prediction job.

        :param project_id: Required. Project to run training in.
        :param region: Required. Location to run training in.
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
            - `bigquery`: output includes a column named `explanation`. The value is a struct that conforms
            to the [aiplatform.gapic.Explanation] object.
            - `jsonl`: The JSON objects on each line include an additional entry keyed `explanation`. The
            value of the entry is a JSON object that conforms to the [aiplatform.gapic.Explanation] object.
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
        :param create_request_timeout: Optional. The timeout for the create request in seconds.
        :param batch_size: Optional. The number of the records (e.g. instances)
            of the operation given in each batch
            to a machine replica. Machine type, and size of a single record should be considered
            when setting this parameter, higher value speeds up the batch operation's execution,
            but too high value will result in a whole batch not fitting in a machine's memory,
            and the whole operation will fail.
            The default value is same as in the aiplatform's BatchPredictionJob.
        """
        self._batch_prediction_job = BatchPredictionJob.create(
            job_display_name=job_display_name,
            model_name=model_name,
            instances_format=instances_format,
            predictions_format=predictions_format,
            gcs_source=gcs_source,
            bigquery_source=bigquery_source,
            gcs_destination_prefix=gcs_destination_prefix,
            bigquery_destination_prefix=bigquery_destination_prefix,
            model_parameters=model_parameters,
            machine_type=machine_type,
            accelerator_type=accelerator_type,
            accelerator_count=accelerator_count,
            starting_replica_count=starting_replica_count,
            max_replica_count=max_replica_count,
            generate_explanation=generate_explanation,
            explanation_metadata=explanation_metadata,
            explanation_parameters=explanation_parameters,
            labels=labels,
            project=project_id,
            location=region,
            credentials=self.get_credentials(),
            encryption_spec_key_name=encryption_spec_key_name,
            sync=sync,
            create_request_timeout=create_request_timeout,
            batch_size=batch_size,
        )
        return self._batch_prediction_job

    @GoogleBaseHook.fallback_to_default_project_id
    def delete_batch_prediction_job(
        self,
        project_id: str,
        region: str,
        batch_prediction_job: str,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> Operation:
        """
        Deletes a BatchPredictionJob. Can only be called on jobs that already finished.

        :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
        :param region: Required. The ID of the Google Cloud region that the service belongs to.
        :param batch_prediction_job: The name of the BatchPredictionJob resource to be deleted.
        :param retry: Designation of what errors, if any, should be retried.
        :param timeout: The timeout for this request.
        :param metadata: Strings which should be sent along with the request as metadata.
        """
        client = self.get_job_service_client(region)
        name = client.batch_prediction_job_path(project_id, region, batch_prediction_job)

        result = client.delete_batch_prediction_job(
            request={
                "name": name,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result

    @GoogleBaseHook.fallback_to_default_project_id
    def get_batch_prediction_job(
        self,
        project_id: str,
        region: str,
        batch_prediction_job: str,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> BatchPredictionJob:
        """
        Gets a BatchPredictionJob.

        :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
        :param region: Required. The ID of the Google Cloud region that the service belongs to.
        :param batch_prediction_job: Required. The name of the BatchPredictionJob resource.
        :param retry: Designation of what errors, if any, should be retried.
        :param timeout: The timeout for this request.
        :param metadata: Strings which should be sent along with the request as metadata.
        """
        client = self.get_job_service_client(region)
        name = client.batch_prediction_job_path(project_id, region, batch_prediction_job)

        result = client.get_batch_prediction_job(
            request={
                "name": name,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result

    @GoogleBaseHook.fallback_to_default_project_id
    def list_batch_prediction_jobs(
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
    ) -> ListBatchPredictionJobsPager:
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
        """
        client = self.get_job_service_client(region)
        parent = client.common_location_path(project_id, region)

        result = client.list_batch_prediction_jobs(
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
