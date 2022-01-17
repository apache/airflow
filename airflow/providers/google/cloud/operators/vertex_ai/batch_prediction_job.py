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
#
"""This module contains Google Vertex AI operators."""

from typing import Dict, Optional, Sequence, Tuple, Union

from google.api_core.exceptions import NotFound
from google.api_core.retry import Retry
from google.cloud.aiplatform_v1.types import BatchPredictionJob

from airflow.models import BaseOperator, BaseOperatorLink
from airflow.models.taskinstance import TaskInstance
from airflow.providers.google.cloud.hooks.vertex_ai.batch_prediction_job import BatchPredictionJobHook

VERTEX_AI_BASE_LINK = "https://console.cloud.google.com/vertex-ai"
VERTEX_AI_BATCH_PREDICTION_JOB_LINK = (
    VERTEX_AI_BASE_LINK
    + "/locations/{region}/batch-predictions/{batch_prediction_job_id}?project={project_id}"
)
VERTEX_AI_BATCH_PREDICTION_JOB_LIST_LINK = VERTEX_AI_BASE_LINK + "/batch-predictions?project={project_id}"


class VertexAIBatchPredictionJobLink(BaseOperatorLink):
    """Helper class for constructing Vertex AI BatchPredictionJob link"""

    name = "Batch Prediction Job"

    def get_link(self, operator, dttm):
        ti = TaskInstance(task=operator, execution_date=dttm)
        batch_prediction_job_conf = ti.xcom_pull(task_ids=operator.task_id, key="batch_prediction_job_conf")
        return (
            VERTEX_AI_BATCH_PREDICTION_JOB_LINK.format(
                region=batch_prediction_job_conf["region"],
                batch_prediction_job_id=batch_prediction_job_conf["batch_prediction_job_id"],
                project_id=batch_prediction_job_conf["project_id"],
            )
            if batch_prediction_job_conf
            else ""
        )


class VertexAIBatchPredictionJobListLink(BaseOperatorLink):
    """Helper class for constructing Vertex AI BatchPredictionJobList link"""

    name = "Batch Prediction Job List"

    def get_link(self, operator, dttm):
        ti = TaskInstance(task=operator, execution_date=dttm)
        project_id = ti.xcom_pull(task_ids=operator.task_id, key="project_id")
        return (
            VERTEX_AI_BATCH_PREDICTION_JOB_LIST_LINK.format(
                project_id=project_id,
            )
            if project_id
            else ""
        )


class CancelBatchPredictionJobOperator(BaseOperator):
    """
    Cancels a BatchPredictionJob.

    :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
    :type project_id: str
    :param region: Required. The ID of the Google Cloud region that the service belongs to.
    :type region: str
    :param batch_prediction_job: Required. The name of the BatchPredictionJob to cancel.
    :type batch_prediction_job: str
    :param retry: Designation of what errors, if any, should be retried.
    :type retry: google.api_core.retry.Retry
    :param timeout: The timeout for this request.
    :type timeout: float
    :param metadata: Strings which should be sent along with the request as metadata.
    :type metadata: Sequence[Tuple[str, str]]
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
    :type gcp_conn_id: str
    :param delegate_to: The account to impersonate using domain-wide delegation of authority,
        if any. For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :type delegate_to: str
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :type impersonation_chain: Union[str, Sequence[str]]
    """

    template_fields = ("region", "project_id", "impersonation_chain")

    def __init__(
        self,
        *,
        region: str,
        project_id: str,
        batch_prediction_job: str,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: Optional[str] = None,
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
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
        self.delegate_to = delegate_to
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Dict):
        hook = BatchPredictionJobHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            impersonation_chain=self.impersonation_chain,
        )
        hook.cancel_batch_prediction_job(
            project_id=self.project_id,
            region=self.region,
            batch_prediction_job=self.batch_prediction_job,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        self.log.info("Batch Prediction job was canceled")


class CreateBatchPredictionJobOperator(BaseOperator):
    """
    Creates a BatchPredictionJob. A BatchPredictionJob once created will right away be attempted to start.

    :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
    :type project_id: str
    :param region: Required. The ID of the Google Cloud region that the service belongs to.
    :type region: str
    :param batch_prediction_job: Required. The BatchPredictionJob to create.
    :type batch_prediction_job: google.cloud.aiplatform_v1.types.BatchPredictionJob
    :param retry: Designation of what errors, if any, should be retried.
    :type retry: google.api_core.retry.Retry
    :param timeout: The timeout for this request.
    :type timeout: float
    :param metadata: Strings which should be sent along with the request as metadata.
    :type metadata: Sequence[Tuple[str, str]]
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
    :type gcp_conn_id: str
    :param delegate_to: The account to impersonate using domain-wide delegation of authority,
        if any. For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :type delegate_to: str
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :type impersonation_chain: Union[str, Sequence[str]]
    """

    template_fields = ("region", "project_id", "impersonation_chain")
    operator_extra_links = (VertexAIBatchPredictionJobLink(),)

    def __init__(
        self,
        *,
        region: str,
        project_id: str,
        batch_prediction_job: BatchPredictionJob,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = "",
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: Optional[str] = None,
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
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
        self.delegate_to = delegate_to
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Dict):
        hook = BatchPredictionJobHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            impersonation_chain=self.impersonation_chain,
        )

        self.log.info("Creating Batch prediction job")
        result = hook.create_batch_prediction_job(
            region=self.region,
            project_id=self.project_id,
            batch_prediction_job=self.batch_prediction_job,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )

        batch_prediction_job = BatchPredictionJob.to_dict(result)
        batch_prediction_job_id = hook.extract_batch_prediction_job_id(batch_prediction_job)
        self.log.info("Batch prediction job was created. Job id: %s", batch_prediction_job_id)

        self.xcom_push(context, key="batch_prediction_job_id", value=batch_prediction_job_id)
        self.xcom_push(
            context,
            key="batch_prediction_job_conf",
            value={
                "batch_prediction_job_id": batch_prediction_job_id,
                "region": self.region,
                "project_id": self.project_id,
            },
        )
        return batch_prediction_job


class DeleteBatchPredictionJobOperator(BaseOperator):
    """
    Deletes a BatchPredictionJob. Can only be called on jobs that already finished.

    :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
    :type project_id: str
    :param region: Required. The ID of the Google Cloud region that the service belongs to.
    :type region: str
    :param batch_prediction_job: The name of the BatchPredictionJob resource to be deleted.
    :type batch_prediction_job: str
    :param retry: Designation of what errors, if any, should be retried.
    :type retry: google.api_core.retry.Retry
    :param timeout: The timeout for this request.
    :type timeout: float
    :param metadata: Strings which should be sent along with the request as metadata.
    :type metadata: Sequence[Tuple[str, str]]
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
    :type gcp_conn_id: str
    :param delegate_to: The account to impersonate using domain-wide delegation of authority,
        if any. For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :type delegate_to: str
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :type impersonation_chain: Union[str, Sequence[str]]
    """

    template_fields = ("region", "project_id", "batch_prediction_job", "impersonation_chain")

    def __init__(
        self,
        *,
        region: str,
        project_id: str,
        batch_prediction_job: str,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = "",
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: Optional[str] = None,
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
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
        self.delegate_to = delegate_to
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Dict):
        hook = BatchPredictionJobHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            impersonation_chain=self.impersonation_chain,
        )

        try:
            self.log.info("Deleting batch prediction job: %s", self.batch_prediction_job)
            operation = hook.delete_batch_prediction_job(
                project_id=self.project_id,
                region=self.region,
                batch_prediction_job=self.batch_prediction_job,
                retry=self.retry,
                timeout=self.timeout,
                metadata=self.metadata,
            )
            hook.wait_for_operation(timeout=self.timeout, operation=operation)
            self.log.info("Batch prediction job was deleted.")
        except NotFound:
            self.log.info("The Batch prediction job %s does not exist.", self.batch_prediction_job)


class GetBatchPredictionJobOperator(BaseOperator):
    """
    Gets a BatchPredictionJob

    :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
    :type project_id: str
    :param region: Required. The ID of the Google Cloud region that the service belongs to.
    :type region: str
    :param batch_prediction_job: Required. The name of the BatchPredictionJob resource.
    :type batch_prediction_job: str
    :param retry: Designation of what errors, if any, should be retried.
    :type retry: google.api_core.retry.Retry
    :param timeout: The timeout for this request.
    :type timeout: float
    :param metadata: Strings which should be sent along with the request as metadata.
    :type metadata: Sequence[Tuple[str, str]]
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
    :type gcp_conn_id: str
    :param delegate_to: The account to impersonate using domain-wide delegation of authority,
        if any. For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :type delegate_to: str
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :type impersonation_chain: Union[str, Sequence[str]]
    """

    template_fields = ("region", "project_id", "impersonation_chain")

    def __init__(
        self,
        *,
        region: str,
        project_id: str,
        batch_prediction_job: str,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: Optional[str] = None,
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
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
        self.delegate_to = delegate_to
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Dict):
        hook = BatchPredictionJobHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
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
            return BatchPredictionJob.to_dict(result)
        except NotFound:
            self.log.info("The Batch prediction job %s does not exist.", self.batch_prediction_job)


class ListBatchPredictionJobsOperator(BaseOperator):
    """
    Lists BatchPredictionJobs in a Location.

    :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
    :type project_id: str
    :param region: Required. The ID of the Google Cloud region that the service belongs to.
    :type region: str
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
    :type filter: str
    :param page_size: The standard list page size.
    :type page_size: int
    :param page_token: The standard list page token.
    :type page_token: str
    :param read_mask: Mask specifying which fields to read.
    :type read_mask: google.protobuf.field_mask_pb2.FieldMask
    :param retry: Designation of what errors, if any, should be retried.
    :type retry: google.api_core.retry.Retry
    :param timeout: The timeout for this request.
    :type timeout: float
    :param metadata: Strings which should be sent along with the request as metadata.
    :type metadata: Sequence[Tuple[str, str]]
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
    :type gcp_conn_id: str
    :param delegate_to: The account to impersonate using domain-wide delegation of authority,
        if any. For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :type delegate_to: str
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :type impersonation_chain: Union[str, Sequence[str]]
    """

    template_fields = ("region", "project_id", "impersonation_chain")

    def __init__(
        self,
        *,
        region: str,
        project_id: str,
        filter: Optional[str] = None,
        page_size: Optional[int] = None,
        page_token: Optional[str] = None,
        read_mask: Optional[str] = None,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = "",
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: Optional[str] = None,
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
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
        self.delegate_to = delegate_to
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Dict):
        hook = BatchPredictionJobHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
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
        self.log.info(f"List: {[BatchPredictionJob.to_dict(result) for result in results]}")
        return [BatchPredictionJob.to_dict(result) for result in results]
