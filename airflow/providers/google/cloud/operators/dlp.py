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

# pylint: disable=R0913, C0302
"""
This module contains various GCP Cloud DLP operators
which allow you to perform basic operations using
Cloud DLP.
"""
from typing import Dict, Optional, Sequence, Tuple, Union

from google.api_core.exceptions import AlreadyExists, NotFound
from google.api_core.retry import Retry
from google.cloud.dlp_v2.types import (
    ByteContentItem, ContentItem, DeidentifyConfig, DeidentifyTemplate, FieldMask, InspectConfig,
    InspectJobConfig, InspectTemplate, JobTrigger, RedactImageRequest, RiskAnalysisJobConfig,
    StoredInfoTypeConfig,
)
from google.protobuf.json_format import MessageToDict

from airflow.models import BaseOperator
from airflow.providers.google.cloud.hooks.dlp import CloudDLPHook
from airflow.utils.decorators import apply_defaults


class CloudDLPCancelDLPJobOperator(BaseOperator):
    """
    Starts asynchronous cancellation on a long-running DlpJob.

    :param dlp_job_id: ID of the DLP job resource to be cancelled.
    :type dlp_job_id: str
    :param project_id: (Optional) Google Cloud Platform project ID where the
        DLP Instance exists. If set to None or missing, the default project_id
        from the GCP connection is used.
    :type project_id: str
    :param retry: (Optional) A retry object used to retry requests.
        If None is specified, requests will not be retried.
    :type retry: google.api_core.retry.Retry
    :param timeout: (Optional) The amount of time, in seconds, to wait for the request
        to complete. Note that if retry is specified, the timeout applies to each
        individual attempt.
    :type timeout: float
    :param metadata: (Optional) Additional metadata that is provided to the method.
    :type metadata: sequence[tuple[str, str]]]
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud Platform.
    :type gcp_conn_id: str
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

    template_fields = ("dlp_job_id", "project_id", "gcp_conn_id", "impersonation_chain",)

    @apply_defaults
    def __init__(
        self, *,
        dlp_job_id: str,
        project_id: Optional[str] = None,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        **kwargs
    ) -> None:
        super().__init__(**kwargs)
        self.dlp_job_id = dlp_job_id
        self.project_id = project_id
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context):
        hook = CloudDLPHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        hook.cancel_dlp_job(
            dlp_job_id=self.dlp_job_id,
            project_id=self.project_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )


class CloudDLPCreateDeidentifyTemplateOperator(BaseOperator):
    """
    Creates a DeidentifyTemplate for re-using frequently used configuration for
    de-identifying content, images, and storage.

    :param organization_id: (Optional) The organization ID. Required to set this
        field if parent resource is an organization.
    :type organization_id: str
    :param project_id: (Optional) Google Cloud Platform project ID where the
        DLP Instance exists. Only set this field if the parent resource is
        a project instead of an organization.
    :type project_id: str
    :param deidentify_template: (Optional) The DeidentifyTemplate to create.
    :type deidentify_template: dict or google.cloud.dlp_v2.types.DeidentifyTemplate
    :param template_id: (Optional) The template ID.
    :type template_id: str
    :param retry: (Optional) A retry object used to retry requests.
        If None is specified, requests will not be retried.
    :type retry: google.api_core.retry.Retry
    :param timeout: (Optional) The amount of time, in seconds, to wait for the request
        to complete. Note that if retry is specified, the timeout applies to each
        individual attempt.
    :type timeout: float
    :param metadata: (Optional) Additional metadata that is provided to the method.
    :type metadata: sequence[tuple[str, str]]]
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud Platform.
    :type gcp_conn_id: str
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :type impersonation_chain: Union[str, Sequence[str]]

    :rtype: google.cloud.dlp_v2.types.DeidentifyTemplate
    """

    template_fields = (
        "organization_id",
        "project_id",
        "deidentify_template",
        "template_id",
        "gcp_conn_id",
        "impersonation_chain",
    )

    @apply_defaults
    def __init__(
        self, *,
        organization_id: Optional[str] = None,
        project_id: Optional[str] = None,
        deidentify_template: Optional[Union[Dict, DeidentifyTemplate]] = None,
        template_id: Optional[str] = None,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        **kwargs
    ) -> None:
        super().__init__(**kwargs)
        self.organization_id = organization_id
        self.project_id = project_id
        self.deidentify_template = deidentify_template
        self.template_id = template_id
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context):
        hook = CloudDLPHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        try:
            template = hook.create_deidentify_template(
                organization_id=self.organization_id,
                project_id=self.project_id,
                deidentify_template=self.deidentify_template,
                template_id=self.template_id,
                retry=self.retry,
                timeout=self.timeout,
                metadata=self.metadata,
            )
        except AlreadyExists:
            template = hook.get_deidentify_template(
                organization_id=self.organization_id,
                project_id=self.project_id,
                template_id=self.template_id,
                retry=self.retry,
                timeout=self.timeout,
                metadata=self.metadata,
            )

        return MessageToDict(template)


class CloudDLPCreateDLPJobOperator(BaseOperator):
    """
    Creates a new job to inspect storage or calculate risk metrics.

    :param project_id: (Optional) Google Cloud Platform project ID where the
        DLP Instance exists. If set to None or missing, the default
        project_id from the GCP connection is used.
    :type project_id: str
    :param inspect_job: (Optional) The configuration for the inspect job.
    :type inspect_job: dict or google.cloud.dlp_v2.types.InspectJobConfig
    :param risk_job: (Optional) The configuration for the risk job.
    :type risk_job: dict or google.cloud.dlp_v2.types.RiskAnalysisJobConfig
    :param job_id: (Optional) The job ID.
    :type job_id: str
    :param retry: (Optional) A retry object used to retry requests.
        If None is specified, requests will not be retried.
    :type retry: google.api_core.retry.Retry
    :param timeout: (Optional) The amount of time, in seconds, to wait for the request
        to complete. Note that if retry is specified, the timeout applies to each
        individual attempt.
    :type timeout: float
    :param metadata: (Optional) Additional metadata that is provided to the method.
    :type metadata: sequence[tuple[str, str]]]
    :param wait_until_finished: (Optional) If true, it will keep polling the job state
        until it is set to DONE.
    :type wait_until_finished: bool
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud Platform.
    :type gcp_conn_id: str
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :type impersonation_chain: Union[str, Sequence[str]]

    :rtype: google.cloud.dlp_v2.types.DlpJob
    """

    template_fields = ("project_id", "inspect_job", "risk_job", "job_id", "gcp_conn_id",
                       "impersonation_chain",)

    @apply_defaults
    def __init__(
        self, *,
        project_id: Optional[str] = None,
        inspect_job: Optional[Union[Dict, InspectJobConfig]] = None,
        risk_job: Optional[Union[Dict, RiskAnalysisJobConfig]] = None,
        job_id: Optional[str] = None,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
        wait_until_finished: bool = True,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        **kwargs
    ) -> None:
        super().__init__(**kwargs)
        self.project_id = project_id
        self.inspect_job = inspect_job
        self.risk_job = risk_job
        self.job_id = job_id
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.wait_until_finished = wait_until_finished
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context):
        hook = CloudDLPHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        try:
            job = hook.create_dlp_job(
                project_id=self.project_id,
                inspect_job=self.inspect_job,
                risk_job=self.risk_job,
                job_id=self.job_id,
                retry=self.retry,
                timeout=self.timeout,
                metadata=self.metadata,
                wait_until_finished=self.wait_until_finished,
            )
        except AlreadyExists:
            job = hook.get_dlp_job(
                project_id=self.project_id,
                dlp_job_id=self.job_id,
                retry=self.retry,
                timeout=self.timeout,
                metadata=self.metadata,
            )
        return MessageToDict(job)


class CloudDLPCreateInspectTemplateOperator(BaseOperator):
    """
    Creates an InspectTemplate for re-using frequently used configuration for
    inspecting content, images, and storage.

    :param organization_id: (Optional) The organization ID. Required to set this
        field if parent resource is an organization.
    :type organization_id: str
    :param project_id: (Optional) Google Cloud Platform project ID where the
        DLP Instance exists. Only set this field if the parent resource is
        a project instead of an organization.
    :type project_id: str
    :param inspect_template: (Optional) The InspectTemplate to create.
    :type inspect_template: dict or google.cloud.dlp_v2.types.InspectTemplate
    :param template_id: (Optional) The template ID.
    :type template_id: str
    :param retry: (Optional) A retry object used to retry requests.
        If None is specified, requests will not be retried.
    :type retry: google.api_core.retry.Retry
    :param timeout: (Optional) The amount of time, in seconds, to wait for the request
        to complete. Note that if retry is specified, the timeout applies to each
        individual attempt.
    :type timeout: float
    :param metadata: (Optional) Additional metadata that is provided to the method.
    :type metadata: sequence[tuple[str, str]]]
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud Platform.
    :type gcp_conn_id: str
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :type impersonation_chain: Union[str, Sequence[str]]

    :rtype: google.cloud.dlp_v2.types.InspectTemplate
    """

    template_fields = (
        "organization_id",
        "project_id",
        "inspect_template",
        "template_id",
        "gcp_conn_id",
        "impersonation_chain",
    )

    @apply_defaults
    def __init__(
        self, *,
        organization_id: Optional[str] = None,
        project_id: Optional[str] = None,
        inspect_template: Optional[InspectTemplate] = None,
        template_id: Optional[Union[Dict, InspectTemplate]] = None,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        **kwargs
    ) -> None:
        super().__init__(**kwargs)
        self.organization_id = organization_id
        self.project_id = project_id
        self.inspect_template = inspect_template
        self.template_id = template_id
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context):
        hook = CloudDLPHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        try:
            template = hook.create_inspect_template(
                organization_id=self.organization_id,
                project_id=self.project_id,
                inspect_template=self.inspect_template,
                template_id=self.template_id,
                retry=self.retry,
                timeout=self.timeout,
                metadata=self.metadata,
            )
        except AlreadyExists:
            template = hook.get_inspect_template(
                organization_id=self.organization_id,
                project_id=self.project_id,
                template_id=self.template_id,
                retry=self.retry,
                timeout=self.timeout,
                metadata=self.metadata,
            )
        return MessageToDict(template)


class CloudDLPCreateJobTriggerOperator(BaseOperator):
    """
    Creates a job trigger to run DLP actions such as scanning storage for sensitive
    information on a set schedule.

    :param project_id: (Optional) Google Cloud Platform project ID where the
        DLP Instance exists. If set to None or missing, the default
        project_id from the GCP connection is used.
    :type project_id: str
    :param job_trigger: (Optional) The JobTrigger to create.
    :type job_trigger: dict or google.cloud.dlp_v2.types.JobTrigger
    :param trigger_id: (Optional) The JobTrigger ID.
    :type trigger_id: str
    :param retry: (Optional) A retry object used to retry requests.
        If None is specified, requests will not be retried.
    :type retry: google.api_core.retry.Retry
    :param timeout: (Optional) The amount of time, in seconds, to wait for the request
        to complete. Note that if retry is specified, the timeout applies to each
        individual attempt.
    :type timeout: float
    :param metadata: (Optional) Additional metadata that is provided to the method.
    :type metadata: sequence[tuple[str, str]]]
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud Platform.
    :type gcp_conn_id: str
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :type impersonation_chain: Union[str, Sequence[str]]

    :rtype: google.cloud.dlp_v2.types.JobTrigger
    """

    template_fields = ("project_id", "job_trigger", "trigger_id", "gcp_conn_id",
                       "impersonation_chain",)

    @apply_defaults
    def __init__(
        self, *,
        project_id: Optional[str] = None,
        job_trigger: Optional[Union[Dict, JobTrigger]] = None,
        trigger_id: Optional[str] = None,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        **kwargs
    ) -> None:
        super().__init__(**kwargs)
        self.project_id = project_id
        self.job_trigger = job_trigger
        self.trigger_id = trigger_id
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context):
        hook = CloudDLPHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        try:
            trigger = hook.create_job_trigger(
                project_id=self.project_id,
                job_trigger=self.job_trigger,
                trigger_id=self.trigger_id,
                retry=self.retry,
                timeout=self.timeout,
                metadata=self.metadata,
            )
        except AlreadyExists:
            trigger = hook.get_job_trigger(
                project_id=self.project_id,
                job_trigger_id=self.trigger_id,
                retry=self.retry,
                timeout=self.timeout,
                metadata=self.metadata,
            )
        return MessageToDict(trigger)


class CloudDLPCreateStoredInfoTypeOperator(BaseOperator):
    """
    Creates a pre-built stored infoType to be used for inspection.

    :param organization_id: (Optional) The organization ID. Required to set this
        field if parent resource is an organization.
    :type organization_id: str
    :param project_id: (Optional) Google Cloud Platform project ID where the
        DLP Instance exists. Only set this field if the parent resource is
        a project instead of an organization.
    :type project_id: str
    :param config: (Optional) The config for the StoredInfoType.
    :type config: dict or google.cloud.dlp_v2.types.StoredInfoTypeConfig
    :param stored_info_type_id: (Optional) The StoredInfoType ID.
    :type stored_info_type_id: str
    :param retry: (Optional) A retry object used to retry requests.
        If None is specified, requests will not be retried.
    :type retry: google.api_core.retry.Retry
    :param timeout: (Optional) The amount of time, in seconds, to wait for the request
        to complete. Note that if retry is specified, the timeout applies to each
        individual attempt.
    :type timeout: float
    :param metadata: (Optional) Additional metadata that is provided to the method.
    :type metadata: sequence[tuple[str, str]]]
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud Platform.
    :type gcp_conn_id: str
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :type impersonation_chain: Union[str, Sequence[str]]

    :rtype: google.cloud.dlp_v2.types.StoredInfoType
    """

    template_fields = (
        "organization_id",
        "project_id",
        "config",
        "stored_info_type_id",
        "gcp_conn_id",
        "impersonation_chain",
    )

    @apply_defaults
    def __init__(
        self, *,
        organization_id: Optional[str] = None,
        project_id: Optional[str] = None,
        config: Optional[StoredInfoTypeConfig] = None,
        stored_info_type_id: Optional[str] = None,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        **kwargs
    ) -> None:
        super().__init__(**kwargs)
        self.organization_id = organization_id
        self.project_id = project_id
        self.config = config
        self.stored_info_type_id = stored_info_type_id
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context):
        hook = CloudDLPHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        try:
            info = hook.create_stored_info_type(
                organization_id=self.organization_id,
                project_id=self.project_id,
                config=self.config,
                stored_info_type_id=self.stored_info_type_id,
                retry=self.retry,
                timeout=self.timeout,
                metadata=self.metadata,
            )
        except AlreadyExists:
            info = hook.get_stored_info_type(
                organization_id=self.organization_id,
                project_id=self.project_id,
                stored_info_type_id=self.stored_info_type_id,
                retry=self.retry,
                timeout=self.timeout,
                metadata=self.metadata,
            )
        return MessageToDict(info)


class CloudDLPDeidentifyContentOperator(BaseOperator):
    """
    De-identifies potentially sensitive info from a ContentItem. This method has limits
    on input size and output size.

    :param project_id: (Optional) Google Cloud Platform project ID where the
        DLP Instance exists. If set to None or missing, the default
        project_id from the GCP connection is used.
    :type project_id: str
    :param deidentify_config: (Optional) Configuration for the de-identification of the
        content item. Items specified here will override the template referenced by the
        deidentify_template_name argument.
    :type deidentify_config: dict or google.cloud.dlp_v2.types.DeidentifyConfig
    :param inspect_config: (Optional) Configuration for the inspector. Items specified
        here will override the template referenced by the inspect_template_name argument.
    :type inspect_config: dict or google.cloud.dlp_v2.types.InspectConfig
    :param item: (Optional) The item to de-identify. Will be treated as text.
    :type item: dict or google.cloud.dlp_v2.types.ContentItem
    :param inspect_template_name: (Optional) Optional template to use. Any configuration
        directly specified in inspect_config will override those set in the template.
    :type inspect_template_name: str
    :param deidentify_template_name: (Optional) Optional template to use. Any
        configuration directly specified in deidentify_config will override those set
        in the template.
    :type deidentify_template_name: str
    :param retry: (Optional) A retry object used to retry requests.
        If None is specified, requests will not be retried.
    :type retry: google.api_core.retry.Retry
    :param timeout: (Optional) The amount of time, in seconds, to wait for the request
        to complete. Note that if retry is specified, the timeout applies to each
        individual attempt.
    :type timeout: float
    :param metadata: (Optional) Additional metadata that is provided to the method.
    :type metadata: sequence[tuple[str, str]]]
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud Platform.
    :type gcp_conn_id: str
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :type impersonation_chain: Union[str, Sequence[str]]

    :rtype: google.cloud.dlp_v2.types.DeidentifyContentResponse
    """

    template_fields = (
        "project_id",
        "deidentify_config",
        "inspect_config",
        "item",
        "inspect_template_name",
        "deidentify_template_name",
        "gcp_conn_id",
        "impersonation_chain",
    )

    @apply_defaults
    def __init__(
        self, *,
        project_id: Optional[str] = None,
        deidentify_config: Optional[Union[Dict, DeidentifyConfig]] = None,
        inspect_config: Optional[Union[Dict, InspectConfig]] = None,
        item: Optional[Union[Dict, ContentItem]] = None,
        inspect_template_name: Optional[str] = None,
        deidentify_template_name: Optional[str] = None,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        **kwargs
    ) -> None:
        super().__init__(**kwargs)
        self.project_id = project_id
        self.deidentify_config = deidentify_config
        self.inspect_config = inspect_config
        self.item = item
        self.inspect_template_name = inspect_template_name
        self.deidentify_template_name = deidentify_template_name
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context):
        hook = CloudDLPHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        response = hook.deidentify_content(
            project_id=self.project_id,
            deidentify_config=self.deidentify_config,
            inspect_config=self.inspect_config,
            item=self.item,
            inspect_template_name=self.inspect_template_name,
            deidentify_template_name=self.deidentify_template_name,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        return MessageToDict(response)


class CloudDLPDeleteDeidentifyTemplateOperator(BaseOperator):
    """
    Deletes a DeidentifyTemplate.

    :param template_id: The ID of deidentify template to be deleted.
    :type template_id: str
    :param organization_id: (Optional) The organization ID. Required to set this
        field if parent resource is an organization.
    :type organization_id: str
    :param project_id: (Optional) Google Cloud Platform project ID where the
        DLP Instance exists. Only set this field if the parent resource is
        a project instead of an organization.
    :type project_id: str
    :param retry: (Optional) A retry object used to retry requests.
        If None is specified, requests will not be retried.
    :type retry: google.api_core.retry.Retry
    :param timeout: (Optional) The amount of time, in seconds, to wait for the request
        to complete. Note that if retry is specified, the timeout applies to each
        individual attempt.
    :type timeout: float
    :param metadata: (Optional) Additional metadata that is provided to the method.
    :type metadata: sequence[tuple[str, str]]]
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud Platform.
    :type gcp_conn_id: str
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

    template_fields = ("template_id", "organization_id", "project_id", "gcp_conn_id",
                       "impersonation_chain",)

    @apply_defaults
    def __init__(
        self, *,
        template_id: str,
        organization_id: Optional[str] = None,
        project_id: Optional[str] = None,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        **kwargs
    ) -> None:
        super().__init__(**kwargs)
        self.template_id = template_id
        self.organization_id = organization_id
        self.project_id = project_id
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context):
        hook = CloudDLPHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        try:
            hook.delete_deidentify_template(
                template_id=self.template_id,
                organization_id=self.organization_id,
                project_id=self.project_id,
                retry=self.retry,
                timeout=self.timeout,
                metadata=self.metadata,
            )
        except NotFound:
            self.log.error("Template %s not found.", self.template_id)


class CloudDLPDeleteDLPJobOperator(BaseOperator):
    """
    Deletes a long-running DlpJob. This method indicates that the client is no longer
    interested in the DlpJob result. The job will be cancelled if possible.

    :param dlp_job_id: The ID of the DLP job resource to be cancelled.
    :type dlp_job_id: str
    :param project_id: (Optional) Google Cloud Platform project ID where the
        DLP Instance exists. If set to None or missing, the default
        project_id from the GCP connection is used.
    :type project_id: str
    :param retry: (Optional) A retry object used to retry requests.
        If None is specified, requests will not be retried.
    :type retry: google.api_core.retry.Retry
    :param timeout: (Optional) The amount of time, in seconds, to wait for the request
        to complete. Note that if retry is specified, the timeout applies to each
        individual attempt.
    :type timeout: float
    :param metadata: (Optional) Additional metadata that is provided to the method.
    :type metadata: sequence[tuple[str, str]]]
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud Platform.
    :type gcp_conn_id: str
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

    template_fields = ("dlp_job_id", "project_id", "gcp_conn_id", "impersonation_chain",)

    @apply_defaults
    def __init__(
        self, *,
        dlp_job_id: str,
        project_id: Optional[str] = None,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        **kwargs
    ) -> None:
        super().__init__(**kwargs)
        self.dlp_job_id = dlp_job_id
        self.project_id = project_id
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context):
        hook = CloudDLPHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        try:
            hook.delete_dlp_job(
                dlp_job_id=self.dlp_job_id,
                project_id=self.project_id,
                retry=self.retry,
                timeout=self.timeout,
                metadata=self.metadata,
            )
        except NotFound:
            self.log.error("Job %s id not found.", self.dlp_job_id)


class CloudDLPDeleteInspectTemplateOperator(BaseOperator):
    """
    Deletes an InspectTemplate.

    :param template_id: The ID of the inspect template to be deleted.
    :type template_id: str
    :param organization_id: (Optional) The organization ID. Required to set this
        field if parent resource is an organization.
    :type organization_id: str
    :param project_id: (Optional) Google Cloud Platform project ID where the
        DLP Instance exists. Only set this field if the parent resource is
        a project instead of an organization.
    :type project_id: str
    :param retry: (Optional) A retry object used to retry requests.
        If None is specified, requests will not be retried.
    :type retry: google.api_core.retry.Retry
    :param timeout: (Optional) The amount of time, in seconds, to wait for the request
        to complete. Note that if retry is specified, the timeout applies to each
        individual attempt.
    :type timeout: float
    :param metadata: (Optional) Additional metadata that is provided to the method.
    :type metadata: sequence[tuple[str, str]]]
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud Platform.
    :type gcp_conn_id: str
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

    template_fields = ("template_id", "organization_id", "project_id", "gcp_conn_id",
                       "impersonation_chain",)

    @apply_defaults
    def __init__(
        self, *,
        template_id: str,
        organization_id: Optional[str] = None,
        project_id: Optional[str] = None,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        **kwargs
    ) -> None:
        super().__init__(**kwargs)
        self.template_id = template_id
        self.organization_id = organization_id
        self.project_id = project_id
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context):
        hook = CloudDLPHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        try:
            hook.delete_inspect_template(
                template_id=self.template_id,
                organization_id=self.organization_id,
                project_id=self.project_id,
                retry=self.retry,
                timeout=self.timeout,
                metadata=self.metadata,
            )
        except NotFound:
            self.log.error("Template %s not found", self.template_id)


class CloudDLPDeleteJobTriggerOperator(BaseOperator):
    """
    Deletes a job trigger.

    :param job_trigger_id: The ID of the DLP job trigger to be deleted.
    :type job_trigger_id: str
    :param project_id: (Optional) Google Cloud Platform project ID where the
        DLP Instance exists. If set to None or missing, the default
        project_id from the GCP connection is used.
    :type project_id: str
    :param retry: (Optional) A retry object used to retry requests.
        If None is specified, requests will not be retried.
    :type retry: google.api_core.retry.Retry
    :param timeout: (Optional) The amount of time, in seconds, to wait for the request
        to complete. Note that if retry is specified, the timeout applies to each
        individual attempt.
    :type timeout: float
    :param metadata: (Optional) Additional metadata that is provided to the method.
    :type metadata: sequence[tuple[str, str]]]
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud Platform.
    :type gcp_conn_id: str
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

    template_fields = ("job_trigger_id", "project_id", "gcp_conn_id", "impersonation_chain",)

    @apply_defaults
    def __init__(
        self, *,
        job_trigger_id: str,
        project_id: Optional[str] = None,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        **kwargs
    ) -> None:
        super().__init__(**kwargs)
        self.job_trigger_id = job_trigger_id
        self.project_id = project_id
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context):
        hook = CloudDLPHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        try:
            hook.delete_job_trigger(
                job_trigger_id=self.job_trigger_id,
                project_id=self.project_id,
                retry=self.retry,
                timeout=self.timeout,
                metadata=self.metadata,
            )
        except NotFound:
            self.log.error("Trigger %s not found", self.job_trigger_id)


class CloudDLPDeleteStoredInfoTypeOperator(BaseOperator):
    """
    Deletes a stored infoType.

    :param stored_info_type_id: The ID of the stored info type to be deleted.
    :type stored_info_type_id: str
    :param organization_id: (Optional) The organization ID. Required to set this
        field if parent resource is an organization.
    :type organization_id: str
    :param project_id: (Optional) Google Cloud Platform project ID where the
        DLP Instance exists. Only set this field if the parent resource is
        a project instead of an organization.
    :type project_id: str
    :param retry: (Optional) A retry object used to retry requests.
        If None is specified, requests will not be retried.
    :type retry: google.api_core.retry.Retry
    :param timeout: (Optional) The amount of time, in seconds, to wait for the request
        to complete. Note that if retry is specified, the timeout applies to each
        individual attempt.
    :type timeout: float
    :param metadata: (Optional) Additional metadata that is provided to the method.
    :type metadata: sequence[tuple[str, str]]]
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud Platform.
    :type gcp_conn_id: str
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

    template_fields = (
        "stored_info_type_id",
        "organization_id",
        "project_id",
        "gcp_conn_id",
        "impersonation_chain",
    )

    @apply_defaults
    def __init__(
        self, *,
        stored_info_type_id: str,
        organization_id: Optional[str] = None,
        project_id: Optional[str] = None,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        **kwargs
    ) -> None:
        super().__init__(**kwargs)
        self.stored_info_type_id = stored_info_type_id
        self.organization_id = organization_id
        self.project_id = project_id
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context):
        hook = CloudDLPHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        try:
            hook.delete_stored_info_type(
                stored_info_type_id=self.stored_info_type_id,
                organization_id=self.organization_id,
                project_id=self.project_id,
                retry=self.retry,
                timeout=self.timeout,
                metadata=self.metadata,
            )
        except NotFound:
            self.log.error("Stored info %s not found", self.stored_info_type_id)


class CloudDLPGetDeidentifyTemplateOperator(BaseOperator):
    """
    Gets a DeidentifyTemplate.

    :param template_id: The ID of deidentify template to be read.
    :type template_id: str
    :param organization_id: (Optional) The organization ID. Required to set this
        field if parent resource is an organization.
    :type organization_id: str
    :param project_id: (Optional) Google Cloud Platform project ID where the
        DLP Instance exists. Only set this field if the parent resource is
        a project instead of an organization.
    :type project_id: str
    :param retry: (Optional) A retry object used to retry requests.
        If None is specified, requests will not be retried.
    :type retry: google.api_core.retry.Retry
    :param timeout: (Optional) The amount of time, in seconds, to wait for the request
        to complete. Note that if retry is specified, the timeout applies to each
        individual attempt.
    :type timeout: float
    :param metadata: (Optional) Additional metadata that is provided to the method.
    :type metadata: sequence[tuple[str, str]]]
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud Platform.
    :type gcp_conn_id: str
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :type impersonation_chain: Union[str, Sequence[str]]

    :rtype: google.cloud.dlp_v2.types.DeidentifyTemplate
    """

    template_fields = ("template_id", "organization_id", "project_id", "gcp_conn_id",
                       "impersonation_chain",)

    @apply_defaults
    def __init__(
        self, *,
        template_id: str,
        organization_id: Optional[str] = None,
        project_id: Optional[str] = None,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        **kwargs
    ) -> None:
        super().__init__(**kwargs)
        self.template_id = template_id
        self.organization_id = organization_id
        self.project_id = project_id
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context):
        hook = CloudDLPHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        template = hook.get_deidentify_template(
            template_id=self.template_id,
            organization_id=self.organization_id,
            project_id=self.project_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        return MessageToDict(template)


class CloudDLPGetDLPJobOperator(BaseOperator):
    """
    Gets the latest state of a long-running DlpJob.

    :param dlp_job_id: The ID of the DLP job resource to be read.
    :type dlp_job_id: str
    :param project_id: (Optional) Google Cloud Platform project ID where the
        DLP Instance exists. If set to None or missing, the default
        project_id from the GCP connection is used.
    :type project_id: str
    :param retry: (Optional) A retry object used to retry requests.
        If None is specified, requests will not be retried.
    :type retry: google.api_core.retry.Retry
    :param timeout: (Optional) The amount of time, in seconds, to wait for the request
        to complete. Note that if retry is specified, the timeout applies to each
        individual attempt.
    :type timeout: float
    :param metadata: (Optional) Additional metadata that is provided to the method.
    :type metadata: sequence[tuple[str, str]]]
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud Platform.
    :type gcp_conn_id: str
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :type impersonation_chain: Union[str, Sequence[str]]

    :rtype: google.cloud.dlp_v2.types.DlpJob
    """

    template_fields = ("dlp_job_id", "project_id", "gcp_conn_id", "impersonation_chain",)

    @apply_defaults
    def __init__(
        self, *,
        dlp_job_id: str,
        project_id: Optional[str] = None,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        **kwargs
    ) -> None:
        super().__init__(**kwargs)
        self.dlp_job_id = dlp_job_id
        self.project_id = project_id
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context):
        hook = CloudDLPHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        job = hook.get_dlp_job(
            dlp_job_id=self.dlp_job_id,
            project_id=self.project_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        return MessageToDict(job)


class CloudDLPGetInspectTemplateOperator(BaseOperator):
    """
    Gets an InspectTemplate.

    :param template_id: The ID of inspect template to be read.
    :type template_id: str
    :param organization_id: (Optional) The organization ID. Required to set this
        field if parent resource is an organization.
    :type organization_id: str
    :param project_id: (Optional) Google Cloud Platform project ID where the
        DLP Instance exists. Only set this field if the parent resource is
        a project instead of an organization.
    :type project_id: str
    :param retry: (Optional) A retry object used to retry requests.
        If None is specified, requests will not be retried.
    :type retry: google.api_core.retry.Retry
    :param timeout: (Optional) The amount of time, in seconds, to wait for the request
        to complete. Note that if retry is specified, the timeout applies to each
        individual attempt.
    :type timeout: float
    :param metadata: (Optional) Additional metadata that is provided to the method.
    :type metadata: sequence[tuple[str, str]]]
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud Platform.
    :type gcp_conn_id: str
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :type impersonation_chain: Union[str, Sequence[str]]

    :rtype: google.cloud.dlp_v2.types.InspectTemplate
    """

    template_fields = ("template_id", "organization_id", "project_id", "gcp_conn_id",
                       "impersonation_chain",)

    @apply_defaults
    def __init__(
        self, *,
        template_id: str,
        organization_id: Optional[str] = None,
        project_id: Optional[str] = None,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        **kwargs
    ) -> None:
        super().__init__(**kwargs)
        self.template_id = template_id
        self.organization_id = organization_id
        self.project_id = project_id
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context):
        hook = CloudDLPHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        template = hook.get_inspect_template(
            template_id=self.template_id,
            organization_id=self.organization_id,
            project_id=self.project_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        return MessageToDict(template)


class CloudDLPGetDLPJobTriggerOperator(BaseOperator):
    """
    Gets a job trigger.

    :param job_trigger_id: The ID of the DLP job trigger to be read.
    :type job_trigger_id: str
    :param project_id: (Optional) Google Cloud Platform project ID where the
        DLP Instance exists. If set to None or missing, the default
        project_id from the GCP connection is used.
    :type project_id: str
    :param retry: (Optional) A retry object used to retry requests.
        If None is specified, requests will not be retried.
    :type retry: google.api_core.retry.Retry
    :param timeout: (Optional) The amount of time, in seconds, to wait for the request
        to complete. Note that if retry is specified, the timeout applies to each
        individual attempt.
    :type timeout: float
    :param metadata: (Optional) Additional metadata that is provided to the method.
    :type metadata: sequence[tuple[str, str]]]
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud Platform.
    :type gcp_conn_id: str
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :type impersonation_chain: Union[str, Sequence[str]]

    :rtype: google.cloud.dlp_v2.types.JobTrigger
    """

    template_fields = ("job_trigger_id", "project_id", "gcp_conn_id", "impersonation_chain",)

    @apply_defaults
    def __init__(
        self, *,
        job_trigger_id: str,
        project_id: Optional[str] = None,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        **kwargs
    ) -> None:
        super().__init__(**kwargs)
        self.job_trigger_id = job_trigger_id
        self.project_id = project_id
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context):
        hook = CloudDLPHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        trigger = hook.get_job_trigger(
            job_trigger_id=self.job_trigger_id,
            project_id=self.project_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        return MessageToDict(trigger)


class CloudDLPGetStoredInfoTypeOperator(BaseOperator):
    """
    Gets a stored infoType.

    :param stored_info_type_id: The ID of the stored info type to be read.
    :type stored_info_type_id: str
    :param organization_id: (Optional) The organization ID. Required to set this
        field if parent resource is an organization.
    :type organization_id: str
    :param project_id: (Optional) Google Cloud Platform project ID where the
        DLP Instance exists. Only set this field if the parent resource is
        a project instead of an organization.
    :type project_id: str
    :param retry: (Optional) A retry object used to retry requests.
        If None is specified, requests will not be retried.
    :type retry: google.api_core.retry.Retry
    :param timeout: (Optional) The amount of time, in seconds, to wait for the request
        to complete. Note that if retry is specified, the timeout applies to each
        individual attempt.
    :type timeout: float
    :param metadata: (Optional) Additional metadata that is provided to the method.
    :type metadata: sequence[tuple[str, str]]]
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud Platform.
    :type gcp_conn_id: str
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :type impersonation_chain: Union[str, Sequence[str]]

    :rtype: google.cloud.dlp_v2.types.StoredInfoType
    """

    template_fields = (
        "stored_info_type_id",
        "organization_id",
        "project_id",
        "gcp_conn_id",
        "impersonation_chain",
    )

    @apply_defaults
    def __init__(
        self, *,
        stored_info_type_id: str,
        organization_id: Optional[str] = None,
        project_id: Optional[str] = None,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        **kwargs
    ) -> None:
        super().__init__(**kwargs)
        self.stored_info_type_id = stored_info_type_id
        self.organization_id = organization_id
        self.project_id = project_id
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context):
        hook = CloudDLPHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        info = hook.get_stored_info_type(
            stored_info_type_id=self.stored_info_type_id,
            organization_id=self.organization_id,
            project_id=self.project_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        return MessageToDict(info)


class CloudDLPInspectContentOperator(BaseOperator):
    """
    Finds potentially sensitive info in content. This method has limits on
    input size, processing time, and output size.

    :param project_id: (Optional) Google Cloud Platform project ID where the
        DLP Instance exists. If set to None or missing, the default
        project_id from the GCP connection is used.
    :type project_id: str
    :param inspect_config: (Optional) Configuration for the inspector. Items specified
        here will override the template referenced by the inspect_template_name argument.
    :type inspect_config: dict or google.cloud.dlp_v2.types.InspectConfig
    :param item: (Optional) The item to de-identify. Will be treated as text.
    :type item: dict or google.cloud.dlp_v2.types.ContentItem
    :param inspect_template_name: (Optional) Optional template to use. Any configuration
        directly specified in inspect_config will override those set in the template.
    :type inspect_template_name: str
    :param retry: (Optional) A retry object used to retry requests.
        If None is specified, requests will not be retried.
    :type retry: google.api_core.retry.Retry
    :param timeout: (Optional) The amount of time, in seconds, to wait for the request
        to complete. Note that if retry is specified, the timeout applies to each
        individual attempt.
    :type timeout: float
    :param metadata: (Optional) Additional metadata that is provided to the method.
    :type metadata: sequence[tuple[str, str]]]
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud Platform.
    :type gcp_conn_id: str
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :type impersonation_chain: Union[str, Sequence[str]]

    :rtype: google.cloud.tasks_v2.types.InspectContentResponse
    """

    template_fields = (
        "project_id",
        "inspect_config",
        "item",
        "inspect_template_name",
        "gcp_conn_id",
        "impersonation_chain",
    )

    @apply_defaults
    def __init__(
        self, *,
        project_id: Optional[str] = None,
        inspect_config: Optional[Union[Dict, InspectConfig]] = None,
        item: Optional[Union[Dict, ContentItem]] = None,
        inspect_template_name: Optional[str] = None,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        **kwargs
    ) -> None:
        super().__init__(**kwargs)
        self.project_id = project_id
        self.inspect_config = inspect_config
        self.item = item
        self.inspect_template_name = inspect_template_name
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context):
        hook = CloudDLPHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        response = hook.inspect_content(
            project_id=self.project_id,
            inspect_config=self.inspect_config,
            item=self.item,
            inspect_template_name=self.inspect_template_name,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        return MessageToDict(response)


class CloudDLPListDeidentifyTemplatesOperator(BaseOperator):
    """
    Lists DeidentifyTemplates.

    :param organization_id: (Optional) The organization ID. Required to set this
        field if parent resource is an organization.
    :type organization_id: str
    :param project_id: (Optional) Google Cloud Platform project ID where the
        DLP Instance exists. Only set this field if the parent resource is
        a project instead of an organization.
    :type project_id: str
    :param page_size: (Optional) The maximum number of resources contained in the
        underlying API response.
    :type page_size: int
    :param order_by: (Optional) Optional comma separated list of fields to order by,
        followed by asc or desc postfix.
    :type order_by: str
    :param retry: (Optional) A retry object used to retry requests.
        If None is specified, requests will not be retried.
    :type retry: google.api_core.retry.Retry
    :param timeout: (Optional) The amount of time, in seconds, to wait for the request
        to complete. Note that if retry is specified, the timeout applies to each
        individual attempt.
    :type timeout: float
    :param metadata: (Optional) Additional metadata that is provided to the method.
    :type metadata: sequence[tuple[str, str]]]
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud Platform.
    :type gcp_conn_id: str
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :type impersonation_chain: Union[str, Sequence[str]]

    :rtype: list[google.cloud.dlp_v2.types.DeidentifyTemplate]
    """

    template_fields = ("organization_id", "project_id", "gcp_conn_id", "impersonation_chain",)

    @apply_defaults
    def __init__(
        self, *,
        organization_id: Optional[str] = None,
        project_id: Optional[str] = None,
        page_size: Optional[int] = None,
        order_by: Optional[str] = None,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        **kwargs
    ) -> None:
        super().__init__(**kwargs)
        self.organization_id = organization_id
        self.project_id = project_id
        self.page_size = page_size
        self.order_by = order_by
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context):
        hook = CloudDLPHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        template = hook.list_deidentify_templates(
            organization_id=self.organization_id,
            project_id=self.project_id,
            page_size=self.page_size,
            order_by=self.order_by,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        return MessageToDict(template)


class CloudDLPListDLPJobsOperator(BaseOperator):
    """
    Lists DlpJobs that match the specified filter in the request.

    :param project_id: (Optional) Google Cloud Platform project ID where the
        DLP Instance exists. If set to None or missing, the default
        project_id from the GCP connection is used.
    :type project_id: str
    :param results_filter: (Optional) Filter used to specify a subset of results.
    :type results_filter: str
    :param page_size: (Optional) The maximum number of resources contained in the
        underlying API response.
    :type page_size: int
    :param job_type: (Optional) The type of job.
    :type job_type: str
    :param order_by: (Optional) Optional comma separated list of fields to order by,
        followed by asc or desc postfix.
    :type order_by: str
    :param retry: (Optional) A retry object used to retry requests.
        If None is specified, requests will not be retried.
    :type retry: google.api_core.retry.Retry
    :param timeout: (Optional) The amount of time, in seconds, to wait for the request
        to complete. Note that if retry is specified, the timeout applies to each
        individual attempt.
    :type timeout: float
    :param metadata: (Optional) Additional metadata that is provided to the method.
    :type metadata: sequence[tuple[str, str]]]
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud Platform.
    :type gcp_conn_id: str
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :type impersonation_chain: Union[str, Sequence[str]]

    :rtype: list[google.cloud.dlp_v2.types.DlpJob]
    """

    template_fields = ("project_id", "gcp_conn_id", "impersonation_chain",)

    @apply_defaults
    def __init__(
        self, *,
        project_id: Optional[str] = None,
        results_filter: Optional[str] = None,
        page_size: Optional[int] = None,
        job_type: Optional[str] = None,
        order_by: Optional[str] = None,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        **kwargs
    ) -> None:
        super().__init__(**kwargs)
        self.project_id = project_id
        self.results_filter = results_filter
        self.page_size = page_size
        self.job_type = job_type
        self.order_by = order_by
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context):
        hook = CloudDLPHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        job = hook.list_dlp_jobs(
            project_id=self.project_id,
            results_filter=self.results_filter,
            page_size=self.page_size,
            job_type=self.job_type,
            order_by=self.order_by,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        return MessageToDict(job)


class CloudDLPListInfoTypesOperator(BaseOperator):
    """
    Returns a list of the sensitive information types that the DLP API supports.

    :param language_code: (Optional) Optional BCP-47 language code for localized infoType
        friendly names. If omitted, or if localized strings are not available, en-US
        strings will be returned.
    :type language_code: str
    :param results_filter: (Optional) Filter used to specify a subset of results.
    :type results_filter: str
    :param retry: (Optional) A retry object used to retry requests.
        If None is specified, requests will not be retried.
    :type retry: google.api_core.retry.Retry
    :param timeout: (Optional) The amount of time, in seconds, to wait for the request
        to complete. Note that if retry is specified, the timeout applies to each
        individual attempt.
    :type timeout: float
    :param metadata: (Optional) Additional metadata that is provided to the method.
    :type metadata: sequence[tuple[str, str]]]
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud Platform.
    :type gcp_conn_id: str
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :type impersonation_chain: Union[str, Sequence[str]]

    :rtype: ListInfoTypesResponse
    """

    template_fields = ("language_code", "gcp_conn_id", "impersonation_chain",)

    @apply_defaults
    def __init__(
        self, *,
        language_code: Optional[str] = None,
        results_filter: Optional[str] = None,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        **kwargs
    ) -> None:
        super().__init__(**kwargs)
        self.language_code = language_code
        self.results_filter = results_filter
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context):
        hook = CloudDLPHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        response = hook.list_info_types(
            language_code=self.language_code,
            results_filter=self.results_filter,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        return MessageToDict(response)


class CloudDLPListInspectTemplatesOperator(BaseOperator):
    """
    Lists InspectTemplates.

    :param organization_id: (Optional) The organization ID. Required to set this
        field if parent resource is an organization.
    :type organization_id: str
    :param project_id: (Optional) Google Cloud Platform project ID where the
        DLP Instance exists. Only set this field if the parent resource is
        a project instead of an organization.
    :type project_id: str
    :param page_size: (Optional) The maximum number of resources contained in the
        underlying API response.
    :type page_size: int
    :param order_by: (Optional) Optional comma separated list of fields to order by,
        followed by asc or desc postfix.
    :type order_by: str
    :param retry: (Optional) A retry object used to retry requests.
        If None is specified, requests will not be retried.
    :type retry: google.api_core.retry.Retry
    :param timeout: (Optional) The amount of time, in seconds, to wait for the request
        to complete. Note that if retry is specified, the timeout applies to each
        individual attempt.
    :type timeout: float
    :param metadata: (Optional) Additional metadata that is provided to the method.
    :type metadata: sequence[tuple[str, str]]]
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud Platform.
    :type gcp_conn_id: str
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :type impersonation_chain: Union[str, Sequence[str]]

    :rtype: list[google.cloud.dlp_v2.types.InspectTemplate]
    """

    template_fields = ("organization_id", "project_id", "gcp_conn_id", "impersonation_chain",)

    @apply_defaults
    def __init__(
        self, *,
        organization_id: Optional[str] = None,
        project_id: Optional[str] = None,
        page_size: Optional[int] = None,
        order_by: Optional[str] = None,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        **kwargs
    ) -> None:
        super().__init__(**kwargs)
        self.organization_id = organization_id
        self.project_id = project_id
        self.page_size = page_size
        self.order_by = order_by
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context):
        hook = CloudDLPHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        templates = hook.list_inspect_templates(
            organization_id=self.organization_id,
            project_id=self.project_id,
            page_size=self.page_size,
            order_by=self.order_by,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        return [MessageToDict(t) for t in templates]


class CloudDLPListJobTriggersOperator(BaseOperator):
    """
    Lists job triggers.

    :param project_id: (Optional) Google Cloud Platform project ID where the
        DLP Instance exists. If set to None or missing, the default
        project_id from the GCP connection is used.
    :type project_id: str
    :param page_size: (Optional) The maximum number of resources contained in the
        underlying API response.
    :type page_size: int
    :param order_by: (Optional) Optional comma separated list of fields to order by,
        followed by asc or desc postfix.
    :type order_by: str
    :param results_filter: (Optional) Filter used to specify a subset of results.
    :type results_filter: str
    :param retry: (Optional) A retry object used to retry requests.
        If None is specified, requests will not be retried.
    :type retry: google.api_core.retry.Retry
    :param timeout: (Optional) The amount of time, in seconds, to wait for the request
        to complete. Note that if retry is specified, the timeout applies to each
        individual attempt.
    :type timeout: float
    :param metadata: (Optional) Additional metadata that is provided to the method.
    :type metadata: sequence[tuple[str, str]]]
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud Platform.
    :type gcp_conn_id: str
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :type impersonation_chain: Union[str, Sequence[str]]

    :rtype: list[google.cloud.dlp_v2.types.JobTrigger]
    """

    template_fields = ("project_id", "gcp_conn_id", "impersonation_chain",)

    @apply_defaults
    def __init__(
        self, *,
        project_id: Optional[str] = None,
        page_size: Optional[int] = None,
        order_by: Optional[str] = None,
        results_filter: Optional[str] = None,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        **kwargs
    ) -> None:
        super().__init__(**kwargs)
        self.project_id = project_id
        self.page_size = page_size
        self.order_by = order_by
        self.results_filter = results_filter
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context):
        hook = CloudDLPHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        jobs = hook.list_job_triggers(
            project_id=self.project_id,
            page_size=self.page_size,
            order_by=self.order_by,
            results_filter=self.results_filter,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        return [MessageToDict(j) for j in jobs]


class CloudDLPListStoredInfoTypesOperator(BaseOperator):
    """
    Lists stored infoTypes.

    :param organization_id: (Optional) The organization ID. Required to set this
        field if parent resource is an organization.
    :type organization_id: str
    :param project_id: (Optional) Google Cloud Platform project ID where the
        DLP Instance exists. Only set this field if the parent resource is
        a project instead of an organization.
    :type project_id: str
    :param page_size: (Optional) The maximum number of resources contained in the
        underlying API response.
    :type page_size: int
    :param order_by: (Optional) Optional comma separated list of fields to order by,
        followed by asc or desc postfix.
    :type order_by: str
    :param retry: (Optional) A retry object used to retry requests.
        If None is specified, requests will not be retried.
    :type retry: google.api_core.retry.Retry
    :param timeout: (Optional) The amount of time, in seconds, to wait for the request
        to complete. Note that if retry is specified, the timeout applies to each
        individual attempt.
    :type timeout: float
    :param metadata: (Optional) Additional metadata that is provided to the method.
    :type metadata: sequence[tuple[str, str]]]
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud Platform.
    :type gcp_conn_id: str
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :type impersonation_chain: Union[str, Sequence[str]]

    :rtype: list[google.cloud.dlp_v2.types.StoredInfoType]
    """

    template_fields = ("organization_id", "project_id", "gcp_conn_id", "impersonation_chain",)

    @apply_defaults
    def __init__(
        self, *,
        organization_id: Optional[str] = None,
        project_id: Optional[str] = None,
        page_size: Optional[int] = None,
        order_by: Optional[str] = None,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        **kwargs
    ) -> None:
        super().__init__(**kwargs)
        self.organization_id = organization_id
        self.project_id = project_id
        self.page_size = page_size
        self.order_by = order_by
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context):
        hook = CloudDLPHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        infos = hook.list_stored_info_types(
            organization_id=self.organization_id,
            project_id=self.project_id,
            page_size=self.page_size,
            order_by=self.order_by,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        return [MessageToDict(i) for i in infos]


class CloudDLPRedactImageOperator(BaseOperator):
    """
    Redacts potentially sensitive info from an image. This method has limits on
    input size, processing time, and output size.

    :param project_id: (Optional) Google Cloud Platform project ID where the
        DLP Instance exists. If set to None or missing, the default
        project_id from the GCP connection is used.
    :type project_id: str
    :param inspect_config: (Optional) Configuration for the inspector. Items specified
        here will override the template referenced by the inspect_template_name argument.
    :type inspect_config: dict or google.cloud.dlp_v2.types.InspectConfig
    :param image_redaction_configs: (Optional) The configuration for specifying what
        content to redact from images.
    :type image_redaction_configs: list[dict] or
        list[google.cloud.dlp_v2.types.RedactImageRequest.ImageRedactionConfig]
    :param include_findings: (Optional) Whether the response should include findings
        along with the redacted image.
    :type include_findings: bool
    :param byte_item: (Optional) The content must be PNG, JPEG, SVG or BMP.
    :type byte_item: dict or google.cloud.dlp_v2.types.ByteContentItem
    :param retry: (Optional) A retry object used to retry requests.
        If None is specified, requests will not be retried.
    :type retry: google.api_core.retry.Retry
    :param timeout: (Optional) The amount of time, in seconds, to wait for the request
        to complete. Note that if retry is specified, the timeout applies to each
        individual attempt.
    :type timeout: float
    :param metadata: (Optional) Additional metadata that is provided to the method.
    :type metadata: sequence[tuple[str, str]]]
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud Platform.
    :type gcp_conn_id: str
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :type impersonation_chain: Union[str, Sequence[str]]

    :rtype: google.cloud.dlp_v2.types.RedactImageResponse
    """

    template_fields = (
        "project_id",
        "inspect_config",
        "image_redaction_configs",
        "include_findings",
        "byte_item",
        "gcp_conn_id",
        "impersonation_chain",
    )

    @apply_defaults
    def __init__(
        self, *,
        project_id: Optional[str] = None,
        inspect_config: Optional[Union[Dict, InspectConfig]] = None,
        image_redaction_configs: Optional[Union[Dict, RedactImageRequest.ImageRedactionConfig]] = None,
        include_findings: Optional[bool] = None,
        byte_item: Optional[Union[Dict, ByteContentItem]] = None,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        **kwargs
    ) -> None:
        super().__init__(**kwargs)
        self.project_id = project_id
        self.inspect_config = inspect_config
        self.image_redaction_configs = image_redaction_configs
        self.include_findings = include_findings
        self.byte_item = byte_item
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context):
        hook = CloudDLPHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        response = hook.redact_image(
            project_id=self.project_id,
            inspect_config=self.inspect_config,
            image_redaction_configs=self.image_redaction_configs,
            include_findings=self.include_findings,
            byte_item=self.byte_item,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        return MessageToDict(response)


class CloudDLPReidentifyContentOperator(BaseOperator):
    """
    Re-identifies content that has been de-identified.

    :param project_id: (Optional) Google Cloud Platform project ID where the
        DLP Instance exists. If set to None or missing, the default
        project_id from the GCP connection is used.
    :type project_id: str
    :param reidentify_config: (Optional) Configuration for the re-identification of
        the content item.
    :type reidentify_config: dict or google.cloud.dlp_v2.types.DeidentifyConfig
    :param inspect_config: (Optional) Configuration for the inspector.
    :type inspect_config: dict or google.cloud.dlp_v2.types.InspectConfig
    :param item: (Optional) The item to re-identify. Will be treated as text.
    :type item: dict or google.cloud.dlp_v2.types.ContentItem
    :param inspect_template_name: (Optional) Optional template to use. Any configuration
        directly specified in inspect_config will override those set in the template.
    :type inspect_template_name: str
    :param reidentify_template_name: (Optional) Optional template to use. References an
        instance of DeidentifyTemplate. Any configuration directly specified in
        reidentify_config or inspect_config will override those set in the template.
    :type reidentify_template_name: str
    :param retry: (Optional) A retry object used to retry requests.
        If None is specified, requests will not be retried.
    :type retry: google.api_core.retry.Retry
    :param timeout: (Optional) The amount of time, in seconds, to wait for the request
        to complete. Note that if retry is specified, the timeout applies to each
        individual attempt.
    :type timeout: float
    :param metadata: (Optional) Additional metadata that is provided to the method.
    :type metadata: sequence[tuple[str, str]]]
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud Platform.
    :type gcp_conn_id: str
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :type impersonation_chain: Union[str, Sequence[str]]

    :rtype: google.cloud.dlp_v2.types.ReidentifyContentResponse
    """

    template_fields = (
        "project_id",
        "reidentify_config",
        "inspect_config",
        "item",
        "inspect_template_name",
        "reidentify_template_name",
        "gcp_conn_id",
        "impersonation_chain",
    )

    @apply_defaults
    def __init__(
        self, *,
        project_id: Optional[str] = None,
        reidentify_config: Optional[Union[Dict, DeidentifyConfig]] = None,
        inspect_config: Optional[Union[Dict, InspectConfig]] = None,
        item: Optional[Union[Dict, ContentItem]] = None,
        inspect_template_name: Optional[str] = None,
        reidentify_template_name: Optional[str] = None,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        **kwargs
    ) -> None:
        super().__init__(**kwargs)
        self.project_id = project_id
        self.reidentify_config = reidentify_config
        self.inspect_config = inspect_config
        self.item = item
        self.inspect_template_name = inspect_template_name
        self.reidentify_template_name = reidentify_template_name
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context):
        hook = CloudDLPHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        response = hook.reidentify_content(
            project_id=self.project_id,
            reidentify_config=self.reidentify_config,
            inspect_config=self.inspect_config,
            item=self.item,
            inspect_template_name=self.inspect_template_name,
            reidentify_template_name=self.reidentify_template_name,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        return MessageToDict(response)


class CloudDLPUpdateDeidentifyTemplateOperator(BaseOperator):
    """
    Updates the DeidentifyTemplate.

    :param template_id: The ID of deidentify template to be updated.
    :type template_id: str
    :param organization_id: (Optional) The organization ID. Required to set this
        field if parent resource is an organization.
    :type organization_id: str
    :param project_id: (Optional) Google Cloud Platform project ID where the
        DLP Instance exists. Only set this field if the parent resource is
        a project instead of an organization.
    :type project_id: str
    :param deidentify_template: New DeidentifyTemplate value.
    :type deidentify_template: dict or google.cloud.dlp_v2.types.DeidentifyTemplate
    :param update_mask: Mask to control which fields get updated.
    :type update_mask: dict or google.cloud.dlp_v2.types.FieldMask
    :param retry: (Optional) A retry object used to retry requests.
        If None is specified, requests will not be retried.
    :type retry: google.api_core.retry.Retry
    :param timeout: (Optional) The amount of time, in seconds, to wait for the request
        to complete. Note that if retry is specified, the timeout applies to each
        individual attempt.
    :type timeout: float
    :param metadata: (Optional) Additional metadata that is provided to the method.
    :type metadata: sequence[tuple[str, str]]]
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud Platform.
    :type gcp_conn_id: str
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :type impersonation_chain: Union[str, Sequence[str]]

    :rtype: google.cloud.dlp_v2.types.DeidentifyTemplate
    """

    template_fields = (
        "template_id",
        "organization_id",
        "project_id",
        "deidentify_template",
        "update_mask",
        "gcp_conn_id",
        "impersonation_chain",
    )

    @apply_defaults
    def __init__(
        self, *,
        template_id: str,
        organization_id: Optional[str] = None,
        project_id: Optional[str] = None,
        deidentify_template: Optional[Union[Dict, DeidentifyTemplate]] = None,
        update_mask: Optional[Union[Dict, FieldMask]] = None,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        **kwargs
    ) -> None:
        super().__init__(**kwargs)
        self.template_id = template_id
        self.organization_id = organization_id
        self.project_id = project_id
        self.deidentify_template = deidentify_template
        self.update_mask = update_mask
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context):
        hook = CloudDLPHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        template = hook.update_deidentify_template(
            template_id=self.template_id,
            organization_id=self.organization_id,
            project_id=self.project_id,
            deidentify_template=self.deidentify_template,
            update_mask=self.update_mask,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        return MessageToDict(template)


class CloudDLPUpdateInspectTemplateOperator(BaseOperator):
    """
    Updates the InspectTemplate.

    :param template_id: The ID of the inspect template to be updated.
    :type template_id: str
    :param organization_id: (Optional) The organization ID. Required to set this
        field if parent resource is an organization.
    :type organization_id: str
    :param project_id: (Optional) Google Cloud Platform project ID where the
        DLP Instance exists. Only set this field if the parent resource is
        a project instead of an organization.
    :type project_id: str
    :param inspect_template: New InspectTemplate value.
    :type inspect_template: dict or google.cloud.dlp_v2.types.InspectTemplate
    :param update_mask: Mask to control which fields get updated.
    :type update_mask: dict or google.cloud.dlp_v2.types.FieldMask
    :param retry: (Optional) A retry object used to retry requests.
        If None is specified, requests will not be retried.
    :type retry: google.api_core.retry.Retry
    :param timeout: (Optional) The amount of time, in seconds, to wait for the request
        to complete. Note that if retry is specified, the timeout applies to each
        individual attempt.
    :type timeout: float
    :param metadata: (Optional) Additional metadata that is provided to the method.
    :type metadata: sequence[tuple[str, str]]]
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud Platform.
    :type gcp_conn_id: str
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :type impersonation_chain: Union[str, Sequence[str]]

    :rtype: google.cloud.dlp_v2.types.InspectTemplate
    """

    template_fields = (
        "template_id",
        "organization_id",
        "project_id",
        "inspect_template",
        "update_mask",
        "gcp_conn_id",
        "impersonation_chain",
    )

    @apply_defaults
    def __init__(
        self, *,
        template_id: str,
        organization_id: Optional[str] = None,
        project_id: Optional[str] = None,
        inspect_template: Optional[Union[Dict, InspectTemplate]] = None,
        update_mask: Optional[Union[Dict, FieldMask]] = None,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        **kwargs
    ) -> None:
        super().__init__(**kwargs)
        self.template_id = template_id
        self.organization_id = organization_id
        self.project_id = project_id
        self.inspect_template = inspect_template
        self.update_mask = update_mask
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context):
        hook = CloudDLPHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        template = hook.update_inspect_template(
            template_id=self.template_id,
            organization_id=self.organization_id,
            project_id=self.project_id,
            inspect_template=self.inspect_template,
            update_mask=self.update_mask,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        return MessageToDict(template)


class CloudDLPUpdateJobTriggerOperator(BaseOperator):
    """
    Updates a job trigger.

    :param job_trigger_id: The ID of the DLP job trigger to be updated.
    :type job_trigger_id: str
    :param project_id: (Optional) Google Cloud Platform project ID where the
        DLP Instance exists. If set to None or missing, the default
        project_id from the GCP connection is used.
    :type project_id: str
    :param job_trigger: New JobTrigger value.
    :type job_trigger: dict or google.cloud.dlp_v2.types.JobTrigger
    :param update_mask: Mask to control which fields get updated.
    :type update_mask: dict or google.cloud.dlp_v2.types.FieldMask
    :param retry: (Optional) A retry object used to retry requests.
        If None is specified, requests will not be retried.
    :type retry: google.api_core.retry.Retry
    :param timeout: (Optional) The amount of time, in seconds, to wait for the request
        to complete. Note that if retry is specified, the timeout applies to each
        individual attempt.
    :type timeout: float
    :param metadata: (Optional) Additional metadata that is provided to the method.
    :type metadata: sequence[tuple[str, str]]]
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud Platform.
    :type gcp_conn_id: str
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :type impersonation_chain: Union[str, Sequence[str]]

    :rtype: google.cloud.dlp_v2.types.InspectTemplate
    """

    template_fields = (
        "job_trigger_id",
        "project_id",
        "job_trigger",
        "update_mask",
        "gcp_conn_id",
        "impersonation_chain",
    )

    @apply_defaults
    def __init__(
        self, *,
        job_trigger_id,
        project_id: Optional[str] = None,
        job_trigger: Optional[JobTrigger] = None,
        update_mask: Optional[Union[Dict, FieldMask]] = None,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        **kwargs
    ) -> None:
        super().__init__(**kwargs)
        self.job_trigger_id = job_trigger_id
        self.project_id = project_id
        self.job_trigger = job_trigger
        self.update_mask = update_mask
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context):
        hook = CloudDLPHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        trigger = hook.update_job_trigger(
            job_trigger_id=self.job_trigger_id,
            project_id=self.project_id,
            job_trigger=self.job_trigger,
            update_mask=self.update_mask,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        return MessageToDict(trigger)


class CloudDLPUpdateStoredInfoTypeOperator(BaseOperator):
    """
    Updates the stored infoType by creating a new version.

    :param stored_info_type_id: The ID of the stored info type to be updated.
    :type stored_info_type_id: str
    :param organization_id: (Optional) The organization ID. Required to set this
        field if parent resource is an organization.
    :type organization_id: str
    :param project_id: (Optional) Google Cloud Platform project ID where the
        DLP Instance exists. Only set this field if the parent resource is
        a project instead of an organization.
    :type project_id: str
    :param config: Updated configuration for the storedInfoType. If not provided, a new
        version of the storedInfoType will be created with the existing configuration.
    :type config: dict or google.cloud.dlp_v2.types.StoredInfoTypeConfig
    :param update_mask: Mask to control which fields get updated.
    :type update_mask: dict or google.cloud.dlp_v2.types.FieldMask
    :param retry: (Optional) A retry object used to retry requests.
        If None is specified, requests will not be retried.
    :type retry: google.api_core.retry.Retry
    :param timeout: (Optional) The amount of time, in seconds, to wait for the request
        to complete. Note that if retry is specified, the timeout applies to each
        individual attempt.
    :type timeout: float
    :param metadata: (Optional) Additional metadata that is provided to the method.
    :type metadata: sequence[tuple[str, str]]]
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud Platform.
    :type gcp_conn_id: str
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :type impersonation_chain: Union[str, Sequence[str]]

    :rtype: google.cloud.dlp_v2.types.StoredInfoType
    """

    template_fields = (
        "stored_info_type_id",
        "organization_id",
        "project_id",
        "config",
        "update_mask",
        "gcp_conn_id",
        "impersonation_chain",
    )

    @apply_defaults
    def __init__(
        self, *,
        stored_info_type_id,
        organization_id: Optional[str] = None,
        project_id: Optional[str] = None,
        config: Optional[Union[Dict, StoredInfoTypeConfig]] = None,
        update_mask: Optional[Union[Dict, FieldMask]] = None,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        **kwargs
    ) -> None:
        super().__init__(**kwargs)
        self.stored_info_type_id = stored_info_type_id
        self.organization_id = organization_id
        self.project_id = project_id
        self.config = config
        self.update_mask = update_mask
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context):
        hook = CloudDLPHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        info = hook.update_stored_info_type(
            stored_info_type_id=self.stored_info_type_id,
            organization_id=self.organization_id,
            project_id=self.project_id,
            config=self.config,
            update_mask=self.update_mask,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        return MessageToDict(info)
