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
This module contains a CloudDLPHook
which allows you to connect to Google Cloud DLP service.

.. spelling::

    ImageRedactionConfig
    RedactImageRequest
"""
from __future__ import annotations

import re
import time
from typing import Sequence

from google.api_core.gapic_v1.method import DEFAULT, _MethodDefault
from google.api_core.retry import Retry
from google.cloud.dlp import DlpServiceClient
from google.cloud.dlp_v2.types import (
    ByteContentItem,
    ContentItem,
    DeidentifyConfig,
    DeidentifyContentResponse,
    DeidentifyTemplate,
    DlpJob,
    InspectConfig,
    InspectContentResponse,
    InspectJobConfig,
    InspectTemplate,
    JobTrigger,
    ListInfoTypesResponse,
    RedactImageRequest,
    RedactImageResponse,
    ReidentifyContentResponse,
    RiskAnalysisJobConfig,
    StoredInfoType,
    StoredInfoTypeConfig,
)
from google.protobuf.field_mask_pb2 import FieldMask

from airflow.exceptions import AirflowException
from airflow.providers.google.common.consts import CLIENT_INFO
from airflow.providers.google.common.hooks.base_google import PROVIDE_PROJECT_ID, GoogleBaseHook

DLP_JOB_PATH_PATTERN = "^projects/[^/]+/dlpJobs/(?P<job>.*?)$"


class CloudDLPHook(GoogleBaseHook):
    """
    Hook for Google Cloud Data Loss Prevention (DLP) APIs.
    Cloud DLP allows clients to detect the presence of Personally Identifiable
    Information (PII) and other privacy-sensitive data in user-supplied,
    unstructured data streams, like text blocks or images. The service also
    includes methods for sensitive data redaction and scheduling of data scans
    on Google Cloud based data sets.

    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account.
    """

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
        self._client: DlpServiceClient | None = None

    def get_conn(self) -> DlpServiceClient:
        """
        Provides a client for interacting with the Cloud DLP API.

        :return: Google Cloud DLP API Client
        """
        if not self._client:
            self._client = DlpServiceClient(credentials=self.get_credentials(), client_info=CLIENT_INFO)
        return self._client

    def _project_deidentify_template_path(self, project_id, template_id):
        return f"{DlpServiceClient.common_project_path(project_id)}/deidentifyTemplates/{template_id}"

    def _project_stored_info_type_path(self, project_id, info_type_id):
        return f"{DlpServiceClient.common_project_path(project_id)}/storedInfoTypes/{info_type_id}"

    def _project_inspect_template_path(self, project_id, inspect_template_id):
        return f"{DlpServiceClient.common_project_path(project_id)}/inspectTemplates/{inspect_template_id}"

    @GoogleBaseHook.fallback_to_default_project_id
    def cancel_dlp_job(
        self,
        dlp_job_id: str,
        project_id: str = PROVIDE_PROJECT_ID,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> None:
        """
        Starts asynchronous cancellation on a long-running DLP job.

        :param dlp_job_id: ID of the DLP job resource to be cancelled.
        :param project_id: (Optional) Google Cloud project ID where the
            DLP Instance exists. If set to None or missing, the default project_id
            from the Google Cloud connection is used.
        :param retry: (Optional) A retry object used to retry requests.
            If None is specified, requests will not be retried.
        :param timeout: (Optional) The amount of time, in seconds, to wait for the request
            to complete. Note that if retry is specified, the timeout applies to each
            individual attempt.
        :param metadata: (Optional) Additional metadata that is provided to the method.
        """
        client = self.get_conn()

        if not dlp_job_id:
            raise AirflowException("Please provide the ID of the DLP job resource to be cancelled.")

        name = DlpServiceClient.dlp_job_path(project_id, dlp_job_id)
        client.cancel_dlp_job(
            request=dict(
                name=name,
            ),
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

    def create_deidentify_template(
        self,
        organization_id: str | None = None,
        project_id: str | None = None,
        deidentify_template: dict | DeidentifyTemplate | None = None,
        template_id: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> DeidentifyTemplate:
        """
        Creates a deidentify template for re-using frequently used configuration for
        de-identifying content, images, and storage.

        :param organization_id: (Optional) The organization ID. Required to set this
            field if parent resource is an organization.
        :param project_id: (Optional) Google Cloud project ID where the
            DLP Instance exists. Only set this field if the parent resource is
            a project instead of an organization.
        :param deidentify_template: (Optional) The de-identify template to create.
        :param template_id: (Optional) The template ID.
        :param retry: (Optional) A retry object used to retry requests.
            If None is specified, requests will not be retried.
        :param timeout: (Optional) The amount of time, in seconds, to wait for the request
            to complete. Note that if retry is specified, the timeout applies to each
            individual attempt.
        :param metadata: (Optional) Additional metadata that is provided to the method.
        """
        client = self.get_conn()
        # Handle project_id from connection configuration
        project_id = project_id or self.project_id

        if organization_id:
            parent = DlpServiceClient.common_organization_path(organization_id)
        elif project_id:
            parent = DlpServiceClient.common_project_path(project_id)
        else:
            raise AirflowException("Please provide either organization_id or project_id.")

        return client.create_deidentify_template(
            request=dict(
                parent=parent,
                deidentify_template=deidentify_template,
                template_id=template_id,
            ),
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

    @GoogleBaseHook.fallback_to_default_project_id
    def create_dlp_job(
        self,
        project_id: str = PROVIDE_PROJECT_ID,
        inspect_job: dict | InspectJobConfig | None = None,
        risk_job: dict | RiskAnalysisJobConfig | None = None,
        job_id: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        wait_until_finished: bool = True,
        time_to_sleep_in_seconds: int = 60,
    ) -> DlpJob:
        """
        Creates a new job to inspect storage or calculate risk metrics.

        :param project_id: (Optional) Google Cloud project ID where the
            DLP Instance exists. If set to None or missing, the default
            project_id from the Google Cloud connection is used.
        :param inspect_job: (Optional) The configuration for the inspect job.
        :param risk_job: (Optional) The configuration for the risk job.
        :param job_id: (Optional) The job ID.
        :param retry: (Optional) A retry object used to retry requests.
            If None is specified, requests will not be retried.
        :param timeout: (Optional) The amount of time, in seconds, to wait for the request
            to complete. Note that if retry is specified, the timeout applies to each
            individual attempt.
        :param metadata: (Optional) Additional metadata that is provided to the method.
        :param wait_until_finished: (Optional) If true, it will keep polling the job state
            until it is set to DONE.
        :param time_to_sleep_in_seconds: (Optional) Time to sleep, in seconds, between active checks
            of the operation results. Defaults to 60.
        """
        client = self.get_conn()

        parent = DlpServiceClient.common_project_path(project_id)
        job = client.create_dlp_job(
            request=dict(
                parent=parent,
                inspect_job=inspect_job,
                risk_job=risk_job,
                job_id=job_id,
            ),
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

        if wait_until_finished:
            pattern = re.compile(DLP_JOB_PATH_PATTERN, re.IGNORECASE)
            match = pattern.match(job.name)
            if match is not None:
                job_name = match.groupdict()["job"]
            else:
                raise AirflowException(f"Unable to retrieve DLP job's ID from {job.name}.")

        while wait_until_finished:
            job = self.get_dlp_job(dlp_job_id=job_name, project_id=project_id)

            self.log.info("DLP job %s state: %s.", job.name, job.state)

            if job.state == DlpJob.JobState.DONE:
                return job
            elif job.state in [
                DlpJob.JobState.PENDING,
                DlpJob.JobState.RUNNING,
                DlpJob.JobState.JOB_STATE_UNSPECIFIED,
            ]:
                time.sleep(time_to_sleep_in_seconds)
            else:
                raise AirflowException(
                    "Stopped polling DLP job state. "
                    f"DLP job {job.name} state: {DlpJob.JobState.Name(job.state)}."
                )
        return job

    def create_inspect_template(
        self,
        organization_id: str | None = None,
        project_id: str | None = None,
        inspect_template: InspectTemplate | None = None,
        template_id: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> InspectTemplate:
        """
        Creates an inspect template for re-using frequently used configuration for
        inspecting content, images, and storage.

        :param organization_id: (Optional) The organization ID. Required to set this
            field if parent resource is an organization.
        :param project_id: (Optional) Google Cloud project ID where the
            DLP Instance exists. Only set this field if the parent resource is
            a project instead of an organization.
        :param inspect_template: (Optional) The inspect template to create.
        :param template_id: (Optional) The template ID.
        :param retry: (Optional) A retry object used to retry requests.
            If None is specified, requests will not be retried.
        :param timeout: (Optional) The amount of time, in seconds, to wait for the request
            to complete. Note that if retry is specified, the timeout applies to each
            individual attempt.
        :param metadata: (Optional) Additional metadata that is provided to the method.
        """
        client = self.get_conn()

        # Handle project_id from connection configuration
        project_id = project_id or self.project_id

        if organization_id:
            parent = DlpServiceClient.common_organization_path(organization_id)
        elif project_id:
            parent = DlpServiceClient.common_project_path(project_id)
        else:
            raise AirflowException("Please provide either organization_id or project_id.")

        return client.create_inspect_template(
            request=dict(
                parent=parent,
                inspect_template=inspect_template,
                template_id=template_id,
            ),
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

    @GoogleBaseHook.fallback_to_default_project_id
    def create_job_trigger(
        self,
        project_id: str = PROVIDE_PROJECT_ID,
        job_trigger: dict | JobTrigger | None = None,
        trigger_id: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> JobTrigger:
        """
        Creates a job trigger to run DLP actions such as scanning storage for sensitive
        information on a set schedule.

        :param project_id: (Optional) Google Cloud project ID where the
            DLP Instance exists. If set to None or missing, the default
            project_id from the Google Cloud connection is used.
        :param job_trigger: (Optional) The job trigger to create.
        :param trigger_id: (Optional) The job trigger ID.
        :param retry: (Optional) A retry object used to retry requests.
            If None is specified, requests will not be retried.
        :param timeout: (Optional) The amount of time, in seconds, to wait for the request
            to complete. Note that if retry is specified, the timeout applies to each
            individual attempt.
        :param metadata: (Optional) Additional metadata that is provided to the method.
        """
        client = self.get_conn()

        parent = DlpServiceClient.common_project_path(project_id)
        return client.create_job_trigger(
            request=dict(
                parent=parent,
                job_trigger=job_trigger,
                trigger_id=trigger_id,
            ),
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

    def create_stored_info_type(
        self,
        organization_id: str | None = None,
        project_id: str | None = None,
        config: dict | StoredInfoTypeConfig | None = None,
        stored_info_type_id: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> StoredInfoType:
        """
        Creates a pre-built stored info type to be used for inspection.

        :param organization_id: (Optional) The organization ID. Required to set this
            field if parent resource is an organization.
        :param project_id: (Optional) Google Cloud project ID where the
            DLP Instance exists. Only set this field if the parent resource is
            a project instead of an organization.
        :param config: (Optional) The config for the stored info type.
        :param stored_info_type_id: (Optional) The stored info type ID.
        :param retry: (Optional) A retry object used to retry requests.
            If None is specified, requests will not be retried.
        :param timeout: (Optional) The amount of time, in seconds, to wait for the request
            to complete. Note that if retry is specified, the timeout applies to each
            individual attempt.
        :param metadata: (Optional) Additional metadata that is provided to the method.
        """
        client = self.get_conn()

        # Handle project_id from connection configuration
        project_id = project_id or self.project_id

        if organization_id:
            parent = DlpServiceClient.common_organization_path(organization_id)
        elif project_id:
            parent = DlpServiceClient.common_project_path(project_id)
        else:
            raise AirflowException("Please provide either organization_id or project_id.")

        return client.create_stored_info_type(
            request=dict(
                parent=parent,
                config=config,
                stored_info_type_id=stored_info_type_id,
            ),
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

    @GoogleBaseHook.fallback_to_default_project_id
    def deidentify_content(
        self,
        project_id: str = PROVIDE_PROJECT_ID,
        deidentify_config: dict | DeidentifyConfig | None = None,
        inspect_config: dict | InspectConfig | None = None,
        item: dict | ContentItem | None = None,
        inspect_template_name: str | None = None,
        deidentify_template_name: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> DeidentifyContentResponse:
        """
        De-identifies potentially sensitive info from a content item. This method has limits
        on input size and output size.

        :param project_id: (Optional) Google Cloud project ID where the
            DLP Instance exists. If set to None or missing, the default
            project_id from the Google Cloud connection is used.
        :param deidentify_config: (Optional) Configuration for the de-identification of the
            content item. Items specified here will override the template referenced by the
            deidentify_template_name argument.
        :param inspect_config: (Optional) Configuration for the inspector. Items specified
            here will override the template referenced by the inspect_template_name argument.
        :param item: (Optional) The item to de-identify. Will be treated as text.
        :param inspect_template_name: (Optional) Optional template to use. Any configuration
            directly specified in inspect_config will override those set in the template.
        :param deidentify_template_name: (Optional) Optional template to use. Any
            configuration directly specified in deidentify_config will override those set
            in the template.
        :param retry: (Optional) A retry object used to retry requests.
            If None is specified, requests will not be retried.
        :param timeout: (Optional) The amount of time, in seconds, to wait for the request
            to complete. Note that if retry is specified, the timeout applies to each
            individual attempt.
        :param metadata: (Optional) Additional metadata that is provided to the method.
        """
        client = self.get_conn()

        parent = DlpServiceClient.common_project_path(project_id)
        return client.deidentify_content(
            request=dict(
                parent=parent,
                deidentify_config=deidentify_config,
                inspect_config=inspect_config,
                item=item,
                inspect_template_name=inspect_template_name,
                deidentify_template_name=deidentify_template_name,
            ),
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

    def delete_deidentify_template(
        self, template_id, organization_id=None, project_id=None, retry=DEFAULT, timeout=None, metadata=()
    ) -> None:
        """
        Deletes a deidentify template.

        :param template_id: The ID of deidentify template to be deleted.
        :param organization_id: (Optional) The organization ID. Required to set this
            field if parent resource is an organization.
        :param project_id: (Optional) Google Cloud project ID where the
            DLP Instance exists. Only set this field if the parent resource is
            a project instead of an organization.
        :param retry: (Optional) A retry object used to retry requests.
            If None is specified, requests will not be retried.
        :param timeout: (Optional) The amount of time, in seconds, to wait for the request
            to complete. Note that if retry is specified, the timeout applies to each
            individual attempt.
        :param metadata: (Optional) Additional metadata that is provided to the method.
        """
        client = self.get_conn()

        if not template_id:
            raise AirflowException("Please provide the ID of deidentify template to be deleted.")

        # Handle project_id from connection configuration
        project_id = project_id or self.project_id

        if organization_id:
            name = DlpServiceClient.deidentify_template_path(organization_id, template_id)
        elif project_id:
            name = self._project_deidentify_template_path(project_id, template_id)
        else:
            raise AirflowException("Please provide either organization_id or project_id.")

        client.delete_deidentify_template(
            request=dict(
                name=name,
            ),
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

    @GoogleBaseHook.fallback_to_default_project_id
    def delete_dlp_job(
        self,
        dlp_job_id: str,
        project_id: str,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> None:
        """
        Deletes a long-running DLP job. This method indicates that the client is no longer
        interested in the DLP job result. The job will be cancelled if possible.

        :param dlp_job_id: The ID of the DLP job resource to be cancelled.
        :param project_id: (Optional) Google Cloud project ID where the
            DLP Instance exists. If set to None or missing, the default
            project_id from the Google Cloud connection is used.
        :param retry: (Optional) A retry object used to retry requests.
            If None is specified, requests will not be retried.
        :param timeout: (Optional) The amount of time, in seconds, to wait for the request
            to complete. Note that if retry is specified, the timeout applies to each
            individual attempt.
        :param metadata: (Optional) Additional metadata that is provided to the method.
        """
        client = self.get_conn()

        if not dlp_job_id:
            raise AirflowException("Please provide the ID of the DLP job resource to be cancelled.")

        name = DlpServiceClient.dlp_job_path(project_id, dlp_job_id)
        client.delete_dlp_job(
            request=dict(
                name=name,
            ),
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

    def delete_inspect_template(
        self,
        template_id: str,
        organization_id: str | None = None,
        project_id: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> None:
        """
        Deletes an inspect template.

        :param template_id: The ID of the inspect template to be deleted.
        :param organization_id: (Optional) The organization ID. Required to set this
            field if parent resource is an organization.
        :param project_id: (Optional) Google Cloud project ID where the
            DLP Instance exists. Only set this field if the parent resource is
            a project instead of an organization.
        :param retry: (Optional) A retry object used to retry requests.
            If None is specified, requests will not be retried.
        :param timeout: (Optional) The amount of time, in seconds, to wait for the request
            to complete. Note that if retry is specified, the timeout applies to each
            individual attempt.
        :param metadata: (Optional) Additional metadata that is provided to the method.
        """
        client = self.get_conn()

        if not template_id:
            raise AirflowException("Please provide the ID of the inspect template to be deleted.")

        # Handle project_id from connection configuration
        project_id = project_id or self.project_id

        if organization_id:
            name = DlpServiceClient.inspect_template_path(organization_id, template_id)
        elif project_id:
            name = self._project_inspect_template_path(project_id, template_id)
        else:
            raise AirflowException("Please provide either organization_id or project_id.")

        client.delete_inspect_template(
            request=dict(
                name=name,
            ),
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

    @GoogleBaseHook.fallback_to_default_project_id
    def delete_job_trigger(
        self,
        job_trigger_id: str,
        project_id: str,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> None:
        """
        Deletes a job trigger.

        :param job_trigger_id: The ID of the DLP job trigger to be deleted.
        :param project_id: (Optional) Google Cloud project ID where the
            DLP Instance exists. If set to None or missing, the default
            project_id from the Google Cloud connection is used.
        :param retry: (Optional) A retry object used to retry requests.
            If None is specified, requests will not be retried.
        :param timeout: (Optional) The amount of time, in seconds, to wait for the request
            to complete. Note that if retry is specified, the timeout applies to each
            individual attempt.
        :param metadata: (Optional) Additional metadata that is provided to the method.
        """
        client = self.get_conn()

        if not job_trigger_id:
            raise AirflowException("Please provide the ID of the DLP job trigger to be deleted.")

        name = DlpServiceClient.job_trigger_path(project_id, job_trigger_id)
        client.delete_job_trigger(
            request=dict(
                name=name,
            ),
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

    def delete_stored_info_type(
        self,
        stored_info_type_id: str,
        organization_id: str | None = None,
        project_id: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> None:
        """
        Deletes a stored info type.

        :param stored_info_type_id: The ID of the stored info type to be deleted.
        :param organization_id: (Optional) The organization ID. Required to set this
            field if parent resource is an organization.
        :param project_id: (Optional) Google Cloud project ID where the
            DLP Instance exists. Only set this field if the parent resource is
            a project instead of an organization.
        :param retry: (Optional) A retry object used to retry requests.
            If None is specified, requests will not be retried.
        :param timeout: (Optional) The amount of time, in seconds, to wait for the request
            to complete. Note that if retry is specified, the timeout applies to each
            individual attempt.
        :param metadata: (Optional) Additional metadata that is provided to the method.
        """
        client = self.get_conn()

        if not stored_info_type_id:
            raise AirflowException("Please provide the ID of the stored info type to be deleted.")

        # Handle project_id from connection configuration
        project_id = project_id or self.project_id

        if organization_id:
            name = DlpServiceClient.stored_info_type_path(organization_id, stored_info_type_id)
        elif project_id:
            name = self._project_stored_info_type_path(project_id, stored_info_type_id)
        else:
            raise AirflowException("Please provide either organization_id or project_id.")

        client.delete_stored_info_type(
            request=dict(
                name=name,
            ),
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

    def get_deidentify_template(
        self,
        template_id: str,
        organization_id: str | None = None,
        project_id: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> DeidentifyTemplate:
        """
        Gets a deidentify template.

        :param template_id: The ID of deidentify template to be read.
        :param organization_id: (Optional) The organization ID. Required to set this
            field if parent resource is an organization.
        :param project_id: (Optional) Google Cloud project ID where the
            DLP Instance exists. Only set this field if the parent resource is
            a project instead of an organization.
        :param retry: (Optional) A retry object used to retry requests.
            If None is specified, requests will not be retried.
        :param timeout: (Optional) The amount of time, in seconds, to wait for the request
            to complete. Note that if retry is specified, the timeout applies to each
            individual attempt.
        :param metadata: (Optional) Additional metadata that is provided to the method.
        """
        client = self.get_conn()

        if not template_id:
            raise AirflowException("Please provide the ID of the deidentify template to be read.")

        # Handle project_id from connection configuration
        project_id = project_id or self.project_id

        if organization_id:
            name = DlpServiceClient.deidentify_template_path(organization_id, template_id)
        elif project_id:
            name = self._project_deidentify_template_path(project_id, template_id)
        else:
            raise AirflowException("Please provide either organization_id or project_id.")

        return client.get_deidentify_template(
            request=dict(
                name=name,
            ),
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

    @GoogleBaseHook.fallback_to_default_project_id
    def get_dlp_job(
        self,
        dlp_job_id: str,
        project_id: str,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> DlpJob:
        """
        Gets the latest state of a long-running Dlp Job.

        :param dlp_job_id: The ID of the DLP job resource to be read.
        :param project_id: (Optional) Google Cloud project ID where the
            DLP Instance exists. If set to None or missing, the default
            project_id from the Google Cloud connection is used.
        :param retry: (Optional) A retry object used to retry requests.
            If None is specified, requests will not be retried.
        :param timeout: (Optional) The amount of time, in seconds, to wait for the request
            to complete. Note that if retry is specified, the timeout applies to each
            individual attempt.
        :param metadata: (Optional) Additional metadata that is provided to the method.
        """
        client = self.get_conn()

        if not dlp_job_id:
            raise AirflowException("Please provide the ID of the DLP job resource to be read.")

        name = DlpServiceClient.dlp_job_path(project_id, dlp_job_id)
        return client.get_dlp_job(
            request=dict(
                name=name,
            ),
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

    def get_inspect_template(
        self,
        template_id: str,
        organization_id: str | None = None,
        project_id: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> InspectTemplate:
        """
        Gets an inspect template.

        :param template_id: The ID of inspect template to be read.
        :param organization_id: (Optional) The organization ID. Required to set this
            field if parent resource is an organization.
        :param project_id: (Optional) Google Cloud project ID where the
            DLP Instance exists. Only set this field if the parent resource is
            a project instead of an organization.
        :param retry: (Optional) A retry object used to retry requests.
            If None is specified, requests will not be retried.
        :param timeout: (Optional) The amount of time, in seconds, to wait for the request
            to complete. Note that if retry is specified, the timeout applies to each
            individual attempt.
        :param metadata: (Optional) Additional metadata that is provided to the method.
        """
        client = self.get_conn()

        if not template_id:
            raise AirflowException("Please provide the ID of the inspect template to be read.")

        # Handle project_id from connection configuration
        project_id = project_id or self.project_id

        if organization_id:
            name = DlpServiceClient.inspect_template_path(organization_id, template_id)
        elif project_id:
            name = self._project_inspect_template_path(project_id, template_id)
        else:
            raise AirflowException("Please provide either organization_id or project_id.")

        return client.get_inspect_template(
            request=dict(
                name=name,
            ),
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

    @GoogleBaseHook.fallback_to_default_project_id
    def get_job_trigger(
        self,
        job_trigger_id: str,
        project_id: str,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> JobTrigger:
        """
        Gets a DLP job trigger.

        :param job_trigger_id: The ID of the DLP job trigger to be read.
        :param project_id: (Optional) Google Cloud project ID where the
            DLP Instance exists. If set to None or missing, the default
            project_id from the Google Cloud connection is used.
        :param retry: (Optional) A retry object used to retry requests.
            If None is specified, requests will not be retried.
        :param timeout: (Optional) The amount of time, in seconds, to wait for the request
            to complete. Note that if retry is specified, the timeout applies to each
            individual attempt.
        :param metadata: (Optional) Additional metadata that is provided to the method.
        """
        client = self.get_conn()

        if not job_trigger_id:
            raise AirflowException("Please provide the ID of the DLP job trigger to be read.")

        name = DlpServiceClient.job_trigger_path(project_id, job_trigger_id)
        return client.get_job_trigger(
            request=dict(
                name=name,
            ),
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

    def get_stored_info_type(
        self,
        stored_info_type_id: str,
        organization_id: str | None = None,
        project_id: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> StoredInfoType:
        """
        Gets a stored info type.

        :param stored_info_type_id: The ID of the stored info type to be read.
        :param organization_id: (Optional) The organization ID. Required to set this
            field if parent resource is an organization.
        :param project_id: (Optional) Google Cloud project ID where the
            DLP Instance exists. Only set this field if the parent resource is
            a project instead of an organization.
        :param retry: (Optional) A retry object used to retry requests.
            If None is specified, requests will not be retried.
        :param timeout: (Optional) The amount of time, in seconds, to wait for the request
            to complete. Note that if retry is specified, the timeout applies to each
            individual attempt.
        :param metadata: (Optional) Additional metadata that is provided to the method.
        """
        client = self.get_conn()

        if not stored_info_type_id:
            raise AirflowException("Please provide the ID of the stored info type to be read.")

        # Handle project_id from connection configuration
        project_id = project_id or self.project_id

        if organization_id:
            name = DlpServiceClient.stored_info_type_path(organization_id, stored_info_type_id)
        elif project_id:
            name = self._project_stored_info_type_path(project_id, stored_info_type_id)
        else:
            raise AirflowException("Please provide either organization_id or project_id.")

        return client.get_stored_info_type(
            request=dict(
                name=name,
            ),
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

    @GoogleBaseHook.fallback_to_default_project_id
    def inspect_content(
        self,
        project_id: str,
        inspect_config: dict | InspectConfig | None = None,
        item: dict | ContentItem | None = None,
        inspect_template_name: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> InspectContentResponse:
        """
        Finds potentially sensitive info in content. This method has limits on input size,
        processing time, and output size.

        :param project_id: (Optional) Google Cloud project ID where the
            DLP Instance exists. If set to None or missing, the default
            project_id from the Google Cloud connection is used.
        :param inspect_config: (Optional) Configuration for the inspector. Items specified
            here will override the template referenced by the inspect_template_name argument.
        :param item: (Optional) The item to de-identify. Will be treated as text.
        :param inspect_template_name: (Optional) Optional template to use. Any configuration
            directly specified in inspect_config will override those set in the template.
        :param retry: (Optional) A retry object used to retry requests.
            If None is specified, requests will not be retried.
        :param timeout: (Optional) The amount of time, in seconds, to wait for the request
            to complete. Note that if retry is specified, the timeout applies to each
            individual attempt.
        :param metadata: (Optional) Additional metadata that is provided to the method.
        """
        client = self.get_conn()

        parent = DlpServiceClient.common_project_path(project_id)
        return client.inspect_content(
            request=dict(
                parent=parent,
                inspect_config=inspect_config,
                item=item,
                inspect_template_name=inspect_template_name,
            ),
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

    def list_deidentify_templates(
        self,
        organization_id: str | None = None,
        project_id: str | None = None,
        page_size: int | None = None,
        order_by: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> list[DeidentifyTemplate]:
        """
        Lists deidentify templates.

        :param organization_id: (Optional) The organization ID. Required to set this
            field if parent resource is an organization.
        :param project_id: (Optional) Google Cloud project ID where the
            DLP Instance exists. Only set this field if the parent resource is
            a project instead of an organization.
        :param page_size: (Optional) The maximum number of resources contained in the
            underlying API response.
        :param order_by: (Optional) Optional comma separated list of fields to order by,
            followed by asc or desc postfix.
        :param retry: (Optional) A retry object used to retry requests.
            If None is specified, requests will not be retried.
        :param timeout: (Optional) The amount of time, in seconds, to wait for the request
            to complete. Note that if retry is specified, the timeout applies to each
            individual attempt.
        :param metadata: (Optional) Additional metadata that is provided to the method.
        """
        client = self.get_conn()

        # Handle project_id from connection configuration
        project_id = project_id or self.project_id

        if organization_id:
            parent = DlpServiceClient.common_organization_path(organization_id)
        elif project_id:
            parent = DlpServiceClient.common_project_path(project_id)
        else:
            raise AirflowException("Please provide either organization_id or project_id.")

        results = client.list_deidentify_templates(
            request=dict(
                parent=parent,
                page_size=page_size,
                order_by=order_by,
            ),
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

        return list(results)

    @GoogleBaseHook.fallback_to_default_project_id
    def list_dlp_jobs(
        self,
        project_id: str,
        results_filter: str | None = None,
        page_size: int | None = None,
        job_type: str | None = None,
        order_by: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> list[DlpJob]:
        """
        Lists DLP jobs that match the specified filter in the request.

        :param project_id: (Optional) Google Cloud project ID where the
            DLP Instance exists. If set to None or missing, the default
            project_id from the Google Cloud connection is used.
        :param results_filter: (Optional) Filter used to specify a subset of results.
        :param page_size: (Optional) The maximum number of resources contained in the
            underlying API response.
        :param job_type: (Optional) The type of job.
        :param order_by: (Optional) Optional comma separated list of fields to order by,
            followed by asc or desc postfix.
        :param retry: (Optional) A retry object used to retry requests.
            If None is specified, requests will not be retried.
        :param timeout: (Optional) The amount of time, in seconds, to wait for the request
            to complete. Note that if retry is specified, the timeout applies to each
            individual attempt.
        :param metadata: (Optional) Additional metadata that is provided to the method.
        """
        client = self.get_conn()

        parent = DlpServiceClient.common_project_path(project_id)
        results = client.list_dlp_jobs(
            request=dict(
                parent=parent,
                filter=results_filter,
                page_size=page_size,
                type_=job_type,
                order_by=order_by,
            ),
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return list(results)

    def list_info_types(
        self,
        language_code: str | None = None,
        results_filter: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> ListInfoTypesResponse:
        """
        Returns a list of the sensitive information types that the DLP API supports.

        :param language_code: (Optional) Optional BCP-47 language code for localized info
            type friendly names. If omitted, or if localized strings are not available,
            en-US strings will be returned.
        :param results_filter: (Optional) Filter used to specify a subset of results.
        :param retry: (Optional) A retry object used to retry requests.
            If None is specified, requests will not be retried.
        :param timeout: (Optional) The amount of time, in seconds, to wait for the request
            to complete. Note that if retry is specified, the timeout applies to each
            individual attempt.
        :param metadata: (Optional) Additional metadata that is provided to the method.
        """
        client = self.get_conn()

        return client.list_info_types(
            request=dict(
                language_code=language_code,
                filter=results_filter,
            ),
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

    def list_inspect_templates(
        self,
        organization_id: str | None = None,
        project_id: str | None = None,
        page_size: int | None = None,
        order_by: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> list[InspectTemplate]:
        """
        Lists inspect templates.

        :param organization_id: (Optional) The organization ID. Required to set this
            field if parent resource is an organization.
        :param project_id: (Optional) Google Cloud project ID where the
            DLP Instance exists. Only set this field if the parent resource is
            a project instead of an organization.
        :param page_size: (Optional) The maximum number of resources contained in the
            underlying API response.
        :param order_by: (Optional) Optional comma separated list of fields to order by,
            followed by asc or desc postfix.
        :param retry: (Optional) A retry object used to retry requests.
            If None is specified, requests will not be retried.
        :param timeout: (Optional) The amount of time, in seconds, to wait for the request
            to complete. Note that if retry is specified, the timeout applies to each
            individual attempt.
        :param metadata: (Optional) Additional metadata that is provided to the method.
        """
        client = self.get_conn()

        # Handle project_id from connection configuration
        project_id = project_id or self.project_id

        if organization_id:
            parent = DlpServiceClient.common_organization_path(organization_id)
        elif project_id:
            parent = DlpServiceClient.common_project_path(project_id)
        else:
            raise AirflowException("Please provide either organization_id or project_id.")

        results = client.list_inspect_templates(
            request=dict(
                parent=parent,
                page_size=page_size,
                order_by=order_by,
            ),
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return list(results)

    @GoogleBaseHook.fallback_to_default_project_id
    def list_job_triggers(
        self,
        project_id: str,
        page_size: int | None = None,
        order_by: str | None = None,
        results_filter: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> list[JobTrigger]:
        """
        Lists job triggers.

        :param project_id: (Optional) Google Cloud project ID where the
            DLP Instance exists. If set to None or missing, the default
            project_id from the Google Cloud connection is used.
        :param page_size: (Optional) The maximum number of resources contained in the
            underlying API response.
        :param order_by: (Optional) Optional comma separated list of fields to order by,
            followed by asc or desc postfix.
        :param results_filter: (Optional) Filter used to specify a subset of results.
        :param retry: (Optional) A retry object used to retry requests.
            If None is specified, requests will not be retried.
        :param timeout: (Optional) The amount of time, in seconds, to wait for the request
            to complete. Note that if retry is specified, the timeout applies to each
            individual attempt.
        :param metadata: (Optional) Additional metadata that is provided to the method.
        """
        client = self.get_conn()

        parent = DlpServiceClient.common_project_path(project_id)
        results = client.list_job_triggers(
            request=dict(
                parent=parent,
                page_size=page_size,
                order_by=order_by,
                filter=results_filter,
            ),
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return list(results)

    def list_stored_info_types(
        self,
        organization_id: str | None = None,
        project_id: str | None = None,
        page_size: int | None = None,
        order_by: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> list[StoredInfoType]:
        """
        Lists stored info types.

        :param organization_id: (Optional) The organization ID. Required to set this
            field if parent resource is an organization.
        :param project_id: (Optional) Google Cloud project ID where the
            DLP Instance exists. Only set this field if the parent resource is
            a project instead of an organization.
        :param page_size: (Optional) The maximum number of resources contained in the
            underlying API response.
        :param order_by: (Optional) Optional comma separated list of fields to order by,
            followed by asc or desc postfix.
        :param retry: (Optional) A retry object used to retry requests.
            If None is specified, requests will not be retried.
        :param timeout: (Optional) The amount of time, in seconds, to wait for the request
            to complete. Note that if retry is specified, the timeout applies to each
            individual attempt.
        :param metadata: (Optional) Additional metadata that is provided to the method.
        """
        client = self.get_conn()

        # Handle project_id from connection configuration
        project_id = project_id or self.project_id

        if organization_id:
            parent = DlpServiceClient.common_organization_path(organization_id)
        elif project_id:
            parent = DlpServiceClient.common_project_path(project_id)
        else:
            raise AirflowException("Please provide either organization_id or project_id.")

        results = client.list_stored_info_types(
            request=dict(
                parent=parent,
                page_size=page_size,
                order_by=order_by,
            ),
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return list(results)

    @GoogleBaseHook.fallback_to_default_project_id
    def redact_image(
        self,
        project_id: str,
        inspect_config: dict | InspectConfig | None = None,
        image_redaction_configs: None | (list[dict] | list[RedactImageRequest.ImageRedactionConfig]) = None,
        include_findings: bool | None = None,
        byte_item: dict | ByteContentItem | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> RedactImageResponse:
        """
        Redacts potentially sensitive info from an image. This method has limits on
        input size, processing time, and output size.

        :param project_id: (Optional) Google Cloud project ID where the
            DLP Instance exists. If set to None or missing, the default
            project_id from the Google Cloud connection is used.
        :param inspect_config: (Optional) Configuration for the inspector. Items specified
            here will override the template referenced by the inspect_template_name argument.
        :param image_redaction_configs: (Optional) The configuration for specifying what
            content to redact from images.
            list[google.cloud.dlp_v2.types.RedactImageRequest.ImageRedactionConfig]
        :param include_findings: (Optional) Whether the response should include findings
            along with the redacted image.
        :param byte_item: (Optional) The content must be PNG, JPEG, SVG or BMP.
        :param retry: (Optional) A retry object used to retry requests.
            If None is specified, requests will not be retried.
        :param timeout: (Optional) The amount of time, in seconds, to wait for the request
            to complete. Note that if retry is specified, the timeout applies to each
            individual attempt.
        :param metadata: (Optional) Additional metadata that is provided to the method.
        """
        client = self.get_conn()

        parent = DlpServiceClient.common_project_path(project_id)
        return client.redact_image(
            request=dict(
                parent=parent,
                inspect_config=inspect_config,
                image_redaction_configs=image_redaction_configs,
                include_findings=include_findings,
                byte_item=byte_item,
            ),
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

    @GoogleBaseHook.fallback_to_default_project_id
    def reidentify_content(
        self,
        project_id: str,
        reidentify_config: dict | DeidentifyConfig | None = None,
        inspect_config: dict | InspectConfig | None = None,
        item: dict | ContentItem | None = None,
        inspect_template_name: str | None = None,
        reidentify_template_name: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> ReidentifyContentResponse:
        """
        Re-identifies content that has been de-identified.

        :param project_id: (Optional) Google Cloud project ID where the
            DLP Instance exists. If set to None or missing, the default
            project_id from the Google Cloud connection is used.
        :param reidentify_config: (Optional) Configuration for the re-identification of
            the content item.
        :param inspect_config: (Optional) Configuration for the inspector.
        :param item: (Optional) The item to re-identify. Will be treated as text.
        :param inspect_template_name: (Optional) Optional template to use. Any configuration
            directly specified in inspect_config will override those set in the template.
        :param reidentify_template_name: (Optional) Optional template to use. References an
            instance of deidentify template. Any configuration directly specified in
            reidentify_config or inspect_config will override those set in the template.
        :param retry: (Optional) A retry object used to retry requests.
            If None is specified, requests will not be retried.
        :param timeout: (Optional) The amount of time, in seconds, to wait for the request
            to complete. Note that if retry is specified, the timeout applies to each
            individual attempt.
        :param metadata: (Optional) Additional metadata that is provided to the method.
        """
        client = self.get_conn()

        parent = DlpServiceClient.common_project_path(project_id)
        return client.reidentify_content(
            request=dict(
                parent=parent,
                reidentify_config=reidentify_config,
                inspect_config=inspect_config,
                item=item,
                inspect_template_name=inspect_template_name,
                reidentify_template_name=reidentify_template_name,
            ),
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

    def update_deidentify_template(
        self,
        template_id: str,
        organization_id: str | None = None,
        project_id: str | None = None,
        deidentify_template: dict | DeidentifyTemplate | None = None,
        update_mask: dict | FieldMask | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> DeidentifyTemplate:
        """
        Updates the deidentify template.

        :param template_id: The ID of deidentify template to be updated.
        :param organization_id: (Optional) The organization ID. Required to set this
            field if parent resource is an organization.
        :param project_id: (Optional) Google Cloud project ID where the
            DLP Instance exists. Only set this field if the parent resource is
            a project instead of an organization.
        :param deidentify_template: New deidentify template value.
        :param update_mask: Mask to control which fields get updated.
        :param retry: (Optional) A retry object used to retry requests.
            If None is specified, requests will not be retried.
        :param timeout: (Optional) The amount of time, in seconds, to wait for the request
            to complete. Note that if retry is specified, the timeout applies to each
            individual attempt.
        :param metadata: (Optional) Additional metadata that is provided to the method.
        """
        client = self.get_conn()

        if not template_id:
            raise AirflowException("Please provide the ID of deidentify template to be updated.")

        # Handle project_id from connection configuration
        project_id = project_id or self.project_id

        if organization_id:
            name = DlpServiceClient.deidentify_template_path(organization_id, template_id)
        elif project_id:
            name = self._project_deidentify_template_path(project_id, template_id)
        else:
            raise AirflowException("Please provide either organization_id or project_id.")

        return client.update_deidentify_template(
            request=dict(
                name=name,
                deidentify_template=deidentify_template,
                update_mask=update_mask,
            ),
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

    def update_inspect_template(
        self,
        template_id: str,
        organization_id: str | None = None,
        project_id: str | None = None,
        inspect_template: dict | InspectTemplate | None = None,
        update_mask: dict | FieldMask | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> InspectTemplate:
        """
        Updates the inspect template.

        :param template_id: The ID of the inspect template to be updated.
        :param organization_id: (Optional) The organization ID. Required to set this
            field if parent resource is an organization.
        :param project_id: (Optional) Google Cloud project ID where the
            DLP Instance exists. Only set this field if the parent resource is
            a project instead of an organization.
        :param inspect_template: New inspect template value.
        :param update_mask: Mask to control which fields get updated.
        :param retry: (Optional) A retry object used to retry requests.
            If None is specified, requests will not be retried.
        :param timeout: (Optional) The amount of time, in seconds, to wait for the request
            to complete. Note that if retry is specified, the timeout applies to each
            individual attempt.
        :param metadata: (Optional) Additional metadata that is provided to the method.
        """
        client = self.get_conn()

        if not template_id:
            raise AirflowException("Please provide the ID of the inspect template to be updated.")
        # Handle project_id from connection configuration
        project_id = project_id or self.project_id

        if organization_id:
            name = DlpServiceClient.inspect_template_path(organization_id, template_id)
        elif project_id:
            name = self._project_inspect_template_path(project_id, template_id)
        else:
            raise AirflowException("Please provide either organization_id or project_id.")

        return client.update_inspect_template(
            request=dict(
                name=name,
                inspect_template=inspect_template,
                update_mask=update_mask,
            ),
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

    @GoogleBaseHook.fallback_to_default_project_id
    def update_job_trigger(
        self,
        job_trigger_id: str,
        project_id: str,
        job_trigger: dict | JobTrigger | None = None,
        update_mask: dict | FieldMask | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> JobTrigger:
        """
        Updates a job trigger.

        :param job_trigger_id: The ID of the DLP job trigger to be updated.
        :param project_id: (Optional) Google Cloud project ID where the
            DLP Instance exists. If set to None or missing, the default
            project_id from the Google Cloud connection is used.
        :param job_trigger: New job trigger value.
        :param update_mask: Mask to control which fields get updated.
        :param retry: (Optional) A retry object used to retry requests.
            If None is specified, requests will not be retried.
        :param timeout: (Optional) The amount of time, in seconds, to wait for the request
            to complete. Note that if retry is specified, the timeout applies to each
            individual attempt.
        :param metadata: (Optional) Additional metadata that is provided to the method.
        """
        client = self.get_conn()

        if isinstance(job_trigger, dict):
            job_trigger = JobTrigger(**job_trigger)

        if isinstance(update_mask, dict):
            update_mask = FieldMask(**update_mask)

        if not job_trigger_id:
            raise AirflowException("Please provide the ID of the DLP job trigger to be updated.")

        name = DlpServiceClient.job_trigger_path(project_id, job_trigger_id)
        return client.update_job_trigger(
            name=name,
            job_trigger=job_trigger,
            update_mask=update_mask,
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

    def update_stored_info_type(
        self,
        stored_info_type_id: str,
        organization_id: str | None = None,
        project_id: str | None = None,
        config: dict | StoredInfoTypeConfig | None = None,
        update_mask: dict | FieldMask | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> StoredInfoType:
        """
        Updates the stored info type by creating a new version.

        :param stored_info_type_id: The ID of the stored info type to be updated.
        :param organization_id: (Optional) The organization ID. Required to set this
            field if parent resource is an organization.
        :param project_id: (Optional) Google Cloud project ID where the
            DLP Instance exists. Only set this field if the parent resource is
            a project instead of an organization.
        :param config: Updated configuration for the stored info type. If not provided, a new
            version of the stored info type will be created with the existing configuration.
        :param update_mask: Mask to control which fields get updated.
        :param retry: (Optional) A retry object used to retry requests.
            If None is specified, requests will not be retried.
        :param timeout: (Optional) The amount of time, in seconds, to wait for the request
            to complete. Note that if retry is specified, the timeout applies to each
            individual attempt.
        :param metadata: (Optional) Additional metadata that is provided to the method.
        """
        client = self.get_conn()

        if not stored_info_type_id:
            raise AirflowException("Please provide the ID of the stored info type to be updated.")

        # Handle project_id from connection configuration
        project_id = project_id or self.project_id

        if organization_id:
            name = DlpServiceClient.stored_info_type_path(organization_id, stored_info_type_id)
        elif project_id:
            name = self._project_stored_info_type_path(project_id, stored_info_type_id)
        else:
            raise AirflowException("Please provide either organization_id or project_id.")

        return client.update_stored_info_type(
            request=dict(
                name=name,
                config=config,
                update_mask=update_mask,
            ),
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
