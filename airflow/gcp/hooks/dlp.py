# -*- coding: utf-8 -*-
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
which allows you to connect to GCP Cloud DLP service.
"""

import re
import time
from typing import List, Optional, Sequence, Tuple, Union

from google.api_core.retry import Retry
from google.cloud.dlp_v2 import DlpServiceClient
from google.cloud.dlp_v2.types import (
    ByteContentItem, ContentItem, DeidentifyConfig, DeidentifyContentResponse, DeidentifyTemplate, DlpJob,
    FieldMask, InspectConfig, InspectContentResponse, InspectJobConfig, InspectTemplate, JobTrigger,
    ListInfoTypesResponse, RedactImageRequest, RedactImageResponse, ReidentifyContentResponse,
    RiskAnalysisJobConfig, StoredInfoType, StoredInfoTypeConfig,
)

from airflow import AirflowException
from airflow.gcp.hooks.base import CloudBaseHook

DLP_JOB_PATH_PATTERN = "^projects/[^/]+/dlpJobs/(?P<job>.*?)$"
# Time to sleep between active checks of the operation results
TIME_TO_SLEEP_IN_SECONDS = 1


# pylint: disable=R0904, C0302
class CloudDLPHook(CloudBaseHook):
    """
    Hook for Google Cloud Data Loss Prevention (DLP) APIs.
    Cloud DLP allows clients to detect the presence of Personally Identifiable
    Information (PII) and other privacy-sensitive data in user-supplied,
    unstructured data streams, like text blocks or images. The service also
    includes methods for sensitive data redaction and scheduling of data scans
    on Google Cloud Platform based data sets.

    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :type gcp_conn_id: str
    :param delegate_to: The account to impersonate, if any.
        For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :type delegate_to: str
    """

    def __init__(self, gcp_conn_id: str = "google_cloud_default", delegate_to: Optional[str] = None) -> None:
        super().__init__(gcp_conn_id, delegate_to)
        self._client = None

    def get_conn(self) -> DlpServiceClient:
        """
        Provides a client for interacting with the Cloud DLP API.

        :return: GCP Cloud DLP API Client
        :rtype: google.cloud.dlp_v2.DlpServiceClient
        """
        if not self._client:
            self._client = DlpServiceClient(credentials=self._get_credentials(), client_info=self.client_info)
        return self._client

    @CloudBaseHook.catch_http_exception
    @CloudBaseHook.fallback_to_default_project_id
    def cancel_dlp_job(
        self,
        dlp_job_id: str,
        project_id: Optional[str] = None,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
    ) -> None:
        """
        Starts asynchronous cancellation on a long-running DLP job.

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
        :type metadata: Sequence[Tuple[str, str]]
        """

        client = self.get_conn()

        if not dlp_job_id:
            raise AirflowException("Please provide the ID of the DLP job resource to be cancelled.")

        name = DlpServiceClient.dlp_job_path(project_id, dlp_job_id)
        client.cancel_dlp_job(name=name, retry=retry, timeout=timeout, metadata=metadata)

    @CloudBaseHook.catch_http_exception
    def create_deidentify_template(
        self,
        organization_id: Optional[str] = None,
        project_id: Optional[str] = None,
        deidentify_template: Optional[Union[dict, DeidentifyTemplate]] = None,
        template_id: Optional[str] = None,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
    ) -> DeidentifyTemplate:
        """
        Creates a deidentify template for re-using frequently used configuration for
        de-identifying content, images, and storage.

        :param organization_id: (Optional) The organization ID. Required to set this
            field if parent resource is an organzation.
        :type organization_id: str
        :param project_id: (Optional) Google Cloud Platform project ID where the
            DLP Instance exists. Only set this field if the parent resource is
            a project instead of an organzation.
        :type project_id: str
        :param deidentify_template: (Optional) The deidentify template to create.
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
        :type metadata: Sequence[Tuple[str, str]]
        :rtype: google.cloud.dlp_v2.types.DeidentifyTemplate
        """

        client = self.get_conn()
        # Handle project_id from connection configuration
        project_id = project_id or self.project_id

        if organization_id:
            parent = DlpServiceClient.organization_path(organization_id)
        elif project_id:
            parent = DlpServiceClient.project_path(project_id)
        else:
            raise AirflowException("Please provide either organization_id or project_id.")

        return client.create_deidentify_template(
            parent=parent,
            deidentify_template=deidentify_template,
            template_id=template_id,
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

    @CloudBaseHook.catch_http_exception
    @CloudBaseHook.fallback_to_default_project_id
    def create_dlp_job(
        self,
        project_id: Optional[str] = None,
        inspect_job: Optional[Union[dict, InspectJobConfig]] = None,
        risk_job: Optional[Union[dict, RiskAnalysisJobConfig]] = None,
        job_id: Optional[str] = None,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
        wait_until_finished: bool = True,
    ) -> DlpJob:
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
        :type metadata: Sequence[Tuple[str, str]]
        :param wait_until_finished: (Optional) If true, it will keep polling the job state
            until it is set to DONE.
        :type wait_until_finished: bool
        :rtype: google.cloud.dlp_v2.types.DlpJob
        """

        client = self.get_conn()

        parent = DlpServiceClient.project_path(project_id)
        job = client.create_dlp_job(
            parent=parent,
            inspect_job=inspect_job,
            risk_job=risk_job,
            job_id=job_id,
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
                raise AirflowException("Unable to retrieve DLP job's ID from {}.".format(job.name))

        while wait_until_finished:
            job = self.get_dlp_job(dlp_job_id=job_name, project_id=project_id)

            self.log.info("DLP job {} state: {}.".format(job.name, DlpJob.JobState.Name(job.state)))

            if job.state == DlpJob.JobState.DONE:
                return job
            elif job.state in [
                DlpJob.JobState.PENDING,
                DlpJob.JobState.RUNNING,
                DlpJob.JobState.JOB_STATE_UNSPECIFIED,
            ]:
                time.sleep(TIME_TO_SLEEP_IN_SECONDS)
            else:
                raise AirflowException(
                    "Stopped polling DLP job state. DLP job {} state: {}.".format(
                        job.name, DlpJob.JobState.Name(job.state)
                    )
                )
        return job

    @CloudBaseHook.catch_http_exception
    def create_inspect_template(
        self,
        organization_id: Optional[str] = None,
        project_id: Optional[str] = None,
        inspect_template: Optional[Union[dict, InspectTemplate]] = None,
        template_id: Optional[str] = None,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
    ) -> InspectTemplate:
        """
        Creates an inspect template for re-using frequently used configuration for
        inspecting content, images, and storage.

        :param organization_id: (Optional) The organization ID. Required to set this
            field if parent resource is an organzation.
        :type organization_id: str
        :param project_id: (Optional) Google Cloud Platform project ID where the
            DLP Instance exists. Only set this field if the parent resource is
            a project instead of an organzation.
        :type project_id: str
        :param inspect_template: (Optional) The inspect template to create.
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
        :type metadata: Sequence[Tuple[str, str]]
        :rtype: google.cloud.dlp_v2.types.InspectTemplate
        """

        client = self.get_conn()

        # Handle project_id from connection configuration
        project_id = project_id or self.project_id

        if organization_id:
            parent = DlpServiceClient.organization_path(organization_id)
        elif project_id:
            parent = DlpServiceClient.project_path(project_id)
        else:
            raise AirflowException("Please provide either organization_id or project_id.")

        return client.create_inspect_template(
            parent=parent,
            inspect_template=inspect_template,
            template_id=template_id,
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

    @CloudBaseHook.catch_http_exception
    @CloudBaseHook.fallback_to_default_project_id
    def create_job_trigger(
        self,
        project_id: Optional[str] = None,
        job_trigger: Optional[Union[dict, JobTrigger]] = None,
        trigger_id: Optional[str] = None,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
    ) -> JobTrigger:
        """
        Creates a job trigger to run DLP actions such as scanning storage for sensitive
        information on a set schedule.

        :param project_id: (Optional) Google Cloud Platform project ID where the
            DLP Instance exists. If set to None or missing, the default
            project_id from the GCP connection is used.
        :type project_id: str
        :param job_trigger: (Optional) The job trigger to create.
        :type job_trigger: dict or google.cloud.dlp_v2.types.JobTrigger
        :param trigger_id: (Optional) The job trigger ID.
        :type trigger_id: str
        :param retry: (Optional) A retry object used to retry requests.
            If None is specified, requests will not be retried.
        :type retry: google.api_core.retry.Retry
        :param timeout: (Optional) The amount of time, in seconds, to wait for the request
            to complete. Note that if retry is specified, the timeout applies to each
            individual attempt.
        :type timeout: float
        :param metadata: (Optional) Additional metadata that is provided to the method.
        :type metadata: Sequence[Tuple[str, str]]
        :rtype: google.cloud.dlp_v2.types.JobTrigger
        """

        client = self.get_conn()

        parent = DlpServiceClient.project_path(project_id)
        return client.create_job_trigger(
            parent=parent,
            job_trigger=job_trigger,
            trigger_id=trigger_id,
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

    @CloudBaseHook.catch_http_exception
    def create_stored_info_type(
        self,
        organization_id: Optional[str] = None,
        project_id: Optional[str] = None,
        config: Optional[Union[dict, StoredInfoTypeConfig]] = None,
        stored_info_type_id: Optional[str] = None,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
    ) -> StoredInfoType:
        """
        Creates a pre-built stored info type to be used for inspection.

        :param organization_id: (Optional) The organization ID. Required to set this
            field if parent resource is an organzation.
        :type organization_id: str
        :param project_id: (Optional) Google Cloud Platform project ID where the
            DLP Instance exists. Only set this field if the parent resource is
            a project instead of an organzation.
        :type project_id: str
        :param config: (Optional) The config for the stored info type.
        :type config: dict or google.cloud.dlp_v2.types.StoredInfoTypeConfig
        :param stored_info_type_id: (Optional) The stored info type ID.
        :type stored_info_type_id: str
        :param retry: (Optional) A retry object used to retry requests.
            If None is specified, requests will not be retried.
        :type retry: google.api_core.retry.Retry
        :param timeout: (Optional) The amount of time, in seconds, to wait for the request
            to complete. Note that if retry is specified, the timeout applies to each
            individual attempt.
        :type timeout: float
        :param metadata: (Optional) Additional metadata that is provided to the method.
        :type metadata: Sequence[Tuple[str, str]]
        :rtype: google.cloud.dlp_v2.types.StoredInfoType
        """

        client = self.get_conn()

        # Handle project_id from connection configuration
        project_id = project_id or self.project_id

        if organization_id:
            parent = DlpServiceClient.organization_path(organization_id)
        elif project_id:
            parent = DlpServiceClient.project_path(project_id)
        else:
            raise AirflowException("Please provide either organization_id or project_id.")

        return client.create_stored_info_type(
            parent=parent,
            config=config,
            stored_info_type_id=stored_info_type_id,
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

    @CloudBaseHook.catch_http_exception
    @CloudBaseHook.fallback_to_default_project_id
    def deidentify_content(
        self,
        project_id: Optional[str] = None,
        deidentify_config: Optional[Union[dict, DeidentifyConfig]] = None,
        inspect_config: Optional[Union[dict, InspectConfig]] = None,
        item: Optional[Union[dict, ContentItem]] = None,
        inspect_template_name: Optional[str] = None,
        deidentify_template_name: Optional[str] = None,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
    ) -> DeidentifyContentResponse:
        """
        De-identifies potentially sensitive info from a content item. This method has limits
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
        :type metadata: Sequence[Tuple[str, str]]
        :rtype: google.cloud.dlp_v2.types.DeidentifyContentResponse
        """

        client = self.get_conn()

        parent = DlpServiceClient.project_path(project_id)
        return client.deidentify_content(
            parent=parent,
            deidentify_config=deidentify_config,
            inspect_config=inspect_config,
            item=item,
            inspect_template_name=inspect_template_name,
            deidentify_template_name=deidentify_template_name,
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

    @CloudBaseHook.catch_http_exception
    def delete_deidentify_template(
        self, template_id, organization_id=None, project_id=None, retry=None, timeout=None, metadata=None
    ) -> None:
        """
        Deletes a deidentify template.

        :param template_id: The ID of deidentify template to be deleted.
        :type template_id: str
        :param organization_id: (Optional) The organization ID. Required to set this
            field if parent resource is an organzation.
        :type organization_id: str
        :param project_id: (Optional) Google Cloud Platform project ID where the
            DLP Instance exists. Only set this field if the parent resource is
            a project instead of an organzation.
        :type project_id: str
        :param retry: (Optional) A retry object used to retry requests.
            If None is specified, requests will not be retried.
        :type retry: google.api_core.retry.Retry
        :param timeout: (Optional) The amount of time, in seconds, to wait for the request
            to complete. Note that if retry is specified, the timeout applies to each
            individual attempt.
        :type timeout: float
        :param metadata: (Optional) Additional metadata that is provided to the method.
        :type metadata: Sequence[Tuple[str, str]]
        """

        client = self.get_conn()

        if not template_id:
            raise AirflowException("Please provide the ID of deidentify template to be deleted.")

        # Handle project_id from connection configuration
        project_id = project_id or self.project_id

        if organization_id:
            name = DlpServiceClient.organization_deidentify_template_path(organization_id, template_id)
        elif project_id:
            name = DlpServiceClient.project_deidentify_template_path(project_id, template_id)
        else:
            raise AirflowException("Please provide either organization_id or project_id.")

        client.delete_deidentify_template(name=name, retry=retry, timeout=timeout, metadata=metadata)

    @CloudBaseHook.catch_http_exception
    @CloudBaseHook.fallback_to_default_project_id
    def delete_dlp_job(
        self,
        dlp_job_id: str,
        project_id: Optional[str] = None,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
    ) -> None:
        """
        Deletes a long-running DLP job. This method indicates that the client is no longer
        interested in the DLP job result. The job will be cancelled if possible.

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
        :type metadata: Sequence[Tuple[str, str]]
        """

        client = self.get_conn()

        if not dlp_job_id:
            raise AirflowException("Please provide the ID of the DLP job resource to be cancelled.")

        name = DlpServiceClient.dlp_job_path(project_id, dlp_job_id)
        client.delete_dlp_job(name=name, retry=retry, timeout=timeout, metadata=metadata)

    @CloudBaseHook.catch_http_exception
    def delete_inspect_template(
        self,
        template_id: str,
        organization_id: Optional[str] = None,
        project_id: Optional[str] = None,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
    ) -> None:
        """
        Deletes an inspect template.

        :param template_id: The ID of the inspect template to be deleted.
        :type template_id: str
        :param organization_id: (Optional) The organization ID. Required to set this
            field if parent resource is an organzation.
        :type organization_id: str
        :param project_id: (Optional) Google Cloud Platform project ID where the
            DLP Instance exists. Only set this field if the parent resource is
            a project instead of an organzation.
        :type project_id: str
        :param retry: (Optional) A retry object used to retry requests.
            If None is specified, requests will not be retried.
        :type retry: google.api_core.retry.Retry
        :param timeout: (Optional) The amount of time, in seconds, to wait for the request
            to complete. Note that if retry is specified, the timeout applies to each
            individual attempt.
        :type timeout: float
        :param metadata: (Optional) Additional metadata that is provided to the method.
        :type metadata: Sequence[Tuple[str, str]]
        """

        client = self.get_conn()

        if not template_id:
            raise AirflowException("Please provide the ID of the inspect template to be deleted.")

        # Handle project_id from connection configuration
        project_id = project_id or self.project_id

        if organization_id:
            name = DlpServiceClient.organization_inspect_template_path(organization_id, template_id)
        elif project_id:
            name = DlpServiceClient.project_inspect_template_path(project_id, template_id)
        else:
            raise AirflowException("Please provide either organization_id or project_id.")

        client.delete_inspect_template(name=name, retry=retry, timeout=timeout, metadata=metadata)

    @CloudBaseHook.catch_http_exception
    @CloudBaseHook.fallback_to_default_project_id
    def delete_job_trigger(
        self,
        job_trigger_id: str,
        project_id: Optional[str] = None,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
    ) -> None:
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
        :type metadata: Sequence[Tuple[str, str]]
        """

        client = self.get_conn()

        if not job_trigger_id:
            raise AirflowException("Please provide the ID of the DLP job trigger to be deleted.")

        name = DlpServiceClient.project_job_trigger_path(project_id, job_trigger_id)
        client.delete_job_trigger(name=name, retry=retry, timeout=timeout, metadata=metadata)

    @CloudBaseHook.catch_http_exception
    def delete_stored_info_type(
        self,
        stored_info_type_id: str,
        organization_id: Optional[str] = None,
        project_id: Optional[str] = None,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
    ) -> None:
        """
        Deletes a stored info type.

        :param stored_info_type_id: The ID of the stored info type to be deleted.
        :type stored_info_type_id: str
        :param organization_id: (Optional) The organization ID. Required to set this
            field if parent resource is an organzation.
        :type organization_id: str
        :param project_id: (Optional) Google Cloud Platform project ID where the
            DLP Instance exists. Only set this field if the parent resource is
            a project instead of an organzation.
        :type project_id: str
        :param retry: (Optional) A retry object used to retry requests.
            If None is specified, requests will not be retried.
        :type retry: google.api_core.retry.Retry
        :param timeout: (Optional) The amount of time, in seconds, to wait for the request
            to complete. Note that if retry is specified, the timeout applies to each
            individual attempt.
        :type timeout: float
        :param metadata: (Optional) Additional metadata that is provided to the method.
        :type metadata: Sequence[Tuple[str, str]]
        """

        client = self.get_conn()

        if not stored_info_type_id:
            raise AirflowException("Please provide the ID of the stored info type to be deleted.")

        # Handle project_id from connection configuration
        project_id = project_id or self.project_id

        if organization_id:
            name = DlpServiceClient.organization_stored_info_type_path(organization_id, stored_info_type_id)
        elif project_id:
            name = DlpServiceClient.project_stored_info_type_path(project_id, stored_info_type_id)
        else:
            raise AirflowException("Please provide either organization_id or project_id.")

        client.delete_stored_info_type(name=name, retry=retry, timeout=timeout, metadata=metadata)

    @CloudBaseHook.catch_http_exception
    def get_deidentify_template(
        self,
        template_id: str,
        organization_id: Optional[str] = None,
        project_id: Optional[str] = None,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
    ) -> DeidentifyTemplate:
        """
        Gets a deidentify template.

        :param template_id: The ID of deidentify template to be read.
        :type template_id: str
        :param organization_id: (Optional) The organization ID. Required to set this
            field if parent resource is an organzation.
        :type organization_id: str
        :param project_id: (Optional) Google Cloud Platform project ID where the
            DLP Instance exists. Only set this field if the parent resource is
            a project instead of an organzation.
        :type project_id: str
        :param retry: (Optional) A retry object used to retry requests.
            If None is specified, requests will not be retried.
        :type retry: google.api_core.retry.Retry
        :param timeout: (Optional) The amount of time, in seconds, to wait for the request
            to complete. Note that if retry is specified, the timeout applies to each
            individual attempt.
        :type timeout: float
        :param metadata: (Optional) Additional metadata that is provided to the method.
        :type metadata: Sequence[Tuple[str, str]]
        :rtype: google.cloud.dlp_v2.types.DeidentifyTemplate
        """

        client = self.get_conn()

        if not template_id:
            raise AirflowException("Please provide the ID of the deidentify template to be read.")

        # Handle project_id from connection configuration
        project_id = project_id or self.project_id

        if organization_id:
            name = DlpServiceClient.organization_deidentify_template_path(organization_id, template_id)
        elif project_id:
            name = DlpServiceClient.project_deidentify_template_path(project_id, template_id)
        else:
            raise AirflowException("Please provide either organization_id or project_id.")

        return client.get_deidentify_template(name=name, retry=retry, timeout=timeout, metadata=metadata)

    @CloudBaseHook.catch_http_exception
    @CloudBaseHook.fallback_to_default_project_id
    def get_dlp_job(
        self,
        dlp_job_id: str,
        project_id: Optional[str] = None,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
    ) -> DlpJob:
        """
        Gets the latest state of a long-running Dlp Job.

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
        :type metadata: Sequence[Tuple[str, str]]
        :rtype: google.cloud.dlp_v2.types.DlpJob
        """

        client = self.get_conn()

        if not dlp_job_id:
            raise AirflowException("Please provide the ID of the DLP job resource to be read.")

        name = DlpServiceClient.dlp_job_path(project_id, dlp_job_id)
        return client.get_dlp_job(name=name, retry=retry, timeout=timeout, metadata=metadata)

    @CloudBaseHook.catch_http_exception
    def get_inspect_template(
        self,
        template_id: str,
        organization_id: Optional[str] = None,
        project_id: Optional[str] = None,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
    ) -> InspectTemplate:
        """
        Gets an inspect template.

        :param template_id: The ID of inspect template to be read.
        :type template_id: str
        :param organization_id: (Optional) The organization ID. Required to set this
            field if parent resource is an organzation.
        :type organization_id: str
        :param project_id: (Optional) Google Cloud Platform project ID where the
            DLP Instance exists. Only set this field if the parent resource is
            a project instead of an organzation.
        :type project_id: str
        :param retry: (Optional) A retry object used to retry requests.
            If None is specified, requests will not be retried.
        :type retry: google.api_core.retry.Retry
        :param timeout: (Optional) The amount of time, in seconds, to wait for the request
            to complete. Note that if retry is specified, the timeout applies to each
            individual attempt.
        :type timeout: float
        :param metadata: (Optional) Additional metadata that is provided to the method.
        :type metadata: Sequence[Tuple[str, str]]
        :rtype: google.cloud.dlp_v2.types.InspectTemplate
        """

        client = self.get_conn()

        if not template_id:
            raise AirflowException("Please provide the ID of the inspect template to be read.")

        # Handle project_id from connection configuration
        project_id = project_id or self.project_id

        if organization_id:
            name = DlpServiceClient.organization_inspect_template_path(organization_id, template_id)
        elif project_id:
            name = DlpServiceClient.project_inspect_template_path(project_id, template_id)
        else:
            raise AirflowException("Please provide either organization_id or project_id.")

        return client.get_inspect_template(name=name, retry=retry, timeout=timeout, metadata=metadata)

    @CloudBaseHook.catch_http_exception
    @CloudBaseHook.fallback_to_default_project_id
    def get_job_trigger(
        self,
        job_trigger_id: str,
        project_id: Optional[str] = None,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
    ) -> JobTrigger:
        """
        Gets a DLP job trigger.

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
        :type metadata: Sequence[Tuple[str, str]]
        :rtype: google.cloud.dlp_v2.types.JobTrigger
        """

        client = self.get_conn()

        if not job_trigger_id:
            raise AirflowException("Please provide the ID of the DLP job trigger to be read.")

        name = DlpServiceClient.project_job_trigger_path(project_id, job_trigger_id)
        return client.get_job_trigger(name=name, retry=retry, timeout=timeout, metadata=metadata)

    @CloudBaseHook.catch_http_exception
    def get_stored_info_type(
        self,
        stored_info_type_id: str,
        organization_id: Optional[str] = None,
        project_id: Optional[str] = None,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
    ) -> StoredInfoType:
        """
        Gets a stored info type.

        :param stored_info_type_id: The ID of the stored info type to be read.
        :type stored_info_type_id: str
        :param organization_id: (Optional) The organization ID. Required to set this
            field if parent resource is an organzation.
        :type organization_id: str
        :param project_id: (Optional) Google Cloud Platform project ID where the
            DLP Instance exists. Only set this field if the parent resource is
            a project instead of an organzation.
        :type project_id: str
        :param retry: (Optional) A retry object used to retry requests.
            If None is specified, requests will not be retried.
        :type retry: google.api_core.retry.Retry
        :param timeout: (Optional) The amount of time, in seconds, to wait for the request
            to complete. Note that if retry is specified, the timeout applies to each
            individual attempt.
        :type timeout: float
        :param metadata: (Optional) Additional metadata that is provided to the method.
        :type metadata: Sequence[Tuple[str, str]]
        :rtype: google.cloud.dlp_v2.types.StoredInfoType
        """

        client = self.get_conn()

        if not stored_info_type_id:
            raise AirflowException("Please provide the ID of the stored info type to be read.")

        # Handle project_id from connection configuration
        project_id = project_id or self.project_id

        if organization_id:
            name = DlpServiceClient.organization_stored_info_type_path(organization_id, stored_info_type_id)
        elif project_id:
            name = DlpServiceClient.project_stored_info_type_path(project_id, stored_info_type_id)
        else:
            raise AirflowException("Please provide either organization_id or project_id.")

        return client.get_stored_info_type(name=name, retry=retry, timeout=timeout, metadata=metadata)

    @CloudBaseHook.catch_http_exception
    @CloudBaseHook.fallback_to_default_project_id
    def inspect_content(
        self,
        project_id: Optional[str] = None,
        inspect_config: Optional[Union[dict, InspectConfig]] = None,
        item: Optional[Union[dict, ContentItem]] = None,
        inspect_template_name: Optional[str] = None,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
    ) -> InspectContentResponse:
        """
        Finds potentially sensitive info in content. This method has limits on input size,
        processing time, and output size.

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
        :type metadata: Sequence[Tuple[str, str]]
        :rtype: google.cloud.dlp_v2.types.InspectContentResponse
        """

        client = self.get_conn()

        parent = DlpServiceClient.project_path(project_id)
        return client.inspect_content(
            parent=parent,
            inspect_config=inspect_config,
            item=item,
            inspect_template_name=inspect_template_name,
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

    @CloudBaseHook.catch_http_exception
    def list_deidentify_templates(
        self,
        organization_id: Optional[str] = None,
        project_id: Optional[str] = None,
        page_size: Optional[int] = None,
        order_by: Optional[str] = None,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
    ) -> List[DeidentifyTemplate]:
        """
        Lists deidentify templates.

        :param organization_id: (Optional) The organization ID. Required to set this
            field if parent resource is an organzation.
        :type organization_id: str
        :param project_id: (Optional) Google Cloud Platform project ID where the
            DLP Instance exists. Only set this field if the parent resource is
            a project instead of an organzation.
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
        :type metadata: Sequence[Tuple[str, str]]
        :rtype: List[google.cloud.dlp_v2.types.DeidentifyTemplate]
        """

        client = self.get_conn()

        # Handle project_id from connection configuration
        project_id = project_id or self.project_id

        if organization_id:
            parent = DlpServiceClient.organization_path(organization_id)
        elif project_id:
            parent = DlpServiceClient.project_path(project_id)
        else:
            raise AirflowException("Please provide either organization_id or project_id.")

        results = client.list_deidentify_templates(
            parent=parent,
            page_size=page_size,
            order_by=order_by,
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

        return list(results)

    @CloudBaseHook.catch_http_exception
    @CloudBaseHook.fallback_to_default_project_id
    def list_dlp_jobs(
        self,
        project_id: Optional[str] = None,
        results_filter: Optional[str] = None,
        page_size: Optional[int] = None,
        job_type: Optional[str] = None,
        order_by: Optional[str] = None,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
    ) -> List[DlpJob]:
        """
        Lists DLP jobs that match the specified filter in the request.

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
        :type metadata: Sequence[Tuple[str, str]]
        :rtype: List[google.cloud.dlp_v2.types.DlpJob]
        """

        client = self.get_conn()

        parent = DlpServiceClient.project_path(project_id)
        results = client.list_dlp_jobs(
            parent=parent,
            filter_=results_filter,
            page_size=page_size,
            type_=job_type,
            order_by=order_by,
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return list(results)

    @CloudBaseHook.catch_http_exception
    def list_info_types(
        self,
        language_code: Optional[str] = None,
        results_filter: Optional[str] = None,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
    ) -> ListInfoTypesResponse:
        """
        Returns a list of the sensitive information types that the DLP API supports.

        :param language_code: (Optional) Optional BCP-47 language code for localized info
            type friendly names. If omitted, or if localized strings are not available,
            en-US strings will be returned.
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
        :type metadata: Sequence[Tuple[str, str]]
        :rtype: google.cloud.dlp_v2.types.ListInfoTypesResponse
        """

        client = self.get_conn()

        return client.list_info_types(
            language_code=language_code,
            filter_=results_filter,
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

    @CloudBaseHook.catch_http_exception
    def list_inspect_templates(
        self,
        organization_id: Optional[str] = None,
        project_id: Optional[str] = None,
        page_size: Optional[int] = None,
        order_by: Optional[str] = None,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
    ) -> List[InspectTemplate]:
        """
        Lists inspect templates.

        :param organization_id: (Optional) The organization ID. Required to set this
            field if parent resource is an organzation.
        :type organization_id: str
        :param project_id: (Optional) Google Cloud Platform project ID where the
            DLP Instance exists. Only set this field if the parent resource is
            a project instead of an organzation.
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
        :type metadata: Sequence[Tuple[str, str]]
        :rtype: List[google.cloud.dlp_v2.types.InspectTemplate]
        """

        client = self.get_conn()

        # Handle project_id from connection configuration
        project_id = project_id or self.project_id

        if organization_id:
            parent = DlpServiceClient.organization_path(organization_id)
        elif project_id:
            parent = DlpServiceClient.project_path(project_id)
        else:
            raise AirflowException("Please provide either organization_id or project_id.")

        results = client.list_inspect_templates(
            parent=parent,
            page_size=page_size,
            order_by=order_by,
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return list(results)

    @CloudBaseHook.catch_http_exception
    @CloudBaseHook.fallback_to_default_project_id
    def list_job_triggers(
        self,
        project_id: Optional[str] = None,
        page_size: Optional[int] = None,
        order_by: Optional[str] = None,
        results_filter: Optional[str] = None,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
    ) -> List[JobTrigger]:
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
        :type metadata: Sequence[Tuple[str, str]]
        :rtype: List[google.cloud.dlp_v2.types.JobTrigger]
        """

        client = self.get_conn()

        parent = DlpServiceClient.project_path(project_id)
        results = client.list_job_triggers(
            parent=parent,
            page_size=page_size,
            order_by=order_by,
            filter_=results_filter,
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return list(results)

    @CloudBaseHook.catch_http_exception
    def list_stored_info_types(
        self,
        organization_id: Optional[str] = None,
        project_id: Optional[str] = None,
        page_size: Optional[int] = None,
        order_by: Optional[str] = None,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
    ) -> List[StoredInfoType]:
        """
        Lists stored info types.

        :param organization_id: (Optional) The organization ID. Required to set this
            field if parent resource is an organzation.
        :type organization_id: str
        :param project_id: (Optional) Google Cloud Platform project ID where the
            DLP Instance exists. Only set this field if the parent resource is
            a project instead of an organzation.
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
        :type metadata: Sequence[Tuple[str, str]]
        :rtype: List[google.cloud.dlp_v2.types.StoredInfoType]
        """

        client = self.get_conn()

        # Handle project_id from connection configuration
        project_id = project_id or self.project_id

        if organization_id:
            parent = DlpServiceClient.organization_path(organization_id)
        elif project_id:
            parent = DlpServiceClient.project_path(project_id)
        else:
            raise AirflowException("Please provide either organization_id or project_id.")

        results = client.list_stored_info_types(
            parent=parent,
            page_size=page_size,
            order_by=order_by,
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return list(results)

    @CloudBaseHook.catch_http_exception
    @CloudBaseHook.fallback_to_default_project_id
    def redact_image(
        self,
        project_id: Optional[str] = None,
        inspect_config: Optional[Union[dict, InspectConfig]] = None,
        image_redaction_configs: Optional[
            Union[List[dict], List[RedactImageRequest.ImageRedactionConfig]]
        ] = None,
        include_findings: Optional[bool] = None,
        byte_item: Optional[Union[dict, ByteContentItem]] = None,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
    ) -> RedactImageResponse:
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
        :type image_redaction_configs: List[dict] or
            List[google.cloud.dlp_v2.types.RedactImageRequest.ImageRedactionConfig]
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
        :type metadata: Sequence[Tuple[str, str]]
        :rtype: google.cloud.dlp_v2.types.RedactImageResponse
        """

        client = self.get_conn()

        parent = DlpServiceClient.project_path(project_id)
        return client.redact_image(
            parent=parent,
            inspect_config=inspect_config,
            image_redaction_configs=image_redaction_configs,
            include_findings=include_findings,
            byte_item=byte_item,
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

    @CloudBaseHook.catch_http_exception
    @CloudBaseHook.fallback_to_default_project_id
    def reidentify_content(
        self,
        project_id: Optional[str] = None,
        reidentify_config: Optional[Union[dict, DeidentifyConfig]] = None,
        inspect_config: Optional[Union[dict, InspectConfig]] = None,
        item: Optional[Union[dict, ContentItem]] = None,
        inspect_template_name: Optional[str] = None,
        reidentify_template_name: Optional[str] = None,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
    ) -> ReidentifyContentResponse:
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
            instance of deidentify template. Any configuration directly specified in
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
        :type metadata: Sequence[Tuple[str, str]]
        :rtype: google.cloud.dlp_v2.types.ReidentifyContentResponse
        """

        client = self.get_conn()

        parent = DlpServiceClient.project_path(project_id)
        return client.reidentify_content(
            parent=parent,
            reidentify_config=reidentify_config,
            inspect_config=inspect_config,
            item=item,
            inspect_template_name=inspect_template_name,
            reidentify_template_name=reidentify_template_name,
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

    @CloudBaseHook.catch_http_exception
    def update_deidentify_template(
        self,
        template_id: str,
        organization_id: Optional[str] = None,
        project_id: Optional[str] = None,
        deidentify_template: Optional[Union[dict, DeidentifyTemplate]] = None,
        update_mask: Optional[Union[dict, FieldMask]] = None,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
    ) -> DeidentifyTemplate:
        """
        Updates the deidentify template.

        :param template_id: The ID of deidentify template to be updated.
        :type template_id: str
        :param organization_id: (Optional) The organization ID. Required to set this
            field if parent resource is an organzation.
        :type organization_id: str
        :param project_id: (Optional) Google Cloud Platform project ID where the
            DLP Instance exists. Only set this field if the parent resource is
            a project instead of an organzation.
        :type project_id: str
        :param deidentify_template: New deidentify template value.
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
        :type metadata: Sequence[Tuple[str, str]]
        :rtype: google.cloud.dlp_v2.types.DeidentifyTemplate
        """

        client = self.get_conn()

        if not template_id:
            raise AirflowException("Please provide the ID of deidentify template to be updated.")

        # Handle project_id from connection configuration
        project_id = project_id or self.project_id

        if organization_id:
            name = DlpServiceClient.organization_deidentify_template_path(organization_id, template_id)
        elif project_id:
            name = DlpServiceClient.project_deidentify_template_path(project_id, template_id)
        else:
            raise AirflowException("Please provide either organization_id or project_id.")

        return client.update_deidentify_template(
            name=name,
            deidentify_template=deidentify_template,
            update_mask=update_mask,
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

    @CloudBaseHook.catch_http_exception
    def update_inspect_template(
        self,
        template_id: str,
        organization_id: Optional[str] = None,
        project_id: Optional[str] = None,
        inspect_template: Optional[Union[dict, InspectTemplate]] = None,
        update_mask: Optional[Union[dict, FieldMask]] = None,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
    ) -> InspectTemplate:
        """
        Updates the inspect template.

        :param template_id: The ID of the inspect template to be updated.
        :type template_id: str
        :param organization_id: (Optional) The organization ID. Required to set this
            field if parent resource is an organzation.
        :type organization_id: str
        :param project_id: (Optional) Google Cloud Platform project ID where the
            DLP Instance exists. Only set this field if the parent resource is
            a project instead of an organzation.
        :type project_id: str
        :param inspect_template: New inspect template value.
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
        :type metadata: Sequence[Tuple[str, str]]
        :rtype: google.cloud.dlp_v2.types.InspectTemplate
        """

        client = self.get_conn()

        if not template_id:
            raise AirflowException("Please provide the ID of the inspect template to be updated.")
        # Handle project_id from connection configuration
        project_id = project_id or self.project_id

        if organization_id:
            name = DlpServiceClient.organization_inspect_template_path(organization_id, template_id)
        elif project_id:
            name = DlpServiceClient.project_inspect_template_path(project_id, template_id)
        else:
            raise AirflowException("Please provide either organization_id or project_id.")

        return client.update_inspect_template(
            name=name,
            inspect_template=inspect_template,
            update_mask=update_mask,
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

    @CloudBaseHook.catch_http_exception
    @CloudBaseHook.fallback_to_default_project_id
    def update_job_trigger(
        self,
        job_trigger_id: str,
        project_id: Optional[str] = None,
        job_trigger: Optional[Union[dict, JobTrigger]] = None,
        update_mask: Optional[Union[dict, FieldMask]] = None,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
    ) -> JobTrigger:
        """
        Updates a job trigger.

        :param job_trigger_id: The ID of the DLP job trigger to be updated.
        :type job_trigger_id: str
        :param project_id: (Optional) Google Cloud Platform project ID where the
            DLP Instance exists. If set to None or missing, the default
            project_id from the GCP connection is used.
        :type project_id: str
        :param job_trigger: New job trigger value.
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
        :type metadata: Sequence[Tuple[str, str]]
        :rtype: google.cloud.dlp_v2.types.JobTrigger
        """

        client = self.get_conn()

        if not job_trigger_id:
            raise AirflowException("Please provide the ID of the DLP job trigger to be updated.")

        name = DlpServiceClient.project_job_trigger_path(project_id, job_trigger_id)
        return client.update_job_trigger(
            name=name,
            job_trigger=job_trigger,
            update_mask=update_mask,
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

    @CloudBaseHook.catch_http_exception
    def update_stored_info_type(
        self,
        stored_info_type_id: str,
        organization_id: Optional[str] = None,
        project_id: Optional[str] = None,
        config: Optional[Union[dict, StoredInfoTypeConfig]] = None,
        update_mask: Optional[Union[dict, FieldMask]] = None,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
    ) -> StoredInfoType:
        """
        Updates the stored info type by creating a new version.

        :param stored_info_type_id: The ID of the stored info type to be updated.
        :type stored_info_type_id: str
        :param organization_id: (Optional) The organization ID. Required to set this
            field if parent resource is an organzation.
        :type organization_id: str
        :param project_id: (Optional) Google Cloud Platform project ID where the
            DLP Instance exists. Only set this field if the parent resource is
            a project instead of an organzation.
        :type project_id: str
        :param config: Updated configuration for the stored info type. If not provided, a new
            version of the stored info type will be created with the existing configuration.
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
        :type metadata: Sequence[Tuple[str, str]]
        :rtype: google.cloud.dlp_v2.types.StoredInfoType
        """

        client = self.get_conn()

        if not stored_info_type_id:
            raise AirflowException("Please provide the ID of the stored info type to be updated.")

        # Handle project_id from connection configuration
        project_id = project_id or self.project_id

        if organization_id:
            name = DlpServiceClient.organization_stored_info_type_path(organization_id, stored_info_type_id)
        elif project_id:
            name = DlpServiceClient.project_stored_info_type_path(project_id, stored_info_type_id)
        else:
            raise AirflowException("Please provide either organization_id or project_id.")

        return client.update_stored_info_type(
            name=name, config=config, update_mask=update_mask, retry=retry, timeout=timeout, metadata=metadata
        )
