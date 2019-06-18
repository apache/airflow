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
#
"""
This module contains a Google Cloud Data Loss Prevention(DLP) Hook.
"""

import time

from apiclient.discovery import build
from googleapiclient.errors import HttpError

from airflow import AirflowException
from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook


class CloudDataLossPreventionHook(GoogleCloudBaseHook):
    """
    Interact with DLP API. This hook uses the Google Cloud Platform
    connection.
    """

    def __init__(self,
                 gcp_conn_id='google_cloud_default',
                 delegate_to=None,
                 num_retries=5,
                 polling_interval=100):
        super(CloudDataLossPreventionHook, self).__init__(gcp_conn_id, delegate_to)
        self.num_retries = num_retries
        self.polling_interval = polling_interval

    def _get_resource(self, name):
        """
        Returns corresponding Google service discovery API client resource object,
        based on the input name.

        :param name: The resource name, for example projects/my-project-id
            or organizations/my-project-id/inspectTemplates/my-template-id.
        :type name: str
        :return: The Google service discovery API client resource object.
        :rtype: object
        """
        if name[:8] == 'projects':
            resource = self.get_conn().projects()
        elif name[:13] == 'organizations':
            resource = self.get_conn().organizations()
        else:
            raise ValueError("parent or name should begin with 'organizations' or 'projects'.")

        return resource

    def get_conn(self):
        """
        Returns a Google Cloud Data Loss Prevention service object.
        """
        http_authorized = self._authorize()
        return build('dlp', 'v2', http=http_authorized, cache_discovery=False)

    def inspect_content(self, parent, body):
        """
        Inspect a contentItem, could be string, table of strings or image.
        See here for more details:

        https://cloud.google.com/dlp/docs/reference/rest/v2/projects.content/inspect

        :param parent: The parent resource name, for example projects/my-project-id.
        :type parent: str
        :param body: The request body containing all inspection configs.
        :type body: dict
        :return: The response returned.
        :rtype: dict
        :exception: AirflowException in case HttpError is returned.
        """
        try:
            service = self.get_conn()
            result = service.projects().content().inspect(
                parent=parent,
                body=body).execute(num_retries=self.num_retries)
            self.log.info('Inspection result: %s\n', result)
            return result
        except HttpError as ex:
            raise AirflowException(
                'Inspecting content failed: {}'.format(ex.content)
            )

    def deidentify_content(self, parent, body):
        """
        De-identify a contentItem, could be string, table of strings or image.
        See here for more details:

        https://cloud.google.com/dlp/docs/reference/rest/v2/projects.content/deidentify

        :param parent: The parent resource name, for example projects/my-project-id.
        :type parent: str
        :param body: The request body containing all inspection configs.
        :type body: dict
        :return: The response returned.
        :rtype: dict
        :exception: AirflowException in case HttpError is returned.
        """
        try:
            service = self.get_conn()
            result = service.projects().content().deidentify(
                parent=parent,
                body=body).execute(num_retries=self.num_retries)
            self.log.info('De-identification result: %s\n', result)
            return result
        except HttpError as ex:
            raise AirflowException(
                'De-identifying content failed: {}'.format(ex.content)
            )

    def reidentify_content(self, parent, body):
        """
        Re-identifies content that has been de-identified.
        See here for more details:

        https://cloud.google.com/dlp/docs/reference/rest/v2/projects.content/reidentify

        :param parent: The parent resource name, for example projects/my-project-id.
        :type parent: str
        :param body: The request body containing all inspection configs.
        :type body: dict
        :return: The response returned.
        :rtype: dict
        :exception: AirflowException in case HttpError is returned.
        """
        try:
            service = self.get_conn()
            result = service.projects().content().reidentify(
                parent=parent,
                body=body).execute(num_retries=self.num_retries)
            self.log.info('Re-identification result: %s\n', result)
            return result
        except HttpError as ex:
            raise AirflowException(
                'Re-identifying content failed: {}'.format(ex.content)
            )

    def create_dlp_job(self, parent, body):
        """
        Creates a new job to inspect storage or calculate risk metrics.
        See here for more details:

        https://cloud.google.com/dlp/docs/reference/rest/v2/projects.dlpJobs/create

        :param parent: The parent resource name, for example projects/my-project-id.
        :type parent: string
        :param body: The request body containing all configs for the job.
        :type body: dict
        :return: The response returned.
        :rtype: dict
        :exception: AirflowException in case HttpError is returned.
        """
        service = self.get_conn()
        try:
            response = service.projects().dlpJobs().create(
                parent=parent,
                body=body).execute(num_retries=self.num_retries)
            self.log.info('Creating dlp job.')
            return response
        except HttpError as ex:
            raise AirflowException(
                'Creating dlp job failed: {}'.format(ex.content)
            )

    def list_dlp_jobs(self,
                      parent,
                      list_filter=None,
                      page_token=None,
                      page_size=None,
                      order_by=None,
                      list_type=None):
        """
        Lists DlpJobs that match the specified filter in the request.
        See here for more details:

        https://cloud.google.com/dlp/docs/reference/rest/v2/projects.dlpJobs/list

        :param parent: The parent resource name, for example projects/my-project-id.
        :type parent: str
        :param list_filter: Optional. Allows filtering.
        :type: str
        :param page_token: Optional. The standard list page token.
        :type page_token: str
        :param page_size: Optional. The standard list page size.
        :type page_size: number
        :param order_by: Optional. Optional comma separated list of fields to order by,
            followed by asc or desc postfix.
        :type order_by: str
        :param list_type: Optional. The type of job. Defaults to DlpJobType.INSPECT.
        :return: The response returned.
        :rtype: dict
        :exception: AirflowException in case HttpError is returned.
        """
        service = self.get_conn()
        try:
            response = service.projects().dlpJobs().list(
                parent=parent,
                filter=list_filter,
                pageToken=page_token,
                pageSize=page_size,
                orderBy=order_by,
                type=list_type).execute(num_retries=self.num_retries)
            self.log.info('Listing dlp job for %s.', parent)
            return response
        except HttpError as ex:
            raise AirflowException(
                'Listing dlp jobs for {} failed: {}'.format(parent, ex.content)
            )

    def get_dlp_job(self, name):
        """
        Get the latest state of a long-running DLPJob.
        See here for more details:

        https://cloud.google.com/dlp/docs/reference/rest/v2/projects.dlpJobs/get

        :param name: The name of the DlpJob resource, e.g. 'projects/*/dlpJobs/*'.
        :type name: str
        :return: The response returned.
        :rtype: dict
        :exception: AirflowException in case HttpError is returned.
        """
        service = self.get_conn()
        try:
            response = service.projects().dlpJobs().get(name=name).execute(num_retries=self.num_retries)
            self.log.info('Getting DLP job %s', name)
            return response
        except HttpError as ex:
            raise AirflowException(
                'Getting dlp job {} failed: {}'.format(name, ex.content)
            )

    def delete_dlp_job(self, name):
        """
        Deletes a long-running DlpJob.
        See here for more details:

        https://cloud.google.com/dlp/docs/reference/rest/v2/projects.dlpJobs/delete

        :param name: The name of the DlpJob resource to be deleted, e.g. projects/*/dlpJobs/*.
        :type name: str
        :return: The response returned.
        :rtype: dict
        :exception: AirflowException in case HttpError is returned.
        """
        service = self.get_conn()
        try:
            response = service.projects().dlpJobs().delete(name=name).execute(num_retries=self.num_retries)
            self.log.info('Deleting dlp job %s', name)
            return response
        except HttpError as ex:
            raise AirflowException(
                'Deleting dlp job {} failed: {}'.format(name, ex.content)
            )

    def cancel_dlp_job(self, name):
        """
        Starts asynchronous cancellation on a long-running DlpJob.
        The server makes a best effort to cancel the DlpJob, but success is not guaranteed.
        See here for more details:

        https://cloud.google.com/dlp/docs/reference/rest/v2/projects.dlpJobs/cancel

        :param name: The name of the DlpJob resource to be cancelled, e.g. 'projects/*/dlpJobs/*'.
        :type name: str
        :return: The response returned.
        :rtype: dict
        :exception: AirflowException in case HttpError is returned.
        """
        service = self.get_conn()
        try:
            response = service.projects().dlpJobs().cancel(name=name).execute(num_retries=self.num_retries)
            self.log.info('Canceling dlp job %s', name)
            return response
        except HttpError as ex:
            raise AirflowException(
                'Canceling dlp job {} failed: {}'.format(name, ex.content)
            )

    def run_dlp_job(self, parent, body):
        """
        Run a dlpJob until finished.

        :param parent: The parent resource name, for example projects/my-project-id.
        :type parent: str
        :param body: The request body.
        :type body: dict
        :return: The response returned.
        :rtype: dict
        :exception: AirflowException in case job failed or canceled.
        """
        # Create a dlpJob
        name = self.create_dlp_job(parent=parent, body=body)['name']

        # Wait for the dlpJob to finish
        keep_polling_job = True
        while keep_polling_job:
            state = self.get_dlp_job(name=name)['state']
            if state == 'DONE':
                self.log.info('DLPjob %s DONE', name)
                keep_polling_job = False
            elif state in ('PENDING', 'RUNNING', 'JOB_STATE_UNSPECIFIED'):
                self.log.info('DLPjob %s %s', name, state)
                time.sleep(self.polling_interval)
            else:
                raise AirflowException(
                    'DlpJob {} failed or canceled: {}'.format(name, state)
                )

    def create_inspect_template(self, parent, body):
        """
        Creates an InspectTemplate for re-using frequently used configuration
        for inspecting content, images, and storage.
        See here for more details:

        https://cloud.google.com/dlp/docs/reference/rest/v2/projects.inspectTemplates/create
        https://cloud.google.com/dlp/docs/reference/rest/v2/organizations.inspectTemplates/create

        :param parent: The parent resource name,
            for example projects/my-project-id or organizations/my-org-id.
        :type parent: str
        :param body: The request body.
        :type body: dict
        :return: The response returned.
        :rtype: dict
        :exception: AirflowException in case HttpError is returned.
        """
        resource = self._get_resource(parent)
        try:
            response = resource.inspectTemplates().create(
                parent=parent,
                body=body).execute(num_retries=self.num_retries)
            self.log.info('Creating inspect template')
            return response
        except HttpError as ex:
            raise AirflowException(
                'Creating inspect template {} failed: {}'.format(parent, ex.content)
            )

    def delete_inspect_template(self, name):
        """
        Deletes an InspectTemplate.
        See here for more details:

        https://cloud.google.com/dlp/docs/reference/rest/v2/projects.inspectTemplates/delete
        https://cloud.google.com/dlp/docs/reference/rest/v2/organizations.inspectTemplates/delete

        :param name: Resource name of the organization and inspectTemplate to be deleted,
            for example organizations/433245324/inspectTemplates/432452342
            or projects/project-id/inspectTemplates/432452342.
        :type name: str
        :return: The response returned.
        :rtype: dict
        :exception: AirflowException in case HttpError is returned.
        """
        resource = self._get_resource(name)
        try:
            response = resource.inspectTemplates().delete(
                name=name).execute(num_retries=self.num_retries)
            self.log.info('Delete inspect template %s.', name)
            return response
        except HttpError as ex:
            raise AirflowException(
                'Deleting inspect template {} failed: {}'.format(name, ex.content)
            )

    def get_inspect_template(self, name):
        """
        Gets an InspectTemplate.
        See here for more details:

        https://cloud.google.com/dlp/docs/reference/rest/v2/projects.inspectTemplates/get
        https://cloud.google.com/dlp/docs/reference/rest/v2/organizations.inspectTemplates/get

        :param name: Resource name of the organization and inspectTemplate to be read,
            for example organizations/433245324/inspectTemplates/432452342
            or projects/project-id/inspectTemplates/432452342.
        :type name: str
        :return: The response returned.
        :rtype: dict
        :exception: AirflowException in case HttpError is returned.
        """
        resource = self._get_resource(name)
        try:
            response = resource.inspectTemplates().get(
                name=name).execute(num_retries=self.num_retries)
            self.log.info('Getting inspect template %s', name)
            return response
        except HttpError as ex:
            raise AirflowException(
                'Getting inspect template {} failed: {}'.format(name, ex.content)
            )

    def list_inspect_templates(self,
                               parent,
                               page_token=None,
                               page_size=None,
                               order_by=None):
        """
        Lists InspectTemplates.
        See here for more details:

        https://cloud.google.com/dlp/docs/reference/rest/v2/projects.inspectTemplates/list
        https://cloud.google.com/dlp/docs/reference/rest/v2/organizations.inspectTemplates/list

        :param parent: The parent resource name,
            for example projects/my-project-id or organizations/my-org-id.
        :type parent: str
        :param page_token: Optional, page token to continue retrieval.
        :type page_token: str
        :param page_size: Optional, size of the page, can be limited by server.
        :type page_size: number
        :param order_by: Optional, comma separated list of fields to order by,
            followed by asc or desc postfix.
        :type order_by: str
        :return: The response returned.
        :rtype: dict
        :exception: AirflowException in case HttpError is returned.
        """
        resource = self._get_resource(parent)
        try:
            response = resource.inspectTemplates().list(
                parent=parent,
                pageToken=page_token,
                pageSize=page_size,
                orderBy=order_by).execute(num_retries=self.num_retries)
            self.log.info('Listing inspect templates for %s', parent)
            return response
        except HttpError as ex:
            raise AirflowException(
                'Listing inspect templates for {} failed: {}'.format(parent, ex.content)
            )

    def update_inspect_template(self, name, body):
        """
        Updates the InspectTemplate.
        See here for more details:

        https://cloud.google.com/dlp/docs/reference/rest/v2/projects.inspectTemplates/patch
        https://cloud.google.com/dlp/docs/reference/rest/v2/organizations.inspectTemplates/patch

        :param name: Resource name of organization and inspectTemplate to be updated,
            for example organizations/433245324/inspectTemplates/432452342
            or projects/project-id/inspectTemplates/432452342.
        :type name: str
        :param body: The request body.
        :type body: dict
        :return: The response returned.
        :rtype: dict
        :exception: AirflowException in case HttpError is returned.
        """
        resource = self._get_resource(name)
        try:
            response = resource.inspectTemplates().patch(
                name=name,
                body=body).execute(num_retries=self.num_retries)
            self.log.info('Updating inspect template %s', name)
            return response
        except HttpError as ex:
            raise AirflowException(
                'Updating inspect template {} failed: {}'.format(name, ex.content)
            )

    def create_stored_info_type(self, parent, body):
        """
        Creates a pre-built stored infoType to be used for inspection.
        See here for more details:

        https://cloud.google.com/dlp/docs/reference/rest/v2/projects.storedInfoTypes/create
        https://cloud.google.com/dlp/docs/reference/rest/v2/organizations.storedInfoTypes/create

        :param parent: The parent resource name,
            for example projects/my-project-id or organizations/my-org-id.
        :type parent: str
        :param body: The request body.
        :type body: dict
        :return: The response returned.
        :rtype: dict
        :exception: AirflowException in case HttpError is returned.
        """
        resource = self._get_resource(parent)
        try:
            response = resource.storedInfoTypes().create(
                parent=parent,
                body=body).execute(num_retries=self.num_retries)
            self.log.info('Creating storedInfoType.')
            return response
        except HttpError as ex:
            raise AirflowException(
                'Creating storedInfoType {} failed: {}'.format(parent, ex.content)
            )

    def delete_stored_info_type(self, name):
        """
        Deletes a stored infoType
        See here for more details:

        https://cloud.google.com/dlp/docs/reference/rest/v2/projects.storedInfoTypes/delete
        https://cloud.google.com/dlp/docs/reference/rest/v2/organizations.storedInfoTypes/delete

        :param name: Resource name of the organization and storedInfoType to be deleted,
            for example organizations/433245324/storedInfoTypes/432452342
            or projects/project-id/storedInfoTypes/432452342.
        :type name: str
        :return: The response returned.
        :rtype: dict
        :exception: AirflowException in case HttpError is returned.
        """
        resource = self._get_resource(name)
        try:
            response = resource.storedInfoTypes().delete(
                name=name).execute(num_retries=self.num_retries)
            self.log.info('Deleting storedInfoType %s', name)
            return response
        except HttpError as ex:
            raise AirflowException(
                'Deleting storedInfoType {} failed: {}'.format(name, ex.content)
            )

    def get_stored_info_type(self, name):
        """
        Gets a stored infoType.
        See here for more details:

        https://cloud.google.com/dlp/docs/reference/rest/v2/projects.storedInfoTypes/get
        https://cloud.google.com/dlp/docs/reference/rest/v2/organizations.storedInfoTypes/get

        :param name: Resource name of the organization and storedInfoType to be read,
            for example organizations/433245324/storedInfoTypes/432452342
            or projects/project-id/storedInfoTypes/432452342.
        :type name: str
        :return: The response returned.
        :rtype: dict
        :exception: AirflowException in case HttpError is returned.
        """
        resource = self._get_resource(name)
        try:
            response = resource.storedInfoTypes().get(
                name=name).execute(num_retries=self.num_retries)
            self.log.info('Getting storedInfoType %s', name)
            return response
        except HttpError as ex:
            raise AirflowException(
                'Getting storedInfoType {} failed: {}'.format(name, ex.content)
            )

    def list_stored_info_types(self,
                               parent,
                               page_token=None,
                               page_size=None,
                               order_by=None):
        """
        Lists stored infoTypes.
        See here for more details:

        https://cloud.google.com/dlp/docs/reference/rest/v2/projects.storedInfoTypes/list
        https://cloud.google.com/dlp/docs/reference/rest/v2/organizations.storedInfoTypes/list

        :param parent: The parent resource name,
            for example projects/my-project-id or organizations/my-org-id.
        :type parent: str
        :param page_token: Optional, page token to continue retrieval.
        :type page_token: str
        :param page_size: Optional, size of the page, can be limited by server.
        :type page_size: number
        :param order_by: Optional comma separated list of fields to order by,
            followed by asc or desc postfix.
        :type order_by: str
        :return: The response returned.
        :rtype: dict
        :exception: AirflowException in case HttpError is returned.
        """
        resource = self._get_resource(parent)
        try:
            response = resource.storedInfoTypes().list(
                parent=parent,
                pageToken=page_token,
                pageSize=page_size,
                orderBy=order_by).execute(num_retries=self.num_retries)
            self.log.info('Listing storedInfoTypes %s', parent)
            return response
        except HttpError as ex:
            raise AirflowException(
                'Listing storedInfoTypes {} failed: {}'.format(parent, ex.content)
            )

    def update_stored_info_type(self, name, body):
        """
        Updates the stored infoType by creating a new version.
        The existing version will continue to be used until the new version is ready.
        See here for more details:

        https://cloud.google.com/dlp/docs/reference/rest/v2/projects.storedInfoTypes/patch
        https://cloud.google.com/dlp/docs/reference/rest/v2/organizations.storedInfoTypes/patch

        :param name: Resource name of organization and storedInfoType to be updated,
            for example organizations/433245324/storedInfoTypes/432452342
            or projects/project-id/storedInfoTypes/432452342.
        :type name: str
        :param body: The request body.
        :type body: dict
        :return: The response returned.
        :rtype: dict
        :exception: AirflowException in case HttpError is returned.
        """
        resource = self._get_resource(name)
        try:
            response = resource.storedInfoTypes().patch(
                name=name,
                body=body).execute(num_retries=self.num_retries)
            self.log.info('Updating storedInfoType %s', name)
            return response
        except HttpError as ex:
            raise AirflowException(
                'Updating storedInfoType {} failed: {}'.format(name, ex.content)
            )
