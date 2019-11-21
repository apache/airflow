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
This module contains a Google Cloud Functions Hook.
"""
import time
from typing import Any, Dict, List, Optional

import requests
from googleapiclient.discovery import build

from airflow import AirflowException
from airflow.gcp.hooks.base import GoogleCloudBaseHook

# Time to sleep between active checks of the operation results
TIME_TO_SLEEP_IN_SECONDS = 1


# noinspection PyAbstractClass
class CloudFunctionsHook(GoogleCloudBaseHook):
    """
    Hook for the Google Cloud Functions APIs.

    All the methods in the hook where project_id is used must be called with
    keyword arguments rather than positional.
    """
    _conn = None  # type: Optional[Any]

    def __init__(
        self,
        api_version: str,
        gcp_conn_id: str = 'google_cloud_default',
        delegate_to: Optional[str] = None
    ) -> None:
        super().__init__(gcp_conn_id, delegate_to)
        self.api_version = api_version

    @staticmethod
    def _full_location(project_id: str, location: str) -> str:
        """
        Retrieve full location of the function in the form of
        ``projects/<GCP_PROJECT_ID>/locations/<GCP_LOCATION>``

        :param project_id: The Google Cloud Project project_id where the function belongs.
        :type project_id: str
        :param location: The location where the function is created.
        :type location: str
        :return:
        """
        return 'projects/{}/locations/{}'.format(project_id, location)

    def get_conn(self):
        """
        Retrieves the connection to Cloud Functions.

        :return: Google Cloud Function services object.
        :rtype: dict
        """
        if not self._conn:
            http_authorized = self._authorize()
            self._conn = build('cloudfunctions', self.api_version,
                               http=http_authorized, cache_discovery=False)
        return self._conn

    def get_function(self, name: str) -> Dict:
        """
        Returns the Cloud Function with the given name.

        :param name: Name of the function.
        :type name: str
        :return: A Cloud Functions object representing the function.
        :rtype: dict
        """
        return self.get_conn().projects().locations().functions().get(  # pylint: disable=no-member
            name=name).execute(num_retries=self.num_retries)

    @GoogleCloudBaseHook.fallback_to_default_project_id
    def create_new_function(self, location: str, body: Dict, project_id: Optional[str] = None) -> None:
        """
        Creates a new function in Cloud Function in the location specified in the body.

        :param location: The location of the function.
        :type location: str
        :param body: The body required by the Cloud Functions insert API.
        :type body: dict
        :param project_id: Optional, Google Cloud Project project_id where the function belongs.
            If set to None or missing, the default project_id from the GCP connection is used.
        :type project_id: str
        :return: None
        """
        assert project_id is not None
        response = self.get_conn().projects().locations().functions().create(  # pylint: disable=no-member
            location=self._full_location(project_id, location),
            body=body
        ).execute(num_retries=self.num_retries)
        operation_name = response["name"]
        self._wait_for_operation_to_complete(operation_name=operation_name)

    def update_function(self, name: str, body: Dict, update_mask: List[str]) -> None:
        """
        Updates Cloud Functions according to the specified update mask.

        :param name: The name of the function.
        :type name: str
        :param body: The body required by the cloud function patch API.
        :type body: dict
        :param update_mask: The update mask - array of fields that should be patched.
        :type update_mask: [str]
        :return: None
        """
        response = self.get_conn().projects().locations().functions().patch(  # pylint: disable=no-member
            updateMask=",".join(update_mask),
            name=name,
            body=body
        ).execute(num_retries=self.num_retries)
        operation_name = response["name"]
        self._wait_for_operation_to_complete(operation_name=operation_name)

    @GoogleCloudBaseHook.fallback_to_default_project_id
    def upload_function_zip(self, location: str, zip_path: str, project_id: Optional[str] = None) -> str:
        """
        Uploads zip file with sources.

        :param location: The location where the function is created.
        :type location: str
        :param zip_path: The path of the valid .zip file to upload.
        :type zip_path: str
        :param project_id: Optional, Google Cloud Project project_id where the function belongs.
            If set to None or missing, the default project_id from the GCP connection is used.
        :type project_id: str
        :return: The upload URL that was returned by generateUploadUrl method.
        :rtype: str
        """
        assert project_id is not None
        response = \
            self.get_conn().projects().locations().functions().generateUploadUrl(  # pylint: disable=no-member # noqa
            parent=self._full_location(project_id, location)
            ).execute(num_retries=self.num_retries)

        upload_url = response.get('uploadUrl')
        with open(zip_path, 'rb') as file:
            requests.put(
                url=upload_url,
                data=file,
                # Those two headers needs to be specified according to:
                # https://cloud.google.com/functions/docs/reference/rest/v1/projects.locations.functions/generateUploadUrl
                # nopep8
                headers={
                    'Content-type': 'application/zip',
                    'x-goog-content-length-range': '0,104857600',
                }
            )
        return upload_url

    def delete_function(self, name: str) -> None:
        """
        Deletes the specified Cloud Function.

        :param name: The name of the function.
        :type name: str
        :return: None
        """
        response = self.get_conn().projects().locations().functions().delete(  # pylint: disable=no-member
            name=name).execute(num_retries=self.num_retries)
        operation_name = response["name"]
        self._wait_for_operation_to_complete(operation_name=operation_name)

    @GoogleCloudBaseHook.fallback_to_default_project_id
    def call_function(
            self,
            function_id: str,
            input_data: Dict,
            location: str,
            project_id: Optional[str] = None
    ) -> Dict:
        """
        Synchronously invokes a deployed Cloud Function. To be used for testing
        purposes as very limited traffic is allowed.

        :param function_id: ID of the function to be called
        :type function_id: str
        :param input_data: Input to be passed to the function
        :type input_data: Dict
        :param location: The location where the function is located.
        :type location: str
        :param project_id: Optional, Google Cloud Project project_id where the function belongs.
            If set to None or missing, the default project_id from the GCP connection is used.
        :type project_id: str
        :return: None
        """
        name = "projects/{project_id}/locations/{location}/functions/{function_id}".format(
            project_id=project_id,
            location=location,
            function_id=function_id
        )
        response = self.get_conn().projects().locations().functions().call(  # pylint:disable=no-member
            name=name,
            body=input_data
        ).execute(num_retries=self.num_retries)
        if 'error' in response:
            raise AirflowException(response['error'])
        return response

    def _wait_for_operation_to_complete(self, operation_name: str) -> Dict:
        """
        Waits for the named operation to complete - checks status of the
        asynchronous call.

        :param operation_name: The name of the operation.
        :type operation_name: str
        :return: The response returned by the operation.
        :rtype: dict
        :exception: AirflowException in case error is returned.
        """
        service = self.get_conn()
        while True:
            operation_response = service.operations().get(  # pylint: disable=no-member
                name=operation_name,
            ).execute(num_retries=self.num_retries)
            if operation_response.get("done"):
                response = operation_response.get("response")
                error = operation_response.get("error")
                # Note, according to documentation always either response or error is
                # set when "done" == True
                if error:
                    raise AirflowException(str(error))
                return response
            time.sleep(TIME_TO_SLEEP_IN_SECONDS)
