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
"""This module contains a Google Cloud Functions Hook."""

from __future__ import annotations

import time
from collections.abc import Sequence

import requests
from googleapiclient.discovery import build

from airflow.providers.common.compat.sdk import AirflowException
from airflow.providers.google.common.hooks.base_google import PROVIDE_PROJECT_ID, GoogleBaseHook

# Time to sleep between active checks of the operation results
TIME_TO_SLEEP_IN_SECONDS = 1


class CloudFunctionsHook(GoogleBaseHook):
    """
    Google Cloud Functions APIs.

    All the methods in the hook where project_id is used must be called with
    keyword arguments rather than positional.
    """

    _conn: build | None = None

    def __init__(
        self,
        api_version: str,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(
            gcp_conn_id=gcp_conn_id,
            impersonation_chain=impersonation_chain,
            **kwargs,
        )
        self.api_version = api_version

    @staticmethod
    def _full_location(project_id: str, location: str) -> str:
        """
        Retrieve full location of the function.

        :param project_id: Google Cloud Project ID where the function belongs.
        :param location: The location where the function is created.
        :return: The full location, in the form of
            ``projects/<GCP_PROJECT_ID>/locations/<GCP_LOCATION>``.
        """
        return f"projects/{project_id}/locations/{location}"

    def get_conn(self) -> build:
        """
        Retrieve the connection to Cloud Functions.

        :return: Google Cloud Function services object.
        """
        if not self._conn:
            http_authorized = self._authorize()
            self._conn = build(
                "cloudfunctions", self.api_version, http=http_authorized, cache_discovery=False
            )
        return self._conn

    def get_function(self, name: str) -> dict:
        """
        Get the Cloud Function with given name.

        :param name: Name of the function.
        :return: A Cloud Functions object representing the function.
        """
        operation = self.get_conn().projects().locations().functions().get(name=name)
        return operation.execute(num_retries=self.num_retries)

    @GoogleBaseHook.fallback_to_default_project_id
    def create_new_function(self, location: str, body: dict, project_id: str) -> None:
        """
        Create a new function at the location specified in the body.

        :param location: The location of the function.
        :param body: The body required by the Cloud Functions insert API.
        :param project_id: Google Cloud Project ID where the function belongs.
            If set to None or missing, the default project ID from the Google
            Cloud connection is used.
        """
        operation = (
            self.get_conn()
            .projects()
            .locations()
            .functions()
            .create(location=self._full_location(project_id, location), body=body)
        )
        response = operation.execute(num_retries=self.num_retries)
        operation_name = response["name"]
        self._wait_for_operation_to_complete(operation_name=operation_name)

    def update_function(self, name: str, body: dict, update_mask: list[str]) -> None:
        """
        Update Cloud Functions according to the specified update mask.

        :param name: The name of the function.
        :param body: The body required by the cloud function patch API.
        :param update_mask: The update mask - array of fields that should be patched.
        """
        operation = (
            self.get_conn()
            .projects()
            .locations()
            .functions()
            .patch(updateMask=",".join(update_mask), name=name, body=body)
        )
        response = operation.execute(num_retries=self.num_retries)
        operation_name = response["name"]
        self._wait_for_operation_to_complete(operation_name=operation_name)

    @GoogleBaseHook.fallback_to_default_project_id
    def upload_function_zip(self, location: str, zip_path: str, project_id: str) -> str:
        """
        Upload ZIP file with sources.

        :param location: The location where the function is created.
        :param zip_path: The path of the valid .zip file to upload.
        :param project_id: Google Cloud Project ID where the function belongs.
            If set to None or missing, the default project ID from the Google
            Cloud connection is used.
        :return: The upload URL that was returned by generateUploadUrl method.
        """
        operation = (
            self.get_conn()
            .projects()
            .locations()
            .functions()
            .generateUploadUrl(parent=self._full_location(project_id, location))
        )
        response = operation.execute(num_retries=self.num_retries)

        upload_url = response.get("uploadUrl")
        with open(zip_path, "rb") as file:
            requests.put(
                url=upload_url,
                data=file,
                # Those two headers needs to be specified according to:
                # https://cloud.google.com/functions/docs/reference/rest/v1/projects.locations.functions/generateUploadUrl
                headers={
                    "Content-type": "application/zip",
                    "x-goog-content-length-range": "0,104857600",
                },
            )
        return upload_url

    def delete_function(self, name: str) -> None:
        """
        Delete the specified Cloud Function.

        :param name: The name of the function.
        """
        operation = self.get_conn().projects().locations().functions().delete(name=name)
        response = operation.execute(num_retries=self.num_retries)
        operation_name = response["name"]
        self._wait_for_operation_to_complete(operation_name=operation_name)

    @GoogleBaseHook.fallback_to_default_project_id
    def call_function(
        self,
        function_id: str,
        input_data: dict,
        location: str,
        project_id: str = PROVIDE_PROJECT_ID,
    ) -> dict:
        """
        Invoke a deployed Cloud Function.

        This is done synchronously and should only be used for testing purposes,
        as very limited traffic is allowed.

        :param function_id: ID of the function to be called
        :param input_data: Input to be passed to the function
        :param location: The location where the function is located.
        :param project_id: Google Cloud Project ID where the function belongs.
            If set to None or missing, the default project ID from the Google
            Cloud connection is used.
        """
        name = f"projects/{project_id}/locations/{location}/functions/{function_id}"
        operation = self.get_conn().projects().locations().functions().call(name=name, body=input_data)
        response = operation.execute(num_retries=self.num_retries)
        if "error" in response:
            raise AirflowException(response["error"])
        return response

    def _wait_for_operation_to_complete(self, operation_name: str) -> dict:
        """
        Wait for the named operation to complete.

        This is used to check the status of an asynchronous call.

        :param operation_name: The name of the operation.
        :return: The response returned by the operation.
        :exception: AirflowException in case error is returned.
        """
        service = self.get_conn()
        while True:
            operation = service.operations().get(name=operation_name)
            operation_response = operation.execute(num_retries=self.num_retries)
            if operation_response.get("done"):
                response = operation_response.get("response")
                error = operation_response.get("error")
                # Note, according to documentation always either response or error is
                # set when "done" == True
                if error:
                    raise AirflowException(str(error))
                return response
            time.sleep(TIME_TO_SLEEP_IN_SECONDS)
