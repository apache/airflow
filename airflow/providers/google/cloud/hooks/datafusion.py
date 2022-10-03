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
"""This module contains Google DataFusion hook."""
from __future__ import annotations

import json
import os
from time import monotonic, sleep
from typing import Any, Dict, Optional, Sequence
from urllib.parse import quote, urlencode

import google.auth
from google.api_core.retry import exponential_sleep_generator
from googleapiclient.discovery import Resource, build

from airflow.exceptions import AirflowException
from airflow.providers.google.common.hooks.base_google import PROVIDE_PROJECT_ID, GoogleBaseHook

Operation = Dict[str, Any]


class PipelineStates:
    """Data Fusion pipeline states"""

    PENDING = "PENDING"
    STARTING = "STARTING"
    RUNNING = "RUNNING"
    SUSPENDED = "SUSPENDED"
    RESUMING = "RESUMING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    KILLED = "KILLED"
    REJECTED = "REJECTED"


FAILURE_STATES = [PipelineStates.FAILED, PipelineStates.KILLED, PipelineStates.REJECTED]
SUCCESS_STATES = [PipelineStates.COMPLETED]


class DataFusionHook(GoogleBaseHook):
    """Hook for Google DataFusion."""

    _conn = None  # type: Optional[Resource]

    def __init__(
        self,
        api_version: str = "v1beta1",
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: str | None = None,
        impersonation_chain: str | Sequence[str] | None = None,
    ) -> None:
        super().__init__(
            gcp_conn_id=gcp_conn_id,
            delegate_to=delegate_to,
            impersonation_chain=impersonation_chain,
        )
        self.api_version = api_version

    def wait_for_operation(self, operation: dict[str, Any]) -> dict[str, Any]:
        """Waits for long-lasting operation to complete."""
        for time_to_wait in exponential_sleep_generator(initial=10, maximum=120):
            sleep(time_to_wait)
            operation = (
                self.get_conn().projects().locations().operations().get(name=operation.get("name")).execute()
            )
            if operation.get("done"):
                break
        if "error" in operation:
            raise AirflowException(operation["error"])
        return operation["response"]

    def wait_for_pipeline_state(
        self,
        pipeline_name: str,
        pipeline_id: str,
        instance_url: str,
        namespace: str = "default",
        success_states: list[str] | None = None,
        failure_states: list[str] | None = None,
        timeout: int = 5 * 60,
    ) -> None:
        """
        Polls pipeline state and raises an exception if the state is one of
        `failure_states` or the operation timed_out.
        """
        failure_states = failure_states or FAILURE_STATES
        success_states = success_states or SUCCESS_STATES
        start_time = monotonic()
        current_state = None
        while monotonic() - start_time < timeout:
            try:
                workflow = self.get_pipeline_workflow(
                    pipeline_name=pipeline_name,
                    pipeline_id=pipeline_id,
                    instance_url=instance_url,
                    namespace=namespace,
                )
                current_state = workflow["status"]
            except AirflowException:
                pass  # Because the pipeline may not be visible in system yet
            if current_state in success_states:
                return
            if current_state in failure_states:
                raise AirflowException(
                    f"Pipeline {pipeline_name} state {current_state} is not one of {success_states}"
                )
            sleep(30)

        # Time is up!
        raise AirflowException(
            f"Pipeline {pipeline_name} state {current_state} is not "
            f"one of {success_states} after {timeout}s"
        )

    @staticmethod
    def _name(project_id: str, location: str, instance_name: str) -> str:
        return f"projects/{project_id}/locations/{location}/instances/{instance_name}"

    @staticmethod
    def _parent(project_id: str, location: str) -> str:
        return f"projects/{project_id}/locations/{location}"

    @staticmethod
    def _base_url(instance_url: str, namespace: str) -> str:
        return os.path.join(instance_url, "v3", "namespaces", quote(namespace), "apps")

    def _cdap_request(
        self, url: str, method: str, body: list | dict | None = None
    ) -> google.auth.transport.Response:
        headers: dict[str, str] = {"Content-Type": "application/json"}
        request = google.auth.transport.requests.Request()

        credentials = self.get_credentials()
        credentials.before_request(request=request, method=method, url=url, headers=headers)

        payload = json.dumps(body) if body else None

        response = request(method=method, url=url, headers=headers, body=payload)
        return response

    @staticmethod
    def _check_response_status_and_data(response, message: str) -> None:
        if response.status != 200:
            raise AirflowException(message)
        if response.data is None:
            raise AirflowException(
                "Empty response received. Please, check for possible root "
                "causes of this behavior either in DAG code or on Cloud Datafusion side"
            )

    def get_conn(self) -> Resource:
        """Retrieves connection to DataFusion."""
        if not self._conn:
            http_authorized = self._authorize()
            self._conn = build(
                "datafusion",
                self.api_version,
                http=http_authorized,
                cache_discovery=False,
            )
        return self._conn

    @GoogleBaseHook.fallback_to_default_project_id
    def restart_instance(self, instance_name: str, location: str, project_id: str) -> Operation:
        """
        Restart a single Data Fusion instance.
        At the end of an operation instance is fully restarted.

        :param instance_name: The name of the instance to restart.
        :param location: The Cloud Data Fusion location in which to handle the request.
        :param project_id: The ID of the Google Cloud project that the instance belongs to.
        """
        operation = (
            self.get_conn()
            .projects()
            .locations()
            .instances()
            .restart(name=self._name(project_id, location, instance_name))
            .execute(num_retries=self.num_retries)
        )
        return operation

    @GoogleBaseHook.fallback_to_default_project_id
    def delete_instance(self, instance_name: str, location: str, project_id: str) -> Operation:
        """
        Deletes a single Date Fusion instance.

        :param instance_name: The name of the instance to delete.
        :param location: The Cloud Data Fusion location in which to handle the request.
        :param project_id: The ID of the Google Cloud project that the instance belongs to.
        """
        operation = (
            self.get_conn()
            .projects()
            .locations()
            .instances()
            .delete(name=self._name(project_id, location, instance_name))
            .execute(num_retries=self.num_retries)
        )
        return operation

    @GoogleBaseHook.fallback_to_default_project_id
    def create_instance(
        self,
        instance_name: str,
        instance: dict[str, Any],
        location: str,
        project_id: str = PROVIDE_PROJECT_ID,
    ) -> Operation:
        """
        Creates a new Data Fusion instance in the specified project and location.

        :param instance_name: The name of the instance to create.
        :param instance: An instance of Instance.
            https://cloud.google.com/data-fusion/docs/reference/rest/v1beta1/projects.locations.instances#Instance
        :param location: The Cloud Data Fusion location in which to handle the request.
        :param project_id: The ID of the Google Cloud project that the instance belongs to.
        """
        operation = (
            self.get_conn()
            .projects()
            .locations()
            .instances()
            .create(
                parent=self._parent(project_id, location),
                body=instance,
                instanceId=instance_name,
            )
            .execute(num_retries=self.num_retries)
        )
        return operation

    @GoogleBaseHook.fallback_to_default_project_id
    def get_instance(self, instance_name: str, location: str, project_id: str) -> dict[str, Any]:
        """
        Gets details of a single Data Fusion instance.

        :param instance_name: The name of the instance.
        :param location: The Cloud Data Fusion location in which to handle the request.
        :param project_id: The ID of the Google Cloud project that the instance belongs to.
        """
        instance = (
            self.get_conn()
            .projects()
            .locations()
            .instances()
            .get(name=self._name(project_id, location, instance_name))
            .execute(num_retries=self.num_retries)
        )
        return instance

    @GoogleBaseHook.fallback_to_default_project_id
    def patch_instance(
        self,
        instance_name: str,
        instance: dict[str, Any],
        update_mask: str,
        location: str,
        project_id: str = PROVIDE_PROJECT_ID,
    ) -> Operation:
        """
        Updates a single Data Fusion instance.

        :param instance_name: The name of the instance to create.
        :param instance: An instance of Instance.
            https://cloud.google.com/data-fusion/docs/reference/rest/v1beta1/projects.locations.instances#Instance
        :param update_mask: Field mask is used to specify the fields that the update will overwrite
            in an instance resource. The fields specified in the updateMask are relative to the resource,
            not the full request. A field will be overwritten if it is in the mask. If the user does not
            provide a mask, all the supported fields (labels and options currently) will be overwritten.
            A comma-separated list of fully qualified names of fields. Example: "user.displayName,photo".
            https://developers.google.com/protocol-buffers/docs/reference/google.protobuf?_ga=2.205612571.-968688242.1573564810#google.protobuf.FieldMask
        :param location: The Cloud Data Fusion location in which to handle the request.
        :param project_id: The ID of the Google Cloud project that the instance belongs to.
        """
        operation = (
            self.get_conn()
            .projects()
            .locations()
            .instances()
            .patch(
                name=self._name(project_id, location, instance_name),
                updateMask=update_mask,
                body=instance,
            )
            .execute(num_retries=self.num_retries)
        )
        return operation

    def create_pipeline(
        self,
        pipeline_name: str,
        pipeline: dict[str, Any],
        instance_url: str,
        namespace: str = "default",
    ) -> None:
        """
        Creates a Cloud Data Fusion pipeline.

        :param pipeline_name: Your pipeline name.
        :param pipeline: The pipeline definition. For more information check:
            https://docs.cdap.io/cdap/current/en/developer-manual/pipelines/developing-pipelines.html#pipeline-configuration-file-format
        :param instance_url: Endpoint on which the REST APIs is accessible for the instance.
        :param namespace: if your pipeline belongs to a Basic edition instance, the namespace ID
            is always default. If your pipeline belongs to an Enterprise edition instance, you
            can create a namespace.
        """
        url = os.path.join(self._base_url(instance_url, namespace), quote(pipeline_name))
        response = self._cdap_request(url=url, method="PUT", body=pipeline)
        self._check_response_status_and_data(
            response, f"Creating a pipeline failed with code {response.status} while calling {url}"
        )

    def delete_pipeline(
        self,
        pipeline_name: str,
        instance_url: str,
        version_id: str | None = None,
        namespace: str = "default",
    ) -> None:
        """
        Deletes a Cloud Data Fusion pipeline.

        :param pipeline_name: Your pipeline name.
        :param version_id: Version of pipeline to delete
        :param instance_url: Endpoint on which the REST APIs is accessible for the instance.
        :param namespace: f your pipeline belongs to a Basic edition instance, the namespace ID
            is always default. If your pipeline belongs to an Enterprise edition instance, you
            can create a namespace.
        """
        url = os.path.join(self._base_url(instance_url, namespace), quote(pipeline_name))
        if version_id:
            url = os.path.join(url, "versions", version_id)

        response = self._cdap_request(url=url, method="DELETE", body=None)
        self._check_response_status_and_data(
            response, f"Deleting a pipeline failed with code {response.status}"
        )

    def list_pipelines(
        self,
        instance_url: str,
        artifact_name: str | None = None,
        artifact_version: str | None = None,
        namespace: str = "default",
    ) -> dict:
        """
        Lists Cloud Data Fusion pipelines.

        :param artifact_version: Artifact version to filter instances
        :param artifact_name: Artifact name to filter instances
        :param instance_url: Endpoint on which the REST APIs is accessible for the instance.
        :param namespace: f your pipeline belongs to a Basic edition instance, the namespace ID
            is always default. If your pipeline belongs to an Enterprise edition instance, you
            can create a namespace.
        """
        url = self._base_url(instance_url, namespace)
        query: dict[str, str] = {}
        if artifact_name:
            query = {"artifactName": artifact_name}
        if artifact_version:
            query = {"artifactVersion": artifact_version}
        if query:
            url = os.path.join(url, urlencode(query))

        response = self._cdap_request(url=url, method="GET", body=None)
        self._check_response_status_and_data(
            response, f"Listing pipelines failed with code {response.status}"
        )
        return json.loads(response.data)

    def get_pipeline_workflow(
        self,
        pipeline_name: str,
        instance_url: str,
        pipeline_id: str,
        namespace: str = "default",
    ) -> Any:
        url = os.path.join(
            self._base_url(instance_url, namespace),
            quote(pipeline_name),
            "workflows",
            "DataPipelineWorkflow",
            "runs",
            quote(pipeline_id),
        )
        response = self._cdap_request(url=url, method="GET")
        self._check_response_status_and_data(
            response, f"Retrieving a pipeline state failed with code {response.status}"
        )
        workflow = json.loads(response.data)
        return workflow

    def start_pipeline(
        self,
        pipeline_name: str,
        instance_url: str,
        namespace: str = "default",
        runtime_args: dict[str, Any] | None = None,
    ) -> str:
        """
        Starts a Cloud Data Fusion pipeline. Works for both batch and stream pipelines.

        :param pipeline_name: Your pipeline name.
        :param instance_url: Endpoint on which the REST APIs is accessible for the instance.
        :param runtime_args: Optional runtime JSON args to be passed to the pipeline
        :param namespace: f your pipeline belongs to a Basic edition instance, the namespace ID
            is always default. If your pipeline belongs to an Enterprise edition instance, you
            can create a namespace.
        """
        # TODO: This API endpoint starts multiple pipelines. There will eventually be a fix
        #  return the run Id as part of the API request to run a single pipeline.
        #  https://github.com/apache/airflow/pull/8954#discussion_r438223116
        url = os.path.join(
            instance_url,
            "v3",
            "namespaces",
            quote(namespace),
            "start",
        )
        runtime_args = runtime_args or {}
        body = [
            {
                "appId": pipeline_name,
                "programType": "workflow",
                "programId": "DataPipelineWorkflow",
                "runtimeargs": runtime_args,
            }
        ]
        response = self._cdap_request(url=url, method="POST", body=body)
        self._check_response_status_and_data(
            response, f"Starting a pipeline failed with code {response.status}"
        )
        response_json = json.loads(response.data)
        return response_json[0]["runId"]

    def stop_pipeline(self, pipeline_name: str, instance_url: str, namespace: str = "default") -> None:
        """
        Stops a Cloud Data Fusion pipeline. Works for both batch and stream pipelines.

        :param pipeline_name: Your pipeline name.
        :param instance_url: Endpoint on which the REST APIs is accessible for the instance.
        :param namespace: f your pipeline belongs to a Basic edition instance, the namespace ID
            is always default. If your pipeline belongs to an Enterprise edition instance, you
            can create a namespace.
        """
        url = os.path.join(
            self._base_url(instance_url, namespace),
            quote(pipeline_name),
            "workflows",
            "DataPipelineWorkflow",
            "stop",
        )
        response = self._cdap_request(url=url, method="POST")
        self._check_response_status_and_data(
            response, f"Stopping a pipeline failed with code {response.status}"
        )
