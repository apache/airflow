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

import asyncio
import json
import os
import time
from typing import Any, Dict, Sequence
from urllib.parse import quote, urlencode, urljoin

import google.auth
from aiohttp import ClientSession
from gcloud.aio.auth import AioSession, Token
from google.api_core.retry import exponential_sleep_generator
from googleapiclient.discovery import Resource, build

from airflow.exceptions import AirflowException, AirflowNotFoundException
from airflow.providers.google.cloud.utils.datafusion import DataFusionPipelineType
from airflow.providers.google.common.hooks.base_google import (
    PROVIDE_PROJECT_ID,
    GoogleBaseAsyncHook,
    GoogleBaseHook,
)

Operation = Dict[str, Any]


class ConflictException(AirflowException):
    """Exception to catch 409 error."""

    pass


class PipelineStates:
    """Data Fusion pipeline states."""

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

    _conn: Resource | None = None

    def __init__(
        self,
        api_version: str = "v1beta1",
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
        self.api_version = api_version

    def wait_for_operation(self, operation: dict[str, Any]) -> dict[str, Any]:
        """Wait for long-lasting operation to complete."""
        for time_to_wait in exponential_sleep_generator(initial=10, maximum=120):
            time.sleep(time_to_wait)
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
        pipeline_type: DataFusionPipelineType = DataFusionPipelineType.BATCH,
        namespace: str = "default",
        success_states: list[str] | None = None,
        failure_states: list[str] | None = None,
        timeout: int = 5 * 60,
    ) -> None:
        """Poll for pipeline state and raises an exception if the state fails or times out."""
        failure_states = failure_states or FAILURE_STATES
        success_states = success_states or SUCCESS_STATES
        start_time = time.monotonic()
        current_state = None
        while time.monotonic() - start_time < timeout:
            try:
                workflow = self.get_pipeline_workflow(
                    pipeline_name=pipeline_name,
                    pipeline_id=pipeline_id,
                    pipeline_type=pipeline_type,
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
            time.sleep(30)

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
        self, url: str, method: str, body: list | dict | None = None, params: dict | None = None
    ) -> google.auth.transport.Response:
        headers: dict[str, str] = {"Content-Type": "application/json"}
        request = google.auth.transport.requests.Request()

        credentials = self.get_credentials()
        credentials.before_request(request=request, method=method, url=url, headers=headers)

        payload = json.dumps(body) if body else None

        response = request(method=method, url=url, headers=headers, body=payload, params=params)
        return response

    @staticmethod
    def _check_response_status_and_data(response, message: str) -> None:
        if response.status == 404:
            raise AirflowNotFoundException(message)
        elif response.status == 409:
            raise ConflictException("Conflict: Resource is still in use.")
        elif response.status != 200:
            raise AirflowException(message)
        if response.data is None:
            raise AirflowException(
                "Empty response received. Please, check for possible root "
                "causes of this behavior either in DAG code or on Cloud DataFusion side"
            )

    def get_conn(self) -> Resource:
        """Retrieve connection to DataFusion."""
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
        Delete a single Date Fusion instance.

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
        Create a new Data Fusion instance in the specified project and location.

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
        Get details of a single Data Fusion instance.

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

    def get_instance_artifacts(
        self, instance_url: str, namespace: str = "default", scope: str = "SYSTEM"
    ) -> Any:
        url = os.path.join(
            instance_url,
            "v3",
            "namespaces",
            quote(namespace),
            "artifacts",
        )
        response = self._cdap_request(url=url, method="GET", params={"scope": scope})
        self._check_response_status_and_data(
            response, f"Retrieving an instance artifacts failed with code {response.status}"
        )
        content = json.loads(response.data)
        return content

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
        Update a single Data Fusion instance.

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
        Create a batch Cloud Data Fusion pipeline.

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
        Delete a batch Cloud Data Fusion pipeline.

        :param pipeline_name: Your pipeline name.
        :param version_id: Version of pipeline to delete
        :param instance_url: Endpoint on which the REST APIs is accessible for the instance.
        :param namespace: if your pipeline belongs to a Basic edition instance, the namespace ID
            is always default. If your pipeline belongs to an Enterprise edition instance, you
            can create a namespace.
        """
        url = os.path.join(self._base_url(instance_url, namespace), quote(pipeline_name))
        if version_id:
            url = os.path.join(url, "versions", version_id)

        for time_to_wait in exponential_sleep_generator(initial=1, maximum=10):
            try:
                response = self._cdap_request(url=url, method="DELETE", body=None)
                self._check_response_status_and_data(
                    response, f"Deleting a pipeline failed with code {response.status}: {response.data}"
                )
            except ConflictException as exc:
                self.log.info(exc)
                time.sleep(time_to_wait)
            else:
                if response.status == 200:
                    break

    def list_pipelines(
        self,
        instance_url: str,
        artifact_name: str | None = None,
        artifact_version: str | None = None,
        namespace: str = "default",
    ) -> dict:
        """
        List Cloud Data Fusion pipelines.

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
        pipeline_type: DataFusionPipelineType = DataFusionPipelineType.BATCH,
        namespace: str = "default",
    ) -> Any:
        url = os.path.join(
            self._base_url(instance_url, namespace),
            quote(pipeline_name),
            f"{self.cdap_program_type(pipeline_type=pipeline_type)}s",
            self.cdap_program_id(pipeline_type=pipeline_type),
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
        pipeline_type: DataFusionPipelineType = DataFusionPipelineType.BATCH,
        namespace: str = "default",
        runtime_args: dict[str, Any] | None = None,
    ) -> str:
        """
        Start a Cloud Data Fusion pipeline. Works for both batch and stream pipelines.

        :param pipeline_name: Your pipeline name.
        :param pipeline_type: Optional pipeline type (BATCH by default).
        :param instance_url: Endpoint on which the REST APIs is accessible for the instance.
        :param runtime_args: Optional runtime JSON args to be passed to the pipeline
        :param namespace: if your pipeline belongs to a Basic edition instance, the namespace ID
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
                "runtimeargs": runtime_args,
                "programType": self.cdap_program_type(pipeline_type=pipeline_type),
                "programId": self.cdap_program_id(pipeline_type=pipeline_type),
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
        Stop a Cloud Data Fusion pipeline. Works for both batch and stream pipelines.

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

    @staticmethod
    def cdap_program_type(pipeline_type: DataFusionPipelineType) -> str:
        """
        Retrieve CDAP Program type depending on the pipeline type.

        :param pipeline_type: Pipeline type.
        """
        program_types = {
            DataFusionPipelineType.BATCH: "workflow",
            DataFusionPipelineType.STREAM: "spark",
        }
        return program_types.get(pipeline_type, "")

    @staticmethod
    def cdap_program_id(pipeline_type: DataFusionPipelineType) -> str:
        """
        Retrieve CDAP Program id depending on the pipeline type.

        :param pipeline_type: Pipeline type.
        """
        program_ids = {
            DataFusionPipelineType.BATCH: "DataPipelineWorkflow",
            DataFusionPipelineType.STREAM: "DataStreamsSparkStreaming",
        }
        return program_ids.get(pipeline_type, "")


class DataFusionAsyncHook(GoogleBaseAsyncHook):
    """Class to get asynchronous hook for DataFusion."""

    sync_hook_class = DataFusionHook
    scopes = ["https://www.googleapis.com/auth/cloud-platform"]

    def __init__(self, **kwargs):
        if kwargs.get("delegate_to") is not None:
            raise RuntimeError(
                "The `delegate_to` parameter has been deprecated before and finally removed in this version"
                " of Google Provider. You MUST convert it to `impersonate_chain`"
            )
        super().__init__(**kwargs)

    @staticmethod
    def _base_url(instance_url: str, namespace: str) -> str:
        return urljoin(f"{instance_url}/", f"v3/namespaces/{quote(namespace)}/apps/")

    async def _get_link(self, url: str, session):
        # Adding sleep generator to catch 404 in case if pipeline was not retrieved during first attempt.
        for time_to_wait in exponential_sleep_generator(initial=10, maximum=120):
            async with Token(scopes=self.scopes) as token:
                session_aio = AioSession(session)
                headers = {
                    "Authorization": f"Bearer {await token.get()}",
                }
                try:
                    pipeline = await session_aio.get(url=url, headers=headers)
                    break
                except Exception as exc:
                    if "404" in str(exc):
                        await asyncio.sleep(time_to_wait)
                    else:
                        raise
        if pipeline:
            return pipeline
        else:
            raise AirflowException("Could not retrieve pipeline. Aborting.")

    async def get_pipeline(
        self,
        instance_url: str,
        namespace: str,
        pipeline_name: str,
        pipeline_id: str,
        session,
        pipeline_type: DataFusionPipelineType = DataFusionPipelineType.BATCH,
    ):
        program_type = self.sync_hook_class.cdap_program_type(pipeline_type=pipeline_type)
        program_id = self.sync_hook_class.cdap_program_id(pipeline_type=pipeline_type)
        base_url_link = self._base_url(instance_url, namespace)
        url = urljoin(
            base_url_link, f"{quote(pipeline_name)}/{program_type}s/{program_id}/runs/{quote(pipeline_id)}"
        )
        return await self._get_link(url=url, session=session)

    async def get_pipeline_status(
        self,
        pipeline_name: str,
        instance_url: str,
        pipeline_id: str,
        pipeline_type: DataFusionPipelineType = DataFusionPipelineType.BATCH,
        namespace: str = "default",
        success_states: list[str] | None = None,
    ) -> str:
        """
        Get a Cloud Data Fusion pipeline status asynchronously.

        :param pipeline_name: Your pipeline name.
        :param instance_url: Endpoint on which the REST APIs is accessible for the instance.
        :param pipeline_id: Unique pipeline ID associated with specific pipeline.
        :param pipeline_type: Optional pipeline type (by default batch).
        :param namespace: if your pipeline belongs to a Basic edition instance, the namespace ID
            is always default. If your pipeline belongs to an Enterprise edition instance, you
            can create a namespace.
        :param success_states: If provided the operator will wait for pipeline to be in one of
            the provided states.
        """
        success_states = success_states or SUCCESS_STATES
        async with ClientSession() as session:
            try:
                pipeline = await self.get_pipeline(
                    instance_url=instance_url,
                    namespace=namespace,
                    pipeline_name=pipeline_name,
                    pipeline_id=pipeline_id,
                    pipeline_type=pipeline_type,
                    session=session,
                )
                pipeline = await pipeline.json(content_type=None)
                current_pipeline_state = pipeline["status"]

                if current_pipeline_state in success_states:
                    pipeline_status = "success"
                elif current_pipeline_state in FAILURE_STATES:
                    pipeline_status = "failed"
                else:
                    pipeline_status = "pending"
            except OSError:
                pipeline_status = "pending"
            except Exception as e:
                self.log.info("Retrieving pipeline status finished with errors...")
                pipeline_status = str(e)
            return pipeline_status
