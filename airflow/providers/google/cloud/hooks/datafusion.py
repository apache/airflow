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
This module contains Google DataFusion hook.
"""
import json
import os
from time import sleep
from typing import Any, Dict, Optional
from urllib.parse import urlencode

import google.auth
from googleapiclient.discovery import Resource, build

from airflow import AirflowException
from airflow.providers.google.cloud.hooks.base import CloudBaseHook

Operation = Dict[str, Any]


class DataFusionHook(CloudBaseHook):
    """
    Hook for Google DataFusion.
    """

    _conn = None  # type: Optional[Resource]

    def __init__(
        self,
        api_version: str = "v1beta1",
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: Optional[str] = None,
    ) -> None:
        super().__init__(gcp_conn_id, delegate_to)
        self.api_version = api_version

    def wait_for_operation(self, operation: Dict[str, Any]) -> Dict[str, Any]:
        """
        Waits for long-lasting operation to complete.
        """
        while not operation.get("done"):
            sleep(30)
            operation = (
                self.get_conn()  # pylint: disable=no-member
                .projects()
                .locations()
                .operations()
                .get(name=operation.get("name"))
                .execute()
            )
        if "error" in operation:
            raise AirflowException(operation["error"])
        return operation["response"]

    @staticmethod
    def _name(project_id: str, location: str, instance_name: str) -> str:
        return f"projects/{project_id}/locations/{location}/instances/{instance_name}"

    @staticmethod
    def _parent(project_id: str, location: str) -> str:
        return f"projects/{project_id}/locations/{location}"

    def _cdap_request(
        self, url: str, method: str, body: Optional[Dict[str, Any]] = None
    ) -> google.auth.transport.Response:
        headers: Dict[str, str] = {"Content-Type": "application/json"}
        request = google.auth.transport.requests.Request()

        credentials = self._get_credentials()
        credentials.before_request(
            request=request, method=method, url=url, headers=headers
        )

        payload = json.dumps(body) if body else None
        response = request(method=method, url=url, headers=headers, body=payload)
        return response

    def get_conn(self) -> Resource:
        """
        Retrieves connection to DataFusion.
        """
        if not self._conn:
            http_authorized = self._authorize()
            self._conn = build(
                "datafusion",
                self.api_version,
                http=http_authorized,
                cache_discovery=False,
            )
        return self._conn

    @CloudBaseHook.fallback_to_default_project_id
    def restart_instance(
        self, instance_name: str, location: str, project_id: Optional[str] = None
    ) -> Operation:
        """
        Restart a single Data Fusion instance.
        At the end of an operation instance is fully restarted.

        :param instance_name: The name of the instance to restart.
        :type instance_name: str
        :param location: The Cloud Data Fusion location in which to handle the request.
        :type location: str
        :param project_id: The ID of the Google Cloud Platform project that the instance belongs to.
        :type project_id: str
        """
        if not project_id:
            raise ValueError("The project_id should be set")

        operation = (
            self.get_conn()  # pylint: disable=no-member
            .projects()
            .locations()
            .instances()
            .restart(name=self._name(project_id, location, instance_name))
            .execute(num_retries=self.num_retries)
        )
        return operation

    @CloudBaseHook.fallback_to_default_project_id
    def delete_instance(
        self, instance_name: str, location: str, project_id: Optional[str] = None
    ) -> Operation:
        """
        Deletes a single Date Fusion instance.

        :param instance_name: The name of the instance to delete.
        :type instance_name: str
        :param location: The Cloud Data Fusion location in which to handle the request.
        :type location: str
        :param project_id: The ID of the Google Cloud Platform project that the instance belongs to.
        :type project_id: str
        """
        if not project_id:
            raise ValueError("The project_id should be set")

        operation = (
            self.get_conn()  # pylint: disable=no-member
            .projects()
            .locations()
            .instances()
            .delete(name=self._name(project_id, location, instance_name))
            .execute(num_retries=self.num_retries)
        )
        return operation

    @CloudBaseHook.fallback_to_default_project_id
    def create_instance(
        self,
        instance_name: str,
        instance: Dict[str, Any],
        location: str,
        project_id: Optional[str] = None,
    ) -> Operation:
        """
        Creates a new Data Fusion instance in the specified project and location.

        :param instance_name: The name of the instance to create.
        :type instance_name: str
        :param instance: An instance of Instance.
            https://cloud.google.com/data-fusion/docs/reference/rest/v1beta1/projects.locations.instances#Instance
        :type instance: Dict[str, Any]
        :param location: The Cloud Data Fusion location in which to handle the request.
        :type location: str
        :param project_id: The ID of the Google Cloud Platform project that the instance belongs to.
        :type project_id: str
        """
        if not project_id:
            raise ValueError("The project_id should be set")

        operation = (
            self.get_conn()  # pylint: disable=no-member
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

    @CloudBaseHook.fallback_to_default_project_id
    def get_instance(
        self, instance_name: str, location: str, project_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Gets details of a single Data Fusion instance.

        :param instance_name: The name of the instance.
        :type instance_name: str
        :param location: The Cloud Data Fusion location in which to handle the request.
        :type location: str
        :param project_id: The ID of the Google Cloud Platform project that the instance belongs to.
        :type project_id: str
        """
        if not project_id:
            raise ValueError("The project_id should be set")

        instance = (
            self.get_conn()  # pylint: disable=no-member
            .projects()
            .locations()
            .instances()
            .get(name=self._name(project_id, location, instance_name))
            .execute(num_retries=self.num_retries)
        )
        return instance

    @CloudBaseHook.fallback_to_default_project_id
    def patch_instance(
        self,
        instance_name: str,
        instance: Dict[str, Any],
        update_mask: str,
        location: str,
        project_id: Optional[str] = None,
    ) -> Operation:
        """
        Updates a single Data Fusion instance.

        :param instance_name: The name of the instance to create.
        :type instance_name: str
        :param instance: An instance of Instance.
            https://cloud.google.com/data-fusion/docs/reference/rest/v1beta1/projects.locations.instances#Instance
        :type instance: Dict[str, Any]
        :param update_mask: Field mask is used to specify the fields that the update will overwrite
            in an instance resource. The fields specified in the updateMask are relative to the resource,
            not the full request. A field will be overwritten if it is in the mask. If the user does not
            provide a mask, all the supported fields (labels and options currently) will be overwritten.
            A comma-separated list of fully qualified names of fields. Example: "user.displayName,photo".
            https://developers.google.com/protocol-buffers/docs/reference/google.protobuf?_ga=2.205612571.-968688242.1573564810#google.protobuf.FieldMask
        :type update_mask: str
        :param location: The Cloud Data Fusion location in which to handle the request.
        :type location: str
        :param project_id: The ID of the Google Cloud Platform project that the instance belongs to.
        :type project_id: str
        """
        if not project_id:
            raise ValueError("The project_id should be set")

        operation = (
            self.get_conn()  # pylint: disable=no-member
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
        pipeline: Dict[str, Any],
        instance_url: str,
        namespace: str = "default",
    ) -> None:
        """
        Creates a Cloud Data Fusion pipeline.

        :param pipeline_name: Your pipeline name.
        :type pipeline_name: str
        :param pipeline: The pipeline definition. For more information check:
            https://docs.cdap.io/cdap/current/en/developer-manual/pipelines/developing-pipelines.html#pipeline-configuration-file-format
        :type pipeline: Dict[str, Any]
        :param instance_url: Endpoint on which the REST APIs is accessible for the instance.
        :type instance_url: str
        :param namespace: f your pipeline belongs to a Basic edition instance, the namespace ID
            is always default. If your pipeline belongs to an Enterprise edition instance, you
            can create a namespace.
        :type namespace: str
        """
        url = os.path.join(
            instance_url, "v3", "namespaces", namespace, "apps", pipeline_name
        )
        response = self._cdap_request(url=url, method="PUT", body=pipeline)
        if response.status != 200:
            raise AirflowException(
                f"Creating a pipeline failed with code {response.status}"
            )

    def delete_pipeline(
        self,
        pipeline_name: str,
        instance_url: str,
        version_id: Optional[str] = None,
        namespace: str = "default",
    ) -> None:
        """
        Deletes a Cloud Data Fusion pipeline.

        :param pipeline_name: Your pipeline name.
        :type pipeline_name: str
        :param version_id: Version of pipeline to delete
        :type version_id: Optional[str]
        :param instance_url: Endpoint on which the REST APIs is accessible for the instance.
        :type instance_url: str
        :param namespace: f your pipeline belongs to a Basic edition instance, the namespace ID
            is always default. If your pipeline belongs to an Enterprise edition instance, you
            can create a namespace.
        :type namespace: str
        """
        url = os.path.join(
            instance_url, "v3", "namespaces", namespace, "apps", pipeline_name
        )
        if version_id:
            url = os.path.join(url, "versions", version_id)

        response = self._cdap_request(url=url, method="DELETE", body=None)
        if response.status != 200:
            raise AirflowException(
                f"Deleting a pipeline failed with code {response.status}"
            )

    def list_pipelines(
        self,
        instance_url: str,
        artifact_name: Optional[str] = None,
        artifact_version: Optional[str] = None,
        namespace: str = "default",
    ) -> Dict[Any, Any]:
        """
        Lists Cloud Data Fusion pipelines.

        :param artifact_version: Artifact version to filter instances
        :type artifact_version: Optional[str]
        :param artifact_name: Artifact name to filter instances
        :type artifact_name: Optional[str]
        :param instance_url: Endpoint on which the REST APIs is accessible for the instance.
        :type instance_url: str
        :param namespace: f your pipeline belongs to a Basic edition instance, the namespace ID
            is always default. If your pipeline belongs to an Enterprise edition instance, you
            can create a namespace.
        :type namespace: str
        """
        url = os.path.join(instance_url, "v3", "namespaces", namespace, "apps")
        query: Dict[str, str] = {}
        if artifact_name:
            query = {"artifactName": artifact_name}
        if artifact_version:
            query = {"artifactVersion": artifact_version}
        if query:
            url = os.path.join(url, urlencode(query))

        response = self._cdap_request(url=url, method="GET", body=None)
        if response.status != 200:
            raise AirflowException(
                f"Listing pipelines failed with code {response.status}"
            )
        return json.loads(response.data)

    def start_pipeline(
        self, pipeline_name: str, instance_url: str, namespace: str = "default"
    ) -> None:
        """
        Starts a Cloud Data Fusion pipeline. Works for both batch and stream pipelines.

        :param pipeline_name: Your pipeline name.
        :type pipeline_name: str
        :param instance_url: Endpoint on which the REST APIs is accessible for the instance.
        :type instance_url: str
        :param namespace: f your pipeline belongs to a Basic edition instance, the namespace ID
            is always default. If your pipeline belongs to an Enterprise edition instance, you
            can create a namespace.
        :type namespace: str
        """
        url = os.path.join(
            instance_url,
            "v3",
            "namespaces",
            namespace,
            "apps",
            pipeline_name,
            "workflows",
            "DataPipelineWorkflow",
            "start",
        )
        response = self._cdap_request(url=url, method="POST")
        if response.status != 200:
            raise AirflowException(
                f"Starting a pipeline failed with code {response.status}"
            )

    def stop_pipeline(
        self, pipeline_name: str, instance_url: str, namespace: str = "default"
    ) -> None:
        """
        Stops a Cloud Data Fusion pipeline. Works for both batch and stream pipelines.

        :param pipeline_name: Your pipeline name.
        :type pipeline_name: str
        :param instance_url: Endpoint on which the REST APIs is accessible for the instance.
        :type instance_url: str
        :param namespace: f your pipeline belongs to a Basic edition instance, the namespace ID
            is always default. If your pipeline belongs to an Enterprise edition instance, you
            can create a namespace.
        :type namespace: str
        """
        url = os.path.join(
            instance_url,
            "v3",
            "namespaces",
            namespace,
            "apps",
            pipeline_name,
            "workflows",
            "DataPipelineWorkflow",
            "stop",
        )
        response = self._cdap_request(url=url, method="POST")
        if response.status != 200:
            raise AirflowException(
                f"Stopping a pipeline failed with code {response.status}"
            )
