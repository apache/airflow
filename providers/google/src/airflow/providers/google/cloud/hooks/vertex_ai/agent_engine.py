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
"""This module contains a Google Cloud Vertex AI Agent Engine hook."""

from __future__ import annotations

import time
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

import google.auth.transport.requests
from asgiref.sync import sync_to_async
from vertexai import Client

from airflow.providers.google.common.hooks.base_google import (
    PROVIDE_PROJECT_ID,
    GoogleBaseAsyncHook,
    GoogleBaseHook,
)

if TYPE_CHECKING:
    from vertexai._genai import types


class AgentEngineHook(GoogleBaseHook):
    """Hook for Google Cloud Vertex AI Agent Engine APIs."""

    def __init__(
        self,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(
            gcp_conn_id=gcp_conn_id,
            impersonation_chain=impersonation_chain,
            **kwargs,
        )

    def get_agent_engine_client(self, project_id: str, location: str):
        """Return the Vertex AI Agent Engine client."""
        return Client(
            project=project_id,
            location=location,
            credentials=self.get_credentials(),
        ).agent_engines

    @staticmethod
    def build_agent_engine_name(project_id: str, location: str, agent_engine_id: str) -> str:
        """Build a fully qualified Agent Engine resource name."""
        return f"projects/{project_id}/locations/{location}/reasoningEngines/{agent_engine_id}"

    @GoogleBaseHook.fallback_to_default_project_id
    def create_agent_engine(
        self,
        location: str,
        agent: Any | None = None,
        config: types.AgentEngineConfigOrDict | None = None,
        project_id: str = PROVIDE_PROJECT_ID,
    ) -> types.AgentEngine:
        """
        Create an Agent Engine.

        :param location: Required. The ID of the Google Cloud location that the service belongs to.
        :param agent: Optional. The agent object to deploy.
        :param config: Optional. Configuration for the Agent Engine.
        :param project_id: Optional. The ID of the Google Cloud project. Defaults to the project
            configured in the connection.
        """
        client = self.get_agent_engine_client(project_id=project_id, location=location)
        return client.create(agent=agent, config=config)

    @GoogleBaseHook.fallback_to_default_project_id
    def get_agent_engine(
        self,
        location: str,
        agent_engine_id: str,
        project_id: str = PROVIDE_PROJECT_ID,
    ) -> types.AgentEngine:
        """
        Get an Agent Engine.

        :param location: Required. The ID of the Google Cloud location that the service belongs to.
        :param agent_engine_id: Required. The Agent Engine ID.
        :param project_id: Optional. The ID of the Google Cloud project. Defaults to the project
            configured in the connection.
        """
        client = self.get_agent_engine_client(project_id=project_id, location=location)
        name = self.build_agent_engine_name(project_id, location, agent_engine_id)
        return client.get(name=name)

    @GoogleBaseHook.fallback_to_default_project_id
    def query_agent_engine(
        self,
        location: str,
        agent_engine_id: str,
        config: types.RunQueryJobAgentEngineConfigOrDict,
        project_id: str = PROVIDE_PROJECT_ID,
    ) -> types.RunQueryJobResult:
        """
        Run a query job on an Agent Engine.

        :param location: Required. The ID of the Google Cloud location that the service belongs to.
        :param agent_engine_id: Required. The Agent Engine ID.
        :param config: Required. Configuration for the query job (``query``, ``output_gcs_uri``).
        :param project_id: Optional. The ID of the Google Cloud project. Defaults to the project
            configured in the connection.
        """
        client = self.get_agent_engine_client(project_id=project_id, location=location)
        name = self.build_agent_engine_name(project_id, location, agent_engine_id)
        return client.run_query_job(name=name, config=config)

    @GoogleBaseHook.fallback_to_default_project_id
    def update_agent_engine(
        self,
        location: str,
        agent_engine_id: str,
        config: types.AgentEngineConfigOrDict,
        agent: Any | None = None,
        project_id: str = PROVIDE_PROJECT_ID,
    ) -> types.AgentEngine:
        """
        Update an Agent Engine.

        :param location: Required. The ID of the Google Cloud location that the service belongs to.
        :param agent_engine_id: Required. The Agent Engine ID.
        :param config: Required. Configuration for the Agent Engine update.
        :param agent: Optional. The updated agent object to deploy.
        :param project_id: Optional. The ID of the Google Cloud project. Defaults to the project
            configured in the connection.
        """
        client = self.get_agent_engine_client(project_id=project_id, location=location)
        name = self.build_agent_engine_name(project_id, location, agent_engine_id)
        return client.update(name=name, agent=agent, config=config)

    @GoogleBaseHook.fallback_to_default_project_id
    def delete_agent_engine(
        self,
        location: str,
        agent_engine_id: str,
        force: bool | None = None,
        config: types.DeleteAgentEngineConfigOrDict | None = None,
        project_id: str = PROVIDE_PROJECT_ID,
    ) -> types.DeleteAgentEngineOperation:
        """
        Delete an Agent Engine.

        :param location: Required. The ID of the Google Cloud location that the service belongs to.
        :param agent_engine_id: Required. The Agent Engine ID.
        :param force: Optional. Whether to forcefully delete child resources. Defaults to ``False``
            when not specified.
        :param config: Optional. Additional deletion configuration.
        :param project_id: Optional. The ID of the Google Cloud project. Defaults to the project
            configured in the connection.
        """
        client = self.get_agent_engine_client(project_id=project_id, location=location)
        name = self.build_agent_engine_name(project_id, location, agent_engine_id)
        return client.delete(name=name, force=force, config=config)

    def get_agent_engine_operation(self, location: str, operation_name: str) -> dict[str, Any]:
        """Return a Vertex AI Agent Engine long-running operation."""
        url = (
            operation_name
            if operation_name.startswith("http")
            else f"https://{location}-aiplatform.googleapis.com/v1beta1/{operation_name}"
        )
        session = google.auth.transport.requests.AuthorizedSession(self.get_credentials())
        response = session.get(url)
        response.raise_for_status()
        return response.json()

    def wait_for_agent_engine_operation(
        self,
        location: str,
        operation_name: str,
        poll_interval: float = 30,
        timeout: float | None = None,
    ) -> None:
        """
        Wait until an Agent Engine operation completes.

        :param location: The ID of the Google Cloud location that the service belongs to.
        :param operation_name: The Agent Engine operation name.
        :param poll_interval: Time, in seconds, to wait between checks.
        :param timeout: Optional timeout, in seconds.
        """
        start_time = time.monotonic()
        while True:
            operation = self.get_agent_engine_operation(location=location, operation_name=operation_name)
            if operation.get("done"):
                if operation.get("error"):
                    raise RuntimeError(
                        f"Agent Engine operation {operation_name} failed: {operation['error']}"
                    )
                return
            if timeout is not None and time.monotonic() - start_time > timeout:
                raise TimeoutError(f"Timed out waiting for Agent Engine operation {operation_name}")
            self.log.info("Waiting for Agent Engine operation %s to complete.", operation_name)
            time.sleep(poll_interval)


class AgentEngineAsyncHook(GoogleBaseAsyncHook):
    """Async hook for Google Cloud Vertex AI Agent Engine APIs."""

    sync_hook_class = AgentEngineHook

    def __init__(
        self,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ):
        super().__init__(
            gcp_conn_id=gcp_conn_id,
            impersonation_chain=impersonation_chain,
            **kwargs,
        )

    async def get_agent_engine_operation(self, location: str, operation_name: str) -> dict[str, Any]:
        """Return a Vertex AI Agent Engine long-running operation."""
        sync_hook = await self.get_sync_hook()
        return await sync_to_async(sync_hook.get_agent_engine_operation)(
            location=location,
            operation_name=operation_name,
        )
