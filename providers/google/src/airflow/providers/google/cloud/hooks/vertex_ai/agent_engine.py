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

import json
import time
from collections.abc import Sequence
from typing import Any

import google.auth.transport.requests
from asgiref.sync import sync_to_async
from google.genai.errors import ClientError
from vertexai import Client

from airflow.providers.google.common.hooks.base_google import (
    PROVIDE_PROJECT_ID,
    GoogleBaseAsyncHook,
    GoogleBaseHook,
)


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

    @GoogleBaseHook.fallback_to_default_project_id
    def create_agent_engine(
        self,
        location: str,
        agent: Any | None = None,
        agent_engine: Any | None = None,
        config: Any | None = None,
        project_id: str = PROVIDE_PROJECT_ID,
    ) -> Any:
        """
        Create an Agent Engine.

        :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
        :param location: Required. The ID of the Google Cloud location that the service belongs to.
        :param agent: Optional. The agent object to deploy.
        :param agent_engine: Optional. Deprecated alias for ``agent``.
        :param config: Optional. Configuration for the Agent Engine.
        """
        client = self.get_agent_engine_client(project_id=project_id, location=location)
        return client.create(agent=agent, agent_engine=agent_engine, config=config)

    @GoogleBaseHook.fallback_to_default_project_id
    def get_agent_engine(
        self,
        location: str,
        name: str,
        project_id: str = PROVIDE_PROJECT_ID,
    ) -> Any:
        """
        Get an Agent Engine.

        :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
        :param location: Required. The ID of the Google Cloud location that the service belongs to.
        :param name: Required. The Agent Engine resource name.
        """
        client = self.get_agent_engine_client(project_id=project_id, location=location)
        return client.get(name=name)

    @GoogleBaseHook.fallback_to_default_project_id
    def query_agent_engine(
        self,
        location: str,
        name: str,
        config: Any | None = None,
        project_id: str = PROVIDE_PROJECT_ID,
    ) -> Any:
        """
        Query an Agent Engine.

        :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
        :param location: Required. The ID of the Google Cloud location that the service belongs to.
        :param name: Required. The Agent Engine resource name.
        :param config: Optional. Configuration for the query request (``class_method``, ``input``).
        """
        # Use the REST API directly rather than the SDK's run_query_job (requires GCS) or
        # _query (private method; triggers a Pydantic parsing bug in google-genai 2.8.0 when
        # the response output type is Any).
        cfg = config if isinstance(config, dict) else {}
        body: dict[str, Any] = {"classMethod": cfg.get("class_method", "query")}
        if "input" in cfg:
            input_val = cfg["input"]
            if isinstance(input_val, str):
                try:
                    input_val = json.loads(input_val)
                except json.JSONDecodeError as err:
                    raise ValueError("Agent Engine query input must be a JSON object.") from err
            if not isinstance(input_val, dict):
                raise ValueError("Agent Engine query input must be a JSON object.")
            body["input"] = input_val

        url = f"https://{location}-aiplatform.googleapis.com/v1beta1/{name}:query"
        session = google.auth.transport.requests.AuthorizedSession(self.get_credentials())
        response = session.post(url, json=body)
        response.raise_for_status()
        data = response.json()
        return data.get("output", data)

    @GoogleBaseHook.fallback_to_default_project_id
    def update_agent_engine(
        self,
        location: str,
        name: str,
        config: Any,
        agent: Any | None = None,
        agent_engine: Any | None = None,
        project_id: str = PROVIDE_PROJECT_ID,
    ) -> Any:
        """
        Update an Agent Engine.

        :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
        :param location: Required. The ID of the Google Cloud location that the service belongs to.
        :param name: Required. The Agent Engine resource name.
        :param agent: Optional. The updated agent object to deploy.
        :param agent_engine: Optional. Deprecated alias for ``agent``.
        :param config: Required. Configuration for the Agent Engine update.
        """
        client = self.get_agent_engine_client(project_id=project_id, location=location)
        return client.update(name=name, agent=agent, agent_engine=agent_engine, config=config)

    @GoogleBaseHook.fallback_to_default_project_id
    def delete_agent_engine(
        self,
        location: str,
        name: str,
        force: bool | None = None,
        config: Any | None = None,
        project_id: str = PROVIDE_PROJECT_ID,
    ) -> Any:
        """
        Delete an Agent Engine.

        :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
        :param location: Required. The ID of the Google Cloud location that the service belongs to.
        :param name: Required. The Agent Engine resource name.
        :param force: Optional. Whether to delete child resources.
        :param config: Optional. Additional deletion configuration.
        """
        client = self.get_agent_engine_client(project_id=project_id, location=location)
        return client.delete(name=name, force=force, config=config)

    def is_agent_engine_deleted(self, project_id: str, location: str, name: str) -> bool:
        """Return whether an Agent Engine no longer exists."""
        try:
            self.get_agent_engine(project_id=project_id, location=location, name=name)
        except ClientError as err:
            if getattr(err, "code", None) == 404:
                return True
            raise
        return False

    def wait_for_agent_engine_deleted(
        self,
        project_id: str,
        location: str,
        name: str,
        poll_interval: float,
        timeout: float | None = None,
    ) -> None:
        """
        Wait until an Agent Engine no longer exists.

        :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
        :param location: Required. The ID of the Google Cloud location that the service belongs to.
        :param name: Required. The Agent Engine resource name.
        :param poll_interval: Time, in seconds, to wait between checks.
        :param timeout: Optional timeout, in seconds.
        """
        start_time = time.monotonic()
        while True:
            if self.is_agent_engine_deleted(project_id=project_id, location=location, name=name):
                return
            if timeout is not None and time.monotonic() - start_time > timeout:
                raise TimeoutError(f"Timed out waiting for Agent Engine {name} to be deleted")
            self.log.info("Waiting for Agent Engine %s to be deleted.", name)
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

    async def is_agent_engine_deleted(self, project_id: str, location: str, name: str) -> bool:
        """Return whether an Agent Engine no longer exists."""
        sync_hook = await self.get_sync_hook()
        return await sync_to_async(sync_hook.is_agent_engine_deleted)(
            project_id=project_id,
            location=location,
            name=name,
        )
