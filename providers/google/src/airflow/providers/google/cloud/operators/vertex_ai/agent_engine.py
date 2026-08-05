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
"""This module contains Google Vertex AI Agent Engine operators."""

from __future__ import annotations

from collections.abc import Sequence
from functools import cached_property
from typing import TYPE_CHECKING, Any

from airflow.providers.common.compat.sdk import conf
from airflow.providers.google.cloud.hooks.vertex_ai.agent_engine import (
    AgentEngineHook,
    extract_operation_id,
    serialize_value,
)
from airflow.providers.google.cloud.operators.cloud_base import GoogleCloudBaseOperator
from airflow.providers.google.cloud.triggers.vertex_ai import AgentEngineQueryJobTrigger

if TYPE_CHECKING:
    from vertexai._genai import types

    from airflow.providers.common.compat.sdk import Context


def _serialize_agent_engine(agent_engine: types.AgentEngine) -> dict[str, Any]:
    api_resource = getattr(agent_engine, "api_resource", None)
    if api_resource is not None:
        return serialize_value(api_resource)
    return serialize_value(agent_engine)


class CreateAgentEngineOperator(GoogleCloudBaseOperator):
    """
    Create a Vertex AI Agent Engine.

    :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
    :param location: Required. The ID of the Google Cloud location that the service belongs to.
    :param agent: Optional. The agent object to deploy.
    :param config: Optional. Configuration for the Agent Engine.
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
    :param impersonation_chain: Optional service account to impersonate using short-term credentials.
    """

    template_fields = (
        "project_id",
        "location",
        "agent",
        "config",
        "gcp_conn_id",
        "impersonation_chain",
    )

    def __init__(
        self,
        *,
        project_id: str,
        location: str,
        agent: Any | None = None,
        config: types.AgentEngineConfigOrDict | None = None,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.project_id = project_id
        self.location = location
        self.agent = agent
        self.config = config
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    @cached_property
    def hook(self) -> AgentEngineHook:
        return AgentEngineHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )

    def execute(self, context: Context) -> dict[str, Any]:
        self.log.info("Creating Agent Engine.")
        agent_engine = self.hook.create_agent_engine(
            project_id=self.project_id,
            location=self.location,
            agent=self.agent,
            config=self.config,
        )
        result = _serialize_agent_engine(agent_engine)
        self.log.info("Agent Engine was created.")
        return result


class GetAgentEngineOperator(GoogleCloudBaseOperator):
    """
    Get a Vertex AI Agent Engine.

    :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
    :param location: Required. The ID of the Google Cloud location that the service belongs to.
    :param agent_engine_id: Required. The Agent Engine ID.
    :param config: Optional. Configuration for getting the Agent Engine.
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
    :param impersonation_chain: Optional service account to impersonate using short-term credentials.
    """

    template_fields = (
        "project_id",
        "location",
        "agent_engine_id",
        "config",
        "gcp_conn_id",
        "impersonation_chain",
    )

    def __init__(
        self,
        *,
        project_id: str,
        location: str,
        agent_engine_id: str,
        config: types.GetAgentEngineConfigOrDict | None = None,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.project_id = project_id
        self.location = location
        self.agent_engine_id = agent_engine_id
        self.config = config
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    @cached_property
    def hook(self) -> AgentEngineHook:
        return AgentEngineHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )

    def execute(self, context: Context) -> dict[str, Any]:
        self.log.info("Getting Agent Engine %s.", self.agent_engine_id)
        agent_engine = self.hook.get_agent_engine(
            project_id=self.project_id,
            location=self.location,
            agent_engine_id=self.agent_engine_id,
            config=self.config,
        )
        result = _serialize_agent_engine(agent_engine)
        self.log.info("Agent Engine %s was retrieved.", self.agent_engine_id)
        return result


class RunQueryJobOperator(GoogleCloudBaseOperator):
    """
    Run a query job on a Vertex AI Agent Engine.

    :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
    :param location: Required. The ID of the Google Cloud location that the service belongs to.
    :param agent_engine_id: Required. The Agent Engine ID.
    :param config: Optional. Configuration for the query job (``query``, ``output_gcs_uri``).
    :param check_config: Optional. Configuration for checking the query job.
    :param wait_for_completion: Whether to wait until the query job completes.
    :param poll_interval: Time, in seconds, to wait between checks.
    :param timeout: Optional timeout, in seconds.
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
    :param impersonation_chain: Optional service account to impersonate using short-term credentials.
    :param deferrable: Run operator in the deferrable mode.
    """

    template_fields = (
        "project_id",
        "location",
        "agent_engine_id",
        "config",
        "check_config",
        "gcp_conn_id",
        "impersonation_chain",
    )

    def __init__(
        self,
        *,
        project_id: str,
        location: str,
        agent_engine_id: str,
        config: types.RunQueryJobAgentEngineConfigOrDict | None = None,
        check_config: types.CheckQueryJobAgentEngineConfigOrDict | None = None,
        wait_for_completion: bool = True,
        poll_interval: float = 30,
        timeout: float | None = None,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.project_id = project_id
        self.location = location
        self.agent_engine_id = agent_engine_id
        self.config = config
        self.check_config = check_config
        self.wait_for_completion = wait_for_completion
        self.poll_interval = poll_interval
        self.timeout = timeout
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain
        self.deferrable = deferrable

    @cached_property
    def hook(self) -> AgentEngineHook:
        return AgentEngineHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )

    def execute(self, context: Context) -> dict[str, Any]:
        self.log.info("Running query job on Agent Engine %s.", self.agent_engine_id)
        query_job = self.hook.run_query_job(
            project_id=self.project_id,
            location=self.location,
            agent_engine_id=self.agent_engine_id,
            config=self.config,
        )
        result = serialize_value(query_job)
        self.log.info("Query job was started on Agent Engine %s.", self.agent_engine_id)
        if not self.wait_for_completion:
            return result

        operation_name = getattr(query_job, "job_name", None)
        if not operation_name:
            raise RuntimeError("Agent Engine query job did not include an operation name.")
        operation_id = extract_operation_id(operation_name)

        if self.deferrable:
            return self.defer(
                trigger=AgentEngineQueryJobTrigger(
                    project_id=self.project_id,
                    location=self.location,
                    operation_id=operation_id,
                    config=self.check_config,
                    gcp_conn_id=self.gcp_conn_id,
                    impersonation_chain=self.impersonation_chain,
                    poll_interval=self.poll_interval,
                    timeout=self.timeout,
                ),
                method_name="execute_complete",
            )

        completed_job = self.hook.wait_for_query_agent_engine_job(
            project_id=self.project_id,
            location=self.location,
            operation_id=operation_id,
            config=self.check_config,
            poll_interval=self.poll_interval,
            timeout=self.timeout,
        )
        self.log.info("Agent Engine query job %s completed.", operation_name)
        return serialize_value(completed_job)

    def execute_complete(self, context: Context, event: dict[str, Any] | None = None) -> dict[str, Any]:
        if event is None:
            raise RuntimeError("No event received in trigger callback")
        if event["status"] == "success":
            self.log.info("Agent Engine query job completed.")
            return event["query_job"]
        if event["status"] == "timeout":
            raise TimeoutError(event["message"])
        raise RuntimeError(event["message"])


class UpdateAgentEngineOperator(GoogleCloudBaseOperator):
    """
    Update a Vertex AI Agent Engine.

    :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
    :param location: Required. The ID of the Google Cloud location that the service belongs to.
    :param agent_engine_id: Required. The Agent Engine ID.
    :param agent: Optional. The updated agent object to deploy.
    :param config: Required. Configuration for the Agent Engine update.
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
    :param impersonation_chain: Optional service account to impersonate using short-term credentials.
    """

    template_fields = (
        "project_id",
        "location",
        "agent_engine_id",
        "agent",
        "config",
        "gcp_conn_id",
        "impersonation_chain",
    )

    def __init__(
        self,
        *,
        project_id: str,
        location: str,
        agent_engine_id: str,
        config: types.AgentEngineConfigOrDict,
        agent: Any | None = None,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.project_id = project_id
        self.location = location
        self.agent_engine_id = agent_engine_id
        self.agent = agent
        self.config = config
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    @cached_property
    def hook(self) -> AgentEngineHook:
        return AgentEngineHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )

    def execute(self, context: Context) -> dict[str, Any]:
        self.log.info("Updating Agent Engine %s.", self.agent_engine_id)
        agent_engine = self.hook.update_agent_engine(
            project_id=self.project_id,
            location=self.location,
            agent_engine_id=self.agent_engine_id,
            agent=self.agent,
            config=self.config,
        )
        result = _serialize_agent_engine(agent_engine)
        self.log.info("Agent Engine %s was updated.", self.agent_engine_id)
        return result


class DeleteAgentEngineOperator(GoogleCloudBaseOperator):
    """
    Delete a Vertex AI Agent Engine.

    :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
    :param location: Required. The ID of the Google Cloud location that the service belongs to.
    :param agent_engine_id: Required. The Agent Engine ID.
    :param force: Optional. Whether to delete child resources.
    :param config: Optional. Additional deletion configuration.
    :param wait_for_completion: Whether to wait until the delete operation completes.
    :param poll_interval: Time, in seconds, to wait between checks.
    :param timeout: Optional timeout, in seconds.
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
    :param impersonation_chain: Optional service account to impersonate using short-term credentials.
    """

    template_fields = (
        "project_id",
        "location",
        "agent_engine_id",
        "force",
        "config",
        "gcp_conn_id",
        "impersonation_chain",
    )

    def __init__(
        self,
        *,
        project_id: str,
        location: str,
        agent_engine_id: str,
        force: bool | None = None,
        config: types.DeleteAgentEngineConfigOrDict | None = None,
        wait_for_completion: bool = True,
        poll_interval: float = 30,
        timeout: float | None = None,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.project_id = project_id
        self.location = location
        self.agent_engine_id = agent_engine_id
        self.force = force
        self.config = config
        self.wait_for_completion = wait_for_completion
        self.poll_interval = poll_interval
        self.timeout = timeout
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    @cached_property
    def hook(self) -> AgentEngineHook:
        return AgentEngineHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )

    def execute(self, context: Context) -> dict[str, Any]:
        self.log.info("Deleting Agent Engine %s.", self.agent_engine_id)
        operation = self.hook.delete_agent_engine(
            project_id=self.project_id,
            location=self.location,
            agent_engine_id=self.agent_engine_id,
            force=self.force,
            config=self.config,
        )
        result = serialize_value(operation)
        if not self.wait_for_completion:
            return result

        operation_name = getattr(operation, "name", None)
        if not operation_name:
            raise RuntimeError("Delete Agent Engine operation did not include an operation name.")
        operation_id = extract_operation_id(operation_name)

        if getattr(operation, "done", False):
            if operation_error := getattr(operation, "error", None):
                raise RuntimeError(f"Agent Engine operation {operation_name} failed: {operation_error}")
            self.log.info("Agent Engine %s was deleted.", self.agent_engine_id)
            return result

        self.hook.wait_for_agent_engine_operation(
            project_id=self.project_id,
            location=self.location,
            operation_id=operation_id,
            poll_interval=self.poll_interval,
            timeout=self.timeout,
        )
        self.log.info("Agent Engine %s was deleted.", self.agent_engine_id)
        return result
