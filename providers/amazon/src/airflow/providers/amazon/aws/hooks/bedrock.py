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
from __future__ import annotations

from typing import Any

from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.utils.helpers import prune_dict


class BedrockHook(AwsBaseHook):
    """
    Interact with Amazon Bedrock.

    Provide thin wrapper around :external+boto3:py:class:`boto3.client("bedrock") <Bedrock.Client>`.

    Additional arguments (such as ``aws_conn_id``) may be specified and
    are passed down to the underlying AwsBaseHook.

    .. seealso::
        - :class:`airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`
    """

    client_type = "bedrock"

    def __init__(self, *args, **kwargs) -> None:
        kwargs["client_type"] = self.client_type
        super().__init__(*args, **kwargs)

    def get_guardrail_id_by_name(self, guardrail_name: str) -> str | None:
        """Get the guardrail ID by name, or None if not found."""
        paginator = self.conn.get_paginator("list_guardrails")
        for page in paginator.paginate():
            for g in page.get("guardrails", []):
                if g.get("name") == guardrail_name:
                    return g["id"]
        return None


class BedrockRuntimeHook(AwsBaseHook):
    """
    Interact with the Amazon Bedrock Runtime.

    Provide thin wrapper around :external+boto3:py:class:`boto3.client("bedrock-runtime") <BedrockRuntime.Client>`.

    Additional arguments (such as ``aws_conn_id``) may be specified and
    are passed down to the underlying AwsBaseHook.

    .. seealso::
        - :class:`airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`
    """

    client_type = "bedrock-runtime"

    def __init__(self, *args, **kwargs) -> None:
        kwargs["client_type"] = self.client_type
        super().__init__(*args, **kwargs)


class BedrockAgentHook(AwsBaseHook):
    """
    Interact with the Amazon Agents for Bedrock API.

    Provide thin wrapper around :external+boto3:py:class:`boto3.client("bedrock-agent") <AgentsforBedrock.Client>`.

    Additional arguments (such as ``aws_conn_id``) may be specified and
    are passed down to the underlying AwsBaseHook.

    .. seealso::
        - :class:`airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`
    """

    client_type = "bedrock-agent"

    def __init__(self, *args, **kwargs) -> None:
        kwargs["client_type"] = self.client_type
        super().__init__(*args, **kwargs)


class BedrockAgentRuntimeHook(AwsBaseHook):
    """
    Interact with the Amazon Agents for Bedrock API.

    Provide thin wrapper around :external+boto3:py:class:`boto3.client("bedrock-agent-runtime") <AgentsforBedrockRuntime.Client>`.

    Additional arguments (such as ``aws_conn_id``) may be specified and
    are passed down to the underlying AwsBaseHook.

    .. seealso::
        - :class:`airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`
    """

    client_type = "bedrock-agent-runtime"

    def __init__(self, *args, **kwargs) -> None:
        kwargs["client_type"] = self.client_type
        super().__init__(*args, **kwargs)


class BedrockAgentCoreControlHook(AwsBaseHook):
    """
    Interact with the Amazon Bedrock AgentCore control plane API.

    Provide thin wrapper around :external+boto3:py:class:`boto3.client("bedrock-agentcore-control") <BedrockAgentCoreControl.Client>`.

    Additional arguments (such as ``aws_conn_id``) may be specified and
    are passed down to the underlying AwsBaseHook.

    .. seealso::
        - :class:`airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`
    """

    client_type = "bedrock-agentcore-control"

    def __init__(self, *args, **kwargs) -> None:
        kwargs["client_type"] = self.client_type
        super().__init__(*args, **kwargs)

    def create_agent_runtime(
        self,
        *,
        agent_runtime_name: str,
        agent_runtime_artifact: dict[str, Any],
        role_arn: str,
        network_configuration: dict[str, Any],
        create_agent_runtime_kwargs: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Create an Amazon Bedrock AgentCore Runtime."""
        return self.conn.create_agent_runtime(
            agentRuntimeName=agent_runtime_name,
            agentRuntimeArtifact=agent_runtime_artifact,
            roleArn=role_arn,
            networkConfiguration=network_configuration,
            **(create_agent_runtime_kwargs or {}),
        )

    def get_agent_runtime(self, *, agent_runtime_id: str, agent_runtime_version: str) -> dict[str, Any]:
        """Get an Amazon Bedrock AgentCore Runtime."""
        return self.conn.get_agent_runtime(
            agentRuntimeId=agent_runtime_id,
            agentRuntimeVersion=agent_runtime_version,
        )

    def delete_agent_runtime(self, *, agent_runtime_id: str) -> dict[str, Any]:
        """Delete an Amazon Bedrock AgentCore Runtime."""
        return self.conn.delete_agent_runtime(agentRuntimeId=agent_runtime_id)


class BedrockAgentCoreHook(AwsBaseHook):
    """
    Interact with the Amazon Bedrock AgentCore runtime plane API.

    Provide thin wrapper around :external+boto3:py:class:`boto3.client("bedrock-agentcore") <BedrockAgentCore.Client>`.

    Additional arguments (such as ``aws_conn_id``) may be specified and
    are passed down to the underlying AwsBaseHook.

    .. seealso::
        - :class:`airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`
    """

    client_type = "bedrock-agentcore"

    def __init__(self, *args, **kwargs) -> None:
        kwargs["client_type"] = self.client_type
        super().__init__(*args, **kwargs)

    def invoke_agent_runtime(
        self,
        *,
        agent_runtime_arn: str,
        payload: bytes,
        content_type: str | None = None,
        accept: str | None = None,
        invoke_agent_runtime_kwargs: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Invoke an Amazon Bedrock AgentCore Runtime."""
        invoke_kwargs = prune_dict(
            {
                "agentRuntimeArn": agent_runtime_arn,
                "payload": payload,
                "contentType": content_type,
                "accept": accept,
                **(invoke_agent_runtime_kwargs or {}),
            }
        )
        return self.conn.invoke_agent_runtime(**invoke_kwargs)
