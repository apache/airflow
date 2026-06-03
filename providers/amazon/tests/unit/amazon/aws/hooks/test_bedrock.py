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

from unittest import mock

import pytest

from airflow.providers.amazon.aws.hooks.bedrock import (
    BedrockAgentCoreControlHook,
    BedrockAgentCoreHook,
    BedrockAgentHook,
    BedrockAgentRuntimeHook,
    BedrockHook,
    BedrockRuntimeHook,
)


class TestBedrockHooks:
    @pytest.mark.parametrize(
        ("test_hook", "service_name"),
        [
            pytest.param(BedrockHook(), "bedrock", id="bedrock"),
            pytest.param(BedrockRuntimeHook(), "bedrock-runtime", id="bedrock-runtime"),
            pytest.param(BedrockAgentHook(), "bedrock-agent", id="bedrock-agent"),
            pytest.param(BedrockAgentRuntimeHook(), "bedrock-agent-runtime", id="bedrock-agent-runtime"),
            pytest.param(BedrockAgentCoreControlHook(), "bedrock-agentcore-control", id="agentcore-control"),
            pytest.param(BedrockAgentCoreHook(), "bedrock-agentcore", id="agentcore"),
        ],
    )
    def test_bedrock_hooks(self, test_hook, service_name):
        assert test_hook.conn is not None
        assert test_hook.conn.meta.service_model.service_name == service_name


class TestBedrockAgentCoreControlHook:
    @mock.patch.object(BedrockAgentCoreControlHook, "conn", new_callable=mock.PropertyMock)
    def test_create_agent_runtime(self, mock_conn):
        mock_client = mock.MagicMock()
        mock_conn.return_value = mock_client
        mock_client.create_agent_runtime.return_value = {"agentRuntimeArn": "runtime_arn"}

        result = BedrockAgentCoreControlHook().create_agent_runtime(
            agent_runtime_name="runtime_name",
            agent_runtime_artifact={"containerConfiguration": {"containerUri": "image_uri"}},
            role_arn="role_arn",
            network_configuration={"networkMode": "PUBLIC"},
            create_agent_runtime_kwargs={"description": "runtime description"},
        )

        assert result == {"agentRuntimeArn": "runtime_arn"}
        mock_client.create_agent_runtime.assert_called_once_with(
            agentRuntimeName="runtime_name",
            agentRuntimeArtifact={"containerConfiguration": {"containerUri": "image_uri"}},
            roleArn="role_arn",
            networkConfiguration={"networkMode": "PUBLIC"},
            description="runtime description",
        )

    @mock.patch.object(BedrockAgentCoreControlHook, "conn", new_callable=mock.PropertyMock)
    def test_get_agent_runtime(self, mock_conn):
        mock_client = mock.MagicMock()
        mock_conn.return_value = mock_client
        mock_client.get_agent_runtime.return_value = {"status": "READY"}

        result = BedrockAgentCoreControlHook().get_agent_runtime(
            agent_runtime_id="runtime_id",
            agent_runtime_version="1",
        )

        assert result == {"status": "READY"}
        mock_client.get_agent_runtime.assert_called_once_with(
            agentRuntimeId="runtime_id",
            agentRuntimeVersion="1",
        )

    @mock.patch.object(BedrockAgentCoreControlHook, "conn", new_callable=mock.PropertyMock)
    def test_create_agent_runtime_no_extra_kwargs(self, mock_conn):
        mock_client = mock.MagicMock()
        mock_conn.return_value = mock_client
        mock_client.create_agent_runtime.return_value = {"agentRuntimeArn": "runtime_arn"}

        BedrockAgentCoreControlHook().create_agent_runtime(
            agent_runtime_name="runtime_name",
            agent_runtime_artifact={"containerConfiguration": {"containerUri": "image_uri"}},
            role_arn="role_arn",
            network_configuration={"networkMode": "PUBLIC"},
        )

        mock_client.create_agent_runtime.assert_called_once_with(
            agentRuntimeName="runtime_name",
            agentRuntimeArtifact={"containerConfiguration": {"containerUri": "image_uri"}},
            roleArn="role_arn",
            networkConfiguration={"networkMode": "PUBLIC"},
        )

    @mock.patch.object(BedrockAgentCoreControlHook, "conn", new_callable=mock.PropertyMock)
    def test_delete_agent_runtime(self, mock_conn):
        mock_client = mock.MagicMock()
        mock_conn.return_value = mock_client
        mock_client.delete_agent_runtime.return_value = {}

        result = BedrockAgentCoreControlHook().delete_agent_runtime(agent_runtime_id="runtime_id")

        assert result == {}
        mock_client.delete_agent_runtime.assert_called_once_with(agentRuntimeId="runtime_id")


class TestBedrockAgentCoreHook:
    @mock.patch.object(BedrockAgentCoreHook, "conn", new_callable=mock.PropertyMock)
    def test_invoke_agent_runtime(self, mock_conn):
        mock_client = mock.MagicMock()
        mock_conn.return_value = mock_client
        mock_client.invoke_agent_runtime.return_value = {"statusCode": 200}

        result = BedrockAgentCoreHook().invoke_agent_runtime(
            agent_runtime_arn="runtime_arn",
            payload=b'{"prompt": "hello"}',
            content_type="application/json",
            accept="application/json",
            invoke_agent_runtime_kwargs={"runtimeSessionId": "session_id", "qualifier": "1"},
        )

        assert result == {"statusCode": 200}
        mock_client.invoke_agent_runtime.assert_called_once_with(
            agentRuntimeArn="runtime_arn",
            payload=b'{"prompt": "hello"}',
            contentType="application/json",
            accept="application/json",
            runtimeSessionId="session_id",
            qualifier="1",
        )

    @mock.patch.object(BedrockAgentCoreHook, "conn", new_callable=mock.PropertyMock)
    def test_invoke_agent_runtime_prunes_none_content_type_and_accept(self, mock_conn):
        mock_client = mock.MagicMock()
        mock_conn.return_value = mock_client
        mock_client.invoke_agent_runtime.return_value = {"statusCode": 200}

        BedrockAgentCoreHook().invoke_agent_runtime(
            agent_runtime_arn="runtime_arn",
            payload=b"hello",
            content_type=None,
            accept=None,
        )

        mock_client.invoke_agent_runtime.assert_called_once_with(
            agentRuntimeArn="runtime_arn",
            payload=b"hello",
        )

    @mock.patch.object(BedrockAgentCoreHook, "conn", new_callable=mock.PropertyMock)
    def test_invoke_agent_runtime_no_extra_kwargs(self, mock_conn):
        mock_client = mock.MagicMock()
        mock_conn.return_value = mock_client
        mock_client.invoke_agent_runtime.return_value = {"statusCode": 200}

        BedrockAgentCoreHook().invoke_agent_runtime(
            agent_runtime_arn="runtime_arn",
            payload=b"hello",
            content_type="text/plain",
            accept="text/plain",
        )

        mock_client.invoke_agent_runtime.assert_called_once_with(
            agentRuntimeArn="runtime_arn",
            payload=b"hello",
            contentType="text/plain",
            accept="text/plain",
        )


class TestBedrockHookGetGuardrailIdByName:
    @mock.patch.object(BedrockHook, "conn", new_callable=mock.PropertyMock)
    def test_found(self, mock_conn):
        mock_client = mock.MagicMock()
        mock_paginator = mock.MagicMock()
        mock_paginator.paginate.return_value = [{"guardrails": [{"name": "my-guardrail", "id": "abc123"}]}]
        mock_client.get_paginator.return_value = mock_paginator
        mock_conn.return_value = mock_client

        hook = BedrockHook()
        assert hook.get_guardrail_id_by_name("my-guardrail") == "abc123"

    @mock.patch.object(BedrockHook, "conn", new_callable=mock.PropertyMock)
    def test_not_found(self, mock_conn):
        mock_client = mock.MagicMock()
        mock_paginator = mock.MagicMock()
        mock_paginator.paginate.return_value = [{"guardrails": []}]
        mock_client.get_paginator.return_value = mock_paginator
        mock_conn.return_value = mock_client

        hook = BedrockHook()
        assert hook.get_guardrail_id_by_name("nonexistent") is None
