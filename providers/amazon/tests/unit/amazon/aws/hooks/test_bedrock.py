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

import io
import json
import subprocess
import sys
from unittest import mock

import pytest
from botocore.config import Config
from botocore.exceptions import ClientError
from botocore.session import Session

from airflow.providers.amazon.aws.hooks.bedrock import (
    BedrockAgentCoreControlHook,
    BedrockAgentCoreHook,
    BedrockAgentHook,
    BedrockAgentRuntimeHook,
    BedrockHook,
    BedrockRuntimeHook,
)

try:
    from airflow.providers.common.ai.exceptions import ManagedAgentInvocationError
    from airflow.providers.common.ai.managed_agents import (
        ManagedAgentCapabilities,
        ManagedAgentRef,
        ManagedAgentRequest,
    )
except ImportError:
    HAS_COMMON_AI = False
else:
    HAS_COMMON_AI = True

# The managed-agent methods of BedrockAgentCoreHook need the optional common.ai extra.
needs_common_ai = pytest.mark.skipif(not HAS_COMMON_AI, reason="requires apache-airflow-providers-common-ai")


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


ARN = "arn:aws:bedrock-agentcore:us-east-1:123456789012:runtime/investigator"
SESSION = "caller-session-0123456789abcdef0123456789abcdef"  # AgentCore needs 33 to 256 characters


@pytest.fixture
def invoke_agent_runtime():
    """The real hook and botocore client, stubbed at the API call."""
    with mock.patch.dict("os.environ", {"AWS_EC2_METADATA_DISABLED": "true"}):
        client = Session().create_client(
            "bedrock-agentcore",
            region_name="us-east-1",
            aws_access_key_id="test",
            aws_secret_access_key="test",
        )
        with mock.patch.object(BedrockAgentCoreHook, "get_client_type", autospec=True, return_value=client):
            with mock.patch.object(client, "invoke_agent_runtime", autospec=True) as call:
                yield call
        client.close()


def respond(call, body, **extra):
    stream = io.BytesIO(json.dumps(body).encode())
    call.return_value = {
        "response": stream,
        "contentType": "application/json",
        "runtimeSessionId": "session-from-aws",
        "ResponseMetadata": {"RequestId": "req-1"},
        **extra,
    }
    return stream


def hook(**kwargs) -> BedrockAgentCoreHook:
    return BedrockAgentCoreHook(aws_conn_id="aws_default", region_name="us-east-1", **kwargs)


@needs_common_ai
class TestBedrockAgentCoreHookAgent:
    def test_resolves_a_runtime_arn(self):
        assert hook().resolve_agent(ARN) == ManagedAgentRef(platform="aws.bedrock_agentcore", name=ARN)

    @pytest.mark.parametrize("agent", ["investigator", "arn:aws:bedrock:us-east-1:1:agent/x"])
    def test_rejects_anything_but_a_runtime_arn(self, agent):
        with pytest.raises(ValueError, match="runtime ARN"):
            hook().resolve_agent(agent)

    def test_capabilities(self):
        assert hook().agent_capabilities(ARN) == ManagedAgentCapabilities(
            sessions=True, structured_output=True, trace=True
        )

    def test_botocore_does_not_retry_an_invocation_with_unknown_effects(self):
        assert hook()._call_config(None).retries == {"total_max_attempts": 1}

    @pytest.mark.parametrize(
        "config", [{"read_timeout": 900}, Config(read_timeout=900)], ids=["dict", "Config"]
    )
    def test_caller_config_is_kept_and_only_retries_defaulted(self, config):
        call_config = hook(config=config)._call_config(None)
        assert call_config.read_timeout == 900
        assert call_config.retries == {"total_max_attempts": 1}

    def test_caller_retries_win_over_the_default(self):
        call_config = hook(config=Config(retries={"total_max_attempts": 3}))._call_config(None)
        assert call_config.retries == {"total_max_attempts": 3}

    def test_connection_config_kwargs_are_honored_like_every_other_aws_hook(self, monkeypatch):
        # Handing the base an explicit config would make the connection's config_kwargs vanish.
        extra = {"config_kwargs": {"read_timeout": 1234, "proxies": {"https": "http://proxy:3128"}}}
        monkeypatch.setenv("AIRFLOW_CONN_AWS_PROBE", json.dumps({"conn_type": "aws", "extra": extra}))
        call_config = BedrockAgentCoreHook(aws_conn_id="aws_probe", region_name="us-east-1")._call_config(
            None
        )
        assert call_config.read_timeout == 1234
        assert call_config.proxies == {"https": "http://proxy:3128"}
        assert call_config.retries == {"total_max_attempts": 1}


@needs_common_ai
class TestBedrockAgentCoreHookInvokeAgent:
    def test_prompt_session_and_passthrough_reach_the_api(self, invoke_agent_runtime):
        stream = respond(invoke_agent_runtime, {"result": "Looks fine", "confidence": 0.9})
        response = (
            hook()
            .agent(ARN)
            .invoke(
                ManagedAgentRequest(prompt="Check it", session_id=SESSION, vendor_options={"qualifier": "v2"})
            )
        )
        kwargs = invoke_agent_runtime.call_args.kwargs
        assert kwargs["agentRuntimeArn"] == ARN
        assert json.loads(kwargs["payload"]) == {"prompt": "Check it"}
        assert kwargs["contentType"] == kwargs["accept"] == "application/json"
        assert kwargs["runtimeSessionId"] == SESSION
        assert kwargs["qualifier"] == "v2"
        assert response.text == "Looks fine"
        assert response.structured == {"result": "Looks fine", "confidence": 0.9}
        assert response.raw["response"] == {"result": "Looks fine", "confidence": 0.9}
        assert response.session_id == "session-from-aws"
        assert response.trace_ref == "req-1"
        assert stream.closed

    def test_messages_are_sent_as_messages(self, invoke_agent_runtime):
        respond(invoke_agent_runtime, {"output": "ok"})
        messages = [{"role": "user", "content": [{"type": "text", "text": "hi"}]}]
        hook().agent(ARN).invoke(ManagedAgentRequest(messages=messages))
        assert json.loads(invoke_agent_runtime.call_args.kwargs["payload"]) == {"messages": messages}

    @pytest.mark.parametrize("session_id", ["short", "x" * 257])
    def test_a_session_id_outside_the_services_bounds_is_refused_before_the_call(
        self, invoke_agent_runtime, session_id
    ):
        with pytest.raises(ValueError, match="33 to 256"):
            hook().agent(ARN).invoke(ManagedAgentRequest(prompt="x", session_id=session_id))
        invoke_agent_runtime.assert_not_called()

    def test_text_key_overrides_the_default_lookup(self, invoke_agent_runtime):
        respond(invoke_agent_runtime, {"output": "wrong", "answer": "right"})
        request = ManagedAgentRequest(prompt="x", vendor_options={"text_key": "answer"})
        assert hook().agent(ARN).invoke(request).text == "right"

    @pytest.mark.parametrize("body", [{"output": "something"}, "just a string"], ids=["dict", "str"])
    def test_missing_text_key_is_terminal_not_silent(self, invoke_agent_runtime, body):
        respond(invoke_agent_runtime, body)
        with pytest.raises(ManagedAgentInvocationError, match="text_key='answer'"):
            hook().agent(ARN).invoke(ManagedAgentRequest(prompt="x", vendor_options={"text_key": "answer"}))

    def test_a_body_that_is_not_json_is_terminal(self, invoke_agent_runtime):
        respond(invoke_agent_runtime, "ignored")
        invoke_agent_runtime.return_value["response"] = io.BytesIO(b"{not json")
        with pytest.raises(ManagedAgentInvocationError, match="not JSON"):
            hook().agent(ARN).invoke(ManagedAgentRequest(prompt="x"))

    def test_terminal_errors_name_the_agent_and_the_connection(self, invoke_agent_runtime):
        invoke_agent_runtime.side_effect = ClientError(
            {"Error": {"Code": "AccessDeniedException", "Message": "m"}}, "InvokeAgentRuntime"
        )
        with pytest.raises(ManagedAgentInvocationError, match=f"{ARN} via connection 'aws_default'"):
            hook().agent(ARN).invoke(ManagedAgentRequest(prompt="x"))

    def test_unknown_shape_is_dumped_not_guessed(self, invoke_agent_runtime):
        respond(invoke_agent_runtime, {"findings": ["a"], "score": 1})
        response = hook().agent(ARN).invoke(ManagedAgentRequest(prompt="x"))
        assert json.loads(response.text) == {"findings": ["a"], "score": 1}

    @pytest.mark.parametrize("option", ["agentRuntimeArn", "accountId", "mcpSessionId"])
    def test_vendor_options_cannot_retarget_the_call(self, invoke_agent_runtime, option):
        with pytest.raises(ValueError, match=option):
            hook().agent(ARN).invoke(ManagedAgentRequest(prompt="x", vendor_options={option: "other"}))
        invoke_agent_runtime.assert_not_called()

    def test_request_timeout_becomes_the_call_clients_timeouts(self, invoke_agent_runtime):
        respond(invoke_agent_runtime, {"output": "ok"})
        hook(config={"read_timeout": 900}).agent(ARN).invoke(ManagedAgentRequest(prompt="x", timeout=15))
        config = BedrockAgentCoreHook.get_client_type.call_args.kwargs["config"]
        assert (config.read_timeout, config.connect_timeout) == (15, 15)
        assert config.retries == {"total_max_attempts": 1}

    def test_no_request_timeout_keeps_the_hooks_config(self, invoke_agent_runtime):
        respond(invoke_agent_runtime, {"output": "ok"})
        hook(config={"read_timeout": 900}).agent(ARN).invoke(ManagedAgentRequest(prompt="x"))
        assert BedrockAgentCoreHook.get_client_type.call_args.kwargs["config"].read_timeout == 900

    def test_one_client_per_timeout_reused_across_calls(self, invoke_agent_runtime):
        bound = hook().agent(ARN)
        for prompt, timeout in (("x", 15), ("y", 15), ("z", 30)):
            respond(invoke_agent_runtime, {"output": "ok"})
            bound.invoke(ManagedAgentRequest(prompt=prompt, timeout=timeout))
        timeouts = [
            c.kwargs["config"].read_timeout for c in BedrockAgentCoreHook.get_client_type.call_args_list
        ]
        assert timeouts == [15, 30]

    @pytest.mark.parametrize(
        ("code", "terminal"),
        [
            ("ValidationException", True),
            ("ResourceNotFoundException", True),
            ("AccessDeniedException", True),
            ("ServiceQuotaExceededException", True),
            ("ThrottlingException", False),
            ("RuntimeClientError", False),
        ],
    )
    def test_client_errors_are_terminal_or_propagate(self, invoke_agent_runtime, code, terminal):
        invoke_agent_runtime.side_effect = ClientError(
            {"Error": {"Code": code, "Message": "m"}}, "InvokeAgentRuntime"
        )
        # Resolved here rather than in the parametrize list, which is evaluated even where common.ai
        # is absent and the class is skipped.
        with pytest.raises(ManagedAgentInvocationError if terminal else ClientError):
            hook().agent(ARN).invoke(ManagedAgentRequest(prompt="x"))

    def test_non_json_content_type_is_terminal(self, invoke_agent_runtime):
        stream = respond(invoke_agent_runtime, "ignored", contentType="text/event-stream")
        with pytest.raises(ManagedAgentInvocationError, match="application/json only"):
            hook().agent(ARN).invoke(ManagedAgentRequest(prompt="x"))
        assert stream.closed

    def test_oversized_body_is_terminal_and_closed(self, invoke_agent_runtime):
        stream = respond(invoke_agent_runtime, "x" * 64)
        with pytest.raises(ManagedAgentInvocationError, match="max_response_bytes=32"):
            hook().agent(ARN).invoke(
                ManagedAgentRequest(prompt="x", vendor_options={"max_response_bytes": 32})
            )
        assert stream.closed

    @pytest.mark.parametrize(
        "options",
        [
            {"text_key": 7},
            {"max_response_bytes": 0},
            {"max_response_bytes": "big"},
            {"max_response_bytes": 1.5},
        ],
        ids=["text_key-not-str", "zero-bytes", "bytes-not-int", "bytes-float"],
    )
    def test_malformed_hook_options_are_rejected_before_the_call(self, invoke_agent_runtime, options):
        with pytest.raises(ValueError, match="vendor_options"):
            hook().agent(ARN).invoke(ManagedAgentRequest(prompt="x", vendor_options=options))
        invoke_agent_runtime.assert_not_called()


def test_the_hook_imports_without_common_ai_and_agent_names_the_missing_extra():
    """The contract base is optional: without it the module loads and ``agent()`` says what to install."""
    script = (
        "import sys\n"
        "for m in ('airflow.providers.common.ai.exceptions', 'airflow.providers.common.ai.managed_agents.base'):\n"
        "    sys.modules[m] = None\n"
        "from airflow.providers.amazon.aws.hooks.bedrock import BedrockAgentCoreHook\n"
        "from airflow.providers.common.compat.sdk import AirflowOptionalProviderFeatureException\n"
        "hook = BedrockAgentCoreHook(aws_conn_id='aws_default', region_name='us-east-1')\n"
        "try:\n"
        "    hook.agent('arn:aws:bedrock-agentcore:us-east-1:1:runtime/x')\n"
        "except AirflowOptionalProviderFeatureException as e:\n"
        "    assert 'amazon[common.ai]' in str(e), e\n"
        "else:\n"
        "    raise SystemExit('agent() did not raise')\n"
    )
    subprocess.run([sys.executable, "-c", script], check=True)
