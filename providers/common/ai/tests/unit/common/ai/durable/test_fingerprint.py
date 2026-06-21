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

import datetime

import httpx
from pydantic_ai.messages import (
    ModelRequest,
    ModelResponse,
    SystemPromptPart,
    ToolCallPart,
    UserPromptPart,
)
from pydantic_ai.models import ModelRequestParameters
from pydantic_ai.tools import ToolDefinition

from airflow.providers.common.ai.durable.fingerprint import (
    fingerprint_model_request,
    fingerprint_tool_call,
)


def make_messages(system: str = "You are a bot.", user: str = "hello", **part_kwargs):
    return [
        ModelRequest(
            parts=[
                SystemPromptPart(content=system, **part_kwargs),
                UserPromptPart(content=user, **part_kwargs),
            ]
        )
    ]


class TestModelRequestFingerprint:
    def test_stable_across_part_timestamps(self):
        """Part timestamps regenerate on every attempt and must not affect the fingerprint."""
        t1 = datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc)
        t2 = datetime.datetime(2026, 1, 2, tzinfo=datetime.timezone.utc)
        fp1 = fingerprint_model_request("m", make_messages(timestamp=t1), None, ModelRequestParameters())
        fp2 = fingerprint_model_request("m", make_messages(timestamp=t2), None, ModelRequestParameters())

        assert fp1 == fp2

    def test_stable_across_separate_message_constructions(self):
        """run_id/conversation_id and other per-run fields must not affect the fingerprint."""
        fp1 = fingerprint_model_request("m", make_messages(), None, ModelRequestParameters())
        fp2 = fingerprint_model_request("m", make_messages(), None, ModelRequestParameters())

        assert fp1 == fp2

    def test_changes_with_system_prompt(self):
        fp1 = fingerprint_model_request("m", make_messages(system="a"), None, ModelRequestParameters())
        fp2 = fingerprint_model_request("m", make_messages(system="b"), None, ModelRequestParameters())

        assert fp1 != fp2

    def test_changes_with_user_prompt(self):
        fp1 = fingerprint_model_request("m", make_messages(user="a"), None, ModelRequestParameters())
        fp2 = fingerprint_model_request("m", make_messages(user="b"), None, ModelRequestParameters())

        assert fp1 != fp2

    def test_changes_with_model_identifier(self):
        fp1 = fingerprint_model_request("openai:gpt-5", make_messages(), None, ModelRequestParameters())
        fp2 = fingerprint_model_request("openai:gpt-5-mini", make_messages(), None, ModelRequestParameters())

        assert fp1 != fp2

    def test_changes_with_model_settings(self):
        fp1 = fingerprint_model_request("m", make_messages(), None, ModelRequestParameters())
        fp2 = fingerprint_model_request("m", make_messages(), {"temperature": 0.5}, ModelRequestParameters())

        assert fp1 != fp2

    def test_changes_with_toolset(self):
        tool = ToolDefinition(name="search", parameters_json_schema={"type": "object"})
        fp1 = fingerprint_model_request("m", make_messages(), None, ModelRequestParameters())
        fp2 = fingerprint_model_request(
            "m", make_messages(), None, ModelRequestParameters(function_tools=[tool])
        )

        assert fp1 != fp2

    def test_changes_with_output_mode(self):
        """The full request parameters are hashed, not just the tool list."""
        fp1 = fingerprint_model_request("m", make_messages(), None, ModelRequestParameters())
        fp2 = fingerprint_model_request(
            "m", make_messages(), None, ModelRequestParameters(output_mode="native")
        )

        assert fp1 != fp2

    def test_changes_with_tool_definition_fields(self):
        """Changes inside a tool definition (e.g. strict mode) affect the fingerprint."""
        strict = ToolDefinition(name="t", parameters_json_schema={"type": "object"}, strict=True)
        lax = ToolDefinition(name="t", parameters_json_schema={"type": "object"}, strict=False)
        fp1 = fingerprint_model_request(
            "m", make_messages(), None, ModelRequestParameters(function_tools=[strict])
        )
        fp2 = fingerprint_model_request(
            "m", make_messages(), None, ModelRequestParameters(function_tools=[lax])
        )

        assert fp1 != fp2

    def test_volatile_keys_inside_user_data_are_not_stripped(self):
        """Only pydantic-ai's own message/part fields are volatile; a tool argument
        legitimately named run_id must still affect the fingerprint."""

        def messages_with_args(args):
            return [
                ModelRequest(parts=[UserPromptPart(content="q")]),
                ModelResponse(parts=[ToolCallPart(tool_name="t", args=args, tool_call_id="id1")]),
            ]

        fp1 = fingerprint_model_request(
            "m", messages_with_args({"run_id": "a"}), None, ModelRequestParameters()
        )
        fp2 = fingerprint_model_request(
            "m", messages_with_args({"run_id": "b"}), None, ModelRequestParameters()
        )

        assert fp1 != fp2

    def test_unserializable_request_returns_none(self):
        fp = fingerprint_model_request("m", [object()], None, ModelRequestParameters())  # type: ignore[list-item]

        assert fp is None

    def test_unserializable_settings_returns_none(self):
        """Non-JSON settings values degrade to unverified replay instead of hashing
        process-local reprs that would never match on retry."""
        fp = fingerprint_model_request(
            "m", make_messages(), {"extra_body": object()}, ModelRequestParameters()
        )  # type: ignore[typeddict-item]

        assert fp is None

    def test_httpx_timeout_does_not_disable_fingerprint(self):
        """``timeout`` may be an ``httpx.Timeout`` (a supported, non-JSON shape).
        It must not force the fingerprint to None and silently disable verification."""
        fp = fingerprint_model_request(
            "m", make_messages(), {"timeout": httpx.Timeout(30.0)}, ModelRequestParameters()
        )

        assert fp is not None

    def test_timeout_excluded_from_fingerprint(self):
        """timeout is transport-only -- changing it (or its type) must not invalidate
        the cached response, so it is the same fingerprint as no timeout at all."""
        no_timeout = fingerprint_model_request("m", make_messages(), None, ModelRequestParameters())
        float_timeout = fingerprint_model_request(
            "m", make_messages(), {"timeout": 30.0}, ModelRequestParameters()
        )
        httpx_timeout = fingerprint_model_request(
            "m", make_messages(), {"timeout": httpx.Timeout(5.0)}, ModelRequestParameters()
        )

        assert no_timeout == float_timeout == httpx_timeout

    def test_content_settings_still_count_when_timeout_present(self):
        """Stripping timeout must not drop content settings sharing the dict."""
        low = fingerprint_model_request(
            "m",
            make_messages(),
            {"temperature": 0.2, "timeout": httpx.Timeout(1.0)},
            ModelRequestParameters(),
        )
        high = fingerprint_model_request(
            "m",
            make_messages(),
            {"temperature": 0.9, "timeout": httpx.Timeout(1.0)},
            ModelRequestParameters(),
        )

        assert low is not None
        assert high is not None
        assert low != high


class TestToolCallFingerprint:
    def test_stable_for_identical_call(self):
        assert fingerprint_tool_call("t", {"a": 1}, "id1") == fingerprint_tool_call("t", {"a": 1}, "id1")

    def test_changes_with_name(self):
        assert fingerprint_tool_call("a", {}, "id1") != fingerprint_tool_call("b", {}, "id1")

    def test_changes_with_args(self):
        assert fingerprint_tool_call("t", {"a": 1}, "id1") != fingerprint_tool_call("t", {"a": 2}, "id1")

    def test_changes_with_tool_call_id(self):
        assert fingerprint_tool_call("t", {}, "id1") != fingerprint_tool_call("t", {}, "id2")

    def test_arg_order_does_not_matter(self):
        assert fingerprint_tool_call("t", {"a": 1, "b": 2}, "id1") == fingerprint_tool_call(
            "t", {"b": 2, "a": 1}, "id1"
        )
