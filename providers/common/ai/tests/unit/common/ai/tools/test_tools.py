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

import asyncio
from typing import Any

import pytest

from airflow.providers.common.ai.tools import AirflowTool, ToolResult
from airflow.sdk._shared.secrets_masker import reset_secrets_masker
from airflow.sdk.log import mask_secret

SECRET = "crm-password-7f2a9c"


def _tool(function) -> AirflowTool:
    return AirflowTool(
        name="lookup",
        description="Look something up.",
        parameters={"type": "object", "properties": {"key": {"type": "string"}}},
        function=function,
    )


class TestAirflowToolCall:
    def test_passes_arguments_and_returns_the_result(self):
        seen: list[dict[str, Any]] = []

        async def function(arguments: dict[str, Any]) -> ToolResult:
            seen.append(arguments)
            return ToolResult(content={"rows": [[1, "Ada"]]})

        result = asyncio.run(_tool(function).call({"key": "a"}))

        assert seen == [{"key": "a"}]
        assert result == ToolResult(content={"rows": [[1, "Ada"]]})

    def test_error_result_keeps_its_status(self):
        async def function(arguments: dict[str, Any]) -> ToolResult:
            return ToolResult(content="Unknown column 'nme'", is_error=True)

        result = asyncio.run(_tool(function).call({}))

        assert result == ToolResult(content="Unknown column 'nme'", is_error=True)

    def test_exception_becomes_an_error_result(self):
        async def function(arguments: dict[str, Any]) -> ToolResult:
            raise RuntimeError("upstream returned 503")

        result = asyncio.run(_tool(function).call({}))

        assert result == ToolResult(content="RuntimeError: upstream returned 503", is_error=True)


@pytest.mark.enable_redact
class TestAirflowToolMasking:
    def setup_method(self):
        reset_secrets_masker()
        mask_secret(SECRET)

    def teardown_method(self):
        reset_secrets_masker()

    def test_masks_a_registered_secret_in_a_text_result(self):
        async def function(arguments: dict[str, Any]) -> ToolResult:
            return ToolResult(content=f"token={SECRET}")

        result = asyncio.run(_tool(function).call({}))

        assert result.content == "token=***"

    def test_masks_a_registered_secret_inside_a_json_result(self):
        async def function(arguments: dict[str, Any]) -> ToolResult:
            return ToolResult(content={"rows": [["svc", f"https://svc:{SECRET}@crm.example.com"]]})

        result = asyncio.run(_tool(function).call({}))

        assert result.content == {"rows": [["svc", "https://svc:***@crm.example.com"]]}

    def test_masks_a_secret_nested_deeper_than_the_masker_default(self):
        """The masker's default depth is 5; an ordinary API payload can be nested further."""
        payload = {"items": [{"spec": {"containers": [{"env": [{"value": f"pw={SECRET}"}]}]}}]}

        async def function(arguments: dict[str, Any]) -> ToolResult:
            return ToolResult(content=payload)

        result = asyncio.run(_tool(function).call({}))

        assert result.content == {"items": [{"spec": {"containers": [{"env": [{"value": "pw=***"}]}]}}]}

    def test_masks_a_registered_secret_in_an_exception_message(self):
        """Client libraries often put the credentialed URL in their error message."""

        async def function(arguments: dict[str, Any]) -> ToolResult:
            raise RuntimeError(f"GET https://svc:{SECRET}@crm.example.com returned 401")

        result = asyncio.run(_tool(function).call({}))

        assert result.is_error
        assert SECRET not in str(result.content)
        assert result.content == "RuntimeError: GET https://svc:***@crm.example.com returned 401"
