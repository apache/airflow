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
import re
import threading
from unittest.mock import MagicMock

import pytest
from pydantic_ai import Agent
from pydantic_ai._run_context import RunContext
from pydantic_ai.messages import ModelResponse, RetryPromptPart, TextPart, ToolCallPart, ToolReturnPart
from pydantic_ai.models.function import FunctionModel
from pydantic_core import ValidationError

from airflow.providers.common.ai.toolsets.hook import (
    HookToolset,
    _build_json_schema_from_signature,
    _extract_description,
    _parse_param_docs,
)
from airflow.providers.common.ai.utils.tool_definition import (
    _SUPPORTS_RETURN_SCHEMA,
    serialize_for_llm,
)


class _FakeHook:
    """Fake hook for testing HookToolset introspection."""

    def list_keys(self, bucket: str, prefix: str | None = None) -> list[str]:
        """List object keys in a bucket.

        :param bucket: Name of the S3 bucket.
        :param prefix: Key prefix to filter by.
        """
        return [f"{prefix or ''}file1.txt", f"{prefix or ''}file2.txt"]

    def read_file(self, key: str) -> str:
        """Read a file from storage."""
        return f"contents of {key}"

    def no_docstring(self, x: int) -> int:
        return x * 2

    def request(
        self, endpoint: str | None = None, data: dict[str, object] | str | None = None, **kwargs: object
    ) -> dict[str, object]:
        return {"endpoint": endpoint, "data": data, **kwargs}


class TestHookToolsetInit:
    def test_requires_non_empty_allowed_methods(self):
        with pytest.raises(ValueError, match="non-empty"):
            HookToolset(MagicMock(), allowed_methods=[])

    def test_rejects_nonexistent_method(self):
        hook = _FakeHook()
        with pytest.raises(ValueError, match="has no method 'nonexistent'"):
            HookToolset(hook, allowed_methods=["nonexistent"])

    def test_rejects_non_callable_attribute(self):
        hook = MagicMock()
        hook.some_attr = "not callable"

        # MagicMock attributes are callable by default, so use a real object
        class HookWithAttr:
            data = [1, 2, 3]

        with pytest.raises(ValueError, match="not callable"):
            HookToolset(HookWithAttr(), allowed_methods=["data"])

    def test_id_includes_hook_class_name(self):
        hook = _FakeHook()
        ts = HookToolset(hook, allowed_methods=["list_keys"])
        assert "FakeHook" in ts.id


class _FakeConnHook(_FakeHook):
    """A hook that names its connection attribute, like every provider hook does."""

    conn_name_attr = "fake_conn_id"

    def __init__(self, fake_conn_id: str = "fake_default"):
        self.fake_conn_id = fake_conn_id


class TestHookToolsetConnId:
    def test_conn_id_is_read_from_the_hooks_conn_name_attr(self):
        ts = HookToolset(_FakeConnHook("warehouse"), allowed_methods=["list_keys"])

        assert ts.conn_id == "warehouse"
        assert ts.id == "hook-_FakeConnHook-warehouse"

    def test_a_hook_without_conn_name_attr_has_no_conn_id(self):
        ts = HookToolset(_FakeHook(), allowed_methods=["list_keys"])

        assert ts.conn_id is None
        assert ts.id == "hook-_FakeHook"

    def test_setting_conn_id_copies_the_hook(self):
        """The hook in the Dag file is shared by every task instance that uses the toolset."""
        hook = _FakeConnHook("tenant_{{ customer }}")
        ts = HookToolset(hook, allowed_methods=["list_keys"])

        ts.conn_id = "tenant_acme"

        assert ts.conn_id == "tenant_acme"
        assert ts._hook is not hook
        assert hook.fake_conn_id == "tenant_{{ customer }}"

    def test_setting_conn_id_on_a_hook_without_one_raises(self):
        ts = HookToolset(_FakeHook(), allowed_methods=["list_keys"])

        with pytest.raises(AttributeError, match="keeps no connection ID"):
            ts.conn_id = "x"

    def test_falls_back_to_conn_id_when_conn_name_attr_is_not_set(self):
        """WasbHook and KubernetesHook declare one attribute and keep the ID in ``conn_id``."""

        class _WasbShapedHook(_FakeHook):
            conn_name_attr = "wasb_conn_id"

            def __init__(self, wasb_conn_id: str):
                self.conn_id = wasb_conn_id

        ts = HookToolset(_WasbShapedHook("blob_{{ customer }}"), allowed_methods=["list_keys"])
        ts.conn_id = "blob_acme"

        assert ts.conn_id == "blob_acme"
        assert ts.id == "hook-_WasbShapedHook-blob_acme"


class TestHookToolsetGetTools:
    def test_returns_tools_for_allowed_methods(self):
        hook = _FakeHook()
        ts = HookToolset(hook, allowed_methods=["list_keys", "read_file"])
        tools = asyncio.run(ts.get_tools(ctx=MagicMock()))
        assert set(tools.keys()) == {"list_keys", "read_file"}

    def test_tool_definitions_have_correct_schemas(self):
        hook = _FakeHook()
        ts = HookToolset(hook, allowed_methods=["list_keys"])
        tools = asyncio.run(ts.get_tools(ctx=MagicMock()))

        tool_def = tools["list_keys"].tool_def
        assert tool_def.name == "list_keys"
        assert "bucket" in tool_def.parameters_json_schema["properties"]
        assert "prefix" in tool_def.parameters_json_schema["properties"]
        assert "bucket" in tool_def.parameters_json_schema["required"]
        # prefix has a default, so it's not required
        assert "prefix" not in tool_def.parameters_json_schema.get("required", [])

    def test_tool_name_prefix(self):
        hook = _FakeHook()
        ts = HookToolset(hook, allowed_methods=["list_keys"], tool_name_prefix="s3_")
        tools = asyncio.run(ts.get_tools(ctx=MagicMock()))
        assert "s3_list_keys" in tools

    def test_description_from_docstring(self):
        hook = _FakeHook()
        ts = HookToolset(hook, allowed_methods=["list_keys"])
        tools = asyncio.run(ts.get_tools(ctx=MagicMock()))

        assert tools["list_keys"].tool_def.description == "List object keys in a bucket."

    def test_description_fallback_for_no_docstring(self):
        hook = _FakeHook()
        ts = HookToolset(hook, allowed_methods=["no_docstring"])
        tools = asyncio.run(ts.get_tools(ctx=MagicMock()))

        assert tools["no_docstring"].tool_def.description == "No docstring"

    def test_tools_are_sequential(self):
        hook = _FakeHook()
        ts = HookToolset(hook, allowed_methods=["list_keys"])
        tools = asyncio.run(ts.get_tools(ctx=MagicMock()))
        assert tools["list_keys"].tool_def.sequential is True

    @pytest.mark.skipif(
        not _SUPPORTS_RETURN_SCHEMA, reason="pydantic-ai too old for ToolDefinition.return_schema"
    )
    def test_tools_declare_string_return_schema(self):
        # call_tool always returns a serialized string, so code mode should see `-> str`.
        hook = _FakeHook()
        ts = HookToolset(hook, allowed_methods=["list_keys"])
        tools = asyncio.run(ts.get_tools(ctx=MagicMock()))
        assert tools["list_keys"].tool_def.return_schema == {"type": "string"}

    def test_param_docs_enriched_in_schema(self):
        hook = _FakeHook()
        ts = HookToolset(hook, allowed_methods=["list_keys"])
        tools = asyncio.run(ts.get_tools(ctx=MagicMock()))

        props = tools["list_keys"].tool_def.parameters_json_schema["properties"]
        assert "description" in props["bucket"]
        assert "S3 bucket" in props["bucket"]["description"]


class TestHookToolsetArgsValidator:
    @pytest.fixture
    def list_keys_tool(self):
        ts = HookToolset(_FakeHook(), allowed_methods=["list_keys"])
        return asyncio.run(ts.get_tools(ctx=MagicMock()))["list_keys"]

    def test_enforces_method_signature(self, list_keys_tool):
        with pytest.raises(ValidationError, match="bucket"):
            list_keys_tool.args_validator.validate_python({"prefix": "data/"})

        assert list_keys_tool.args_validator.validate_python({"bucket": "my-bucket", "prefix": None}) == {
            "bucket": "my-bucket",
            "prefix": None,
        }

    def test_rejects_undeclared_args(self, list_keys_tool):
        with pytest.raises(ValidationError, match="bogus"):
            list_keys_tool.args_validator.validate_python({"bucket": "my-bucket", "bogus": 1})

    def test_preserves_kwargs_accepted_by_method(self):
        ts = HookToolset(_FakeHook(), allowed_methods=["request"])
        tool = asyncio.run(ts.get_tools(ctx=MagicMock()))["request"]
        args = {"endpoint": None, "data": {"key": "value"}, "timeout": 10}
        assert tool.args_validator.validate_python(args) == args


class TestHookToolsetCallTool:
    def test_dispatches_to_hook_method(self):
        hook = _FakeHook()
        ts = HookToolset(hook, allowed_methods=["list_keys"])
        tools = asyncio.run(ts.get_tools(ctx=MagicMock()))

        result = asyncio.run(
            ts.call_tool(
                "list_keys",
                {"bucket": "my-bucket", "prefix": "data/"},
                ctx=MagicMock(),
                tool=tools["list_keys"],
            )
        )
        assert "data/file1.txt" in result

    def test_dispatches_with_prefix(self):
        hook = _FakeHook()
        ts = HookToolset(hook, allowed_methods=["read_file"], tool_name_prefix="storage_")
        tools = asyncio.run(ts.get_tools(ctx=MagicMock()))

        result = asyncio.run(
            ts.call_tool(
                "storage_read_file",
                {"key": "test.txt"},
                ctx=MagicMock(spec=RunContext),
                tool=tools["storage_read_file"],
            )
        )
        assert result == "contents of test.txt"

    @pytest.mark.enable_redact
    def test_a_result_carrying_a_registered_secret_reaches_the_model_masked(self, registered_secret):
        hook = _FakeHook()
        ts = HookToolset(hook, allowed_methods=["read_file"])
        tools = asyncio.run(ts.get_tools(ctx=MagicMock(spec=RunContext)))

        result = asyncio.run(
            ts.call_tool(
                "read_file",
                {"key": registered_secret},
                ctx=MagicMock(spec=RunContext),
                tool=tools["read_file"],
            )
        )

        assert result == "contents of ***"

    def test_the_hook_method_runs_off_the_event_loop_thread(self):
        calls: list[int] = []

        class _ThreadRecordingHook:
            def whoami(self) -> str:
                """Report the calling thread."""
                calls.append(threading.get_ident())
                return "ok"

        ts = HookToolset(_ThreadRecordingHook(), allowed_methods=["whoami"])

        async def call() -> int:
            tools = await ts.get_tools(ctx=MagicMock(spec=RunContext))
            await ts.call_tool("whoami", {}, ctx=MagicMock(spec=RunContext), tool=tools["whoami"])
            return threading.get_ident()

        loop_thread = asyncio.run(call())

        assert calls
        assert calls[0] != loop_thread


class TestBuildJsonSchemaFromSignature:
    def test_basic_types(self):
        def fn(name: str, count: int, rate: float, active: bool):
            pass

        schema = _build_json_schema_from_signature(fn)
        assert schema["properties"]["name"] == {"type": "string"}
        assert schema["properties"]["count"] == {"type": "integer"}
        assert schema["properties"]["rate"] == {"type": "number"}
        assert schema["properties"]["active"] == {"type": "boolean"}
        assert set(schema["required"]) == {"name", "count", "rate", "active"}

    def test_optional_params_accept_null(self):
        def fn(name: str, prefix: str | None = None):
            pass

        schema = _build_json_schema_from_signature(fn)
        assert schema["required"] == ["name"]
        assert schema["properties"]["prefix"] == {"anyOf": [{"type": "string"}, {"type": "null"}]}

    def test_union_types(self):
        def fn(data: dict[str, object] | str):
            pass

        schema = _build_json_schema_from_signature(fn)
        assert schema["properties"]["data"] == {"anyOf": [{"type": "object"}, {"type": "string"}]}

    def test_list_type(self):
        def fn(items: list[str]):
            pass

        schema = _build_json_schema_from_signature(fn)
        assert schema["properties"]["items"] == {"type": "array", "items": {"type": "string"}}

    def test_no_annotation_is_untyped(self):
        def fn(x):
            pass

        schema = _build_json_schema_from_signature(fn)
        assert schema["properties"]["x"] == {}

    def test_kwargs_allow_additional_properties(self):
        def fn(x: int, **kwargs):
            pass

        schema = _build_json_schema_from_signature(fn)
        assert schema["additionalProperties"] is True

    def test_skips_self_and_cls(self):
        class Foo:
            def method(self, x: int):
                pass

        schema = _build_json_schema_from_signature(Foo().method)
        assert "self" not in schema["properties"]

    def test_skips_var_args(self):
        def fn(x: int, *args, **kwargs):
            pass

        schema = _build_json_schema_from_signature(fn)
        assert set(schema["properties"].keys()) == {"x"}


class TestExtractDescription:
    def test_first_paragraph(self):
        def fn():
            """First paragraph.

            Second paragraph with details.
            """

        assert _extract_description(fn) == "First paragraph."

    def test_multiline_first_paragraph(self):
        def fn():
            """First line of
            the first paragraph.

            Second paragraph.
            """

        assert _extract_description(fn) == "First line of the first paragraph."

    def test_no_docstring_uses_method_name(self):
        def some_method():
            pass

        assert _extract_description(some_method) == "Some method"


class TestParseParamDocs:
    def test_sphinx_style(self):
        docstring = """Do something.

        :param name: The name of the thing.
        :param count: How many items.
        """
        result = _parse_param_docs(docstring)
        assert result["name"] == "The name of the thing."
        assert result["count"] == "How many items."

    def test_google_style(self):
        docstring = """Do something.

        Args:
            name: The name of the thing.
            count: How many items.
        """
        result = _parse_param_docs(docstring)
        assert result["name"] == "The name of the thing."
        assert result["count"] == "How many items."


class TestSerializeForLlm:
    def test_string_passthrough(self):
        assert serialize_for_llm("hello") == "hello"

    def test_none_returns_null(self):
        assert serialize_for_llm(None) == "null"

    def test_dict_to_json(self):
        result = serialize_for_llm({"key": "value"})
        assert result == '{"key": "value"}'

    def test_list_to_json(self):
        result = serialize_for_llm([1, 2, 3])
        assert result == "[1, 2, 3]"

    def test_non_serializable_falls_back_to_str(self):
        obj = object()
        result = serialize_for_llm(obj)
        assert "object" in result


class TestHookToolsetPinnedArguments:
    @staticmethod
    def _tools(ts: HookToolset) -> dict:
        return asyncio.run(ts.get_tools(ctx=MagicMock(spec=RunContext)))

    def test_a_pinned_argument_is_left_out_of_the_schema(self):
        ts = HookToolset(_FakeHook(), allowed_methods=["list_keys"], pinned_arguments={"bucket": "reports"})

        schema = self._tools(ts)["list_keys"].tool_def.parameters_json_schema

        assert "bucket" not in schema["properties"]
        assert "required" not in schema
        assert "prefix" in schema["properties"]

    def test_the_pinned_value_is_passed_on_every_call(self):
        hook = _RecordingHook()
        ts = HookToolset(hook, allowed_methods=["list_keys"], pinned_arguments={"bucket": "reports"})
        tools = self._tools(ts)

        asyncio.run(
            ts.call_tool(
                "list_keys", {"prefix": "2026/"}, ctx=MagicMock(spec=RunContext), tool=tools["list_keys"]
            )
        )

        assert hook.calls == [("reports", "2026/")]

    @pytest.mark.parametrize(
        ("method", "missing"),
        [
            pytest.param("read_file", "read_file() does not take bucket", id="another_name"),
            pytest.param("request", "request() does not take bucket", id="kwargs_only"),
        ],
    )
    def test_every_allowed_method_has_to_take_the_pin_by_name(self, method, missing):
        """A method that does not would let the model choose the value through it."""
        with pytest.raises(ValueError, match=re.escape(missing)):
            HookToolset(_FakeHook(), allowed_methods=["list_keys", method], pinned_arguments={"bucket": "x"})

    def test_a_pin_on_a_catch_all_parameter_is_rejected(self):
        with pytest.raises(ValueError, match=r"request\(\) does not take kwargs"):
            HookToolset(_FakeHook(), allowed_methods=["request"], pinned_arguments={"kwargs": {"b": "x"}})

    def test_methods_that_all_take_the_pin_by_name_are_accepted(self):
        ts = HookToolset(
            _RecordingKwargsHook(), allowed_methods=["copy", "list_keys"], pinned_arguments={"bucket": "x"}
        )

        assert set(self._tools(ts)) == {"copy", "list_keys"}

    def test_the_model_cannot_override_it_in_a_real_run(self):
        """A method taking **kwargs would accept the model's value, so the toolset refuses it."""
        hook = _RecordingKwargsHook()
        ts = HookToolset(hook, allowed_methods=["list_keys"], pinned_arguments={"bucket": "reports"})
        attempts = iter([{"bucket": "payroll", "prefix": "x"}, {"prefix": "x"}])

        def model(messages, info):
            retried = [p for m in messages for p in m.parts if isinstance(p, RetryPromptPart)]
            returned = [p for m in messages for p in m.parts if isinstance(p, ToolReturnPart)]
            if returned:
                return ModelResponse(parts=[TextPart(str(retried[0].content))])
            return ModelResponse(parts=[ToolCallPart("list_keys", next(attempts), tool_call_id="c")])

        answer = Agent(FunctionModel(model), toolsets=[ts]).run_sync("list").output

        assert "bucket is fixed for this tool" in answer
        assert hook.calls == [("reports", "x", {})]

    def test_a_method_that_modifies_its_argument_cannot_change_the_pin(self):
        hook = _RecordingKwargsHook()
        ts = HookToolset(
            hook, allowed_methods=["copy"], pinned_arguments={"bucket": "reports", "tags": {"a": "1"}}
        )
        tools = self._tools(ts)
        ctx = MagicMock(spec=RunContext)

        for _ in range(2):
            asyncio.run(ts.call_tool("copy", {"key": "k"}, ctx=ctx, tool=tools["copy"]))

        assert [call[2]["tags"] for call in hook.calls] == [{"a": "1"}, {"a": "1"}]


class _RecordingKwargsHook:
    """Records its calls; ``list_keys`` takes **kwargs, so validation alone would let extra names in."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, str | None, dict[str, object]]] = []

    def list_keys(self, bucket: str, prefix: str | None = None, **kwargs: object) -> list[str]:
        """List object keys in a bucket."""
        self.calls.append((bucket, prefix, kwargs))
        return [f"{bucket}/{prefix}"]

    def copy(self, bucket: str, key: str, tags: dict[str, str] | None = None) -> str:
        """Copy an object, adding a tag as a side effect."""
        self.calls.append((bucket, key, {"tags": dict(tags or {})}))
        if tags is not None:
            tags["copied"] = "yes"
        return key


class _RecordingHook:
    """Records the arguments its method receives."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, str | None]] = []

    def list_keys(self, bucket: str, prefix: str | None = None) -> list[str]:
        """
        List object keys in a bucket.

        :param bucket: Name of the bucket.
        :param prefix: Key prefix to filter by.
        """
        self.calls.append((bucket, prefix))
        return [f"{bucket}/{prefix}"]
