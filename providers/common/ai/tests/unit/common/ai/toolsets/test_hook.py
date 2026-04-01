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
from unittest.mock import MagicMock

import pytest

from airflow.providers.common.ai.toolsets.hook import (
    HookToolset,
    _build_json_schema_from_signature,
    _extract_description,
    _parse_param_docs,
    _serialize_for_llm,
)


class _FakeHook:
    """Fake hook for testing HookToolset introspection."""

    def list_keys(self, bucket: str, prefix: str = "") -> list[str]:
        """List object keys in a bucket.

        :param bucket: Name of the S3 bucket.
        :param prefix: Key prefix to filter by.
        """
        return [f"{prefix}file1.txt", f"{prefix}file2.txt"]

    def read_file(self, key: str) -> str:
        """Read a file from storage."""
        return f"contents of {key}"

    def no_docstring(self, x: int) -> int:
        return x * 2


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

    def test_param_docs_enriched_in_schema(self):
        hook = _FakeHook()
        ts = HookToolset(hook, allowed_methods=["list_keys"])
        tools = asyncio.run(ts.get_tools(ctx=MagicMock()))

        props = tools["list_keys"].tool_def.parameters_json_schema["properties"]
        assert "description" in props["bucket"]
        assert "S3 bucket" in props["bucket"]["description"]


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
                "storage_read_file", {"key": "test.txt"}, ctx=MagicMock(), tool=tools["storage_read_file"]
            )
        )
        assert result == "contents of test.txt"


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

    def test_optional_params_not_required(self):
        def fn(name: str, prefix: str = ""):
            pass

        schema = _build_json_schema_from_signature(fn)
        assert schema["required"] == ["name"]

    def test_list_type(self):
        def fn(items: list[str]):
            pass

        schema = _build_json_schema_from_signature(fn)
        assert schema["properties"]["items"] == {"type": "array", "items": {"type": "string"}}

    def test_no_annotation_defaults_to_string(self):
        def fn(x):
            pass

        schema = _build_json_schema_from_signature(fn)
        assert schema["properties"]["x"] == {"type": "string"}

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
        assert _serialize_for_llm("hello") == "hello"

    def test_none_returns_null(self):
        assert _serialize_for_llm(None) == "null"

    def test_dict_to_json(self):
        result = _serialize_for_llm({"key": "value"})
        assert result == '{"key": "value"}'

    def test_list_to_json(self):
        result = _serialize_for_llm([1, 2, 3])
        assert result == "[1, 2, 3]"

    def test_non_serializable_falls_back_to_str(self):
        obj = object()
        result = _serialize_for_llm(obj)
        assert "object" in result
