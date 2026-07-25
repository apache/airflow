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
import base64
import json
from typing import TYPE_CHECKING
from unittest.mock import MagicMock

import pytest
from pydantic_ai.exceptions import ModelRetry
from pydantic_core import ValidationError

from airflow.providers.common.ai.toolsets.hook import (
    _BASE64_PARAM_NOTE,
    HookToolset,
    _extract_description,
    _introspect_signature,
    _parse_param_docs,
    _serialize_for_llm,
)
from airflow.providers.common.ai.utils.tool_definition import _SUPPORTS_RETURN_SCHEMA

if TYPE_CHECKING:
    from decimal import Decimal


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

    def upload_bytes(self, data: bytes, key: str) -> str:
        """Upload raw bytes to storage.

        :param data: Content to store.
        :param key: Destination key.
        """
        return f"uploaded {len(data)} bytes to {key} (type={type(data).__name__})"

    def upload_optional_bytes(self, data: bytes | None = None) -> str:
        """Upload optional raw bytes to storage."""
        return f"data type={type(data).__name__}"

    def echo_bytes(self, data: bytes) -> str:
        """Return the hex representation of raw bytes."""
        return data.hex()


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

    def test_bytes_params_declare_base64_encoding(self):
        ts = HookToolset(_FakeHook(), allowed_methods=["upload_bytes"])
        tools = asyncio.run(ts.get_tools(ctx=MagicMock()))

        props = tools["upload_bytes"].tool_def.parameters_json_schema["properties"]
        assert props["data"] == {
            "type": "string",
            "contentEncoding": "base64",
            "description": f"Content to store. {_BASE64_PARAM_NOTE}",
        }
        assert props["key"] == {"type": "string", "description": "Destination key."}

    def test_base64_note_survives_a_param_without_docs(self):
        ts = HookToolset(_FakeHook(), allowed_methods=["upload_optional_bytes"])
        tools = asyncio.run(ts.get_tools(ctx=MagicMock()))

        props = tools["upload_optional_bytes"].tool_def.parameters_json_schema["properties"]
        assert props["data"] == {
            "anyOf": [{"type": "string"}, {"type": "null"}],
            "contentEncoding": "base64",
            "description": _BASE64_PARAM_NOTE,
        }


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
                "storage_read_file", {"key": "test.txt"}, ctx=MagicMock(), tool=tools["storage_read_file"]
            )
        )
        assert result == "contents of test.txt"

    @pytest.mark.parametrize(
        ("method_name", "tool_args", "expected"),
        [
            pytest.param(
                "upload_bytes",
                {"data": "aGVsbG8gd29ybGQ=", "key": "greeting.txt"},
                "uploaded 11 bytes to greeting.txt (type=bytes)",
                id="bytes",
            ),
            pytest.param("upload_optional_bytes", {"data": "aGk="}, "data type=bytes", id="optional-bytes"),
            pytest.param("upload_optional_bytes", {"data": None}, "data type=NoneType", id="explicit-null"),
            pytest.param("upload_optional_bytes", {}, "data type=NoneType", id="omitted"),
        ],
    )
    def test_decodes_base64_for_bytes_params(self, method_name, tool_args, expected):
        ts = HookToolset(_FakeHook(), allowed_methods=[method_name])
        tools = asyncio.run(ts.get_tools(ctx=MagicMock()))

        result = asyncio.run(ts.call_tool(method_name, tool_args, ctx=MagicMock(), tool=tools[method_name]))
        assert result == expected

    def test_decoded_bytes_are_byte_exact(self):
        ts = HookToolset(_FakeHook(), allowed_methods=["echo_bytes"])
        tools = asyncio.run(ts.get_tools(ctx=MagicMock()))
        payload = b"\x89PNG\r\n\x1a\n"

        result = asyncio.run(
            ts.call_tool(
                "echo_bytes",
                {"data": base64.b64encode(payload).decode("ascii")},
                ctx=MagicMock(),
                tool=tools["echo_bytes"],
            )
        )
        assert result == payload.hex()

    @pytest.mark.parametrize(
        "value", ["hello world", "not base64!!", "abc"], ids=["text", "punctuation", "bad-padding"]
    )
    def test_undecodable_value_asks_the_model_to_retry(self, value):
        ts = HookToolset(_FakeHook(), allowed_methods=["upload_bytes"])
        tools = asyncio.run(ts.get_tools(ctx=MagicMock()))

        with pytest.raises(ModelRetry, match="'data' must be base64-encoded"):
            asyncio.run(
                ts.call_tool(
                    "upload_bytes",
                    {"data": value, "key": "greeting.txt"},
                    ctx=MagicMock(),
                    tool=tools["upload_bytes"],
                )
            )


def _takes_bytes(value: bytes): ...


def _takes_optional_bytes(value: bytes | None = None): ...


def _takes_bytes_or_str(value: bytes | str): ...


def _takes_list_of_bytes(value: list[bytes]): ...


def _takes_str(value: str): ...


def _takes_unannotated(value): ...


class TestBytesParamDetection:
    @pytest.mark.parametrize(
        ("func", "is_bytes_param"),
        [
            pytest.param(_takes_bytes, True, id="bytes"),
            pytest.param(_takes_optional_bytes, True, id="optional-bytes"),
            pytest.param(_takes_bytes_or_str, False, id="bytes-or-str"),
            pytest.param(_takes_list_of_bytes, False, id="list-of-bytes"),
            pytest.param(_takes_str, False, id="str"),
            pytest.param(_takes_unannotated, False, id="unannotated"),
        ],
    )
    def test_only_bytes_and_optional_bytes_are_decoded(self, func, is_bytes_param):
        schema, bytes_params = _introspect_signature(func)
        assert ("value" in bytes_params) is is_bytes_param
        # A parameter advertised as base64 that is never decoded would hand the
        # hook the encoded text — the corruption this decoding exists to prevent.
        assert ("base64" in json.dumps(schema["properties"]["value"])) is is_bytes_param

    def test_var_args_are_never_decoded(self):
        def fn(*chunks: bytes, **extra: bytes): ...

        schema, bytes_params = _introspect_signature(fn)
        assert bytes_params == frozenset()
        assert schema["properties"] == {}

    def test_unresolvable_annotation_does_not_disable_sibling_params(self):
        # Mirrors CloudKMSHook.encrypt, whose bytes params sit next to a parameter
        # annotated with a TYPE_CHECKING-only import.
        def fn(data: bytes, precision: Decimal | None = None): ...

        _, bytes_params = _introspect_signature(fn)
        assert bytes_params == {"data"}


class TestBuildJsonSchemaFromSignature:
    def test_basic_types(self):
        def fn(name: str, count: int, rate: float, active: bool):
            pass

        schema, _ = _introspect_signature(fn)
        assert schema["properties"]["name"] == {"type": "string"}
        assert schema["properties"]["count"] == {"type": "integer"}
        assert schema["properties"]["rate"] == {"type": "number"}
        assert schema["properties"]["active"] == {"type": "boolean"}
        assert set(schema["required"]) == {"name", "count", "rate", "active"}

    def test_optional_params_accept_null(self):
        def fn(name: str, prefix: str | None = None):
            pass

        schema, _ = _introspect_signature(fn)
        assert schema["required"] == ["name"]
        assert schema["properties"]["prefix"] == {"anyOf": [{"type": "string"}, {"type": "null"}]}

    def test_union_types(self):
        def fn(data: dict[str, object] | str):
            pass

        schema, _ = _introspect_signature(fn)
        assert schema["properties"]["data"] == {"anyOf": [{"type": "object"}, {"type": "string"}]}

    def test_list_type(self):
        def fn(items: list[str]):
            pass

        schema, _ = _introspect_signature(fn)
        assert schema["properties"]["items"] == {"type": "array", "items": {"type": "string"}}

    def test_no_annotation_is_untyped(self):
        def fn(x):
            pass

        schema, _ = _introspect_signature(fn)
        assert schema["properties"]["x"] == {}

    def test_kwargs_allow_additional_properties(self):
        def fn(x: int, **kwargs):
            pass

        schema, _ = _introspect_signature(fn)
        assert schema["additionalProperties"] is True

    def test_skips_self_and_cls(self):
        class Foo:
            def method(self, x: int):
                pass

        schema, _ = _introspect_signature(Foo().method)
        assert "self" not in schema["properties"]

    def test_skips_var_args(self):
        def fn(x: int, *args, **kwargs):
            pass

        schema, _ = _introspect_signature(fn)
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
