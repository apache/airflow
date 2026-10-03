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

import base64
import dataclasses
import datetime
import enum
import functools
import hashlib
import itertools
import json
import math
import os
import subprocess
import sys
import textwrap
from collections import deque
from collections.abc import Iterable, Iterator, Mapping
from decimal import Decimal
from pathlib import PurePosixPath
from typing import Annotated, Any
from unittest.mock import patch

import httpx
import pydantic
import pydantic.alias_generators
import pytest
from pydantic import TypeAdapter
from pydantic_ai import Agent
from pydantic_ai.messages import (
    BinaryContent,
    ModelMessagesTypeAdapter,
    ModelRequest,
    ModelResponse,
    SystemPromptPart,
    TextPart,
    ToolCallPart,
    ToolReturnPart,
    UserPromptPart,
)
from pydantic_ai.models import ModelRequestParameters
from pydantic_ai.models.function import FunctionModel
from pydantic_ai.settings import ToolOrOutput
from pydantic_ai.tools import ToolDefinition
from pydantic_core import to_jsonable_python

from airflow.providers.common.ai.durable import fingerprint as fingerprint_module
from airflow.providers.common.ai.durable.fingerprint import (
    _digest,
    _render,
    fingerprint_model_request,
    fingerprint_tool_call,
)

# Not valid UTF-8, so a renderer that decodes bytes as text raises on it.
_PNG = b"\x89PNG\r\n\x1a\n" + bytes(range(16))


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
        """Non-JSON settings values force live execution instead of hashing
        process-local reprs that would never match on retry."""
        fp = fingerprint_model_request(
            "m", make_messages(), {"extra_body": object()}, ModelRequestParameters()
        )  # type: ignore[typeddict-item]

        assert fp is None

    def test_httpx_timeout_does_not_disable_fingerprint(self):
        """``timeout`` may be an ``httpx.Timeout`` (a supported, non-JSON shape).
        It must not force the fingerprint to None, which would stop every model step
        of the run from being cached or replayed."""
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

    def test_prompt_cache_settings_excluded_from_fingerprint(self):
        """Turning ``cache_prompt`` on or off between attempts must not re-run cached steps."""
        uncached = fingerprint_model_request(
            "m", make_messages(), {"temperature": 0.2}, ModelRequestParameters()
        )
        cached = fingerprint_model_request(
            "m",
            make_messages(),
            {
                "temperature": 0.2,
                "anthropic_cache": True,
                "anthropic_cache_messages": True,
                "bedrock_cache_instructions": "1h",
                "openrouter_cache_tool_definitions": True,
            },
            ModelRequestParameters(),
        )

        assert uncached == cached

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


class TestPydanticNativeValues:
    """Values that are not JSON types but hash the same on every attempt must still fingerprint.

    Tool arguments reach ``fingerprint_tool_call`` already coerced by pydantic, and
    ``tool_choice`` accepts a dataclass while genuinely affecting the response, so it
    cannot be stripped as transport-only. Since a step that cannot be fingerprinted is
    no longer cached at all, refusing these values would stop an ordinary typed tool
    from ever being cached.
    """

    def test_datetime_tool_argument_fingerprints(self):
        when = datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc)

        assert fingerprint_tool_call("t", {"when": when}, "id1") is not None

    def test_decimal_tool_argument_fingerprints(self):
        assert fingerprint_tool_call("t", {"amount": Decimal("10.5")}, "id1") is not None

    def test_datetime_tool_argument_is_stable_and_distinguishing(self):
        early = datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc)
        late = datetime.datetime(2026, 6, 1, tzinfo=datetime.timezone.utc)

        assert fingerprint_tool_call("t", {"when": early}, "id1") == fingerprint_tool_call(
            "t", {"when": early}, "id1"
        )
        assert fingerprint_tool_call("t", {"when": early}, "id1") != fingerprint_tool_call(
            "t", {"when": late}, "id1"
        )

    def test_tool_choice_dataclass_fingerprints(self):
        fp = fingerprint_model_request(
            "m",
            make_messages(),
            {"tool_choice": ToolOrOutput(function_tools=["my_tool"])},
            ModelRequestParameters(),
        )

        assert fp is not None

    def test_tool_choice_dataclass_still_affects_the_fingerprint(self):
        one = fingerprint_model_request(
            "m",
            make_messages(),
            {"tool_choice": ToolOrOutput(function_tools=["a"])},
            ModelRequestParameters(),
        )
        other = fingerprint_model_request(
            "m",
            make_messages(),
            {"tool_choice": ToolOrOutput(function_tools=["b"])},
            ModelRequestParameters(),
        )

        assert one is not None
        assert one != other

    def test_value_pydantic_cannot_serialize_still_returns_none(self):
        """Normalization must not turn a genuinely unserializable value into a hash."""
        assert fingerprint_tool_call("t", {"v": object()}, "id1") is None

    def test_plain_payload_digest_is_unchanged_by_normalization(self):
        """Fingerprints recorded before normalization must still match, so cached entries survive."""
        payload = {"model": "m", "args": {"b": [1, True, None, "x", 2.5]}, "settings": None}
        pre_normalization = hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()

        assert _digest(_render(payload)) == pre_normalization

    def test_dict_keys_render_as_a_json_mode_dump_renders_them(self):
        by_day = {datetime.date(2026, 1, 1): 1.5, datetime.date(2026, 1, 2): 2.5}

        assert _render({"series": by_day}) == {"series": {"2026-01-01": 1.5, "2026-01-02": 2.5}}
        assert _render({1: "a", "b": 2}) == {"1": "a", "b": 2}

    def test_keys_that_collide_once_rendered_are_refused(self):
        """``1`` and ``"1"`` render alike, so hashing either payload could replay the other."""
        assert fingerprint_tool_call("t", {"d": {1: "a", "1": "b"}}, "id1") is None

    def test_keys_that_collide_only_in_the_message_dump_are_refused(self):
        """The message dump renders ``inf`` and ``nan`` keys alike, where pydantic's defaults do not."""
        fp = fingerprint_model_request(
            "m", _with_tool_return({math.inf: 100, math.nan: "unpriced"}), None, ModelRequestParameters()
        )

        assert fp is None

    def test_bytes_that_are_not_utf8_render_as_base64(self):
        """Decoding bytes as text would raise on an image and stop the tool from being cached."""
        assert _render({"image": _PNG}) == {"image": base64.urlsafe_b64encode(_PNG).decode()}
        assert fingerprint_tool_call("t", {"image": _PNG}, "id1") is not None

    def test_bytes_dict_keys_render_as_base64_with_the_sets_under_them_ordered(self):
        key = base64.urlsafe_b64encode(_PNG).decode()

        assert _render({"d": {_PNG: _MANY}}) == {"d": {key: sorted(_MANY)}}


def _json_mode_reference(model_identifier, messages, model_request_parameters, settings=None):
    """The fingerprint main computes: pydantic's json-mode dump, hashed as is, with plain JSON settings.

    For anything that is not a set, fingerprints must equal this, or entries stored
    by an earlier version stop matching and the first retry after an upgrade re-runs
    the whole agent.
    """
    dumped = ModelMessagesTypeAdapter.dump_python(messages, mode="json")
    stripped = [
        {
            **{k: v for k, v in message.items() if k not in ("timestamp", "run_id", "conversation_id")},
            "parts": [{k: v for k, v in part.items() if k != "timestamp"} for part in message["parts"]],
        }
        for message in dumped
    ]
    params = TypeAdapter(ModelRequestParameters).dump_python(model_request_parameters, mode="json")
    payload = {"model": model_identifier, "messages": stripped, "settings": settings, "params": params}
    return hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()


def _with_tool_return(content):
    return [
        ModelRequest(parts=[UserPromptPart(content="go")]),
        ModelResponse(parts=[ToolCallPart(tool_name="t", args={}, tool_call_id="c1")]),
        ModelRequest(parts=[ToolReturnPart(tool_name="t", content=content, tool_call_id="c1")]),
    ]


class TestMessageHistoryMatchesJsonModeDump:
    """The message history must hash exactly as pydantic's json-mode dump renders it.

    Each case here is something a hand-written renderer gets wrong: raw bytes (not
    valid UTF-8), dict keys that are not strings, ``NaN``, tuples, and fields whose
    serializer only applies in JSON mode, such as the set-typed request parameters
    and, on pydantic-ai versions that have it, ``InstructionPart.id``.
    """

    @pytest.mark.parametrize(
        "messages",
        [
            pytest.param(
                [
                    ModelRequest(
                        parts=[
                            UserPromptPart(content=["look", BinaryContent(data=_PNG, media_type="image/png")])
                        ]
                    )
                ],
                id="binary-content-in-prompt",
            ),
            pytest.param(_with_tool_return(_PNG), id="bytes-tool-return"),
            pytest.param(
                _with_tool_return({datetime.date(2026, 1, 1): 1.5, datetime.date(2026, 1, 2): 2.5}),
                id="date-keyed-tool-return",
            ),
            pytest.param(_with_tool_return({1: "a", "b": 2}), id="mixed-key-tool-return"),
            pytest.param(_with_tool_return({"x": math.nan}), id="nan-tool-return"),
            pytest.param(
                _with_tool_return({"when": datetime.datetime(2026, 1, 1)}), id="datetime-tool-return"
            ),
            pytest.param([ModelRequest(parts=(UserPromptPart(content="hi"),))], id="tuple-parts"),
            pytest.param(_with_tool_return({"pair": ("a", 1)}), id="tuple-tool-return"),
        ],
    )
    def test_fingerprint_equals_the_json_mode_digest(self, messages):
        fp = fingerprint_model_request("m", messages, None, ModelRequestParameters())

        assert fp is not None
        assert fp == _json_mode_reference("m", messages, ModelRequestParameters())

    def test_request_parameters_hash_as_their_json_mode_dump(self):
        """The request parameters dump differently in python and json mode: set-typed fields
        such as ``revealed_tool_names`` on every supported version, and ``InstructionPart.id``
        on the versions that have it."""
        seen = {}

        class Spy(FunctionModel):
            async def request(self, messages, model_settings, model_request_parameters):
                seen.setdefault("messages", messages)
                seen.setdefault("params", model_request_parameters)
                return await super().request(messages, model_settings, model_request_parameters)

        async def respond(messages, info):
            return ModelResponse(parts=[TextPart(content="ok")])

        Agent(Spy(respond), instructions="Be terse.").run_sync("hi")

        fp = fingerprint_model_request("m", seen["messages"], None, seen["params"])

        assert fp == _json_mode_reference("m", seen["messages"], seen["params"])

    def test_tuple_parts_still_drop_part_timestamps(self):
        """Message history passed as objects can hold its parts in a tuple."""
        t1 = datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc)
        t2 = datetime.datetime(2026, 1, 2, tzinfo=datetime.timezone.utc)

        def history(timestamp):
            return [ModelRequest(parts=(UserPromptPart(content="hi", timestamp=timestamp),))]

        assert fingerprint_model_request(
            "m", history(t1), None, ModelRequestParameters()
        ) == fingerprint_model_request("m", history(t2), None, ModelRequestParameters())

    def test_set_in_a_tool_return_hashes_as_its_ordered_members(self):
        assert fingerprint_model_request(
            "m", _with_tool_return({"tags": {"b", "c", "a"}}), None, ModelRequestParameters()
        ) == fingerprint_model_request(
            "m", _with_tool_return({"tags": ["a", "b", "c"]}), None, ModelRequestParameters()
        )


class TestSetMemberOrdering:
    """Sets must hash in a fixed order rather than the interpreter's iteration order.

    A ``set[str]`` iterates in an order derived from the process hash seed, and every
    task attempt runs in a fresh process. Hashing that order would produce a digest
    the next attempt never reproduces, so the step would re-run live on every retry
    -- worse than declining to cache it, which at least costs nothing extra.
    """

    def test_set_hashes_as_its_ordered_members(self):
        assert _render({"tags": {"beta", "alpha"}}) == {"tags": ["alpha", "beta"]}

    def test_set_matches_the_equivalent_list(self):
        assert _render({"tags": {"alpha", "beta", "gamma"}}) == _render({"tags": ["alpha", "beta", "gamma"]})

    def test_frozenset_matches_set(self):
        assert _render({"tags": frozenset({"a", "b"})}) == _render({"tags": {"b", "a"}})

    def test_different_members_still_produce_different_digests(self):
        assert _render({"tags": {"a", "b"}}) != _render({"tags": {"a", "c"}})

    def test_set_nested_inside_a_list(self):
        assert _render({"filters": [{"z", "y"}]}) == {"filters": [["y", "z"]]}

    def test_set_inside_a_dataclass_field(self):
        @dataclasses.dataclass
        class Filter:
            tags: set

        assert _render(Filter(tags={"b", "a"})) == {"tags": ["a", "b"]}

    def test_set_inside_a_basemodel_field(self):
        class Filter(pydantic.BaseModel):
            tags: set[str]

        assert _render(Filter(tags={"b", "a"})) == {"tags": ["a", "b"]}

    def test_digest_is_stable_across_process_hash_seeds(self):
        """The real proof: two fresh interpreters must agree, as two attempts would.

        In-process comparisons cannot catch a hash-seed dependency, since one process
        has one seed. The subprocess loads the module by path so it does not pay for
        importing Airflow.
        """
        snippet = (
            "import importlib.util;"
            f"spec = importlib.util.spec_from_file_location('fp', r'{fingerprint_module.__file__}');"
            "mod = importlib.util.module_from_spec(spec);"
            "spec.loader.exec_module(mod);"
            "print(mod._digest(mod._render({'tags': {'alpha', 'beta', 'gamma', 'delta'}})))"
        )
        digests = set()
        for seed in ("0", "1", "2", "42"):
            completed = subprocess.run(
                [sys.executable, "-c", snippet],
                capture_output=True,
                text=True,
                env={**os.environ, "PYTHONHASHSEED": seed, "PYTHONWARNINGS": "ignore"},
                check=True,
            )
            digests.add(completed.stdout.strip().splitlines()[-1])

        assert len(digests) == 1, f"digest depends on the hash seed: {digests}"

    def test_set_returned_by_a_tool_is_stable_across_process_hash_seeds(self):
        """The same proof through the message history, where a tool's set return lands."""
        snippet = (
            "import importlib.util;"
            "from pydantic_ai.messages import ModelRequest, ToolReturnPart;"
            "from pydantic_ai.models import ModelRequestParameters;"
            f"spec = importlib.util.spec_from_file_location('fp', r'{fingerprint_module.__file__}');"
            "mod = importlib.util.module_from_spec(spec);"
            "spec.loader.exec_module(mod);"
            "part = ToolReturnPart(tool_name='t', content={'alpha', 'beta', 'gamma', 'delta'}, tool_call_id='c1');"
            "print(mod.fingerprint_model_request('m', [ModelRequest(parts=[part])], None, ModelRequestParameters()))"
        )
        digests = set()
        for seed in ("0", "1", "2", "42"):
            completed = subprocess.run(
                [sys.executable, "-c", snippet],
                capture_output=True,
                text=True,
                env={**os.environ, "PYTHONHASHSEED": seed, "PYTHONWARNINGS": "ignore"},
                check=True,
            )
            digests.add(completed.stdout.strip().splitlines()[-1])

        assert "None" not in digests
        assert len(digests) == 1, f"digest depends on the hash seed: {digests}"


class TestLazilyValidatedIterable:
    """A lazily validated ``Iterable`` must be refused, not consumed.

    ``to_jsonable_python`` drains an iterator to render it. Tool arguments are
    fingerprinted before the tool runs, so draining one would hand the tool an
    exhausted iterator and cache that wrong result under the fingerprint of the
    full input. Declining to fingerprint leaves the step uncached instead.
    """

    def test_validator_iterator_argument_is_not_fingerprinted(self):
        values = pydantic.TypeAdapter(Iterable[int]).validate_python([1, 2, 3])

        assert fingerprint_tool_call("total", {"values": values}, "id1") is None

    def test_the_iterator_is_left_unread(self):
        values = pydantic.TypeAdapter(Iterable[int]).validate_python([1, 2, 3])

        fingerprint_tool_call("total", {"values": values}, "id1")

        assert list(values) == [1, 2, 3]

    def test_a_generator_is_refused_too(self):
        assert fingerprint_tool_call("total", {"values": (n for n in (1, 2))}, "id1") is None

    def test_an_ordinary_list_argument_still_fingerprints(self):
        assert fingerprint_tool_call("total", {"values": [1, 2, 3]}, "id1") is not None


class _RowsWithFirst(pydantic.BaseModel):
    rows: list[int]

    @pydantic.computed_field  # type: ignore[prop-decorator]
    @property
    def first(self) -> int:
        return self.rows[0]


@dataclasses.dataclass
class _SizedName:
    name: str
    size: int = dataclasses.field(init=False)


class _UnreadableMapping(Mapping):
    def __getitem__(self, key):
        raise KeyError(key)

    def __iter__(self):
        return iter(["a"])

    def __len__(self):
        return 1


class _TicketWithHeadline(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(extra="allow")
    tags: list[str]

    @property
    def headline(self) -> str:
        return self.tags[0]


class TestUnwalkablePayloadsDegradeRatherThanRaise:
    """Whatever the renderer cannot walk must become ``None``, never propagate.

    A failed fingerprint costs a re-run; an exception escaping here would fail the
    task outright, which durable execution must never do on its own account.
    """

    def test_circular_reference_returns_none(self):
        """The iterator and set walks recurse before pydantic can apply its own cycle check."""
        cycle: dict = {}
        cycle["self"] = cycle

        assert fingerprint_tool_call("t", {"v": cycle}, "id1") is None

    def test_circular_reference_in_model_settings_returns_none(self):
        cycle: dict = {}
        cycle["self"] = cycle

        fp = fingerprint_model_request(
            "m",
            make_messages(),
            {"extra_body": cycle},
            ModelRequestParameters(),
        )

        assert fp is None

    @pytest.mark.parametrize(
        "value",
        [
            pytest.param(_RowsWithFirst(rows=[]), id="computed-field-raises"),
            pytest.param(_SizedName(name="a"), id="unset-init-false-field"),
            pytest.param(_UnreadableMapping(), id="mapping-raises"),
        ],
    )
    def test_error_raised_by_user_code_returns_none(self, value):
        """Rendering runs computed fields, reads dataclass fields and iterates mappings."""
        assert fingerprint_tool_call("t", {"v": value}, "id1") is None

    def test_error_raised_by_user_code_in_a_tool_return_returns_none(self):
        fp = fingerprint_model_request(
            "m", _with_tool_return(_RowsWithFirst(rows=[])), None, ModelRequestParameters()
        )

        assert fp is None

    def test_warning_names_the_type_of_the_error(self):
        # Patched rather than captured: on Airflow 2 the task logger wraps the stdlib logger,
        # which structlog's test capture does not see.
        with patch("airflow.providers.common.ai.durable.fingerprint.log") as fingerprint_log:
            fingerprint_tool_call("t", {"v": _RowsWithFirst(rows=[])}, "id1")

        fingerprint_log.warning.assert_called_once()
        assert fingerprint_log.warning.call_args.kwargs["error_type"] == "IndexError"

    def test_extra_named_like_a_property_does_not_run_the_property(self):
        """The property is no part of the rendering, so running it could only raise."""
        ticket = _TicketWithHeadline.model_validate({"tags": [], "headline": "n/a"})

        assert fingerprint_tool_call("t", {"ticket": ticket}, "id1") is not None


def _lazy_ints():
    return pydantic.TypeAdapter(Iterable[int]).validate_python([1, 2, 3])


class _QueryWithIterableField(pydantic.BaseModel):
    ids: Iterable[int]


class _QueryWithIterableExtra(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(extra="allow")
    __pydantic_extra__: dict[str, Iterable[int]]


@dataclasses.dataclass
class _GroupWithIterable:
    ids: Iterable[int]


class _QueryWithPrivateIterator(pydantic.BaseModel):
    _ids: Iterator[int] = pydantic.PrivateAttr()

    @pydantic.computed_field  # type: ignore[prop-decorator]
    @property
    def ids(self) -> list[int]:
        return list(self._ids)


class _BatchWithPendingIds(pydantic.BaseModel):
    ids: list[int]

    @pydantic.computed_field  # type: ignore[prop-decorator]
    @functools.cached_property
    def pending(self) -> Iterable[int]:
        return iter(self.ids)


class TestNestedIterators:
    """An iterator nested anywhere in the arguments is refused, and left unread for the tool."""

    @pytest.mark.parametrize(
        "shape", ["list", "tuple", "dict", "model-field", "dataclass-field", "model-extra"]
    )
    def test_nested_iterator_is_refused_and_left_unread(self, shape):
        if shape == "model-field":
            holder = _QueryWithIterableField(ids=[1, 2, 3])
            args, values = {"query": holder}, holder.ids
        elif shape == "model-extra":
            holder = _QueryWithIterableExtra(ids=[1, 2, 3])
            args, values = {"query": holder}, holder.model_extra["ids"]
        else:
            values = _lazy_ints()
            args = {
                "list": {"groups": [values]},
                "tuple": {"groups": (values,)},
                "dict": {"groups": {"a": values}},
                "dataclass-field": {"group": _GroupWithIterable(ids=values)},
            }[shape]

        assert fingerprint_tool_call("total", args, "id1") is None
        assert list(values) == [1, 2, 3]

    def test_generator_in_a_tool_return_is_refused_and_left_unread(self):
        """The model still receives every value, where rendering it first would hand it []."""
        returned = (n for n in (1, 2, 3))

        fp = fingerprint_model_request("m", _with_tool_return(returned), None, ModelRequestParameters())

        assert fp is None
        assert list(returned) == [1, 2, 3]

    def test_iterator_a_computed_field_reads_is_read_from_a_copy(self):
        """Tool arguments render from copies, so the tool still gets every value."""

        def query():
            built = _QueryWithPrivateIterator()
            built._ids = iter([1, 2, 3])
            return built

        original = query()

        fp = fingerprint_tool_call("total", {"query": original}, "id1")

        assert fp is not None
        assert fp == fingerprint_tool_call("total", {"query": query()}, "id1")
        assert list(original._ids) == [1, 2, 3]

    def test_iterator_a_computed_field_caches_is_refused_and_left_unread(self):
        """Rendering would evaluate the cached property and drain what it caches."""
        batch = _BatchWithPendingIds(ids=[1, 2, 3])

        assert fingerprint_tool_call("total", {"batch": batch}, "id1") is None
        assert list(batch.pending) == [1, 2, 3]

    def test_iterator_a_computed_field_caches_in_a_tool_return_is_left_unread(self):
        batch = _BatchWithPendingIds(ids=[1, 2, 3])

        fp = fingerprint_model_request("m", _with_tool_return(batch), None, ModelRequestParameters())

        assert fp is None
        assert list(batch.pending) == [1, 2, 3]


class _HeadersWithoutAuth(pydantic.BaseModel):
    headers: dict[str, str]

    @pydantic.field_serializer("headers")
    def _drop_authorization(self, headers: dict[str, str]) -> dict[str, str]:
        return {key: value for key, value in headers.items() if key != "authorization"}


class _QueryWithSortedFilters(pydantic.BaseModel):
    filters: dict[str, Any]

    @pydantic.field_serializer("filters")
    def _sort(self, filters: dict[str, Any]) -> dict[str, Any]:
        return dict(sorted(filters.items()))


def _drop_authorization(headers: dict[str, str]) -> dict[str, str]:
    return {key: value for key, value in headers.items() if key != "authorization"}


def _sort_filters(filters: dict[str, Any]) -> dict[str, Any]:
    return dict(sorted(filters.items()))


def _split_groups(groups: Iterable[str]) -> list[str]:
    return [part for group in groups for part in group.split(",")]


def _split_groups_sorted(groups: Iterable[str]) -> list[str]:
    return sorted(_split_groups(groups))


# A serializer nested inside the annotation is not one of the field's own, so the
# rendering under it is still walked, and the checks below are what keep it safe.
class _RequestWithNestedRedaction(pydantic.BaseModel):
    headers: Annotated[dict[str, str], pydantic.PlainSerializer(_drop_authorization)] | None
    tags: set[str]


class _QueryWithNestedSortedFilters(pydantic.BaseModel):
    filters: Annotated[dict[str, Any], pydantic.PlainSerializer(_sort_filters)] | None


class _CsvGroupList(pydantic.BaseModel):
    groups: Annotated[list[str], pydantic.PlainSerializer(_split_groups)] | None
    tags: set[str]


class _CsvGroupSet(pydantic.BaseModel):
    groups: Annotated[set[str], pydantic.PlainSerializer(_split_groups_sorted)] | None
    tags: set[str]


class _SizesByPath(pydantic.BaseModel):
    sizes: dict[PurePosixPath, int]
    tags: set[str]


class TestSerializersThatChangeKeys:
    """A serializer that drops or reorders keys is left as pydantic rendered it."""

    def test_dropped_key_is_not_mistaken_for_a_collision(self):
        messages = _with_tool_return(
            _HeadersWithoutAuth(headers={"authorization": "secret", "accept": "json"})
        )

        fp = fingerprint_model_request("m", messages, None, ModelRequestParameters())

        assert fp is not None
        assert fp == _json_mode_reference("m", messages, ModelRequestParameters())

    def test_key_dropped_by_a_nested_serializer_is_not_mistaken_for_a_collision(self):
        """The set makes the rendering worth checking, so the collision check does run here."""
        request = _RequestWithNestedRedaction(
            headers={"authorization": "secret", "accept": "json"}, tags=set(_MANY)
        )

        assert _render({"r": request}) == {"r": {"headers": {"accept": "json"}, "tags": sorted(_MANY)}}
        assert (
            fingerprint_model_request("m", _with_tool_return(request), None, ModelRequestParameters())
            is not None
        )

    @pytest.mark.parametrize(
        "model", [_QueryWithSortedFilters, _QueryWithNestedSortedFilters], ids=["field", "nested"]
    )
    def test_reordered_keys_do_not_pair_a_value_with_the_wrong_rendering(self, model):
        """Pairing by position would sort ``order`` as if it were the set, so both orders hash alike."""
        one = model(filters={"status": {3, 7}, "order": ["created", "id"]})
        other = model(filters={"status": {3, 7}, "order": ["id", "created"]})

        assert fingerprint_tool_call("t", {"q": one}, "id1") != fingerprint_tool_call(
            "t", {"q": other}, "id1"
        )

    @pytest.mark.parametrize("model", [_CsvGroupList, _CsvGroupSet], ids=["list", "set"])
    def test_serializer_that_changes_a_length_is_not_truncated(self, model):
        """Pairing by position would stop at the shorter side, so ``a,b`` and ``a,c`` would hash alike."""
        one = model(groups=["a,b"], tags={"x"})
        other = model(groups=["a,c"], tags={"x"})

        assert fingerprint_tool_call("t", {"q": one}, "id1") != fingerprint_tool_call(
            "t", {"q": other}, "id1"
        )

    def test_colliding_keys_are_still_refused(self):
        assert fingerprint_tool_call("t", {"d": {1: "a", "1": "b"}}, "id1") is None

    def test_keys_only_the_enclosing_schema_can_render_still_fingerprint(self):
        """pydantic renders a ``PurePosixPath`` key through the model's schema, but not on its own."""
        value = _SizesByPath(sizes={PurePosixPath("a/b"): 1}, tags={"x"})

        assert fingerprint_tool_call("t", {"v": value}, "id1") is not None
        assert (
            fingerprint_model_request("m", _with_tool_return(value), None, ModelRequestParameters())
            is not None
        )


_MANY = {f"tag-{n}" for n in range(12)}


class TestAliasedAndRootModelSets:
    """Sets under an alias or in a ``RootModel`` are ordered too."""

    def test_set_under_an_alias_is_ordered(self):
        class Aliased(pydantic.BaseModel):
            tags: set[str] = pydantic.Field(alias="labels")

        assert _render(Aliased(labels=_MANY)) == {"labels": sorted(_MANY)}

    def test_set_under_a_generated_alias_is_ordered(self):
        class Camel(pydantic.BaseModel):
            model_config = pydantic.ConfigDict(
                alias_generator=pydantic.alias_generators.to_camel, serialize_by_alias=True
            )
            my_tags: set[str]

        assert _render(Camel(myTags=_MANY)) == {"myTags": sorted(_MANY)}

    def test_root_model_set_is_ordered(self):
        assert _render(pydantic.RootModel[set[str]](_MANY)) == sorted(_MANY)

    def test_set_under_a_serialization_alias_is_ordered(self):
        class SerializationAlias(pydantic.BaseModel):
            tags: set[str] = pydantic.Field(serialization_alias="labels")

        assert _render(SerializationAlias(tags=_MANY)) == {"labels": sorted(_MANY)}

    def test_set_under_an_alias_in_a_pydantic_dataclass_is_ordered(self):
        @pydantic.dataclasses.dataclass
        class Aliased:
            tags: set[str] = pydantic.Field(alias="labels")

        assert _render(Aliased(labels=_MANY)) == {"labels": sorted(_MANY)}  # type: ignore[call-arg]

    def test_digests_are_stable_across_process_hash_seeds(self):
        """Aliased, camelCase and root-model sets, and ``revealed_tool_names`` in real request params."""
        snippet = textwrap.dedent(
            f"""
            import importlib.util
            import pydantic
            from pydantic.alias_generators import to_camel
            from pydantic_ai.messages import ModelRequest, ToolReturnPart
            from pydantic_ai.models import ModelRequestParameters

            spec = importlib.util.spec_from_file_location("fp", r"{fingerprint_module.__file__}")
            mod = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(mod)

            class Aliased(pydantic.BaseModel):
                tags: set[str] = pydantic.Field(alias="labels")

            class Camel(pydantic.BaseModel):
                model_config = pydantic.ConfigDict(alias_generator=to_camel, serialize_by_alias=True)
                my_tags: set[str]

            tags = {{"alpha", "beta", "gamma", "delta", "epsilon"}}
            print(mod.fingerprint_tool_call(
                "t", {{"a": Aliased(labels=tags), "r": pydantic.RootModel[set[str]](tags)}}, "c1"
            ))
            history = [ModelRequest(parts=[ToolReturnPart(tool_name="t", content=Camel(myTags=tags), tool_call_id="c1")])]
            params = ModelRequestParameters(revealed_tool_names={{"search", "fetch", "summarize", "rank"}})
            print(mod.fingerprint_model_request("m", history, None, params))
            """
        )
        outputs = set()
        for seed in ("0", "1", "2", "42"):
            completed = subprocess.run(
                [sys.executable, "-c", snippet],
                capture_output=True,
                text=True,
                env={**os.environ, "PYTHONHASHSEED": seed, "PYTHONWARNINGS": "ignore"},
                check=True,
            )
            outputs.add(tuple(completed.stdout.strip().splitlines()[-2:]))

        assert len(outputs) == 1, f"digest depends on the hash seed: {outputs}"
        assert "None" not in next(iter(outputs))


def _earlier_tool_fingerprint(name, tool_args, tool_call_id):
    """What an earlier version stored for a tool call: the raw arguments, hashed by ``json``."""
    payload = {"name": name, "args": tool_args, "tool_call_id": tool_call_id}
    return hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()


class TestPlainJsonHashesAsBefore:
    """Plain JSON arguments and settings keep the digests an earlier version stored.

    ``json`` orders numeric keys by value and writes ``None`` and ``inf`` keys as
    ``null`` and ``Infinity``, and it nests far deeper than pydantic renders. Sending
    plain JSON through pydantic would change or lose those digests, so an entry cached
    before an upgrade would stop matching and a tool that is not idempotent would run
    twice.
    """

    @pytest.mark.parametrize(
        "tool_args",
        [
            pytest.param({"bias": {9: 0.5, 10: 0.25}}, id="int-keys"),
            pytest.param({"bands": {2.5: "a", 10.5: "b"}}, id="float-keys"),
            pytest.param({"d": {None: 1}}, id="none-key"),
            pytest.param({"d": {math.inf: 1}}, id="inf-key"),
        ],
    )
    def test_tool_arguments_hash_as_before(self, tool_args):
        assert fingerprint_tool_call("t", tool_args, "id1") == _earlier_tool_fingerprint(
            "t", tool_args, "id1"
        )

    def test_settings_hash_as_before(self):
        settings: Any = {"extra_body": {"logit_bias": {9: 1, 10: -1}}}
        messages = make_messages()

        fp = fingerprint_model_request("m", messages, settings, ModelRequestParameters())

        assert fp == _json_mode_reference("m", messages, ModelRequestParameters(), settings=settings)

    def test_deeply_nested_arguments_hash_as_before(self):
        tree: dict[str, Any] = {"leaf": 1}
        for _ in range(200):
            tree = {"child": tree}

        assert fingerprint_tool_call("t", {"tree": tree}, "id1") == _earlier_tool_fingerprint(
            "t", {"tree": tree}, "id1"
        )


class _ColumnsWithExtras(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(extra="allow")
    columns: set[str] = pydantic.Field(alias="Columns")


class _SwappedAliases(pydantic.BaseModel):
    first: list[str] = pydantic.Field(alias="second")
    second: set[str] = pydantic.Field(alias="first")


class _RankedTags(pydantic.BaseModel):
    tags: set[str] = pydantic.Field(alias="labels")
    ranking: list[str] = pydantic.Field(exclude=True)

    @pydantic.computed_field(alias="tags")  # type: ignore[prop-decorator]
    @property
    def ranked(self) -> list[str]:
        return self.ranking


class _RowsWithTotal(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(extra="allow")
    rows: list[int]

    @pydantic.computed_field  # type: ignore[prop-decorator]
    @property
    def total(self) -> int:
        return sum(self.rows)


class TestRenderedKeysPairWithTheirOwnValues:
    """A set is sorted where it rendered, never a list that rendered under a key it shares a name with.

    Sorting such a list would make two payloads that differ only in its order hash
    alike, and replay the cached result of one for the other.
    """

    def test_extra_named_like_an_aliased_field(self):
        def query(order):
            return _ColumnsWithExtras.model_validate({"Columns": ["x", "y"], "columns": order})

        assert fingerprint_tool_call("t", {"q": query(["a", "b"])}, "id1") != fingerprint_tool_call(
            "t", {"q": query(["b", "a"])}, "id1"
        )

    def test_swapped_aliases_in_a_tool_return(self):
        """The message history renders fields by name, so an alias must not pair them."""

        def fingerprint(order):
            returned = _SwappedAliases.model_validate({"second": order, "first": {"x", "y"}})
            return fingerprint_model_request("m", _with_tool_return(returned), None, ModelRequestParameters())

        assert fingerprint(["a", "b"]) is not None
        assert fingerprint(["a", "b"]) != fingerprint(["b", "a"])

    def test_computed_field_aliased_like_a_field_name(self):
        def query(ranking):
            return _RankedTags(labels={"x", "y"}, ranking=ranking)

        assert fingerprint_tool_call("t", {"q": query(["x", "y"])}, "id1") != fingerprint_tool_call(
            "t", {"q": query(["y", "x"])}, "id1"
        )

    def test_extra_that_overwrites_a_field_is_hashed_as_rendered(self):
        """By name the extra's list renders under the field's key, and is not sorted as the field's set."""
        query = _ColumnsWithExtras.model_validate({"Columns": ["x", "y"], "columns": ["b", "a"]})
        messages = _with_tool_return(query)

        assert fingerprint_model_request("m", messages, None, ModelRequestParameters()) == (
            _json_mode_reference("m", messages, ModelRequestParameters())
        )
        # By alias the two keys differ, and only the field's set is sorted.
        assert _render({"q": query}) == {"q": {"Columns": ["x", "y"], "columns": ["b", "a"]}}

    def test_extra_that_a_computed_field_overwrites_is_hashed_as_rendered(self):
        messages = _with_tool_return(_RowsWithTotal.model_validate({"rows": [1], "total": 99}))

        assert fingerprint_model_request("m", messages, None, ModelRequestParameters()) == (
            _json_mode_reference("m", messages, ModelRequestParameters())
        )


# U+00E9, an accented "e", sorts after "z" as text, but its JSON escape "\\u00e9" sorts
# before it, so re-sorting a serializer's output by its JSON encoding would reorder it.
_ACCENTED = frozenset({"z", "é"})


class _SortedByFieldSerializer(pydantic.BaseModel):
    tags: frozenset[str]

    @pydantic.field_serializer("tags")
    def _sorted(self, tags: frozenset[str]) -> list[str]:
        return sorted(tags)


class _SortedByWildcardSerializer(pydantic.BaseModel):
    tags: frozenset[str]

    @pydantic.field_serializer("*")
    def _sorted(self, value: Any) -> Any:
        return sorted(value)


class _SortedByAnnotation(pydantic.BaseModel):
    tags: Annotated[frozenset[str], pydantic.PlainSerializer(sorted)]


class _SortedByModelSerializer(pydantic.BaseModel):
    tags: frozenset[str]

    @pydantic.model_serializer
    def _sorted(self) -> dict[str, Any]:
        return {"tags": sorted(self.tags)}


class _SortedRoot(pydantic.RootModel[frozenset[str]]):
    @pydantic.field_serializer("root")
    def _sorted(self, tags: frozenset[str]) -> list[str]:
        return sorted(tags)


class _SortedNested(pydantic.BaseModel):
    tags: Annotated[frozenset[str], pydantic.PlainSerializer(sorted)] | None


class _SortedInList(pydantic.BaseModel):
    groups: list[Annotated[frozenset[str], pydantic.PlainSerializer(sorted)]]


class TestCustomSerializersKeepTheirOrder:
    """A list a serializer produced is hashed as the serializer produced it."""

    @pytest.mark.parametrize(
        "content",
        [
            pytest.param(_SortedByFieldSerializer(tags=_ACCENTED), id="field-serializer"),
            pytest.param(_SortedByWildcardSerializer(tags=_ACCENTED), id="wildcard-field-serializer"),
            pytest.param(_SortedByAnnotation(tags=_ACCENTED), id="annotated-serializer"),
            pytest.param(_SortedByModelSerializer(tags=_ACCENTED), id="model-serializer"),
            pytest.param(_SortedRoot(_ACCENTED), id="root-model-serializer"),
            pytest.param(_SortedNested(tags=_ACCENTED), id="serializer-nested-in-optional"),
            pytest.param(_SortedInList(groups=[_ACCENTED]), id="serializer-nested-in-list"),
        ],
    )
    def test_serializer_output_is_not_resorted(self, content):
        messages = _with_tool_return(content)

        assert fingerprint_model_request("m", messages, None, ModelRequestParameters()) == (
            _json_mode_reference("m", messages, ModelRequestParameters())
        )
        assert _render({"c": content}) == to_jsonable_python({"c": content})


class _FilterWithExtras(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(extra="allow")
    name: str


class _FilterWithCount(pydantic.BaseModel):
    tags: set[str]

    @pydantic.computed_field  # type: ignore[prop-decorator]
    @property
    def count(self) -> int:
        return len(self.tags)


class _FilterWithVersion(pydantic.BaseModel):
    tags: set[str]

    @pydantic.model_serializer(mode="wrap")
    def _with_version(self, handler: Any) -> dict[str, Any]:
        return {**handler(self), "version": 2}


class _FilterWithTagView(pydantic.BaseModel):
    tags: list[str]

    @pydantic.computed_field(alias="Tags")  # type: ignore[prop-decorator]
    @property
    def view(self) -> set[str]:
        return set(self.tags)


class TestRenderedKeysWithoutAField:
    """Keys that belong to no declared field -- extras, computed fields, keys a serializer adds -- still fingerprint."""

    @pytest.mark.parametrize(
        ("build", "expected"),
        [
            pytest.param(
                lambda: _FilterWithExtras.model_validate({"name": "a", "tags": set(_MANY)}),
                {"name": "a", "tags": sorted(_MANY)},
                id="extra",
            ),
            pytest.param(
                lambda: _FilterWithCount(tags=set(_MANY)),
                {"tags": sorted(_MANY), "count": 12},
                id="computed-field",
            ),
            pytest.param(
                lambda: _FilterWithTagView(tags=sorted(_MANY, reverse=True)),
                {"tags": sorted(_MANY, reverse=True), "Tags": sorted(_MANY)},
                id="computed-set",
            ),
            pytest.param(
                lambda: _FilterWithVersion(tags=set(_MANY)),
                {"tags": sorted(_MANY), "version": 2},
                id="serializer-added-key",
            ),
        ],
    )
    def test_sets_are_ordered_and_the_other_keys_kept(self, build, expected):
        assert _render({"f": build()}) == {"f": expected}
        assert (
            fingerprint_model_request("m", _with_tool_return(build()), None, ModelRequestParameters())
            is not None
        )


class _Scope(enum.Enum):
    READ = frozenset(_MANY)


class TestSetsElsewhereAreOrdered:
    """Sets reached through an ``Enum``, model settings or another set are ordered too."""

    def test_set_that_is_an_enum_value_is_ordered(self):
        assert _render({"scope": _Scope.READ}) == {"scope": sorted(_MANY)}
        assert fingerprint_model_request(
            "m", _with_tool_return(_Scope.READ), None, ModelRequestParameters()
        ) == fingerprint_model_request("m", _with_tool_return(sorted(_MANY)), None, ModelRequestParameters())

    def test_set_in_model_settings_is_ordered(self):
        def fingerprint(include):
            settings: Any = {"extra_body": {"include": include}}
            return fingerprint_model_request("m", make_messages(), settings, ModelRequestParameters())

        assert fingerprint(set(_MANY)) == fingerprint(sorted(_MANY))

    def test_sets_inside_a_set_are_ordered(self):
        groups = {frozenset(_MANY), frozenset({"epsilon", "delta"})}

        assert _render({"groups": groups}) == {"groups": [["delta", "epsilon"], sorted(_MANY)]}

    def test_digests_are_stable_across_process_hash_seeds(self):
        snippet = textwrap.dedent(
            f"""
            import enum
            import importlib.util
            from pydantic_ai.messages import ModelRequest, ToolReturnPart, UserPromptPart
            from pydantic_ai.models import ModelRequestParameters

            spec = importlib.util.spec_from_file_location("fp", r"{fingerprint_module.__file__}")
            mod = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(mod)

            tags = frozenset({{"alpha", "beta", "gamma", "delta", "epsilon"}})

            class Scope(enum.Enum):
                READ = tags

            history = [ModelRequest(parts=[ToolReturnPart(tool_name="t", content=Scope.READ, tool_call_id="c1")])]
            print(mod.fingerprint_model_request("m", history, None, ModelRequestParameters()))
            groups = {{tags, frozenset({{"zeta", "eta", "theta"}})}}
            print(mod.fingerprint_tool_call("t", {{"scope": Scope.READ, "groups": groups}}, "c1"))
            prompt = [ModelRequest(parts=[UserPromptPart(content="hi")])]
            settings = {{"extra_body": {{"include": set(tags)}}}}
            print(mod.fingerprint_model_request("m", prompt, settings, ModelRequestParameters()))
            """
        )
        outputs = set()
        for seed in ("0", "1", "2", "42"):
            completed = subprocess.run(
                [sys.executable, "-c", snippet],
                capture_output=True,
                text=True,
                env={**os.environ, "PYTHONHASHSEED": seed, "PYTHONWARNINGS": "ignore"},
                check=True,
            )
            outputs.add(tuple(completed.stdout.strip().splitlines()[-3:]))

        assert len(outputs) == 1, f"digest depends on the hash seed: {outputs}"
        assert "None" not in next(iter(outputs))


class _Windows(pydantic.BaseModel):
    windows: deque[Iterable[int]]


class _RecentTags(pydantic.BaseModel):
    recent: deque[frozenset[str]]


@pydantic.with_config(pydantic.ConfigDict(alias_generator=pydantic.alias_generators.to_camel))
@dataclasses.dataclass
class _CamelTags:
    user_tags: set[str]


class _HoldsCamelTags(pydantic.BaseModel):
    tags: _CamelTags


class _RankedOverExcluded(pydantic.BaseModel):
    tags: set[str] = pydantic.Field(exclude=True)
    ranking: list[str] = pydantic.Field(exclude=True)

    @pydantic.computed_field(alias="tags")  # type: ignore[prop-decorator]
    @property
    def ranked(self) -> list[str]:
        return self.ranking


class _Folder(pydantic.BaseModel):
    name: str
    children: list[_Folder] = []
    _parent: _Folder | None = pydantic.PrivateAttr(default=None)


class _Cart(pydantic.BaseModel):
    prices: list[float]

    @pydantic.computed_field  # type: ignore[prop-decorator]
    @functools.cached_property
    def total(self) -> float:
        return sum(self.prices)


class _Upload(pydantic.BaseModel):
    rows: Iterable[int]

    @pydantic.computed_field  # type: ignore[prop-decorator]
    @functools.cached_property
    def preview(self) -> list[int]:
        return list(itertools.islice(self.rows, 2))


@pydantic.dataclasses.dataclass
class _PendingBatch:
    ids: list[int]

    @pydantic.computed_field  # type: ignore[prop-decorator]
    @functools.cached_property
    def pending(self) -> Iterable[int]:
        return iter(self.ids)


def _camel_without_none(options: dict[str, Any]) -> dict[str, Any]:
    return {
        pydantic.alias_generators.to_camel(key): value for key, value in options.items() if value is not None
    }


class _Job(pydantic.BaseModel):
    options: Annotated[dict[str, Any], pydantic.PlainSerializer(_camel_without_none)] | None


class _JsonOnlyLonger(pydantic.BaseModel):
    tags: set[str]
    rows: list[str]

    @pydantic.model_serializer(mode="wrap", when_used="json")
    def _append(self, handler: Any) -> dict[str, Any]:
        data = handler(self)
        return {"tags": [*data["tags"], "extra"], "rows": [*data["rows"], "extra"]}


class _JsonOnlyRenamed(pydantic.BaseModel):
    tags: set[str]

    @pydantic.model_serializer(mode="wrap", when_used="json")
    def _rename(self, handler: Any) -> dict[str, Any]:
        return {"labels": handler(self)["tags"]}


class _JsonOnlyRedacted(pydantic.BaseModel):
    headers: dict[str, str]
    tags: set[str]

    @pydantic.field_serializer("headers", when_used="json")
    def _redact(self, headers: dict[str, str]) -> dict[str, str]:
        return {key: value for key, value in headers.items() if key != "authorization"}


class TestShapesPydanticGivesTheRendering:
    """Wherever pydantic reshapes a value, the python-mode dump that guides the ordering is reshaped alike.

    Aliases a model's config gives a stdlib dataclass, a serializer nested in an
    annotation, a computed field over excluded fields, a deque: each renders the same
    way in both modes, so a set is sorted where it rendered and nothing else moves.
    """

    def test_iterator_in_a_deque_is_refused_and_left_unread(self):
        windows = _Windows.model_validate({"windows": [[1, 2, 3]]})

        assert fingerprint_tool_call("total", {"w": windows}, "id1") is None
        assert (
            fingerprint_model_request("m", _with_tool_return(windows), None, ModelRequestParameters()) is None
        )
        assert list(windows.windows[0]) == [1, 2, 3]

    def test_set_in_a_deque_is_ordered(self):
        assert _render({"r": _RecentTags(recent=deque([frozenset(_MANY)]))}) == {
            "r": {"recent": [sorted(_MANY)]}
        }

    def test_set_in_a_dataclass_a_model_config_renames_is_ordered(self):
        value = _HoldsCamelTags(tags=_CamelTags(user_tags=set(_MANY)))

        assert _render({"h": value}) == {"h": {"tags": {"userTags": sorted(_MANY)}}}

    def test_none_and_nan_keys_that_collide_in_the_message_dump_are_refused(self):
        messages = _with_tool_return({None: 4, math.nan: 2, 1.5: 3})

        assert fingerprint_model_request("m", messages, None, ModelRequestParameters()) is None

    def test_computed_list_over_an_excluded_set_is_not_sorted(self):
        """The computed list renders under the excluded set's key, so its order must still count."""

        def fingerprint(ranking):
            return fingerprint_tool_call(
                "t", {"q": _RankedOverExcluded(tags={"x", "y"}, ranking=ranking)}, "id1"
            )

        assert fingerprint(["x", "y"]) != fingerprint(["y", "x"])

    def test_back_link_in_private_state_does_not_cost_the_fingerprint(self):
        """Rendering never reads private state, so a parent pointer there is no cycle."""
        root = _Folder(name="root", children=[_Folder(name="child")])
        root.children[0]._parent = root
        messages = _with_tool_return(root)

        assert fingerprint_model_request("m", messages, None, ModelRequestParameters()) == (
            _json_mode_reference("m", messages, ModelRequestParameters())
        )

    def test_cached_computed_field_is_not_left_on_the_tool_argument(self):
        """A tool that derives an updated copy must not inherit a total computed while fingerprinting."""
        cart = _Cart(prices=[10.0, 2.5])

        assert fingerprint_tool_call("quote", {"cart": cart}, "id1") is not None
        assert "total" not in cart.__dict__
        assert cart.model_copy(update={"prices": [10.0, 2.5, 5.0]}).total == 17.5

    def test_getter_that_reads_an_iterator_field_leaves_it_unread(self):
        upload = _Upload(rows=[1, 2, 3, 4])

        assert fingerprint_tool_call("load", {"upload": upload}, "id1") is None
        assert list(upload.rows) == [1, 2, 3, 4]

    def test_iterator_a_dataclass_computed_field_caches_is_refused_and_left_unread(self):
        tool_arg = _PendingBatch(ids=[1, 2, 3])
        returned = _PendingBatch(ids=[1, 2, 3])

        assert fingerprint_tool_call("total", {"batch": tool_arg}, "id1") is None
        assert "pending" not in tool_arg.__dict__
        assert (
            fingerprint_model_request("m", _with_tool_return(returned), None, ModelRequestParameters())
            is None
        )
        assert list(returned.pending) == [1, 2, 3]

    @pytest.mark.parametrize(
        "value",
        [
            pytest.param(_JsonOnlyLonger(tags=set(_MANY), rows=["b", "a"]), id="json-only-longer"),
            pytest.param(_JsonOnlyRenamed(tags=set(_MANY)), id="json-only-renamed"),
        ],
    )
    def test_rendering_a_json_only_serializer_reshapes_is_kept_as_rendered(self, value):
        """Python mode skips a json-only serializer, so where the two dumps disagree nothing is paired."""
        assert _render({"q": value}) == to_jsonable_python({"q": value}, by_alias=True, bytes_mode="base64")

    def test_key_a_json_only_serializer_drops_is_not_a_collision(self):
        """Python mode keeps the key, so only a key that is not a string can mean a collision."""
        value = _JsonOnlyRedacted(headers={"authorization": "secret", "accept": "json"}, tags=set(_MANY))

        assert _render({"q": value}) == {"q": {"headers": {"accept": "json"}, "tags": sorted(_MANY)}}

    def test_nested_serializer_that_renames_and_drops_keys_is_not_a_collision(self):
        """An int-keyed dict elsewhere turns the check on, and the renamed dict must still pass it."""
        job = _Job(options={"max_retries": 3, "retry_delay": None})
        messages = _with_tool_return({"job": job, "counts": {2025: 3}})

        assert fingerprint_model_request("m", messages, None, ModelRequestParameters()) == (
            _json_mode_reference("m", messages, ModelRequestParameters())
        )
