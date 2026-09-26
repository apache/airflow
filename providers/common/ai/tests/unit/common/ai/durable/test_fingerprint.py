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
import hashlib
import json
import math
import os
import subprocess
import sys
from collections.abc import Iterable
from decimal import Decimal

import httpx
import pydantic
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

    def test_bytes_that_are_not_utf8_render_as_base64(self):
        """Decoding bytes as text would raise on an image and stop the tool from being cached."""
        assert _render({"image": _PNG}) == {"image": base64.urlsafe_b64encode(_PNG).decode()}
        assert fingerprint_tool_call("t", {"image": _PNG}, "id1") is not None


def _json_mode_reference(model_identifier, messages, model_request_parameters):
    """The fingerprint main computes: pydantic's json-mode dump, hashed as is.

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
    payload = {"model": model_identifier, "messages": stripped, "settings": None, "params": params}
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
    serializer only applies in JSON mode, such as ``InstructionPart.id``.
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
        ],
    )
    def test_fingerprint_equals_the_json_mode_digest(self, messages):
        fp = fingerprint_model_request("m", messages, None, ModelRequestParameters())

        assert fp is not None
        assert fp == _json_mode_reference("m", messages, ModelRequestParameters())

    def test_request_parameters_hash_as_their_json_mode_dump(self):
        """An agent's instructions reach the request parameters with a json-only serializer."""
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
