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
import json
from contextlib import AsyncExitStack
from datetime import UTC, datetime
from unittest import mock

import pytest
from pydantic import BaseModel, TypeAdapter
from structlog.typing import FilteringBoundLogger

pytest.importorskip("anthropic")
pytest.importorskip("openai")
pytest.importorskip("google.genai")

from google.genai.batches import AsyncBatches
from google.genai.types import BatchJob, JobState
from openai.resources.batches import AsyncBatches as OpenAIAsyncBatches
from openai.types import Batch as OpenAIBatch
from pydantic_ai.exceptions import ModelHTTPError, UserError
from pydantic_ai.messages import (
    ModelMessage,
    ModelRequest,
    ModelResponse,
    SystemPromptPart,
    TextPart,
    ToolCallPart,
    UserPromptPart,
)
from pydantic_ai.models._batch_codec import BatchJobError
from pydantic_ai.models.anthropic import AnthropicModel
from pydantic_ai.models.batch import Batch, BatchResult, BatchStatus
from pydantic_ai.models.function import AgentInfo, FunctionModel
from pydantic_ai.models.google import GoogleModel
from pydantic_ai.models.openai import OpenAIResponsesModel
from pydantic_ai.profiles import ModelProfile
from pydantic_ai.providers.anthropic import AnthropicProvider
from pydantic_ai.providers.google import GoogleProvider
from pydantic_ai.providers.openai import OpenAIProvider

from airflow.providers.common.ai.batch import pydantic_ai_backend as backend_module
from airflow.providers.common.ai.batch.output_schema import build_output_spec
from airflow.providers.common.ai.batch.pydantic_ai_backend import (
    AnthropicBatchAdapter,
    GoogleBatchAdapter,
    OpenAIBatchAdapter,
    PydanticAIBatchAdapter,
    _run,
)
from airflow.providers.common.ai.exceptions import (
    LLMBatchInputError,
    LLMBatchJobError,
    LLMBatchLimitExceededError,
    LLMBatchModelMismatchError,
    LLMBatchStateReadError,
)
from airflow.providers.common.ai.hooks.pydantic_ai import PydanticAIHook
from airflow.providers.common.compat.sdk import Connection

KEY = "k" * 16
CLAUDE = "anthropic:claude-sonnet-4-5"
CLAUDE_OLD = "anthropic:claude-3-5-haiku-latest"
GEMINI = "google:gemini-2.5-flash"
GPT = "openai:gpt-5-mini"
GPT_4 = "openai:gpt-4.1-mini"


class Diagnosis(BaseModel):
    label: str
    score: float


def _prompt_of(messages: list[ModelMessage]) -> str:
    last = messages[-1]
    assert isinstance(last, ModelRequest)
    return str(next(part.content for part in last.parts if isinstance(part, UserPromptPart)))


class _Recorder:
    """A ``FunctionModel`` whose function records every call it serves and answers from ``reply``."""

    def __init__(self, reply=None):
        self.calls: list[tuple[list[ModelMessage], AgentInfo]] = []
        self.reply = reply or (lambda prompt: ModelResponse(parts=[TextPart(f"echo:{prompt}")]))
        self.model = FunctionModel(self._function)

    def _function(self, messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        self.calls.append((messages, info))
        return self.reply(_prompt_of(messages))


def _adapter(model, *, cls=AnthropicBatchAdapter, model_id=CLAUDE, handle=None, output_spec=None):
    return cls(
        model_id=model_id,
        batch_handle=handle,
        output_spec=output_spec,
        model_factory=lambda _model_id: model,
    )


def _submit(adapter, requests, *, spec=None, **kwargs):
    spec = spec or build_output_spec(str)
    kwargs.setdefault("system_prompt", "")
    kwargs.setdefault("max_tokens", 256)
    return adapter.submit(
        requests,
        model=CLAUDE.partition(":")[2],
        idempotency_key=KEY,
        input_fingerprint="fp",
        output_spec=spec,
        **kwargs,
    )


def _real_encode(adapter, requests, *, spec=None, **kwargs) -> list:
    """Encode ``requests`` on the adapter's real model class, which sends nothing."""
    spec = spec or build_output_spec(str)

    async def encode():
        async with AsyncExitStack() as stack:
            _, _, encoded = await adapter._encode(
                stack,
                requests,
                model=CLAUDE.partition(":")[2],
                idempotency_key=KEY,
                output_spec=spec,
                system_prompt=kwargs.pop("system_prompt", ""),
                max_tokens=kwargs.pop("max_tokens", 256),
                request_params=kwargs.pop("request_params", None),
            )
            return encoded

    return asyncio.run(encode())


def _claude(name: str = "claude-sonnet-4-5") -> AnthropicModel:
    return AnthropicModel(name, provider=AnthropicProvider(api_key="sk-ant-test"))


def _gemini(name: str = "gemini-2.5-flash") -> GoogleModel:
    return GoogleModel(name, provider=GoogleProvider(api_key="gemini-test"))


def _gpt(name: str = "gpt-5-mini") -> OpenAIResponsesModel:
    return OpenAIResponsesModel(name, provider=OpenAIProvider(api_key="sk-test"))


def _submitted_on(adapter_cls, model, *, model_name: str, provider_name: str) -> str:
    """Return the handle of a batch submitted on ``model``, with the group the real model class computes."""
    submitter = adapter_cls(model_id=f"{provider_name}:{model_name}", model_factory=lambda _id: model)
    group = _real_encode(submitter, [{"prompt": "x"}])[0].group
    batch = Batch(
        id="batches/1",
        model_name=model_name,
        provider_name=provider_name,
        group=group,
        status="in_progress",
        created_at=datetime(2026, 10, 1, tzinfo=UTC),
        request_count=1,
    )
    return TypeAdapter(Batch).dump_json(batch).decode()


class TestExistingBatchesUseTheModelTheyWereSubmittedWith:
    """pydantic-ai checks a batch's group, which names the Gemini model, on every poll, collect and cancel."""

    @pytest.fixture
    def pro_batch(self):
        """A Gemini batch submitted on a model other than the task's own ``gemini-2.5-flash``."""
        handle = _submitted_on(
            GoogleBatchAdapter, _gemini("gemini-2.5-pro"), model_name="gemini-2.5-pro", provider_name="google"
        )
        return GoogleBatchAdapter(
            model_id=GEMINI,
            batch_handle=handle,
            model_factory=lambda model_id: _gemini(model_id.partition(":")[2]),
        )

    @pytest.fixture
    def job(self):
        with mock.patch.object(
            AsyncBatches,
            "get",
            autospec=True,
            return_value=BatchJob(name="batches/1", state=JobState.JOB_STATE_SUCCEEDED),
        ) as get:
            yield get

    def test_a_gemini_batch_submitted_on_another_model_is_polled(self, pro_batch, job):
        assert pro_batch.get_batch("batches/1").status == "completed"

    def test_a_gemini_batch_submitted_on_another_model_is_collected(self, pro_batch, job):
        assert list(pro_batch.iter_results("batches/1")) == []

    def test_a_gemini_batch_submitted_on_another_model_is_cancelled(self, pro_batch, job):
        with mock.patch.object(AsyncBatches, "cancel", autospec=True) as cancel:
            pro_batch.cancel_batch("batches/1")

        assert cancel.call_args.kwargs["name"] == "batches/1"

    @pytest.mark.parametrize("job_state", [JobState.JOB_STATE_CANCELLED, JobState.JOB_STATE_EXPIRED])
    def test_a_job_that_ended_unrun_without_a_results_file_collects_as_no_results(self, pro_batch, job_state):
        """pydantic-ai's own collect raises its job error for a job with no results file; the backend reads it as none."""
        with mock.patch.object(
            AsyncBatches, "get", autospec=True, return_value=BatchJob(name="batches/1", state=job_state)
        ):
            assert list(pro_batch.iter_results("batches/1")) == []

    def test_a_failed_job_without_a_results_file_is_a_job_error(self, pro_batch):
        with mock.patch.object(
            AsyncBatches,
            "get",
            autospec=True,
            return_value=BatchJob(name="batches/1", state=JobState.JOB_STATE_FAILED),
        ):
            with pytest.raises(LLMBatchJobError, match="has no results"):
                list(pro_batch.iter_results("batches/1"))

    @pytest.mark.parametrize("operation", ["get_batch", "iter_results", "cancel_batch"])
    def test_every_operation_builds_the_model_named_by_the_handle_under_the_task_provider(self, operation):
        recorder = _Recorder()
        submitted = _submit(_adapter(recorder.model), [{"prompt": "a"}])
        batch = TypeAdapter(Batch).validate_json(submitted.handle)
        batch.model_name = "claude-haiku-4-5"
        asked: list[str] = []

        def factory(model_id):
            asked.append(model_id)
            return recorder.model

        adapter = AnthropicBatchAdapter(
            model_id=CLAUDE,
            batch_handle=TypeAdapter(Batch).dump_json(batch).decode(),
            model_factory=factory,
        )

        getattr(adapter, operation)(submitted.batch_id)

        assert asked == ["anthropic:claude-haiku-4-5"]

    def test_a_batch_recorded_without_a_handle_uses_the_task_model(self):
        asked: list[str] = []

        def factory(model_id):
            asked.append(model_id)
            return _claude()

        adapter = AnthropicBatchAdapter(model_id=CLAUDE, model_factory=factory)

        with mock.patch.object(AnthropicModel, "batch_cancel", autospec=True):
            adapter.cancel_batch("msgbatch_old")

        assert asked == [CLAUDE]

    def test_a_trigger_serialized_without_a_model_id_fails_with_the_cause(self):
        adapter = AnthropicBatchAdapter(model_factory=lambda _id: _claude())

        with pytest.raises(LLMBatchStateReadError, match="earlier version of this provider.*Retry the task"):
            adapter.get_batch("msgbatch_old")


class _NoLegacyAdapter(PydanticAIBatchAdapter):
    """A provider that has no way to rebuild a handle from a batch id."""

    name = "plain"


class TestInMemoryBatchRoundTrip:
    """The whole lifecycle against pydantic-ai's in-memory batch API."""

    def test_submit_returns_the_real_batch_id_and_a_handle_that_round_trips(self):
        recorder = _Recorder()
        adapter = _adapter(recorder.model)

        result = _submit(adapter, [{"prompt": "a"}, {"prompt": "b"}])

        batch = TypeAdapter(Batch).validate_json(result.handle)
        assert batch.id == result.batch_id
        assert batch.request_count == 2
        assert TypeAdapter(Batch).dump_json(batch).decode() == result.handle

    def test_a_new_adapter_bound_to_the_stored_handle_polls_and_reads_the_batch(self):
        recorder = _Recorder()
        submitted = _submit(_adapter(recorder.model), [{"prompt": "a"}, {"prompt": "b"}])

        later = _adapter(recorder.model, handle=submitted.handle)
        state = later.get_batch(submitted.batch_id)
        rows = sorted(later.iter_results(submitted.batch_id), key=lambda item: item.index)

        assert (state.status, state.counts, state.error_message) == ("completed", None, None)
        assert [row.custom_id for row in rows] == [f"{KEY}-0", f"{KEY}-1"]
        assert [later.extract_output(row, build_output_spec(str)).text for row in rows] == [
            "echo:a",
            "echo:b",
        ]
        assert all(row.provider_status == "success" for row in rows)
        assert all(row.usage is not None and row.model for row in rows)

    def test_the_async_poll_matches_the_sync_one(self):
        recorder = _Recorder()
        submitted = _submit(_adapter(recorder.model), [{"prompt": "a"}])

        state = asyncio.run(_adapter(recorder.model, handle=submitted.handle).aget_batch(submitted.batch_id))

        assert state.status == "completed"

    def test_cancel_goes_to_the_model_with_the_stored_handle(self):
        recorder = _Recorder()
        submitted = _submit(_adapter(recorder.model), [{"prompt": "a"}])
        adapter = _adapter(recorder.model, handle=submitted.handle)

        with mock.patch.object(
            type(recorder.model), "batch_cancel", autospec=True, side_effect=type(recorder.model).batch_cancel
        ) as cancel:
            adapter.cancel_batch(submitted.batch_id)
            asyncio.run(adapter.acancel_batch(submitted.batch_id))

        assert [call.args[1].id for call in cancel.call_args_list] == [submitted.batch_id] * 2

    def test_polling_without_a_recorded_handle_says_how_to_recover(self):
        adapter = _adapter(_Recorder().model, cls=_NoLegacyAdapter)

        with pytest.raises(LLMBatchJobError, match="retry the task so it re-attaches"):
            adapter.get_batch("msgbatch_old")

    def test_a_batch_that_has_not_ended_cannot_be_read(self):
        recorder = _Recorder()
        submitted = _submit(_adapter(recorder.model), [{"prompt": "a"}])
        adapter = _adapter(recorder.model, handle=submitted.handle)
        in_progress = TypeAdapter(Batch).validate_json(submitted.handle)
        in_progress.status = "in_progress"

        with mock.patch.object(type(recorder.model), "batch_status", autospec=True, return_value=in_progress):
            assert adapter.get_batch(submitted.batch_id).status == "in_progress"
            with pytest.raises(LLMBatchJobError, match="has not ended"):
                adapter.iter_results(submitted.batch_id)

    def test_the_system_prompt_and_settings_reach_the_model(self):
        recorder = _Recorder()
        _submit(
            _adapter(recorder.model),
            [
                {"prompt": "a"},
                {"prompt": "b", "system_prompt": "own", "max_tokens": 9, "params": {"top_p": 0.5}},
            ],
            system_prompt="batch-wide",
            request_params={"temperature": 0.2},
        )

        (first_messages, first_info), (second_messages, second_info) = recorder.calls
        system = lambda messages: [
            part.content for part in messages[0].parts if isinstance(part, SystemPromptPart)
        ]
        assert system(first_messages) == ["batch-wide"]
        assert system(second_messages) == ["own"]
        assert first_info.model_settings["temperature"] == 0.2
        assert first_info.model_settings["max_tokens"] == 256
        assert second_info.model_settings["max_tokens"] == 9
        assert second_info.model_settings["top_p"] == 0.5

    def test_structured_output_is_requested_natively_when_the_model_supports_json_schema(self):
        recorder = _Recorder()
        recorder.model._profile = ModelProfile(supports_json_schema_output=True)
        _submit(_adapter(recorder.model), [{"prompt": "a"}], spec=build_output_spec(Diagnosis))

        _, info = recorder.calls[0]
        parameters = info.model_request_parameters
        assert parameters.output_mode == "native"
        assert parameters.output_object.json_schema["properties"].keys() == {"label", "score"}
        assert info.output_tools == []

    def test_structured_output_falls_back_to_a_forced_output_tool(self):
        recorder = _Recorder()
        recorder.model._profile = ModelProfile(supports_json_schema_output=False)
        _submit(_adapter(recorder.model), [{"prompt": "a"}], spec=build_output_spec(Diagnosis))

        _, info = recorder.calls[0]
        assert info.model_request_parameters.output_mode == "tool"
        assert [tool.name for tool in info.output_tools] == ["Diagnosis"]
        assert info.allow_text_output is False

    def test_structured_output_arrives_as_a_tool_call_or_as_json_text(self):
        spec = build_output_spec(Diagnosis)
        by_prompt = {
            "tool": ModelResponse(parts=[ToolCallPart("Diagnosis", {"label": "a", "score": 1.0})]),
            "text": ModelResponse(parts=[TextPart('{"label": "b", "score": 2.0}')]),
            "none": ModelResponse(parts=[]),
        }
        recorder = _Recorder(lambda prompt: by_prompt[prompt])
        submitted = _submit(
            _adapter(recorder.model), [{"prompt": "tool"}, {"prompt": "text"}, {"prompt": "none"}], spec=spec
        )

        adapter = _adapter(recorder.model, handle=submitted.handle, output_spec=spec)
        rows = sorted(adapter.iter_results(submitted.batch_id), key=lambda item: item.index)
        extracted = [adapter.extract_output(row, spec) for row in rows]

        assert [(item.kind, item.value, item.text) for item in extracted] == [
            ("json_value", {"label": "a", "score": 1.0}, None),
            ("json_text", None, '{"label": "b", "score": 2.0}'),
            ("absent", None, None),
        ]

    def test_an_http_error_becomes_an_errored_row_with_its_status(self):
        def reply(prompt):
            raise ModelHTTPError(
                429, "claude", {"error": {"type": "rate_limit_error", "message": "slow down"}}
            )

        recorder = _Recorder(reply)
        submitted = _submit(_adapter(recorder.model), [{"prompt": "a"}])

        (row,) = _adapter(recorder.model, handle=submitted.handle).iter_results(submitted.batch_id)

        assert row.provider_status == "errored"
        assert row.error["provider_code"] == "rate_limit_error"
        assert row.error["stage"] == "provider"
        assert "429" in row.error["message"]

    def test_an_unreadable_result_is_skipped_so_the_merge_counts_it_missing(self):
        def reply(prompt):
            raise RuntimeError("boom")

        recorder = _Recorder(reply)
        submitted = _submit(_adapter(recorder.model), [{"prompt": "a"}])

        rows = list(_adapter(recorder.model, handle=submitted.handle).iter_results(submitted.batch_id))

        assert rows == []

    def test_a_result_with_a_foreign_custom_id_is_skipped(self):
        recorder = _Recorder()
        adapter = _adapter(recorder.model)
        bad = BatchResult(custom_id="nodash", status="succeeded", body="{}")

        assert list(adapter._decode_all(recorder.model, [bad], "b")) == []

    @pytest.mark.parametrize(
        ("status", "expected"), [("expired", "expired"), ("canceled", "cancelled"), ("errored", "errored")]
    )
    def test_unrun_batch_results_keep_their_own_status(self, status, expected):
        recorder = _Recorder()
        adapter = _adapter(recorder.model)
        result = BatchResult(
            custom_id=f"{KEY}-3",
            status=status,
            body=json.dumps({"status_code": 500, "body": None}) if status == "errored" else None,
        )

        row = adapter._decode_one(
            recorder.model, result, adapter._output_parameters(recorder.model, build_output_spec(str))
        )

        assert (row.index, row.provider_status) == (3, expected)
        assert row.raw is None
        assert row.error["stage"] == "provider"


class TestSyncEntryPointsFromARunningLoop:
    def test_a_kill_signal_arriving_inside_a_running_loop_can_still_cancel(self):
        """``on_kill`` can interrupt the thread while ``asyncio.run`` is on its stack."""

        async def inside_a_loop():
            return _run(asyncio.sleep(0, "done"))

        assert asyncio.run(inside_a_loop()) == "done"

    def test_an_error_from_the_batch_call_is_not_chained_onto_the_loop_probe(self):
        async def fails():
            raise LLMBatchInputError("bad request")

        with pytest.raises(LLMBatchInputError) as raised:
            _run(fails())

        assert raised.value.__context__ is None


class TestRequestSettings:
    def test_typed_keys_become_settings_and_raw_provider_fields_go_through_extra_body(self):
        adapter = AnthropicBatchAdapter(model_id=CLAUDE)

        settings, dropped = adapter._build_settings(
            _claude(),
            {
                "temperature": 0.2,
                "top_k": 5,
                "metadata": {"user_id": "u"},
                "anthropic_cache_instructions": True,
            },
            {"temperature": 0.9, "thinking": {"type": "enabled", "budget_tokens": 1024}},
            max_tokens=77,
        )

        assert settings == {
            "temperature": 0.9,
            "top_k": 5,
            "anthropic_cache_instructions": True,
            "max_tokens": 77,
            "extra_body": {
                "metadata": {"user_id": "u"},
                "thinking": {"type": "enabled", "budget_tokens": 1024},
            },
        }
        assert dropped == set()

    def test_an_explicit_extra_body_is_merged_with_the_raw_fields(self):
        adapter = AnthropicBatchAdapter(model_id=CLAUDE)

        settings, _ = adapter._build_settings(_claude(), {"extra_body": {"a": 1}, "b": 2}, max_tokens=8)

        assert settings["extra_body"] == {"a": 1, "b": 2}

    def test_keys_the_batch_sets_itself_are_dropped(self):
        adapter = AnthropicBatchAdapter(model_id=CLAUDE)

        settings, dropped = adapter._build_settings(
            _claude(),
            {"model": "other", "messages": [], "max_tokens": 1, "system": "x", "extra_body": {"input": 1}},
            max_tokens=8,
        )

        assert settings == {"max_tokens": 8}
        assert dropped == {"model", "messages", "max_tokens", "system", "input"}

    def test_the_dropped_keys_are_logged_once_for_the_whole_batch(self):
        adapter = AnthropicBatchAdapter(model_id=CLAUDE, model_factory=lambda _id: _claude())

        with mock.patch.object(backend_module, "log", spec=FilteringBoundLogger) as log:
            _real_encode(
                adapter,
                [{"prompt": "a"}, {"prompt": "b", "params": {"model": "x"}}, {"prompt": "c"}],
                request_params={"system": "x"},
            )

        log.warning.assert_called_once_with(
            "Ignoring request parameters the batch sets itself", keys=["model", "system"]
        )

    def test_a_typed_setting_the_model_would_silently_drop_is_rejected(self):
        adapter = AnthropicBatchAdapter(model_id=CLAUDE)

        with pytest.raises(LLMBatchInputError, match="'seed' is not supported by AnthropicModel"):
            adapter._build_settings(_claude(), {"seed": 3}, max_tokens=8)

    def test_gemini_accepts_typed_settings_and_rejects_raw_fields(self):
        adapter = GoogleBatchAdapter(model_id=GEMINI)

        assert adapter._build_settings(_gemini(), {"temperature": 0.1, "top_k": 4}, max_tokens=8)[0] == {
            "temperature": 0.1,
            "top_k": 4,
            "max_tokens": 8,
        }
        with pytest.raises(LLMBatchInputError, match="Unknown request parameter 'metadata'"):
            adapter._build_settings(_gemini(), {"metadata": {}}, max_tokens=8)

    def test_gemini_rejects_extra_body_because_the_batch_api_has_no_passthrough(self):
        adapter = GoogleBatchAdapter(model_id=GEMINI)

        with pytest.raises(LLMBatchInputError, match="'extra_body' is not supported by GoogleModel"):
            adapter._build_settings(_gemini(), {"extra_body": {"a": 1}}, max_tokens=8)


class TestRealModelEncoding:
    """One real model object per provider, so the library validates what the adapter hands it."""

    def test_anthropic_requests_carry_the_settings_the_system_prompt_and_raw_fields(self):
        adapter = AnthropicBatchAdapter(model_id=CLAUDE, model_factory=lambda _id: _claude())

        (encoded,) = _real_encode(
            adapter,
            [{"prompt": "hi", "params": {"metadata": {"user_id": "u"}}}],
            system_prompt="be brief",
            request_params={"temperature": 0.2},
        )

        body = json.loads(encoded.body)
        assert encoded.custom_id == f"{KEY}-0"
        assert body["model"] == "claude-sonnet-4-5"
        assert body["max_tokens"] == 256
        assert body["temperature"] == 0.2
        assert body["metadata"] == {"user_id": "u"}
        assert body["messages"][0]["role"] == "user"
        assert "be brief" in json.dumps(body["system"])

    def test_anthropic_structured_output_sends_the_schema_natively(self):
        adapter = AnthropicBatchAdapter(model_id=CLAUDE, model_factory=lambda _id: _claude())

        (encoded,) = _real_encode(adapter, [{"prompt": "hi"}], spec=build_output_spec(Diagnosis))

        body = json.loads(encoded.body)
        assert body["output_config"]["format"]["type"] == "json_schema"
        assert body["output_config"]["format"]["schema"]["properties"].keys() == {"label", "score"}

    def test_a_model_without_native_json_schema_gets_a_forced_output_tool(self):
        adapter = AnthropicBatchAdapter(
            model_id=CLAUDE_OLD, model_factory=lambda _id: _claude("claude-3-5-haiku-latest")
        )

        (encoded,) = _real_encode(adapter, [{"prompt": "hi"}], spec=build_output_spec(Diagnosis))

        body = json.loads(encoded.body)
        assert [tool["name"] for tool in body["tools"]] == ["Diagnosis"]
        assert body["tool_choice"]["type"] == "any"

    def test_gemini_requests_carry_the_schema_and_the_sampling_settings(self):
        adapter = GoogleBatchAdapter(
            model_id=GEMINI, model_factory=lambda _id: _gemini(), output_spec=build_output_spec(Diagnosis)
        )

        (encoded,) = _real_encode(
            adapter,
            [{"prompt": "hi"}],
            spec=build_output_spec(Diagnosis),
            system_prompt="be brief",
            request_params={"temperature": 0.3},
        )

        body = json.loads(encoded.body)
        config = body["generationConfig"]
        assert config["responseMimeType"] == "application/json"
        assert config["responseJsonSchema"]["properties"].keys() == {"label", "score"}
        assert config["temperature"] == 0.3
        assert config["maxOutputTokens"] == 256
        assert "be brief" in encoded.body

    def test_a_setting_the_batch_api_cannot_honor_is_an_input_error_naming_the_request(self):
        adapter = AnthropicBatchAdapter(model_id=CLAUDE, model_factory=lambda _id: _claude())

        with pytest.raises(LLMBatchInputError, match="Request 1 cannot be sent as a batch"):
            _real_encode(adapter, [{"prompt": "ok"}, {"prompt": "x", "params": {"anthropic_speed": "fast"}}])

    def test_validate_requests_encodes_without_sending_anything(self):
        adapter = AnthropicBatchAdapter(model_id=CLAUDE, model_factory=lambda _id: _claude())

        with (
            mock.patch.object(
                AnthropicModel, "batch_encode", autospec=True, side_effect=AnthropicModel.batch_encode
            ) as encode,
            mock.patch.object(AnthropicModel, "batch_create", autospec=True) as create,
        ):
            adapter.validate_requests(
                [{"prompt": "hi"}],
                model="claude-sonnet-4-5",
                output_spec=build_output_spec(str),
                idempotency_key=KEY,
            )

        assert encode.call_count == 1
        create.assert_not_called()

    def test_a_custom_id_that_would_exceed_the_provider_limit_is_rejected_before_encoding(self):
        adapter = AnthropicBatchAdapter(model_id=CLAUDE, model_factory=lambda _id: _claude())

        with pytest.raises(LLMBatchLimitExceededError, match="custom_id"):
            adapter.validate_requests(
                [{"prompt": "hi"}],
                model="claude-sonnet-4-5",
                output_spec=build_output_spec(str),
                idempotency_key="k" * 70,
            )

    def test_submit_sends_every_encoded_request_in_one_batch(self):
        adapter = AnthropicBatchAdapter(model_id=CLAUDE, model_factory=lambda _id: _claude())
        created = Batch(
            id="msgbatch_1",
            model_name="claude-sonnet-4-5",
            provider_name="anthropic",
            group="g",
            status="in_progress",
            created_at=datetime(2026, 10, 1, tzinfo=UTC),
            request_count=2,
        )

        with mock.patch.object(AnthropicModel, "batch_create", autospec=True, return_value=created) as create:
            result = adapter.submit(
                [{"prompt": "a"}, {"prompt": "b"}],
                model="claude-sonnet-4-5",
                output_spec=build_output_spec(str),
                idempotency_key=KEY,
                input_fingerprint="fp",
            )

        assert [r.custom_id for r in create.call_args.args[1]] == [f"{KEY}-0", f"{KEY}-1"]
        assert result.batch_id == "msgbatch_1"

    def test_the_batch_size_limits_come_from_the_model(self):
        adapter = AnthropicBatchAdapter(model_id=CLAUDE, model_factory=lambda _id: _claude())
        model = _claude()

        with (
            mock.patch.object(type(model), "batch_max_requests", new=1),
            pytest.raises(LLMBatchLimitExceededError, match="at most 1 requests.*expand"),
        ):
            _real_encode(adapter, [{"prompt": "a"}, {"prompt": "b"}])

        with (
            mock.patch.object(type(model), "batch_max_bytes", new=10),
            pytest.raises(LLMBatchLimitExceededError, match="at most 10 bytes.*expand"),
        ):
            _real_encode(adapter, [{"prompt": "a"}])


class TestModelGroups:
    def test_claude_models_share_a_group_so_one_batch_can_mix_them(self):
        models = {CLAUDE: _claude(), "anthropic:claude-haiku-4-5": _claude("claude-haiku-4-5")}
        adapter = AnthropicBatchAdapter(model_id=CLAUDE, model_factory=models.__getitem__)

        encoded = _real_encode(
            adapter, [{"prompt": "a"}, {"prompt": "b", "model": "anthropic:claude-haiku-4-5"}]
        )

        assert {json.loads(item.body)["model"] for item in encoded} == {
            "claude-sonnet-4-5",
            "claude-haiku-4-5",
        }
        assert len({item.group for item in encoded}) == 1

    def test_gemini_models_do_not_share_a_group_so_a_mixed_batch_is_rejected_before_any_call(self):
        models = {GEMINI: _gemini(), "google:gemini-2.5-pro": _gemini("gemini-2.5-pro")}
        adapter = GoogleBatchAdapter(model_id=GEMINI, model_factory=models.__getitem__)

        with pytest.raises(LLMBatchModelMismatchError, match="Request 1 cannot share a batch with request 0"):
            _real_encode(adapter, [{"prompt": "a"}, {"prompt": "b", "model": "google:gemini-2.5-pro"}])

    def test_a_per_request_model_from_another_provider_is_rejected(self):
        adapter = AnthropicBatchAdapter(model_id=CLAUDE, model_factory=lambda _id: _claude())

        with pytest.raises(LLMBatchModelMismatchError, match="Request 0"):
            _real_encode(adapter, [{"prompt": "a", "model": "openai:gpt-5"}])


class TestAnthropicLegacyBatches:
    def test_a_v1_batch_id_is_turned_into_a_handle_the_provider_accepts(self):
        model = _claude()
        adapter = AnthropicBatchAdapter(model_id=CLAUDE, model_factory=lambda _id: model)

        batch = asyncio.run(adapter._unrecorded_batch(model, "msgbatch_old"))

        assert (batch.id, batch.status) == ("msgbatch_old", "in_progress")
        assert batch.group == _real_encode(adapter, [{"prompt": "x"}])[0].group
        assert TypeAdapter(Batch).validate_json(TypeAdapter(Batch).dump_json(batch)) == batch

    def test_a_v1_batch_is_polled_through_the_real_status_call(self):
        model = _claude()
        adapter = AnthropicBatchAdapter(model_id=CLAUDE, model_factory=lambda _id: model)
        ended = Batch(
            id="msgbatch_old",
            model_name="claude-sonnet-4-5",
            provider_name="anthropic",
            group=_real_encode(adapter, [{"prompt": "x"}])[0].group,
            status="ended",
            created_at=datetime(2026, 10, 1, 12, tzinfo=UTC),
            request_count=3,
        )

        with mock.patch.object(AnthropicModel, "batch_status", autospec=True, return_value=ended) as status:
            state = adapter.get_batch("msgbatch_old")

        assert state.status == "completed"
        assert status.call_args.args[1].id == "msgbatch_old"
        assert status.call_args.args[1].group == ended.group

    def test_a_v1_batch_is_cancelled_through_the_rebuilt_handle(self):
        model = _claude()
        adapter = AnthropicBatchAdapter(model_id=CLAUDE, model_factory=lambda _id: model)

        with mock.patch.object(AnthropicModel, "batch_cancel", autospec=True) as cancel:
            adapter.cancel_batch("msgbatch_old")

        assert cancel.call_args.args[1].id == "msgbatch_old"

    def test_a_connection_the_batch_api_cannot_use_fails_loudly(self):
        adapter = AnthropicBatchAdapter(model_id=CLAUDE, model_factory=lambda _id: _claude())

        with (
            mock.patch.object(
                AnthropicModel,
                "batch_encode",
                autospec=True,
                side_effect=UserError("requires static credentials"),
            ),
            pytest.raises(LLMBatchStateReadError, match="msgbatch_old.*static credentials"),
        ):
            adapter.get_batch("msgbatch_old")


def _ended(state: str | None, *, status: BatchStatus = "ended", count: int = 2) -> Batch:
    return Batch(
        id="batches/1",
        model_name="gemini-2.5-flash",
        provider_name="google-gla",
        group="{}",
        status=status,
        created_at=datetime(2026, 10, 1, tzinfo=UTC),
        request_count=count,
        provider_details={"state": state} if state else None,
    )


class _FailingResults(FunctionModel):
    """A model whose job ended without a results file, so ``batch_results`` raises as Gemini's does."""

    error: ModelHTTPError = BatchJobError(500, "gemini", {"error": {"message": "quota exhausted"}})

    async def batch_results(self, batch):
        raise self.error


class TestGeminiJobStates:
    @pytest.fixture
    def adapter(self):
        model = _FailingResults(lambda messages, info: ModelResponse(parts=[]))
        adapter = GoogleBatchAdapter(model_id=GEMINI, model_factory=lambda _id: model)
        return adapter, model

    @pytest.mark.parametrize(
        ("job_state", "status", "counts"),
        [
            ("JOB_STATE_SUCCEEDED", "completed", None),
            ("JOB_STATE_PARTIALLY_SUCCEEDED", "completed", None),
            ("JOB_STATE_CANCELLED", "cancelled", {"cancelled": 2}),
            ("JOB_STATE_EXPIRED", "expired", {"expired": 2}),
        ],
    )
    def test_an_ended_job_is_classified_by_its_own_state(self, adapter, job_state, status, counts):
        adapter, model = adapter

        state = asyncio.run(adapter._describe(model, _ended(job_state)))

        assert (state.status, state.counts) == (status, counts)

    def test_a_failed_job_reports_the_providers_reason(self, adapter):
        adapter, model = adapter

        state = asyncio.run(adapter._describe(model, _ended("JOB_STATE_FAILED")))

        assert state.status == "failed"
        assert state.counts is None
        assert "quota exhausted" in state.error_message

    @pytest.mark.parametrize("status", ["in_progress", "canceling"])
    def test_a_job_that_has_not_ended_is_in_progress(self, adapter, status):
        adapter, model = adapter

        state = asyncio.run(adapter._describe(model, _ended("JOB_STATE_RUNNING", status=status)))

        assert state.status == "in_progress"

    @pytest.mark.parametrize("job_state", ["JOB_STATE_CANCELLED", "JOB_STATE_EXPIRED"])
    def test_a_job_that_ended_before_running_anything_has_no_results_rather_than_an_error(
        self, adapter, job_state
    ):
        adapter, model = adapter

        assert asyncio.run(adapter._collect(model, _ended(job_state))) == []

    @pytest.mark.parametrize("job_state", ["JOB_STATE_CANCELLED", "JOB_STATE_EXPIRED", "JOB_STATE_FAILED"])
    def test_a_transient_api_error_collecting_an_unrun_job_propagates_instead_of_reading_as_no_results(
        self, adapter, job_state
    ):
        adapter, model = adapter
        model.error = ModelHTTPError(503, "gemini", {"error": {"message": "unavailable"}})

        with pytest.raises(ModelHTTPError, match="unavailable") as raised:
            asyncio.run(adapter._collect(model, _ended(job_state)))

        assert not isinstance(raised.value, LLMBatchJobError)

    def test_a_transient_api_error_describing_a_failed_job_is_not_reported_as_the_jobs_failure(self, adapter):
        adapter, model = adapter
        model.error = ModelHTTPError(503, "gemini", {"error": {"message": "unavailable"}})

        with pytest.raises(ModelHTTPError, match="unavailable"):
            asyncio.run(adapter._describe(model, _ended("JOB_STATE_FAILED")))

    def test_a_failed_job_without_results_is_a_job_error(self, adapter):
        adapter, model = adapter

        with pytest.raises(LLMBatchJobError, match="no results: quota exhausted"):
            asyncio.run(adapter._collect(model, _ended("JOB_STATE_FAILED")))

    @pytest.mark.parametrize("job_state", ["JOB_STATE_SUCCEEDED", "JOB_STATE_PARTIALLY_SUCCEEDED"])
    def test_an_api_error_collecting_a_finished_job_propagates_so_a_retry_collects_it(
        self, adapter, job_state
    ):
        adapter, model = adapter

        with pytest.raises(ModelHTTPError, match="quota exhausted"):
            asyncio.run(adapter._collect(model, _ended(job_state)))


class TestAdapterRegistration:
    def test_an_adapter_built_without_a_model_id_says_what_it_needs(self):
        adapter = AnthropicBatchAdapter(model_factory=lambda _id: _claude())

        with pytest.raises(LLMBatchInputError, match="needs the batch's model_id"):
            adapter._require_model_id()

    def test_an_adapter_built_without_a_connection_says_so(self):
        adapter = AnthropicBatchAdapter(model_id=CLAUDE)

        with pytest.raises(LLMBatchInputError, match="built without a connection"):
            asyncio.run(adapter._amodel(CLAUDE))

    def test_fallback_connections_are_ignored_with_a_warning(self):
        hook = mock.create_autospec(PydanticAIHook, instance=True)
        hook._get_fallback_conn_ids.return_value = ["backup"]
        conn = Connection(conn_id="primary", conn_type="pydanticai")
        with (
            mock.patch.object(PydanticAIHook, "get_hook", autospec=True, return_value=hook),
            mock.patch.object(backend_module, "log", spec=FilteringBoundLogger) as log,
        ):
            adapter = AnthropicBatchAdapter.from_connection(conn, model_id=CLAUDE)

        log.warning.assert_called_once()
        assert "fallback" in log.warning.call_args.args[0]
        assert adapter._hook is hook

    def test_the_hook_builds_the_model_the_adapter_polls(self):
        hook = mock.create_autospec(PydanticAIHook, instance=True)
        hook.aget_primary_model.return_value = _claude()
        adapter = AnthropicBatchAdapter(hook=hook, model_id=CLAUDE)

        asyncio.run(adapter._amodel("anthropic:claude-haiku-4-5"))

        hook.aget_primary_model.assert_awaited_once_with("anthropic:claude-haiku-4-5")


class TestOpenAIBatches:
    """OpenAI batches run on the Responses model, so the requests are Responses API bodies."""

    def test_requests_are_responses_bodies_with_the_settings_the_system_prompt_and_raw_fields(self):
        adapter = OpenAIBatchAdapter(model_id=GPT_4, model_factory=lambda _id: _gpt("gpt-4.1-mini"))

        (encoded,) = _real_encode(
            adapter,
            [{"prompt": "hi", "params": {"user": "u-1"}}],
            system_prompt="be brief",
            request_params={"temperature": 0.2},
        )

        body = json.loads(encoded.body)
        assert encoded.custom_id == f"{KEY}-0"
        assert body["model"] == "gpt-4.1-mini"
        assert body["max_output_tokens"] == 256
        assert body["temperature"] == 0.2
        assert body["user"] == "u-1"
        assert "be brief" in json.dumps(body["input"])

    def test_a_provider_prefixed_setting_reaches_the_request_in_its_typed_form(self):
        adapter = OpenAIBatchAdapter(model_id=GPT, model_factory=lambda _id: _gpt())

        (encoded,) = _real_encode(
            adapter, [{"prompt": "hi"}], request_params={"openai_reasoning_effort": "low"}
        )

        assert json.loads(encoded.body)["reasoning"]["effort"] == "low"

    def test_structured_output_sends_the_schema_natively(self):
        adapter = OpenAIBatchAdapter(model_id=GPT, model_factory=lambda _id: _gpt())

        (encoded,) = _real_encode(adapter, [{"prompt": "hi"}], spec=build_output_spec(Diagnosis))

        fmt = json.loads(encoded.body)["text"]["format"]
        assert fmt["type"] == "json_schema"
        assert fmt["schema"]["properties"].keys() == {"label", "score"}

    @pytest.mark.parametrize("key", ["seed", "top_k"])
    def test_a_setting_the_responses_api_would_silently_drop_is_rejected(self, key):
        adapter = OpenAIBatchAdapter(model_id=GPT)

        with pytest.raises(LLMBatchInputError, match=f"'{key}' is not supported by OpenAIResponsesModel"):
            adapter._build_settings(_gpt(), {key: 3}, max_tokens=8)

    def test_raw_fields_the_batch_sets_itself_cannot_replace_the_prompt_or_the_model(self):
        adapter = OpenAIBatchAdapter(model_id=GPT, model_factory=lambda _id: _gpt())

        (encoded,) = _real_encode(
            adapter,
            [{"prompt": "hi"}],
            request_params={"model": "evil", "input": "x", "extra_body": {"model": "evil", "user": "u"}},
        )

        body = json.loads(encoded.body)
        assert body["model"] == "gpt-5-mini"
        assert body["input"] != "x"
        assert body["user"] == "u"

    def test_models_do_not_share_a_group_so_a_mixed_batch_is_rejected_before_any_call(self):
        models = {GPT: _gpt(), "openai:gpt-4.1-mini": _gpt("gpt-4.1-mini")}
        adapter = OpenAIBatchAdapter(model_id=GPT, model_factory=models.__getitem__)

        with pytest.raises(LLMBatchModelMismatchError, match="Request 1 cannot share a batch with request 0"):
            _real_encode(adapter, [{"prompt": "a"}, {"prompt": "b", "model": "openai:gpt-4.1-mini"}])

    def test_a_per_request_model_from_another_provider_is_rejected(self):
        adapter = OpenAIBatchAdapter(model_id=GPT, model_factory=lambda _id: _gpt())

        with pytest.raises(LLMBatchModelMismatchError, match="Request 0"):
            _real_encode(adapter, [{"prompt": "a", "model": CLAUDE}])

    @pytest.mark.parametrize("prefix", ["openai", "openai-chat"])
    def test_the_model_is_always_built_as_a_responses_model(self, prefix):
        hook = mock.create_autospec(PydanticAIHook, instance=True)
        hook.aget_primary_model.return_value = _gpt()
        adapter = OpenAIBatchAdapter(hook=hook, model_id=f"{prefix}:gpt-5-mini")

        with structlog.testing.capture_logs() as logs:
            asyncio.run(adapter._amodel(f"{prefix}:gpt-5-mini"))

        hook.aget_primary_model.assert_awaited_once_with("openai:gpt-5-mini")
        rewritten = [entry for entry in logs if "openai-chat" in entry["event"]]
        assert [entry["log_level"] for entry in rewritten] == (["info"] if prefix == "openai-chat" else [])

    def test_a_per_request_openai_chat_model_is_accepted_and_built_as_a_responses_model(self):
        hook = mock.create_autospec(PydanticAIHook, instance=True)
        hook.aget_primary_model.side_effect = lambda model_id: _gpt(model_id.partition(":")[2])
        adapter = OpenAIBatchAdapter(hook=hook, model_id=GPT)

        encoded = _real_encode(adapter, [{"prompt": "a", "model": "openai-chat:gpt-5-mini"}])

        assert json.loads(encoded[0].body)["model"] == "gpt-5-mini"
        hook.aget_primary_model.assert_awaited_once_with("openai:gpt-5-mini")

    def test_the_lifecycle_runs_on_an_in_memory_model(self):
        recorder = _Recorder()
        adapter = _adapter(recorder.model, cls=OpenAIBatchAdapter, model_id=GPT)

        submitted = _submit(adapter, [{"prompt": "a"}, {"prompt": "b"}])
        later = _adapter(recorder.model, cls=OpenAIBatchAdapter, model_id=GPT, handle=submitted.handle)
        rows = sorted(later.iter_results(submitted.batch_id), key=lambda item: item.index)

        assert later.get_batch(submitted.batch_id).status == "completed"
        assert [later.extract_output(row, build_output_spec(str)).text for row in rows] == [
            "echo:a",
            "echo:b",
        ]

    def test_a_batch_an_earlier_version_submitted_to_chat_completions_is_not_re_attached(self):
        adapter = OpenAIBatchAdapter(model_id=GPT, model_factory=lambda _id: _gpt())

        with pytest.raises(LLMBatchStateReadError, match="batch_old.*Chat Completions.*cancel it at OpenAI"):
            adapter.get_batch("batch_old")

    def test_there_is_no_batch_to_find_after_a_crash_between_submit_and_record(self):
        adapter = OpenAIBatchAdapter(model_id=GPT)

        assert adapter.find_orphaned_batch(KEY, "fp", "2026-10-01T00:00:00+00:00") is None


def _openai_batch(status: str, *, errors: list | None = None, state: BatchStatus = "ended") -> Batch:
    details: dict = {"status": status, "input_file_id": "file-in"}
    if errors is not None:
        details["errors"] = errors
    return Batch(
        id="batch_1",
        model_name="gpt-5-mini",
        provider_name="openai",
        group="{}",
        status=state,
        created_at=datetime(2026, 10, 1, tzinfo=UTC),
        request_count=3,
        provider_details=details,
    )


class TestOpenAISubmit:
    def test_the_uploaded_input_file_is_recorded_under_the_id_pydantic_ai_returns(self):
        adapter = OpenAIBatchAdapter(model_id=GPT, model_factory=lambda _id: _gpt())
        created = _openai_batch("validating", state="in_progress")

        with mock.patch.object(OpenAIResponsesModel, "batch_create", autospec=True, return_value=created):
            result = adapter.submit(
                [{"prompt": "a"}],
                model="gpt-5-mini",
                output_spec=build_output_spec(str),
                idempotency_key=KEY,
                input_fingerprint="fp",
            )

        assert result.provider_input_ref == "file-in"


class TestOpenAIExistingBatches:
    @pytest.fixture
    def gpt5_batch(self):
        """An OpenAI batch submitted on ``gpt-5``, polled by a task whose own model is ``gpt-5-mini``."""
        handle = _submitted_on(OpenAIBatchAdapter, _gpt("gpt-5"), model_name="gpt-5", provider_name="openai")
        return OpenAIBatchAdapter(
            model_id=GPT,
            batch_handle=handle,
            model_factory=lambda model_id: _gpt(model_id.partition(":")[2]),
        )

    @pytest.fixture
    def job(self):
        created_at = int(datetime(2026, 10, 1, tzinfo=UTC).timestamp())
        with mock.patch.object(
            OpenAIAsyncBatches,
            "retrieve",
            autospec=True,
            return_value=OpenAIBatch(
                id="batches/1",
                completion_window="24h",
                created_at=created_at,
                endpoint="/v1/responses",
                input_file_id="file-in",
                object="batch",
                status="in_progress",
            ),
        ) as retrieve:
            yield retrieve

    def test_an_openai_batch_submitted_on_another_model_is_polled(self, gpt5_batch, job):
        assert gpt5_batch.get_batch("batches/1").status == "in_progress"

    def test_an_openai_batch_submitted_on_another_model_is_cancelled(self, gpt5_batch, job):
        with mock.patch.object(
            OpenAIAsyncBatches, "cancel", autospec=True, return_value=job.return_value
        ) as cancel:
            gpt5_batch.cancel_batch("batches/1")

        assert cancel.call_args.args[1] == "batches/1"

    def test_the_chat_completions_prefix_of_the_task_still_builds_the_responses_model(self):
        recorder = _Recorder()
        submitted = _submit(_adapter(recorder.model), [{"prompt": "a"}])
        batch = TypeAdapter(Batch).validate_json(submitted.handle)
        batch.model_name = "gpt-5"
        asked: list[str] = []

        def factory(model_id):
            asked.append(model_id)
            return recorder.model

        adapter = OpenAIBatchAdapter(
            model_id="openai-chat:gpt-5-mini",
            batch_handle=TypeAdapter(Batch).dump_json(batch).decode(),
            model_factory=factory,
        )

        adapter.get_batch(submitted.batch_id)

        assert asked == ["openai:gpt-5"]


class TestOpenAIJobStates:
    @pytest.fixture
    def adapter(self):
        model = FunctionModel(lambda messages, info: ModelResponse(parts=[]))
        return OpenAIBatchAdapter(model_id=GPT, model_factory=lambda _id: model), model

    @pytest.mark.parametrize(
        ("job_status", "status"),
        [("completed", "completed"), ("expired", "expired"), ("cancelled", "cancelled")],
    )
    def test_an_ended_job_is_classified_by_its_own_status(self, adapter, job_status, status):
        adapter, model = adapter

        state = asyncio.run(adapter._describe(model, _openai_batch(job_status)))

        assert (state.status, state.counts, state.error_message) == (status, None, None)

    def test_a_job_rejected_before_running_is_failed_with_the_providers_reasons(self, adapter):
        adapter, model = adapter
        errors = [
            {"code": "invalid_json_line", "line": 2, "message": "bad line"},
            {"code": "token_limit", "message": "too many tokens"},
        ]

        state = asyncio.run(adapter._describe(model, _openai_batch("failed", errors=errors)))

        assert state.status == "failed"
        assert state.counts is None
        assert "line 2: bad line" in state.error_message
        assert "too many tokens" in state.error_message

    @pytest.mark.parametrize("status", ["in_progress", "canceling"])
    def test_a_job_that_has_not_ended_is_in_progress(self, adapter, status):
        adapter, model = adapter

        state = asyncio.run(adapter._describe(model, _openai_batch("in_progress", state=status)))

        assert state.status == "in_progress"
