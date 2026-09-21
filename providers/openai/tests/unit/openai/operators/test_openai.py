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

from datetime import datetime
from decimal import Decimal
from fractions import Fraction
from unittest import mock
from unittest.mock import Mock

import jinja2
import pytest
from openai.types.batch import Batch
from openai.types.responses import Response, ResponseUsage
from openai.types.responses.response import IncompleteDetails
from openai.types.responses.response_usage import InputTokensDetails, OutputTokensDetails

from airflow.providers.common.compat.sdk import DAG, BaseOperator, Context, TaskDeferred, XComArg
from airflow.providers.openai.exceptions import OpenAIBatchJobException, OpenAITriggerEventError
from airflow.providers.openai.hooks.openai import OpenAIHook
from airflow.providers.openai.operators.openai import (
    OpenAIEmbeddingOperator,
    OpenAIResponseOperator,
    OpenAITriggerBatchOperator,
)
from airflow.providers.openai.triggers.openai import OpenAIBatchTrigger

openai = pytest.importorskip("openai")
TASK_ID = "TaskId"
CONN_ID = "test_conn_id"
BATCH_ID = "batch_id"
FILE_ID = "file_id"
BATCH_ENDPOINT = "/v1/chat/completions"


@pytest.fixture
def mock_batch():
    return Batch(
        id=BATCH_ID,
        object="batch",
        completion_window="24h",
        created_at=1699061776,
        endpoint=BATCH_ENDPOINT,
        input_file_id=FILE_ID,
        status="in_progress",
    )


def test_execute_with_input_text():
    operator = OpenAIEmbeddingOperator(
        task_id=TASK_ID, conn_id=CONN_ID, model="test_model", input_text="Test input text"
    )
    mock_hook_instance = Mock(spec=OpenAIHook)
    mock_hook_instance.create_embeddings.return_value = [1.0, 2.0, 3.0]
    operator.hook = mock_hook_instance

    context = Context()
    embeddings = operator.execute(context)

    assert embeddings == [1.0, 2.0, 3.0]


@pytest.mark.parametrize("invalid_input", ["", None, 123])
def test_execute_with_invalid_input(invalid_input):
    operator = OpenAIEmbeddingOperator(
        task_id=TASK_ID, conn_id=CONN_ID, model="test_model", input_text=invalid_input
    )
    context = Context()
    with pytest.raises(
        ValueError,
        match="The 'input_text' must be a non-empty string, list of strings, list of integers, or list of lists of integers.",
    ):
        operator.execute(context)


def _build_execute_context(try_number: int = 1) -> Context:
    # OpenAIResponseOperator.execute pushes to XCom through context["ti"], so a test that lets
    # execute run to completion has to put a task instance in the context.
    context = Context()
    context["ti"] = Mock()
    context["ti"].try_number = try_number
    return context


@pytest.mark.parametrize(
    ("do_xcom_push", "expected_push_count"),
    [
        pytest.param(True, 2, id="enabled"),
        pytest.param(False, 0, id="disabled"),
    ],
)
def test_openai_response_operator_execute_xcom_push(do_xcom_push, expected_push_count):
    operator = OpenAIResponseOperator(
        task_id=TASK_ID,
        conn_id=CONN_ID,
        input_text="Write a haiku.",
        model="test_model",
        do_xcom_push=do_xcom_push,
        response_kwargs={"instructions": "Be concise.", "previous_response_id": "resp_prev"},
    )
    mock_hook_instance = Mock(spec=OpenAIHook)
    usage = ResponseUsage(
        input_tokens=5,
        input_tokens_details=InputTokensDetails(cached_tokens=1, cache_write_tokens=0),
        output_tokens=7,
        output_tokens_details=OutputTokensDetails(reasoning_tokens=2),
        total_tokens=12,
    )
    mock_hook_instance.create_response.return_value = _build_completed_response(usage=usage)
    operator.hook = mock_hook_instance

    context = _build_execute_context()
    result = operator.execute(context)

    # Backward compat: the return value is still the aggregated output text, unchanged
    # by the new XCom pushes below.
    assert result == "haiku text"
    mock_hook_instance.create_response.assert_called_once_with(
        input="Write a haiku.",
        model="test_model",
        instructions="Be concise.",
        previous_response_id="resp_prev",
    )
    # Pins the exact number of pushes so a stray extra key regresses this test instead of
    # slipping through assert_any_call, which only checks presence, not exhaustiveness.
    assert context["ti"].xcom_push.call_count == expected_push_count
    if do_xcom_push:
        context["ti"].xcom_push.assert_any_call(key="response_id", value="resp_123")
        context["ti"].xcom_push.assert_any_call(
            key="usage",
            value={
                "input_tokens": 5,
                "input_tokens_details": {"cache_write_tokens": 0, "cached_tokens": 1},
                "output_tokens": 7,
                "output_tokens_details": {"reasoning_tokens": 2},
                "total_tokens": 12,
                "try_number": 1,
            },
        )


def test_openai_response_operator_execute_records_try_number():
    # Uses a non-default try_number so this test only passes if the "try_number" value in the
    # pushed usage dict is actually read from context["ti"].try_number at execute time, not a
    # value that happens to coincide with _build_execute_context()'s default of 1.
    operator = OpenAIResponseOperator(
        task_id=TASK_ID,
        conn_id=CONN_ID,
        input_text="Write a haiku.",
        model="test_model",
    )
    mock_hook_instance = Mock(spec=OpenAIHook)
    usage = ResponseUsage(
        input_tokens=5,
        input_tokens_details=InputTokensDetails(cached_tokens=1, cache_write_tokens=0),
        output_tokens=7,
        output_tokens_details=OutputTokensDetails(reasoning_tokens=2),
        total_tokens=12,
    )
    mock_response = Mock(
        spec=Response, output_text="haiku text", id="resp_123", status="completed", usage=usage
    )
    mock_hook_instance.create_response.return_value = mock_response
    operator.hook = mock_hook_instance

    context = _build_execute_context(try_number=3)
    operator.execute(context)

    context["ti"].xcom_push.assert_any_call(
        key="usage",
        value={
            "input_tokens": 5,
            "input_tokens_details": {"cache_write_tokens": 0, "cached_tokens": 1},
            "output_tokens": 7,
            "output_tokens_details": {"reasoning_tokens": 2},
            "total_tokens": 12,
            "try_number": 3,
        },
    )


def test_openai_response_operator_execute_without_usage():
    operator = OpenAIResponseOperator(
        task_id=TASK_ID, conn_id=CONN_ID, input_text="Write a haiku.", model="test_model"
    )
    mock_hook_instance = Mock(spec=OpenAIHook)
    mock_response = Mock(
        spec=Response, output_text="haiku text", id="resp_123", status="completed", usage=None
    )
    mock_hook_instance.create_response.return_value = mock_response
    operator.hook = mock_hook_instance

    context = _build_execute_context()
    result = operator.execute(context)

    assert result == "haiku text"
    context["ti"].xcom_push.assert_any_call(key="usage", value=None)


@pytest.mark.parametrize(
    ("response_kwargs", "extra_params", "expected_response_kwargs"),
    [
        pytest.param(
            {"previous_response_id": "{{ params.previous_response_id }}"},
            {"previous_response_id": "resp_prev_123"},
            {"previous_response_id": "resp_prev_123"},
            id="flat-string",
        ),
        pytest.param(
            {"instructions": "{% raw %}{{ not_a_variable }}{% endraw %}"},
            {},
            {"instructions": "{{ not_a_variable }}"},
            id="raw-escape",
        ),
        pytest.param(
            {
                "tools": [{"type": "function", "parameters": {"k": "{{ params.input_text }}"}}],
                "max_retries": 3,
            },
            {},
            {
                "tools": [{"type": "function", "parameters": {"k": "Write a haiku."}}],
                "max_retries": 3,
            },
            id="nested-tool-schema",
        ),
    ],
)
def test_openai_response_operator_templates_input_text_and_response_kwargs(
    response_kwargs, extra_params, expected_response_kwargs
):
    with DAG(dag_id="test_openai_response_template_fields", schedule=None, start_date=datetime(2021, 1, 1)):
        operator = OpenAIResponseOperator(
            task_id=TASK_ID,
            conn_id=CONN_ID,
            input_text="{{ params.input_text }}",
            response_kwargs=response_kwargs,
        )

    operator.render_template_fields({"params": {"input_text": "Write a haiku.", **extra_params}})

    assert operator.input_text == "Write a haiku."
    assert operator.response_kwargs == expected_response_kwargs
    # The nested tool-schema case must not stringify the non-template int leaf.
    if "max_retries" in expected_response_kwargs:
        assert isinstance(operator.response_kwargs["max_retries"], int)


def _build_completed_response(**overrides):
    # execute() reads response.usage; Mock(spec=Response) does not synthesise pydantic
    # fields, so it has to be set explicitly even when a test does not care about usage.
    defaults = {"output_text": "haiku text", "id": "resp_123", "status": "completed", "usage": None}
    return Mock(spec=Response, **{**defaults, **overrides})


def test_openai_response_operator_resolves_xcom_arg_nested_in_response_kwargs():
    with DAG("test_dag", schedule=None) as dag:
        upstream = BaseOperator(task_id="upstream")

    operator = OpenAIResponseOperator(
        task_id=TASK_ID,
        conn_id=CONN_ID,
        input_text="Write a haiku.",
        response_kwargs={"previous_response_id": XComArg(upstream, key="response_id")},
        dag=dag,
    )

    # Construction must not fail or eagerly resolve the XComArg.
    assert isinstance(operator.response_kwargs["previous_response_id"], XComArg)

    mock_ti = Mock()
    mock_ti.xcom_pull.return_value = "resp_123"
    operator.render_template_fields(Context(ti=mock_ti, expanded_ti_count=None))

    assert operator.response_kwargs["previous_response_id"] == "resp_123"

    mock_hook_instance = Mock(spec=OpenAIHook)
    mock_hook_instance.create_response.return_value = _build_completed_response()
    operator.hook = mock_hook_instance

    operator.execute(_build_execute_context())

    call_kwargs = mock_hook_instance.create_response.call_args.kwargs
    assert call_kwargs["previous_response_id"] == "resp_123"


class TestOpenAIResponseOperatorTokenCeilings:
    @pytest.mark.parametrize(
        ("kwargs", "expected_extra"),
        [
            pytest.param({"max_output_tokens": 100}, {"max_output_tokens": 100}, id="max_output_tokens-int"),
            pytest.param({"max_tool_calls": 5}, {"max_tool_calls": 5}, id="max_tool_calls-int"),
            pytest.param(
                {"max_output_tokens": "100"}, {"max_output_tokens": 100}, id="max_output_tokens-numeric-str"
            ),
            pytest.param({"max_tool_calls": "5"}, {"max_tool_calls": 5}, id="max_tool_calls-numeric-str"),
            pytest.param(
                {"max_output_tokens": 100, "max_tool_calls": 5},
                {"max_output_tokens": 100, "max_tool_calls": 5},
                id="both",
            ),
        ],
    )
    def test_valid_ceiling_forwarded_as_int(self, kwargs, expected_extra):
        operator = OpenAIResponseOperator(
            task_id=TASK_ID, conn_id=CONN_ID, input_text="Write a haiku.", **kwargs
        )
        mock_hook_instance = Mock(spec=OpenAIHook)
        mock_hook_instance.create_response.return_value = _build_completed_response()
        operator.hook = mock_hook_instance

        operator.execute(_build_execute_context())

        mock_hook_instance.create_response.assert_called_once_with(
            input="Write a haiku.", model="gpt-4o-mini", **expected_extra
        )

    @pytest.mark.parametrize(
        "invalid_value",
        [
            pytest.param("not-a-number", id="non-integer-string"),
            pytest.param("-5", id="negative-string"),
            pytest.param("None", id="literal-none-string"),
            pytest.param(Decimal("10.5"), id="decimal"),
            pytest.param(Fraction(21, 2), id="fraction"),
        ],
    )
    @pytest.mark.parametrize("param_name", ["max_output_tokens", "max_tool_calls"])
    def test_invalid_ceiling_raises_before_request(self, param_name, invalid_value):
        operator = OpenAIResponseOperator(
            task_id=TASK_ID, conn_id=CONN_ID, input_text="Write a haiku.", **{param_name: invalid_value}
        )
        mock_hook_instance = Mock(spec=OpenAIHook)
        operator.hook = mock_hook_instance

        with pytest.raises(ValueError, match=param_name):
            operator.execute(_build_execute_context())

        mock_hook_instance.create_response.assert_not_called()

    @pytest.mark.parametrize(
        "invalid_value",
        [
            pytest.param(0, id="zero"),
            pytest.param(-1, id="negative"),
            pytest.param(10.5, id="float"),
            pytest.param(True, id="bool-true"),
            pytest.param(False, id="bool-false"),
        ],
    )
    @pytest.mark.parametrize("param_name", ["max_output_tokens", "max_tool_calls"])
    def test_non_string_invalid_ceiling_raises_at_construction(self, param_name, invalid_value):
        with pytest.raises(ValueError, match=param_name):
            OpenAIResponseOperator(
                task_id=TASK_ID,
                conn_id=CONN_ID,
                input_text="Write a haiku.",
                **{param_name: invalid_value},
            )

    @pytest.mark.parametrize(
        "operator_value",
        [
            pytest.param(100, id="int"),
            pytest.param("", id="blank"),
        ],
    )
    @pytest.mark.parametrize("param_name", ["max_output_tokens", "max_tool_calls"])
    def test_ceiling_conflicting_with_response_kwargs_raises(self, param_name, operator_value):
        # A blank operator_value must still conflict with response_kwargs; that's checked at
        # construction time, before rendering. pytest.raises() itself fails with "DID NOT RAISE"
        # if construction succeeded, so there's no operator instance afterwards to assert
        # anything further against.
        with pytest.raises(ValueError, match=param_name):
            OpenAIResponseOperator(
                task_id=TASK_ID,
                conn_id=CONN_ID,
                input_text="Write a haiku.",
                response_kwargs={param_name: 50},
                **{param_name: operator_value},
            )

    def test_conflict_error_precedes_literal_type_error(self):
        # 0 is both invalid on its own (not positive) and conflicting with response_kwargs; the
        # conflict message must win, since fixing the duplicate is the actionable first step.
        with pytest.raises(ValueError, match="was set both as an operator argument"):
            OpenAIResponseOperator(
                task_id=TASK_ID,
                conn_id=CONN_ID,
                input_text="Write a haiku.",
                response_kwargs={"max_output_tokens": 50},
                max_output_tokens=0,
            )

    def test_xcom_arg_ceiling_does_not_fail_on_construction(self):
        with DAG("test_dag", schedule=None) as dag:
            upstream = BaseOperator(task_id="upstream")

        operator = OpenAIResponseOperator(
            task_id=TASK_ID,
            conn_id=CONN_ID,
            input_text="Write a haiku.",
            max_output_tokens=upstream.output,
            dag=dag,
        )

        assert isinstance(operator.max_output_tokens, XComArg)

    @pytest.mark.parametrize(
        "blank_value", [pytest.param("", id="empty"), pytest.param("   ", id="whitespace")]
    )
    @pytest.mark.parametrize("param_name", ["max_output_tokens", "max_tool_calls"])
    def test_blank_ceiling_is_treated_as_unset(self, param_name, blank_value):
        operator = OpenAIResponseOperator(
            task_id=TASK_ID, conn_id=CONN_ID, input_text="Write a haiku.", **{param_name: blank_value}
        )
        mock_hook_instance = Mock(spec=OpenAIHook)
        mock_hook_instance.create_response.return_value = _build_completed_response()
        operator.hook = mock_hook_instance

        operator.execute(_build_execute_context())

        call_kwargs = mock_hook_instance.create_response.call_args.kwargs
        assert param_name not in call_kwargs

    def test_max_output_tokens_and_max_tool_calls_are_templated(self):
        operator = OpenAIResponseOperator(
            task_id=TASK_ID,
            conn_id=CONN_ID,
            input_text="Write a haiku.",
            max_output_tokens="{{ params.tokens }}",
            max_tool_calls="{{ params.calls }}",
        )

        operator.render_template_fields(Context(params={"tokens": 100, "calls": 5}))

        assert operator.max_output_tokens == "100"
        assert operator.max_tool_calls == "5"
        assert "max_output_tokens" in operator.template_fields
        assert "max_tool_calls" in operator.template_fields

        # The rendered strings must still make it to the SDK as real ints, not left as strings.
        mock_hook_instance = Mock(spec=OpenAIHook)
        mock_hook_instance.create_response.return_value = _build_completed_response()
        operator.hook = mock_hook_instance

        operator.execute(_build_execute_context())

        call_kwargs = mock_hook_instance.create_response.call_args.kwargs
        for key, expected in (
            ("max_output_tokens", 100),
            ("max_tool_calls", 5),
        ):
            assert call_kwargs[key] == expected
            assert isinstance(call_kwargs[key], int)
            assert not isinstance(call_kwargs[key], bool)

    @pytest.mark.parametrize("param_name", ["max_output_tokens", "max_tool_calls"])
    def test_supplied_xcom_arg_ceiling_resolving_to_none_raises(self, param_name):
        # An XComArg bound at construction time (e.g. upstream.output) is not "unset" -- if
        # rendering it later resolves to None (no XCom was ever pushed), that must raise instead
        # of silently disabling the ceiling.
        with DAG("test_dag", schedule=None) as dag:
            upstream = BaseOperator(task_id="upstream")

        operator = OpenAIResponseOperator(
            task_id=TASK_ID,
            conn_id=CONN_ID,
            input_text="Write a haiku.",
            dag=dag,
            **{param_name: upstream.output},
        )

        mock_ti = Mock()
        mock_ti.xcom_pull.return_value = None
        # Airflow 2's XComArg.resolve() reads context["expanded_ti_count"] unconditionally, so the
        # key has to be present for this to render under the compatibility test suite.
        operator.render_template_fields(Context(ti=mock_ti, expanded_ti_count=None))

        mock_hook_instance = Mock(spec=OpenAIHook)
        operator.hook = mock_hook_instance

        with pytest.raises(ValueError, match=param_name):
            operator.execute(_build_execute_context())

        mock_hook_instance.create_response.assert_not_called()

    @pytest.mark.parametrize("param_name", ["max_output_tokens", "max_tool_calls"])
    def test_native_rendered_none_ceiling_raises(self, param_name):
        # render_template_as_native_obj=True can render a Jinja template straight to a real
        # None -- that is also "supplied but resolved to None", not "unset".
        param_key = "tokens" if param_name == "max_output_tokens" else "calls"
        with DAG("test_dag", schedule=None, render_template_as_native_obj=True):
            operator = OpenAIResponseOperator(
                task_id=TASK_ID,
                conn_id=CONN_ID,
                input_text="Write a haiku.",
                **{param_name: f"{{{{ params.{param_key} }}}}"},
            )

        operator.render_template_fields(Context(params={param_key: None}))

        assert getattr(operator, param_name) is None

        mock_hook_instance = Mock(spec=OpenAIHook)
        operator.hook = mock_hook_instance

        with pytest.raises(ValueError, match=param_name):
            operator.execute(_build_execute_context())

        mock_hook_instance.create_response.assert_not_called()

    def test_native_response_kwargs_valid_ceiling_forwarded_as_int(self):
        operator = OpenAIResponseOperator(
            task_id=TASK_ID,
            conn_id=CONN_ID,
            input_text="Write a haiku.",
            response_kwargs={"max_output_tokens": "500"},
        )
        operator.render_template_fields(Context(params={}))

        mock_hook_instance = Mock(spec=OpenAIHook)
        mock_hook_instance.create_response.return_value = _build_completed_response()
        operator.hook = mock_hook_instance

        operator.execute(_build_execute_context())

        call_kwargs = mock_hook_instance.create_response.call_args.kwargs
        assert call_kwargs["max_output_tokens"] == 500
        assert isinstance(call_kwargs["max_output_tokens"], int)

    @pytest.mark.parametrize(
        "invalid_value",
        [
            pytest.param(0, id="zero"),
            pytest.param(-1, id="negative"),
            pytest.param(10.5, id="float"),
            pytest.param(True, id="bool-true"),
            pytest.param(False, id="bool-false"),
        ],
    )
    @pytest.mark.parametrize("param_name", ["max_output_tokens", "max_tool_calls"])
    def test_native_response_kwargs_invalid_literal_ceiling_raises_at_construction(
        self, param_name, invalid_value
    ):
        with pytest.raises(ValueError, match=param_name):
            OpenAIResponseOperator(
                task_id=TASK_ID,
                conn_id=CONN_ID,
                input_text="Write a haiku.",
                response_kwargs={param_name: invalid_value},
            )

    @pytest.mark.parametrize(
        "blank_value", [pytest.param("", id="empty"), pytest.param("   ", id="whitespace")]
    )
    @pytest.mark.parametrize("param_name", ["max_output_tokens", "max_tool_calls"])
    def test_native_response_kwargs_blank_ceiling_is_popped(self, param_name, blank_value):
        operator = OpenAIResponseOperator(
            task_id=TASK_ID,
            conn_id=CONN_ID,
            input_text="Write a haiku.",
            response_kwargs={param_name: blank_value},
        )
        mock_hook_instance = Mock(spec=OpenAIHook)
        mock_hook_instance.create_response.return_value = _build_completed_response()
        operator.hook = mock_hook_instance

        operator.execute(_build_execute_context())

        call_kwargs = mock_hook_instance.create_response.call_args.kwargs
        assert param_name not in call_kwargs

    @pytest.mark.parametrize("param_name", ["max_output_tokens", "max_tool_calls"])
    def test_native_response_kwargs_none_value_raises(self, param_name):
        # Unlike the operator-argument path (a literal None argument means "not supplied" --
        # filtered out of _supplied_ceilings by the "is not None" check in __init__), a None
        # value that is a *present key* in response_kwargs must still raise: the dict key's
        # existence, not the value, is what "supplied" means for the native path.
        operator = OpenAIResponseOperator(
            task_id=TASK_ID,
            conn_id=CONN_ID,
            input_text="Write a haiku.",
            response_kwargs={param_name: None},
        )
        mock_hook_instance = Mock(spec=OpenAIHook)
        operator.hook = mock_hook_instance

        with pytest.raises(ValueError, match=param_name):
            operator.execute(_build_execute_context())

        mock_hook_instance.create_response.assert_not_called()

    def test_or_fallback_idiom_raises_under_strict_undefined_dag_binding(self):
        with DAG("test_dag", schedule=None) as dag:
            operator = OpenAIResponseOperator(
                task_id=TASK_ID,
                conn_id=CONN_ID,
                input_text="Write a haiku.",
                max_output_tokens="{{ params.tokens or '' }}",
                dag=dag,
            )

        with pytest.raises(jinja2.UndefinedError):
            operator.render_template_fields(Context(params={}))

    def test_default_filter_idiom_renders_blank_under_strict_undefined_dag_binding(self):
        with DAG("test_dag", schedule=None) as dag:
            operator = OpenAIResponseOperator(
                task_id=TASK_ID,
                conn_id=CONN_ID,
                input_text="Write a haiku.",
                max_output_tokens="{{ params.tokens | default('', true) }}",
                dag=dag,
            )

        assert operator.get_template_env().undefined is jinja2.StrictUndefined

        operator.render_template_fields(Context(params={}))
        assert operator.max_output_tokens == ""

        mock_hook_instance = Mock(spec=OpenAIHook)
        mock_hook_instance.create_response.return_value = _build_completed_response()
        operator.hook = mock_hook_instance

        operator.execute(_build_execute_context())

        call_kwargs = mock_hook_instance.create_response.call_args.kwargs
        assert "max_output_tokens" not in call_kwargs

    @pytest.mark.parametrize(
        ("reason", "output_text", "expected_fragment"),
        [
            pytest.param(
                "max_output_tokens",
                "Truncated hai",
                "the returned output text is truncated, not empty.",
                id="max_output_tokens-nonempty-output",
            ),
            pytest.param(
                "content_filter",
                "",
                "the returned output text may be empty.",
                id="content_filter-empty-output",
            ),
            pytest.param(
                # A reasoning model can spend the entire max_output_tokens ceiling on reasoning
                # tokens and produce no visible output text -- the wording must be decided by
                # output_text, not by reason, even when reason is "max_output_tokens".
                "max_output_tokens",
                "",
                "the returned output text may be empty.",
                id="max_output_tokens-empty-output",
            ),
        ],
    )
    def test_incomplete_details_reason_is_logged(self, caplog, reason, output_text, expected_fragment):
        operator = OpenAIResponseOperator(
            task_id=TASK_ID, conn_id=CONN_ID, input_text="Write a haiku.", max_output_tokens=10
        )
        mock_hook_instance = Mock(spec=OpenAIHook)
        mock_hook_instance.create_response.return_value = _build_completed_response(
            status="incomplete",
            incomplete_details=IncompleteDetails(reason=reason),
            output_text=output_text,
        )
        operator.hook = mock_hook_instance

        with caplog.at_level("WARNING"):
            result = operator.execute(_build_execute_context())

        assert result == output_text
        assert any(
            f"incomplete_details.reason={reason}" in message and expected_fragment in message
            for message in caplog.messages
        )
        # The wording is decided by output_text, not by reason: a truthy output_text always gets
        # the "truncated, not empty" message and an empty one always gets "may be empty",
        # regardless of what reason is.
        if output_text:
            assert not any("may be empty" in message for message in caplog.messages)
        else:
            assert not any("truncated, not empty" in message for message in caplog.messages)

    def test_incomplete_without_details_uses_truncated_or_empty_message(self, caplog):
        operator = OpenAIResponseOperator(task_id=TASK_ID, conn_id=CONN_ID, input_text="Write a haiku.")
        mock_hook_instance = Mock(spec=OpenAIHook)
        mock_hook_instance.create_response.return_value = _build_completed_response(
            status="incomplete", incomplete_details=None, output_text=""
        )
        operator.hook = mock_hook_instance

        with caplog.at_level("WARNING"):
            result = operator.execute(_build_execute_context())

        assert result == ""
        assert any("may be truncated or empty" in message for message in caplog.messages)
        assert not any("truncated, not empty" in message for message in caplog.messages)
        assert not any("may be empty" in message for message in caplog.messages)

    def test_non_completed_without_incomplete_details_keeps_may_be_empty_message(self, caplog):
        operator = OpenAIResponseOperator(task_id=TASK_ID, conn_id=CONN_ID, input_text="Write a haiku.")
        mock_hook_instance = Mock(spec=OpenAIHook)
        mock_hook_instance.create_response.return_value = _build_completed_response(
            status="failed", output_text=""
        )
        operator.hook = mock_hook_instance

        with caplog.at_level("WARNING"):
            operator.execute(_build_execute_context())

        assert any(
            "ended with status failed" in message and "may be empty" in message for message in caplog.messages
        )


@pytest.mark.parametrize("wait_for_completion", [True, False])
def test_openai_trigger_batch_operator_not_deferred(mock_batch, wait_for_completion):
    operator = OpenAITriggerBatchOperator(
        task_id=TASK_ID,
        conn_id=CONN_ID,
        file_id=FILE_ID,
        endpoint=BATCH_ENDPOINT,
        wait_for_completion=wait_for_completion,
        deferrable=False,
    )
    mock_hook_instance = Mock(spec=OpenAIHook)
    mock_hook_instance.get_batch.return_value = mock_batch
    mock_hook_instance.create_batch.return_value = mock_batch
    operator.hook = mock_hook_instance

    context = Context()
    batch_id = operator.execute(context)
    assert batch_id == BATCH_ID


@pytest.mark.parametrize(
    ("metadata", "batch_kwargs", "expected_kwargs"),
    [
        pytest.param(None, None, {}, id="no-passthrough"),
        pytest.param(
            {"key": "value"},
            {"output_expires_after": {"anchor": "created_at", "seconds": 3600}},
            {"output_expires_after": {"anchor": "created_at", "seconds": 3600}},
            id="metadata-and-batch-kwargs",
        ),
    ],
)
def test_openai_trigger_batch_operator_create_batch_passthrough(
    mock_batch, metadata, batch_kwargs, expected_kwargs
):
    """metadata/batch_kwargs reach create_batch verbatim; unset batch_kwargs forwards none."""
    operator = OpenAITriggerBatchOperator(
        task_id=TASK_ID,
        conn_id=CONN_ID,
        file_id=FILE_ID,
        endpoint=BATCH_ENDPOINT,
        metadata=metadata,
        batch_kwargs=batch_kwargs,
        deferrable=False,
        wait_for_completion=False,
    )
    mock_hook_instance = Mock(spec=OpenAIHook)
    mock_hook_instance.create_batch.return_value = mock_batch
    operator.hook = mock_hook_instance

    operator.execute(Context())

    mock_hook_instance.create_batch.assert_called_once_with(
        file_id=FILE_ID,
        endpoint=BATCH_ENDPOINT,
        metadata=metadata,
        **expected_kwargs,
    )


def test_openai_trigger_batch_operator_templates_endpoint_and_metadata():
    operator = OpenAITriggerBatchOperator(
        task_id=TASK_ID,
        conn_id=CONN_ID,
        file_id=FILE_ID,
        endpoint="{{ ti.endpoint }}",
        metadata={"run": "{{ ti.run_id }}"},
    )

    class FakeTaskInstance:
        endpoint = BATCH_ENDPOINT
        run_id = "run-123"

    operator.render_template_fields(context={"ti": FakeTaskInstance()})

    assert operator.endpoint == BATCH_ENDPOINT
    assert operator.metadata == {"run": "run-123"}


@pytest.mark.parametrize(
    ("wait_for_completion", "poll_interval_kwargs", "expected_poll_interval"),
    [
        pytest.param(False, {}, None, id="not-deferred"),
        pytest.param(True, {}, 60, id="deferred-default-poll-interval"),
        pytest.param(True, {"poll_interval": 5}, 5, id="deferred-custom-poll-interval"),
    ],
)
def test_openai_trigger_batch_operator_with_deferred(
    mock_batch, wait_for_completion, poll_interval_kwargs, expected_poll_interval
):
    operator = OpenAITriggerBatchOperator(
        task_id=TASK_ID,
        conn_id=CONN_ID,
        file_id=FILE_ID,
        endpoint=BATCH_ENDPOINT,
        deferrable=True,
        wait_for_completion=wait_for_completion,
        **poll_interval_kwargs,
    )
    mock_hook_instance = Mock(spec=OpenAIHook)
    mock_hook_instance.get_batch.return_value = mock_batch
    mock_hook_instance.create_batch.return_value = mock_batch
    operator.hook = mock_hook_instance

    context = Context()
    if wait_for_completion:
        with pytest.raises(TaskDeferred) as exc:
            operator.execute(context)
        assert isinstance(exc.value.trigger, OpenAIBatchTrigger)
        assert exc.value.trigger.poll_interval == expected_poll_interval
    else:
        batch_id = operator.execute(context)
        assert batch_id == BATCH_ID


@mock.patch.object(OpenAITriggerBatchOperator, "log")
def test_openai_trigger_batch_operator_not_deferred_logs_active_knob(mock_log, mock_batch):
    """Non-deferred mode names wait_seconds' value and states poll_interval is unused."""
    operator = OpenAITriggerBatchOperator(
        task_id=TASK_ID,
        conn_id=CONN_ID,
        file_id=FILE_ID,
        endpoint=BATCH_ENDPOINT,
        deferrable=False,
        wait_seconds=7,
        poll_interval=99,
    )
    mock_hook_instance = Mock(spec=OpenAIHook)
    mock_hook_instance.get_batch.return_value = mock_batch
    mock_hook_instance.create_batch.return_value = mock_batch
    operator.hook = mock_hook_instance

    operator.execute(Context())

    mock_log.info.assert_any_call(
        "Waiting for batch %s to complete, polling every %s seconds via wait_seconds "
        "(poll_interval is not used in non-deferrable mode)",
        BATCH_ID,
        7,
    )


@mock.patch.object(OpenAITriggerBatchOperator, "log")
def test_openai_trigger_batch_operator_deferred_logs_active_knob(mock_log, mock_batch):
    """Deferred mode names poll_interval's value and states wait_seconds is unused."""
    operator = OpenAITriggerBatchOperator(
        task_id=TASK_ID,
        conn_id=CONN_ID,
        file_id=FILE_ID,
        endpoint=BATCH_ENDPOINT,
        deferrable=True,
        wait_seconds=71,
        poll_interval=13,
    )
    mock_hook_instance = Mock(spec=OpenAIHook)
    mock_hook_instance.get_batch.return_value = mock_batch
    mock_hook_instance.create_batch.return_value = mock_batch
    operator.hook = mock_hook_instance

    with pytest.raises(TaskDeferred):
        operator.execute(Context())

    mock_log.info.assert_any_call(
        "Deferring batch %s, polling every %s seconds via poll_interval "
        "(wait_seconds is not used in deferrable mode)",
        BATCH_ID,
        13,
    )


class TestOpenAITriggerBatchOperatorExecuteComplete:
    def _operator(self):
        return OpenAITriggerBatchOperator(
            task_id=TASK_ID,
            conn_id=CONN_ID,
            file_id=FILE_ID,
            endpoint=BATCH_ENDPOINT,
        )

    def test_success_returns_batch_id(self):
        event = {"status": "success", "message": "done", "batch_id": BATCH_ID}
        assert self._operator().execute_complete(Context(), event) == BATCH_ID

    @pytest.mark.parametrize(
        "event",
        [
            pytest.param({"status": "error", "message": "boom", "batch_id": BATCH_ID}, id="error"),
            pytest.param(
                {"status": "cancelled", "message": "Batch has been cancelled.", "batch_id": BATCH_ID},
                id="cancelled",
            ),
        ],
    )
    def test_failed_event_raises(self, event):
        with pytest.raises(OpenAIBatchJobException, match=event["message"]):
            self._operator().execute_complete(Context(), event)

    @pytest.mark.parametrize(
        "event",
        [
            pytest.param(None, id="none"),
            pytest.param({"status": "expired", "batch_id": BATCH_ID}, id="unknown-status"),
        ],
    )
    def test_invalid_event_raises_instead_of_succeeding(self, event):
        with pytest.raises(OpenAITriggerEventError):
            self._operator().execute_complete(Context(), event)
