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

import contextlib
import datetime
import typing
from typing import Any

import pendulum
import pytest

from airflow.providers.common.compat.sdk import DAG, task_group
from airflow.providers.standard.decorators.stub import _infer_value_schema, stub

from tests_common.test_utils.version_compat import AIRFLOW_V_3_3_PLUS, AIRFLOW_V_3_4_PLUS


def fn_ellipsis(): ...


def fn_pass(): ...


def fn_doc():
    """Some string"""


def fn_doc_pass():
    """Some string"""
    pass


def fn_code():
    return None


@pytest.mark.parametrize(
    ("fn", "error"),
    [
        pytest.param(fn_ellipsis, contextlib.nullcontext(), id="ellipsis"),
        pytest.param(fn_pass, contextlib.nullcontext(), id="pass"),
        pytest.param(fn_doc, contextlib.nullcontext(), id="doc"),
        pytest.param(fn_doc_pass, contextlib.nullcontext(), id="doc-and-pass"),
        pytest.param(fn_code, pytest.raises(ValueError, match="must be an empty function"), id="not-empty"),
    ],
)
def test_stub_signature(fn, error):
    with error:
        stub(fn)()


@pytest.mark.skipif(not AIRFLOW_V_3_3_PLUS, reason="retry_policy added in Airflow 3.3")
def test_stub_rejects_retry_policy():
    from airflow.sdk.definitions.retry_policy import ExceptionRetryPolicy

    with pytest.raises(ValueError, match="does not support `retry_policy`"):
        stub(fn_pass, retry_policy=ExceptionRetryPolicy(rules=[]))()


def test_stub_allows_retries():
    stub(fn_pass, retries=5)()


def fn_extract(): ...


def fn_transform(country: str, extracted: dict, retries_num: int = 3): ...


def fn_untyped(a, b): ...


def fn_varargs(*args): ...


def fn_kwonly_varkw(**kwargs): ...


def fn_context_key(ti): ...


class TestStubTaskflowArgs:
    """The TaskFlow call on a stub captures the ordered positional-arg spec (``_arg_bindings``)."""

    def test_literal_and_xcom_spec(self):
        with DAG(dag_id="d"):
            extracted = stub(fn_extract)()
            result = stub(fn_transform)("uk", extracted)

        op = result.operator
        assert op._arg_bindings == [
            {"name": "country", "kind": "literal", "value_schema": {"type": "string"}, "value": "uk"},
            {
                "name": "extracted",
                "kind": "xcom",
                "value_schema": {"type": "object", "additionalProperties": True},
                "task_id": "fn_extract",
            },
            {
                "name": "retries_num",
                "kind": "literal",
                "value_schema": {"type": "integer", "format": "int64"},
                "value": 3,
                "from_default": True,
            },
        ]
        assert op.upstream_task_ids == {"fn_extract"}

    def test_kwargs_normalize_to_declaration_order(self):
        with DAG(dag_id="d"):
            extracted = stub(fn_extract)()
            result = stub(fn_transform)(extracted=extracted, country="fr", retries_num=7)

        assert result.operator._arg_bindings == [
            {"name": "country", "kind": "literal", "value_schema": {"type": "string"}, "value": "fr"},
            {
                "name": "extracted",
                "kind": "xcom",
                "value_schema": {"type": "object", "additionalProperties": True},
                "task_id": "fn_extract",
            },
            {
                "name": "retries_num",
                "kind": "literal",
                "value_schema": {"type": "integer", "format": "int64"},
                "value": 7,
            },
        ]

    def test_explicitly_passing_the_default_value_is_not_from_default(self):
        """The flag tracks provenance, not value equality: an author-passed argument is explicit
        even when it equals the signature default, so keyword-style consumers must still claim it."""
        with DAG(dag_id="d"):
            extracted = stub(fn_extract)()
            result = stub(fn_transform)("uk", extracted, retries_num=3)

        assert result.operator._arg_bindings[2] == {
            "name": "retries_num",
            "kind": "literal",
            "value_schema": {"type": "integer", "format": "int64"},
            "value": 3,
        }

    def test_custom_xcom_key_rejected(self):
        with DAG(dag_id="d"):
            extracted = stub(fn_extract)()
            with pytest.raises(ValueError, match="indexing an output by a custom key"):
                stub(fn_transform)("uk", extracted["part"])

    def test_zero_param_stub_has_no_spec(self):
        assert stub(fn_pass)().operator._arg_bindings is None

    def test_untyped_params_omit_value_schema(self):
        """Key absence (never ``None``) is the wire contract for an unconstrained argument."""
        with DAG(dag_id="d"):
            result = stub(fn_untyped)(1, "x")

        assert result.operator._arg_bindings == [
            {"name": "a", "kind": "literal", "value": 1},
            {"name": "b", "kind": "literal", "value": "x"},
        ]

    def test_unresolvable_annotation_omits_value_schema(self):
        def fn(x): ...

        fn.__annotations__ = {"x": "NotARealType"}
        with DAG(dag_id="d"):
            result = stub(fn)("v")

        assert result.operator._arg_bindings == [{"name": "x", "kind": "literal", "value": "v"}]

    def test_varargs_rejected(self):
        with pytest.raises(ValueError, match="fixed number of parameters"):
            stub(fn_varargs)(1, 2)

    def test_varkw_rejected(self):
        with pytest.raises(ValueError, match="fixed number of parameters"):
            stub(fn_kwonly_varkw)(x=1)

    def test_context_key_param_rejected(self):
        with pytest.raises(ValueError, match="is an Airflow context key"):
            stub(fn_context_key)(1)

    @pytest.mark.parametrize("fn", [fn_varargs, fn_kwonly_varkw, fn_context_key], ids=lambda f: f.__name__)
    def test_argless_call_skips_signature_checks(self, fn):
        """Pre-TaskFlow stub Dags never passed arguments; their signatures must keep parsing."""
        assert stub(fn)().operator._arg_bindings is None

    def test_argless_call_captures_no_spec_for_defaulted_params(self):
        def fn(limit: int = 10): ...

        assert stub(fn)().operator._arg_bindings is None

    def test_non_json_literal_rejected(self):
        with DAG(dag_id="d"), pytest.raises(ValueError, match="not JSON-serializable"):
            stub(fn_transform)("uk", object())

    def test_nan_literal_rejected(self):
        with DAG(dag_id="d"), pytest.raises(ValueError, match="not JSON-serializable"):
            stub(fn_transform)("uk", {"ratio": float("nan")})

    def test_mapped_xcom_arg_rejected(self):
        with DAG(dag_id="d"):
            extracted = stub(fn_extract)()
            with pytest.raises(ValueError, match="only direct upstream task outputs"):
                stub(fn_transform)("uk", extracted.map(lambda v: v))

    def test_arg_bindings_survive_dag_serialization_round_trip(self):
        """The captured spec must survive whichever core serializer the provider runs against."""
        try:
            from airflow.serialization.serialized_objects import DagSerialization
        except ImportError:  # Airflow 2 exposes the round-trip API on SerializedDAG
            from airflow.serialization.serialized_objects import SerializedDAG as DagSerialization

        with DAG(dag_id="d") as dag:
            extracted = stub(fn_extract)()
            stub(fn_transform)("uk", extracted)

        round_tripped = DagSerialization.from_dict(DagSerialization.to_dict(dag))
        assert round_tripped.task_dict["fn_transform"]._arg_bindings == [
            {"name": "country", "kind": "literal", "value_schema": {"type": "string"}, "value": "uk"},
            {
                "name": "extracted",
                "kind": "xcom",
                "value_schema": {"type": "object", "additionalProperties": True},
                "task_id": "fn_extract",
            },
            {
                "name": "retries_num",
                "kind": "literal",
                "value_schema": {"type": "integer", "format": "int64"},
                "value": 3,
                "from_default": True,
            },
        ]

    @pytest.mark.skipif(
        not AIRFLOW_V_3_4_PLUS, reason="task-sdk honors the supports_expand opt-out from Airflow 3.4"
    )
    def test_expand_rejected_at_parse_time(self):
        with DAG(dag_id="d"):
            with pytest.raises(TypeError, match="do not support dynamic task mapping"):
                stub(fn_transform).expand(country=["uk", "fr"], extracted=[{}, {}])

    def test_stub_with_args_inside_mapped_task_group_rejected(self):
        @task_group
        def group(n):
            stub(fn_transform)("uk", {})

        with DAG(dag_id="d"):
            with pytest.raises(ValueError, match="mapped task group"):
                group.expand(n=[1, 2])

    def test_argless_stub_inside_mapped_task_group_allowed(self):
        @task_group
        def group(n):
            stub(fn_extract)()

        with DAG(dag_id="d"):
            group.expand(n=[1, 2])


@pytest.mark.parametrize(
    ("annotation", "expected"),
    [
        pytest.param(str, {"type": "string"}, id="str"),
        pytest.param(bool, {"type": "boolean"}, id="bool"),
        pytest.param(int, {"type": "integer", "format": "int64"}, id="int"),
        pytest.param(float, {"type": "number", "format": "double"}, id="float"),
        pytest.param(dict, {"type": "object", "additionalProperties": True}, id="dict"),
        pytest.param(
            dict[str, int],
            {"type": "object", "additionalProperties": {"type": "integer", "format": "int64"}},
            id="dict-parameterized",
        ),
        pytest.param(
            typing.Mapping[str, int],
            {"type": "object", "additionalProperties": {"type": "integer", "format": "int64"}},
            id="mapping",
        ),
        pytest.param(list, {"type": "array", "items": {}}, id="list"),
        pytest.param(
            list[int],
            {"type": "array", "items": {"type": "integer", "format": "int64"}},
            id="list-parameterized",
        ),
        pytest.param(tuple, {"type": "array", "items": {}}, id="tuple"),
        pytest.param(set, {"type": "array", "items": {}, "uniqueItems": True}, id="set"),
        pytest.param(
            typing.Sequence[int],
            {"type": "array", "items": {"type": "integer", "format": "int64"}},
            id="sequence",
        ),
        pytest.param(datetime.datetime, {"type": "string", "format": "date-time"}, id="datetime"),
        pytest.param(datetime.date, {"type": "string", "format": "date"}, id="date"),
        pytest.param(datetime.time, {"type": "string", "format": "time"}, id="time"),
        pytest.param(datetime.timedelta, {"type": "string", "format": "duration"}, id="timedelta"),
        pytest.param(bytes, {"type": "string", "format": "binary"}, id="bytes"),
        pytest.param(
            typing.Literal["a", "b"],
            {"type": "string", "enum": ["a", "b"]},
            id="literal",
        ),
        pytest.param(Any, None, id="any"),
        pytest.param(None, None, id="none"),
        pytest.param(type(None), None, id="nonetype"),
        pytest.param(
            pendulum.DateTime,
            None,
            id="pendulum-datetime",
            # pydantic has no schema for arbitrary datetime subclasses; decode-only fallback.
        ),
        pytest.param(
            typing.Optional[str],  # noqa: UP045 -- legacy form on purpose
            {"anyOf": [{"type": "string"}, {"type": "null"}]},
            id="optional-str",
        ),
        pytest.param(
            typing.Union[int, str],  # noqa: UP007 -- legacy form on purpose
            {"anyOf": [{"type": "integer", "format": "int64"}, {"type": "string"}]},
            id="union",
        ),
        pytest.param(str | None, {"anyOf": [{"type": "string"}, {"type": "null"}]}, id="pep604-optional"),
        pytest.param(
            int | None,
            {"anyOf": [{"type": "integer", "format": "int64"}, {"type": "null"}]},
            id="optional-int",
        ),
        pytest.param(
            datetime.datetime | None,
            {"anyOf": [{"type": "string", "format": "date-time"}, {"type": "null"}]},
            id="optional-datetime",
        ),
        pytest.param(
            dict | bool,
            {"anyOf": [{"type": "object", "additionalProperties": True}, {"type": "boolean"}]},
            id="union-dict-bool",
        ),
        pytest.param(
            str | int | None,
            {"anyOf": [{"type": "string"}, {"type": "integer", "format": "int64"}, {"type": "null"}]},
            id="union-with-null",
        ),
        pytest.param(list | tuple, {"type": "array", "items": {}}, id="union-dedupes-equal-members"),
        pytest.param(
            datetime.datetime | str,
            {"anyOf": [{"type": "string", "format": "date-time"}, {"type": "string"}]},
            id="mixed-format-union-keeps-both",
        ),
        pytest.param(
            str | contextlib.AbstractContextManager,
            None,
            id="union-unclassifiable-member",
        ),
        pytest.param(contextlib.AbstractContextManager, None, id="custom-class"),
    ],
)
def test_infer_value_schema(annotation, expected):
    assert _infer_value_schema(annotation) == expected
