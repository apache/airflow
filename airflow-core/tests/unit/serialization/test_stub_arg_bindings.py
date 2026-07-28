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

from airflow.providers.standard.decorators.stub import stub
from airflow.sdk import DAG, task_group
from airflow.serialization.stub_arg_bindings import _infer_value_schema, build_mapped_arg_binding_params


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
            {"type": "string", "format": "date-time"},
            id="pendulum-datetime",
        ),
        pytest.param(
            pendulum.DateTime | None,
            {"anyOf": [{"type": "string", "format": "date-time"}, {"type": "null"}]},
            id="optional-pendulum-datetime",
        ),
        pytest.param(
            list[pendulum.DateTime],
            {"type": "array", "items": {"type": "string", "format": "date-time"}},
            id="list-pendulum-datetime",
        ),
        pytest.param(pendulum.Duration, {"type": "string", "format": "duration"}, id="pendulum-duration"),
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
        # pydantic raises PydanticUserError (not the JSON-schema subclasses) for these; they
        # must still degrade to no schema rather than crash Dag serialization.
        pytest.param(typing.ClassVar, None, id="pydantic-user-error"),
        pytest.param(typing.Callable[[int], str], None, id="callable-invalid-for-json-schema"),
        pytest.param(
            pendulum.DateTime | contextlib.AbstractContextManager,
            None,
            id="union-temporal-and-unclassifiable",
        ),
    ],
)
def test_infer_value_schema(annotation, expected):
    assert _infer_value_schema(annotation) == expected


def test_infer_value_schema_cache_returns_isolated_copies():
    first = _infer_value_schema(dict)
    second = _infer_value_schema(dict)
    assert first == second
    assert first is not second, "callers embed and serialize the fragment, so it must not alias the cache"


def test_infer_value_schema_unhashable_annotation_generates_uncached():
    annotation = typing.Annotated[int, {"unhashable": True}]
    assert _infer_value_schema(annotation) == {"type": "integer", "format": "int64"}


def test_infer_value_schema_degrades_on_pydantic_typeerror(monkeypatch):
    """A bare TypeError from pydantic degrades to no schema rather than crashing Dag serialization."""
    from airflow.serialization import stub_arg_bindings

    def _raise_type_error(_annotation):
        raise TypeError("pydantic cannot build a schema for this")

    monkeypatch.setattr(stub_arg_bindings, "TypeAdapter", _raise_type_error)

    # A fresh class dodges the process-lifetime schema cache and exercises the hashable-but-
    # unschemable path, where a naive ``except TypeError`` retry would re-raise and crash.
    class _Unschemable: ...

    assert _infer_value_schema(_Unschemable) is None


def fn_extract(): ...


def fn_transform(country: str, extracted: dict, retries_num: int = 3): ...


def fn_untyped(a, b): ...


class TestMappedStubArgBindingParams:
    """A mapped stub captures ordered per-parameter metadata for ti_run's per-map-index derivation."""

    def get_hook_fields(self, operator):
        params = build_mapped_arg_binding_params(operator)
        return {} if params is None else {"_mapped_arg_binding_params": params}

    def test_params_follow_declaration_order_with_defaults_and_schemas(self):
        with DAG(dag_id="d"):
            # The partial() kwarg is declared *after* the expanded one: the captured
            # order must come from the signature, not from the call sites.
            result = stub(fn_transform).partial(extracted={"a": 1}).expand(country=["uk", "fr"])

        assert self.get_hook_fields(result.operator) == {
            "_mapped_arg_binding_params": [
                {"name": "country", "value_schema": {"type": "string"}},
                {"name": "extracted", "value_schema": {"type": "object", "additionalProperties": True}},
                {
                    "name": "retries_num",
                    "value_schema": {"type": "integer", "format": "int64"},
                    "default": 3,
                },
            ]
        }

    def test_none_default_is_captured_by_key_presence(self):
        def fn(x: str, y=None): ...

        with DAG(dag_id="d"):
            result = stub(fn).expand(x=["a"])

        params = self.get_hook_fields(result.operator)["_mapped_arg_binding_params"]
        assert params[1] == {"name": "y", "default": None}

    def test_untyped_params_omit_value_schema(self):
        with DAG(dag_id="d"):
            result = stub(fn_untyped).expand(a=[1], b=[2])

        assert self.get_hook_fields(result.operator) == {
            "_mapped_arg_binding_params": [{"name": "a"}, {"name": "b"}]
        }

    def test_parameterless_stub_captures_nothing(self):
        with DAG(dag_id="d"):
            result = stub(fn_extract).expand_kwargs([{}])

        assert self.get_hook_fields(result.operator) == {}

    def test_expand_kwargs_rejected_for_parameterful_stub(self):
        with DAG(dag_id="d"):
            result = stub(fn_transform).expand_kwargs([{"country": "uk", "extracted": {}}])

        with pytest.raises(ValueError, match="does not support expand_kwargs"):
            self.get_hook_fields(result.operator)

    def test_missing_required_parameter_rejected(self):
        with DAG(dag_id="d"):
            result = stub(fn_transform).expand(country=["uk"])

        with pytest.raises(ValueError, match="does not bind to its signature"):
            self.get_hook_fields(result.operator)

    def test_partial_kwarg_over_mapped_upstream_rejected(self):
        def fn_produce(n: int): ...

        with DAG(dag_id="d"):
            vals = stub(fn_produce).expand(n=[1, 2])
            result = stub(fn_transform).partial(extracted=vals).expand(country=["uk"])

        with pytest.raises(ValueError, match="aggregated output of the mapped task"):
            self.get_hook_fields(result.operator)

    def test_expand_over_mapped_upstream_allowed(self):
        def fn_produce(n: int): ...

        with DAG(dag_id="d"):
            vals = stub(fn_produce).expand(n=[1, 2])
            result = stub(fn_transform).partial(country="uk").expand(extracted=vals)

        params = self.get_hook_fields(result.operator)["_mapped_arg_binding_params"]
        assert [p["name"] for p in params] == ["country", "extracted", "retries_num"]

    def test_non_json_expand_literal_rejected(self):
        with DAG(dag_id="d"):
            result = stub(fn_transform).partial(country="uk").expand(extracted=[object()])

        with pytest.raises(ValueError, match="not JSON-serializable"):
            self.get_hook_fields(result.operator)

    def test_non_json_needed_default_rejected(self):
        not_jsonable = object()

        def fn(x: str, y=not_jsonable): ...

        with DAG(dag_id="d"):
            result = stub(fn).expand(x=["a"])

        with pytest.raises(ValueError, match="not JSON-serializable"):
            self.get_hook_fields(result.operator)

    def test_mapped_stub_inside_mapped_task_group_unconstructible(self):
        """The SDK bans expansion inside an expanded group outright, so no guard is needed here."""

        @task_group
        def group(n):
            stub(fn_transform).partial(country="uk").expand(extracted=[{}])

        with DAG(dag_id="d"):
            with pytest.raises(NotImplementedError, match="expansion in an expanded task group"):
                group.expand(n=[1, 2])
