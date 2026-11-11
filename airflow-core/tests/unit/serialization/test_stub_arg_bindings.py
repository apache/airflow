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
from airflow.serialization.stub_arg_bindings import (
    _infer_value_schema,
    build_arg_bindings,
    build_mapped_arg_bindings,
)


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


class TestMappedStubArgBindings:
    """A mapped stub captures ordered ``param`` bindings for ti_run's per-map-index derivation."""

    def get_hook_fields(self, operator):
        arg_bindings = build_mapped_arg_bindings(operator)
        return {} if arg_bindings is None else {"_arg_bindings": arg_bindings}

    def test_params_follow_declaration_order_with_defaults_and_schemas(self):
        with DAG(dag_id="d"):
            # The partial() kwarg is declared *after* the expanded one: the captured
            # order must come from the signature, not from the call sites.
            result = stub(fn_transform).partial(extracted={"a": 1}).expand(country=["uk", "fr"])

        assert self.get_hook_fields(result.operator) == {
            "_arg_bindings": [
                {"name": "country", "kind": "param", "value_schema": {"type": "string"}},
                {
                    "name": "extracted",
                    "kind": "param",
                    "value_schema": {"type": "object", "additionalProperties": True},
                },
                {
                    "name": "retries_num",
                    "kind": "param",
                    "value_schema": {"type": "integer", "format": "int64"},
                    "default": 3,
                },
            ]
        }

    def test_none_default_is_captured_by_key_presence(self):
        def fn(x: str, y=None): ...

        with DAG(dag_id="d"):
            result = stub(fn).expand(x=["a"])

        params = self.get_hook_fields(result.operator)["_arg_bindings"]
        assert params[1] == {"name": "y", "kind": "param", "default": None}

    def test_untyped_params_omit_value_schema(self):
        with DAG(dag_id="d"):
            result = stub(fn_untyped).partial(a=1).expand(b=[2])

        assert self.get_hook_fields(result.operator) == {
            "_arg_bindings": [{"name": "a", "kind": "param"}, {"name": "b", "kind": "param"}]
        }

    def test_parameterless_stub_captures_nothing(self):
        with DAG(dag_id="d"):
            result = stub(fn_extract).expand_kwargs([{}])

        assert self.get_hook_fields(result.operator) == {}

    @pytest.mark.parametrize(
        "mappings",
        [
            pytest.param(
                [{"country": "uk", "extracted": {}}, {"country": "fr", "extracted": {}}], id="literal"
            ),
            pytest.param([{"country": "uk"}], id="literal-partial-mapping"),
        ],
    )
    def test_expand_kwargs_over_literal_mappings_captures_params(self, mappings):
        with DAG(dag_id="d"):
            result = stub(fn_transform).expand_kwargs(mappings)

        params = self.get_hook_fields(result.operator)["_arg_bindings"]
        assert [p["name"] for p in params] == ["country", "extracted", "retries_num"]

    def test_expand_kwargs_over_upstream_output_captures_params(self):
        with DAG(dag_id="d"):
            result = stub(fn_transform).expand_kwargs(stub(fn_extract)())

        params = self.get_hook_fields(result.operator)["_arg_bindings"]
        assert [p["name"] for p in params] == ["country", "extracted", "retries_num"]

    def test_expand_kwargs_mapping_with_unknown_parameter_rejected(self):
        with DAG(dag_id="d"):
            result = stub(fn_transform).expand_kwargs([{"country": "uk", "region": "eu"}])

        with pytest.raises(ValueError, match=r"unknown parameters \['region'\]"):
            self.get_hook_fields(result.operator)

    def test_missing_required_parameter_rejected(self):
        with DAG(dag_id="d"):
            result = stub(fn_transform).expand(country=["uk"])

        with pytest.raises(ValueError, match="does not bind to its signature"):
            self.get_hook_fields(result.operator)

    def test_partial_kwarg_over_mapped_upstream_allowed(self):
        def fn_produce(n: int): ...

        with DAG(dag_id="d"):
            vals = stub(fn_produce).expand(n=[1, 2])
            result = stub(fn_transform).partial(extracted=vals).expand(country=["uk"])

        params = self.get_hook_fields(result.operator)["_arg_bindings"]
        assert [p["name"] for p in params] == ["country", "extracted", "retries_num"]

    @pytest.mark.parametrize(
        "make_value",
        [
            pytest.param(lambda vals, out: out.map(str), id="map"),
            pytest.param(lambda vals, out: out.zip(vals), id="zip"),
            pytest.param(lambda vals, out: out.concat(vals), id="concat"),
        ],
    )
    def test_partial_kwarg_over_whole_map_zip_concat_rejected(self, make_value):
        def fn_produce(n: int): ...

        with DAG(dag_id="d"):
            vals = stub(fn_produce).expand(n=[1, 2])
            out = stub(fn_extract)()
            result = stub(fn_transform).partial(extracted=make_value(vals, out)).expand(country=["uk"])

        with pytest.raises(ValueError, match=r"receives a whole map\(\), zip\(\) or concat\(\) result"):
            self.get_hook_fields(result.operator)

    @pytest.mark.parametrize(
        ("make_value", "match"),
        [
            pytest.param(
                lambda a, b: a.map(str).zip(b), r"map\(\) inside a zip\(\) or concat\(\)", id="map-in-zip"
            ),
            pytest.param(
                lambda a, b: a.map(str).concat(b),
                r"map\(\) inside a zip\(\) or concat\(\)",
                id="map-in-concat",
            ),
            pytest.param(lambda a, b: a.zip(b).zip(b), r"zip\(\) inside a zip\(\)", id="zip-in-zip"),
            pytest.param(
                lambda a, b: a.concat(b.zip(a)).zip(b), r"zip\(\) inside a zip\(\)", id="zip-in-concat-in-zip"
            ),
        ],
    )
    def test_expand_shape_no_binding_carries_rejected(self, make_value, match):
        with DAG(dag_id="d"):
            a = stub(fn_extract)()
            b = stub(fn_extract)()
            result = stub(fn_transform).partial(country="uk").expand(extracted=make_value(a, b))

        with pytest.raises(ValueError, match=match):
            self.get_hook_fields(result.operator)

    @pytest.mark.parametrize(
        "make_value",
        [
            pytest.param(lambda a, b: a.map(str), id="map"),
            pytest.param(lambda a, b: a.zip(b).map(str), id="map-over-zip"),
            pytest.param(lambda a, b: a.zip(b).concat(a), id="zip-in-concat"),
            pytest.param(lambda a, b: a.concat(b).zip(b), id="concat-in-zip"),
        ],
    )
    def test_expand_shape_with_outermost_map_allowed(self, make_value):
        with DAG(dag_id="d"):
            a = stub(fn_extract)()
            b = stub(fn_extract)()
            result = stub(fn_transform).partial(country="uk").expand(extracted=make_value(a, b))

        params = self.get_hook_fields(result.operator)["_arg_bindings"]
        assert [p["kind"] for p in params] == ["param", "param", "param"]

    def test_custom_xcom_key_inside_partial_zip_rejected(self):
        with DAG(dag_id="d"):
            out = stub(fn_extract)()
            other = stub(fn_extract)()
            result = stub(fn_transform).partial(country=out.zip(other["rows"])).expand(extracted=out)

        with pytest.raises(ValueError, match="references the XCom key 'rows'"):
            self.get_hook_fields(result.operator)

    def test_expand_over_mapped_upstream_allowed(self):
        def fn_produce(n: int): ...

        with DAG(dag_id="d"):
            vals = stub(fn_produce).expand(n=[1, 2])
            result = stub(fn_transform).partial(country="uk").expand(extracted=vals)

        params = self.get_hook_fields(result.operator)["_arg_bindings"]
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

    def test_multi_kwarg_expand_records_dag_file_key_order(self):
        with DAG(dag_id="d"):
            result = stub(fn_untyped).expand(b=[1], a=[2])

        assert self.get_hook_fields(result.operator) == {
            "_arg_bindings": [
                {"name": "a", "kind": "param", "expand_keys": ["b", "a"]},
                {"name": "b", "kind": "param", "expand_keys": ["b", "a"]},
            ]
        }

    def test_literal_dict_expand_value_rejected(self):
        with DAG(dag_id="d"):
            result = stub(fn_transform).partial(country="uk").expand(extracted={"b": {}, "a": {}})

        with pytest.raises(ValueError, match="expands over a dict"):
            self.get_hook_fields(result.operator)

    @pytest.mark.parametrize(
        "kwargs",
        [
            pytest.param({"partial": {"cfg": {"a": 1}}, "expand": {"x": ["a"]}}, id="partial"),
            pytest.param({"partial": {}, "expand": {"x": ["a"], "cfg": [{}]}}, id="expand"),
        ],
    )
    def test_non_json_unused_default_allowed(self, kwargs):
        not_jsonable = object()

        def fn(x: str, cfg=not_jsonable): ...

        with DAG(dag_id="d"):
            result = stub(fn).partial(**kwargs["partial"]).expand(**kwargs["expand"])

        params = self.get_hook_fields(result.operator)["_arg_bindings"]
        assert "default" not in params[1]

    def test_zip_fillvalue_not_json_rejected(self):
        with DAG(dag_id="d"):
            result = (
                stub(fn_untyped)
                .partial(a=1)
                .expand(b=stub(fn_extract)().zip(stub(fn_extract)(), fillvalue=float("nan")))
            )

        with pytest.raises(ValueError, match="not JSON-serializable"):
            self.get_hook_fields(result.operator)

    @pytest.mark.parametrize("strict", [True, False])
    def test_expand_kwargs_mapping_overriding_partial(self, strict):
        with DAG(dag_id="d"):
            result = (
                stub(fn_transform).partial(country="uk").expand_kwargs([{"country": "fr"}], strict=strict)
            )

        if strict:
            with pytest.raises(ValueError, match=r"passes \['country'\], which partial\(\) already sets"):
                self.get_hook_fields(result.operator)
        else:
            assert self.get_hook_fields(result.operator)["_arg_bindings"]

    @pytest.mark.parametrize(
        "make_mappings",
        [
            pytest.param(lambda out: out, id="upstream-output"),
            pytest.param(lambda out: [{"country": "uk"}, out], id="upstream-item"),
        ],
    )
    @pytest.mark.parametrize("strict", [True, False])
    def test_mapped_whole_output_fallback_of_upstream_mapping(self, make_mappings, strict):
        def fn_produce(n: int): ...

        with DAG(dag_id="d"):
            vals = stub(fn_produce).expand(n=[1, 2])
            result = (
                stub(fn_transform)
                .partial(extracted=vals)
                .expand_kwargs(make_mappings(stub(fn_extract)()), strict=strict)
            )

        if strict:
            assert self.get_hook_fields(result.operator)["_arg_bindings"]
        else:
            with pytest.raises(ValueError, match="which no binding can carry"):
                self.get_hook_fields(result.operator)

    def test_expand_kwargs_mapped_whole_output_item_rejected(self):
        def fn_produce(n: int): ...

        with DAG(dag_id="d"):
            vals = stub(fn_produce).expand(n=[1, 2])
            result = stub(fn_transform).expand_kwargs([vals])

        with pytest.raises(ValueError, match="a list rather than a dict"):
            self.get_hook_fields(result.operator)

    def test_mapped_stub_inside_mapped_task_group_unconstructible(self):
        """The SDK bans expansion inside an expanded group outright, so no guard is needed here."""

        @task_group
        def group(n):
            stub(fn_transform).partial(country="uk").expand(extracted=[{}])

        with DAG(dag_id="d"):
            with pytest.raises(NotImplementedError, match="expansion in an expanded task group"):
                group.expand(n=[1, 2])


class TestTaskGroupMemberArgBindings:
    """A stub inside a mapped task group binds at parse time all but the group's expand arguments."""

    def test_group_arg_is_param_and_other_args_are_bound(self):
        captured = {}

        @task_group
        def group(country):
            captured["op"] = stub(fn_transform)(country, extracted)

        with DAG(dag_id="d"):
            extracted = stub(fn_extract)()
            group.expand(country=["uk", "fr"])

        assert build_arg_bindings(captured["op"].operator) == [
            {
                "name": "country",
                "kind": "param",
                "group_arg": {"group_id": "group", "key": "country"},
                "value_schema": {"type": "string"},
            },
            {
                "name": "extracted",
                "kind": "xcom",
                "task_id": "fn_extract",
                "value_schema": {"type": "object", "additionalProperties": True},
            },
            {
                "name": "retries_num",
                "kind": "literal",
                "value": 3,
                "from_default": True,
                "value_schema": {"type": "integer", "format": "int64"},
            },
        ]

    def test_argless_call_captures_nothing(self):
        captured = {}

        @task_group
        def group(country):
            captured["op"] = stub(fn_extract)()

        with DAG(dag_id="d"):
            group.expand(country=["uk"])

        assert build_arg_bindings(captured["op"].operator) is None

    def test_member_reading_task_mapped_again_inside_shared_group_rejected(self):
        captured = {}

        def fn_produce(n: int): ...

        @task_group
        def inner(m):
            captured["producer"] = stub(fn_produce)(m)

        @task_group
        def outer(n):
            inner.expand(m=[1, 2])
            captured["op"] = stub(fn_transform)("uk", captured["producer"])

        with DAG(dag_id="d"):
            outer.expand(n=[1, 2])

        with pytest.raises(ValueError, match="mapped again inside the mapped task group they share"):
            build_arg_bindings(captured["op"].operator)

    @pytest.mark.parametrize(
        ("make_value", "match"),
        [
            pytest.param(lambda a, b: a.map(str).zip(b), r"map\(\) inside a zip\(\)", id="map-in-zip"),
            pytest.param(lambda a, b: a.zip(b).zip(b), r"zip\(\) inside a zip\(\)", id="zip-in-zip"),
        ],
    )
    def test_group_expand_shape_no_binding_carries_rejected(self, make_value, match):
        captured = {}

        @task_group
        def group(country):
            captured["op"] = stub(fn_transform)(country, {})

        with DAG(dag_id="d"):
            group.expand(country=make_value(stub(fn_extract)(), stub(fn_extract)()))

        with pytest.raises(ValueError, match=match):
            build_arg_bindings(captured["op"].operator)

    def test_group_multi_kwarg_expand_records_dag_file_key_order(self):
        captured = {}

        @task_group
        def group(b, a):
            captured["op"] = stub(fn_untyped)(a, b)

        with DAG(dag_id="d"):
            group.expand(b=[1], a=[2])

        assert build_arg_bindings(captured["op"].operator) == [
            {
                "name": "a",
                "kind": "param",
                "group_arg": {"group_id": "group", "key": "a"},
                "expand_keys": ["b", "a"],
            },
            {
                "name": "b",
                "kind": "param",
                "group_arg": {"group_id": "group", "key": "b"},
                "expand_keys": ["b", "a"],
            },
        ]

    def test_group_literal_dict_expand_value_rejected(self):
        captured = {}

        @task_group
        def group(country):
            captured["op"] = stub(fn_transform)(country, {})

        with DAG(dag_id="d"):
            group.expand(country={"uk": 1, "fr": 2})

        with pytest.raises(ValueError, match="expands over a dict"):
            build_arg_bindings(captured["op"].operator)

    def test_group_expand_kwargs_mapped_whole_output_item_rejected(self):
        captured = {}

        def fn_produce(n: int): ...

        @task_group
        def group(country):
            captured["op"] = stub(fn_transform)(country, {})

        with DAG(dag_id="d"):
            group.expand_kwargs([stub(fn_produce).expand(n=[1, 2])])

        with pytest.raises(ValueError, match="a list rather than a dict"):
            build_arg_bindings(captured["op"].operator)

    def test_group_expand_over_outermost_map_is_param(self):
        captured = {}

        @task_group
        def group(country):
            captured["op"] = stub(fn_transform)(country, {})

        with DAG(dag_id="d"):
            group.expand(country=stub(fn_extract)().map(str))

        (country, *_) = build_arg_bindings(captured["op"].operator)
        assert country["kind"] == "param"


@pytest.mark.parametrize(
    "make_value",
    [
        pytest.param(lambda a, b: a.map(str), id="map"),
        pytest.param(lambda a, b: a.zip(b), id="zip"),
        pytest.param(lambda a, b: a.concat(b), id="concat"),
    ],
)
def test_unmapped_stub_whole_map_zip_concat_rejected(make_value):
    with DAG(dag_id="d"):
        result = stub(fn_transform)("uk", make_value(stub(fn_extract)(), stub(fn_extract)()))

    with pytest.raises(ValueError, match=r"receives a whole map\(\), zip\(\) or concat\(\) result"):
        build_arg_bindings(result.operator)


def test_expand_kwargs_whole_map_mapping_rejected():
    with DAG(dag_id="d"):
        result = stub(fn_transform).expand_kwargs([stub(fn_extract)().map(dict)])

    with pytest.raises(ValueError, match=r"receives a whole map\(\), zip\(\) or concat\(\) result"):
        build_mapped_arg_bindings(result.operator)
