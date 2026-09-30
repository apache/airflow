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

# Licensed to the ASF under Apache-2.0.
"""Unit tests for airflow.sdk.task_handlers (registry, declaration, runtime binder)."""

from __future__ import annotations

import re

import pytest

from airflow.sdk.task_handlers import (
    call_handler,
    declare_handler,
    get_registry,
    task_handler,
)


@pytest.fixture(autouse=True)
def _clean_registry():
    get_registry()._handlers.clear()
    yield
    get_registry()._handlers.clear()


def _bound(bindings, resolver=lambda *_: None):
    """Return the kwargs ``call_handler`` would pass to a handler (identity handler)."""
    return call_handler(lambda **kwargs: kwargs, bindings, resolver)


class TestRegistry:
    def test_registers_and_looks_up_by_dag_and_task(self):
        def transform(region, events):
            return len(events)

        transform_h = task_handler("events_pipeline", "transform")(transform)
        assert transform is transform_h, "@task_handler should return the original function"
        assert get_registry().get("events_pipeline", "transform") is transform

    def test_task_id_defaults_to_function_name(self):
        @task_handler("d")
        def extract():
            return 1

        assert get_registry().get("d", "extract") is extract

    def test_unknown_lookup_returns_none(self):
        assert get_registry().get("nope", "nope") is None

    def test_duplicate_registration_rejected(self):
        @task_handler("d", "t")
        def a(): ...

        with pytest.raises(
            ValueError, match=re.escape("handler already registered for dag_id='d' task_id='t'")
        ):

            @task_handler("d", "t")
            def b(): ...


class TestDeclaration:
    def test_params_ordered_with_required_flag(self):
        def transform(region: str, events: list, limit: int = 10): ...

        decl = declare_handler("transform", transform)
        assert decl.task_id == "transform"
        assert [p.name for p in decl.params] == ["region", "events", "limit"]
        assert [p.required for p in decl.params] == [True, True, False]

    def test_value_schema_generated_with_int64_format(self):
        def f(n: int): ...

        (param,) = declare_handler("f", f).params
        assert param.value_schema is not None
        assert param.value_schema.get("format") == "int64"

    def test_unannotated_param_has_no_schema(self):
        def f(x): ...

        assert declare_handler("f", f).params[0].value_schema is None

    def test_injected_schema_fn_is_used(self):
        def f(x: int): ...

        decl = declare_handler("f", f, schema_fn=lambda ann: {"marker": True})
        assert decl.params[0].value_schema == {"marker": True}

    def test_varargs_rejected(self):
        def f(*args): ...

        with pytest.raises(
            ValueError, match=re.escape("task handler 'f' may not use varadic *args or **kwargs")
        ):
            declare_handler("f", f)

    def test_positional_only_rejected(self):
        def f(region, /): ...

        with pytest.raises(
            ValueError,
            match=re.escape("task handler 'f' may not use positional-only parameter 'region'"),
        ):
            declare_handler("f", f)

    def test_context_key_rejected(self):
        def f(ti, region: str): ...

        with pytest.raises(
            ValueError, match=re.escape("task handler 'f' argument 'ti' is a reserved context key")
        ):
            declare_handler("f", f)


class TestBinding:
    def test_literal_and_xcom_bind_to_kwargs(self):
        pulled = []

        def resolver(task_id, key):
            pulled.append((task_id, key))
            return [{"row": 1}]

        kwargs = _bound(
            [
                {"name": "region", "kind": "literal", "value": "eu"},
                {"name": "events", "kind": "xcom", "task_id": "extract"},
            ],
            resolver,
        )
        assert kwargs == {"region": "eu", "events": [{"row": 1}]}
        assert pulled == [("extract", "return_value")]

    def test_from_default_literal_is_skipped(self):
        bindings = [
            {"name": "region", "kind": "literal", "value": "eu"},
            {"name": "limit", "kind": "literal", "value": 10, "from_default": True},
        ]
        assert _bound(bindings) == {"region": "eu"}

    def test_value_schema_on_wire_is_ignored(self):
        bindings = [{"name": "n", "kind": "literal", "value": 5, "value_schema": {"type": "integer"}}]
        assert _bound(bindings) == {"n": 5}

    def test_call_handler_end_to_end(self):
        def transform(region, events):
            return f"{region}:{len(events)}"

        result = call_handler(
            transform,
            [
                {"name": "region", "kind": "literal", "value": "us"},
                {"name": "events", "kind": "xcom", "task_id": "extract"},
            ],
            lambda tid, key: [1, 2, 3],
        )
        assert result == "us:3"

    def test_none_bindings_is_empty(self):
        assert _bound(None) == {}

    def test_missing_name_errors(self):
        with pytest.raises(ValueError, match=re.escape("arg_bindings[0]: missing 'name'")):
            _bound([{"kind": "literal", "value": 1}])

    def test_xcom_missing_task_id_errors(self):
        for bad in ([{"name": "x", "kind": "xcom"}], [{"name": "x", "kind": "xcom", "task_id": ""}]):
            with pytest.raises(
                ValueError, match=re.escape("arg_bindings[0] ('x'): xcom binding missing 'task_id'")
            ):
                _bound(bad)

    def test_unknown_kind_errors(self):
        with pytest.raises(ValueError, match=re.escape("arg_bindings[0] ('x'): unknown kind 'bogus'")):
            _bound([{"name": "x", "kind": "bogus"}])

    def test_missing_kind_errors(self):
        with pytest.raises(ValueError, match=re.escape("arg_bindings[0] ('x'): missing 'kind'")):
            _bound([{"name": "x"}])
