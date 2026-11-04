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

"""Unit tests for airflow.sdk.definitions.task_handlers (registry, declaration, runtime binder)."""

from __future__ import annotations

import re

import pytest

from airflow.sdk import task_handler
from airflow.sdk.api.datamodels._generated import LiteralArgBinding, TaskArgBinding, XComArgBinding
from airflow.sdk.definitions.task_handlers import TaskHandlerDeclaration, call_handler, get_handler
from airflow.sdk.definitions.task_handlers.declaration import TaskHandlerSignatureError

pytestmark = pytest.mark.usefixtures("clean_task_handlers")


def _bound(bindings, resolver=lambda *_: None):
    """Return the kwargs ``call_handler`` would pass to a handler (identity handler)."""
    return call_handler(lambda **kwargs: kwargs, bindings, resolver)


def _lit(name, value, from_default=False):
    root = LiteralArgBinding(name=name, kind="literal", value=value, from_default=from_default)
    return TaskArgBinding(root=root)


def _xc(name, task_id):
    return TaskArgBinding(root=XComArgBinding(name=name, kind="xcom", task_id=task_id))


class TestRegistry:
    def test_registers_and_looks_up_by_dag_and_task(self):
        def transform(region, events):
            return len(events)

        transform_h = task_handler("events_pipeline", "transform")(transform)
        assert transform is transform_h, "@task_handler should return the original function"
        assert get_handler("events_pipeline", "transform") is transform

    def test_task_id_defaults_to_function_name(self):
        @task_handler("d")
        def extract():
            return 1

        assert get_handler("d", "extract") is extract

    def test_unknown_lookup_returns_none(self):
        assert get_handler("nope", "nope") is None

    @staticmethod
    def _register_from(filename, dag_id="d", task_id="t"):
        """Register a handler as if defined in *filename*, to drive the fileloc check."""
        src = f"from airflow.sdk import task_handler\n@task_handler({dag_id!r}, {task_id!r})\ndef h(): ...\n"
        exec(compile(src, filename, "exec"), ns := {})
        return ns["h"]

    def test_same_file_reregistration_overwrites(self):
        self._register_from("/bundle/handlers.py")
        second = self._register_from("/bundle/handlers.py")
        assert get_handler("d", "t") is second

    def test_cross_file_duplicate_rejected(self):
        self._register_from("/bundle_a/handlers.py")
        with pytest.raises(
            ValueError,
            match=re.escape(
                "handler already registered for dag_id='d' task_id='t' in '/bundle_a/handlers.py'"
            ),
        ):
            self._register_from("/bundle_b/handlers.py")


class TestDeclaration:
    def test_params_ordered_with_required_flag(self):
        def transform(region: str, events: list, limit: int = 10): ...

        decl = TaskHandlerDeclaration.from_handler(transform)
        assert [p.name for p in decl.params] == ["region", "events", "limit"]
        assert [p.required for p in decl.params] == [True, True, False]

    def test_varargs_rejected(self):
        def f(*args): ...

        with pytest.raises(
            TaskHandlerSignatureError,
            match=re.escape("may not use variadic *args or **kwargs"),
        ):
            TaskHandlerDeclaration.from_handler(f)

    def test_positional_only_rejected(self):
        def f(region, /): ...

        with pytest.raises(
            TaskHandlerSignatureError,
            match=re.escape("may not use positional-only parameter 'region'"),
        ):
            TaskHandlerDeclaration.from_handler(f)

    def test_context_key_rejected(self):
        def f(ti, region: str): ...

        with pytest.raises(
            TaskHandlerSignatureError,
            match=re.escape("argument 'ti' is a reserved context key"),
        ):
            TaskHandlerDeclaration.from_handler(f)


class TestBinding:
    def test_literal_and_xcom_bind_to_kwargs(self):
        pulled = []

        def resolver(task_id, key):
            pulled.append((task_id, key))
            return [{"row": 1}]

        kwargs = _bound([_lit("region", "eu"), _xc("events", "extract")], resolver)
        assert kwargs == {"region": "eu", "events": [{"row": 1}]}
        assert pulled == [("extract", "return_value")]

    def test_from_default_literal_is_skipped(self):
        bindings = [_lit("region", "eu"), _lit("limit", 10, from_default=True)]
        assert _bound(bindings) == {"region": "eu"}

    def test_none_bindings_is_empty(self):
        assert _bound(None) == {}
