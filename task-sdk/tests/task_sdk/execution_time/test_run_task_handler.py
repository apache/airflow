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

from types import SimpleNamespace
from unittest import mock

import pytest

from airflow.sdk.api.datamodels._generated import (
    LiteralArgBinding,
    TaskArgBinding,
    XComArgBinding,
)
from airflow.sdk.execution_time.task_runner import _TaskRunner
from airflow.sdk.task_handlers import TaskHandlerDeclaration


def _literal(name, value):
    return TaskArgBinding(root=LiteralArgBinding(name=name, kind="literal", value=value))


def _xcom(name, task_id):
    return TaskArgBinding(root=XComArgBinding(name=name, kind="xcom", task_id=task_id))


def _fake_ti(*, arg_bindings, xcom_values=None, queue="default"):
    xcom_values = xcom_values or {}
    pulled = []

    def xcom_pull(task_ids, key):
        pulled.append((task_ids, key))
        return xcom_values.get(task_ids)

    ti = SimpleNamespace(
        dag_id="events_pipeline",
        task_id="transform",
        task=SimpleNamespace(queue=queue, is_stub=True),
        _ti_context_from_server=SimpleNamespace(arg_bindings=arg_bindings),
        xcom_pull=xcom_pull,
    )
    ti.pulled = pulled
    return ti


def _run(ti):
    # A stub ti always routes through the public entry to the handler path; context is unused there.
    return _TaskRunner.run(ti, mock.MagicMock())


def _patch_coordinator(monkeypatch, declaration):
    coordinator = SimpleNamespace(describe_task_handler=lambda dag_id, task_id: declaration)
    manager = SimpleNamespace(for_queue=lambda queue: coordinator)
    monkeypatch.setattr("airflow.sdk.execution_time.coordinator.get_coordinator_manager", lambda: manager)


def test_binds_literal_and_xcom_then_calls_handler(monkeypatch):
    def transform(region, events):
        return f"{region}:{len(events)}"

    _patch_coordinator(monkeypatch, TaskHandlerDeclaration(task_id="transform", handler=transform))
    ti = _fake_ti(
        arg_bindings=[_literal("region", "eu"), _xcom("events", "extract")],
        xcom_values={"extract": [1, 2, 3]},
    )

    result = _run(ti)

    assert result == "eu:3"
    assert ti.pulled == [("extract", "return_value")]


def test_no_arg_bindings_calls_handler_with_nothing(monkeypatch):
    def extract():
        return [1, 2]

    _patch_coordinator(monkeypatch, TaskHandlerDeclaration(task_id="extract", handler=extract))
    ti = _fake_ti(arg_bindings=None)

    assert _run(ti) == [1, 2]


def test_missing_declaration_raises(monkeypatch):
    _patch_coordinator(monkeypatch, None)
    ti = _fake_ti(arg_bindings=None)

    with pytest.raises(RuntimeError, match="No task handler"):
        _run(ti)


def test_declaration_without_callable_raises(monkeypatch):
    # e.g. a declaration that came from a foreign runtime over the wire carries no callable.
    _patch_coordinator(monkeypatch, TaskHandlerDeclaration(task_id="transform", handler=None))
    ti = _fake_ti(arg_bindings=None)

    with pytest.raises(RuntimeError, match="No task handler"):
        _run(ti)


def test_run_dispatches_is_stub_to_handler(monkeypatch):
    # run() must route an is_stub task to the handler and never touch the stub operator's
    # execute() (the fake task has none); the returned value becomes the return_value XCom.
    def transform(region, events):
        return f"{region}:{len(events)}"

    _patch_coordinator(monkeypatch, TaskHandlerDeclaration(task_id="transform", handler=transform))
    ti = _fake_ti(
        arg_bindings=[_literal("region", "us"), _xcom("events", "extract")],
        xcom_values={"extract": [1, 2]},
    )

    assert _run(ti) == "us:2"
