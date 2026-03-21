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

from unittest.mock import MagicMock

import pytest

from airflow.providers.informatica.hooks.edc import InformaticaEDCError
from airflow.providers.informatica.plugins.listener import InformaticaListener


class DummyTask:
    def __init__(self, inlets=None, outlets=None):
        self.inlets = inlets or []
        self.outlets = outlets or []


class DummyTaskInstance:
    def __init__(self, task, task_id="dummy"):
        self.task = task
        self.task_id = task_id


@pytest.fixture
def listener():
    informatica_listener = InformaticaListener()
    informatica_listener.hook = MagicMock()
    informatica_listener.log = MagicMock()
    return informatica_listener


def test_handle_lineage_success_str(listener):
    listener.hook.get_object.side_effect = lambda x: {"id": x}
    listener.hook.create_lineage_link.return_value = {"metadata": {}}
    task = DummyTask(inlets=["in1"], outlets=["out1"])
    ti = DummyTaskInstance(task)
    listener._handle_lineage(ti, state="success")
    listener.hook.get_object.assert_any_call("in1")
    listener.hook.get_object.assert_any_call("out1")
    listener.hook.create_lineage_link.assert_called_once_with("in1", "out1")


def test_handle_lineage_success_dict(listener):
    listener.hook.get_object.side_effect = lambda x: {"id": x}
    listener.hook.create_lineage_link.return_value = {"metadata": {}}
    task = DummyTask(inlets=[{"dataset_uri": "in1"}], outlets=[{"dataset_uri": "out1"}])
    ti = DummyTaskInstance(task)
    listener._handle_lineage(ti, state="success")
    listener.hook.get_object.assert_any_call("in1")
    listener.hook.get_object.assert_any_call("out1")
    listener.hook.create_lineage_link.assert_called_once_with("in1", "out1")


def test_handle_lineage_skips_missing_objectid(listener):
    listener.hook.get_object.return_value = {}
    task = DummyTask(inlets=["in1"], outlets=["out1"])
    ti = DummyTaskInstance(task)
    listener._handle_lineage(ti, state="success")
    listener.hook.create_lineage_link.assert_not_called()


def test_handle_lineage_edc_error_on_inlet(listener):
    listener.hook.get_object.side_effect = [InformaticaEDCError("fail"), {"id": "out1"}]
    task = DummyTask(inlets=["in1"], outlets=["out1"])
    ti = DummyTaskInstance(task)
    listener._handle_lineage(ti, state="success")
    listener.hook.create_lineage_link.assert_not_called()


def test_handle_lineage_non_success_state(listener):
    task = DummyTask(inlets=["in1"], outlets=["out1"])
    ti = DummyTaskInstance(task)
    listener._handle_lineage(ti, state="failed")
    listener.hook.get_object.assert_not_called()
    listener.hook.create_lineage_link.assert_not_called()


def test_handle_lineage_link_creation_error_logs(listener):
    listener.hook.get_object.side_effect = lambda x: {"id": x}
    listener.hook.create_lineage_link.side_effect = Exception("fail link")
    listener.log = MagicMock()
    task = DummyTask(inlets=["in1"], outlets=["out1"])
    ti = DummyTaskInstance(task)
    listener._handle_lineage(ti, state="success")

    calls = listener.log.exception.call_args_list
    assert any("Failed to create lineage link from" in str(call) for call, *_ in calls)


def test_handle_lineage_inlet_outlet_type_error(listener):
    task = DummyTask(inlets=[123], outlets=[None])
    ti = DummyTaskInstance(task)
    listener._handle_lineage(ti, state="success")
    listener.hook.get_object.assert_not_called()
    listener.hook.create_lineage_link.assert_not_called()
