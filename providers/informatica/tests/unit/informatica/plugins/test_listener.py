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

import logging
from unittest.mock import MagicMock, patch

import pytest

from airflow.providers.informatica.extractors.informatica import InformaticaLineageExtractor
from airflow.providers.informatica.hooks.edc import InformaticaEDCError
from airflow.providers.informatica.lineage.sql_parser import TableRef
from airflow.providers.informatica.plugins.listener import (
    InformaticaLineageResolutionError,
    InformaticaListener,
    _resolve_uri_to_object_id,
)


class DummyTask:
    def __init__(self, inlets=None, outlets=None, dag_id="dag"):
        self.inlets = inlets or []
        self.outlets = outlets or []
        self.params = {}
        self.dag_id = dag_id


class DummyTaskInstance:
    def __init__(self, task, task_id="dummy", run_id="run1", map_index=-1, dag_id=None, try_number=None):
        self.task = task
        self.task_id = task_id
        self.run_id = run_id
        self.map_index = map_index
        self.dag_id = dag_id if dag_id is not None else getattr(task, "dag_id", None)
        self.try_number = try_number


@pytest.fixture
def listener():
    lsnr = InformaticaListener()
    lsnr.hook = MagicMock(spec=InformaticaLineageExtractor)
    lsnr.log = MagicMock(spec=logging.Logger)
    return lsnr


# ---------------------------------------------------------------------------
# Manual lineage — running hook pre-validates, success hook creates links
# ---------------------------------------------------------------------------


@patch("airflow.providers.informatica.plugins.listener._resolve_uri_to_object_id")
def test_running_then_success_creates_link_str(mock_resolve, listener):
    """String URIs: running resolves, success creates links."""
    mock_resolve.side_effect = lambda hook, uri: uri
    listener.hook.create_lineage_link.return_value = {"metadata": {}}
    task = DummyTask(inlets=["in1"], outlets=["out1"])
    ti = DummyTaskInstance(task)

    listener.on_task_instance_running(previous_state=None, task_instance=ti)
    mock_resolve.assert_any_call(listener.hook, "in1")
    mock_resolve.assert_any_call(listener.hook, "out1")
    listener.hook.create_lineage_link.assert_not_called()  # not yet

    listener.on_task_instance_success(previous_state=None, task_instance=ti)
    listener.hook.create_lineage_link.assert_called_once_with("in1", "out1")


@patch("airflow.providers.informatica.plugins.listener._resolve_uri_to_object_id")
def test_running_then_success_creates_link_dict(mock_resolve, listener):
    """dict(dataset_uri=…) inlets/outlets: running resolves, success creates links."""
    mock_resolve.side_effect = lambda hook, uri: uri
    listener.hook.create_lineage_link.return_value = {"metadata": {}}
    task = DummyTask(inlets=[{"dataset_uri": "in1"}], outlets=[{"dataset_uri": "out1"}])
    ti = DummyTaskInstance(task)

    listener.on_task_instance_running(previous_state=None, task_instance=ti)
    mock_resolve.assert_any_call(listener.hook, "in1")
    mock_resolve.assert_any_call(listener.hook, "out1")

    listener.on_task_instance_success(previous_state=None, task_instance=ti)
    listener.hook.create_lineage_link.assert_called_once_with("in1", "out1")


@patch("airflow.providers.informatica.plugins.listener._resolve_uri_to_object_id")
def test_running_raises_when_uri_not_found(mock_resolve, listener):
    """If any URI cannot be resolved, running hook raises and task must not execute."""
    mock_resolve.side_effect = InformaticaLineageResolutionError("not found")
    task = DummyTask(inlets=["in1"], outlets=["out1"])
    ti = DummyTaskInstance(task)

    with pytest.raises(InformaticaLineageResolutionError):
        listener.on_task_instance_running(previous_state=None, task_instance=ti)

    listener.hook.create_lineage_link.assert_not_called()


@patch("airflow.providers.informatica.plugins.listener._resolve_uri_to_object_id")
def test_running_raises_on_invalid_inlet_type(mock_resolve, listener):
    """Non-string, non-dict inlet raises InformaticaLineageResolutionError."""
    task = DummyTask(inlets=[123], outlets=["out1"])
    ti = DummyTaskInstance(task)

    with pytest.raises(InformaticaLineageResolutionError, match="Invalid inlet entry"):
        listener.on_task_instance_running(previous_state=None, task_instance=ti)

    listener.hook.create_lineage_link.assert_not_called()


def test_success_without_prior_running_is_noop(listener):
    """If running hook was never called (no cache entry), success is a no-op."""
    task = DummyTask(inlets=["in1"], outlets=["out1"])
    ti = DummyTaskInstance(task)

    listener.on_task_instance_success(previous_state=None, task_instance=ti)
    listener.hook.create_lineage_link.assert_not_called()


def test_failed_clears_cache(listener):
    """on_task_instance_failed removes the cache entry to avoid stale state."""
    task = DummyTask(inlets=["in1"], outlets=["out1"])
    ti = DummyTaskInstance(task)
    key = listener._cache_key(ti)
    listener._resolved_cache[key] = ([("in1", "id_in")], [("out1", "id_out")])

    listener.on_task_instance_failed(previous_state=None, task_instance=ti)

    assert key not in listener._resolved_cache
    listener.hook.create_lineage_link.assert_not_called()


def test_manual_uri_resolution_uses_get_object_directly(listener):
    """Manual lineage URIs should be validated via get_object, not find_object_id."""
    listener.hook.get_object.return_value = {"id": "TEST_PSTGRS://mydb/public/customers"}

    object_id = _resolve_uri_to_object_id(listener.hook, "TEST_PSTGRS://mydb/public/customers")

    assert object_id == "TEST_PSTGRS://mydb/public/customers"
    listener.hook.get_object.assert_called_once_with("TEST_PSTGRS://mydb/public/customers")


def test_manual_uri_resolution_raises_when_get_object_returns_no_id(listener):
    """Manual lineage URI resolution fails when get_object returns no object id."""
    listener.hook.get_object.return_value = {"name": "customers"}

    with pytest.raises(InformaticaLineageResolutionError, match="Could not resolve EDC object for URI"):
        _resolve_uri_to_object_id(listener.hook, "TEST_PSTGRS://mydb/public/customers")


@patch("airflow.providers.informatica.plugins.listener._resolve_uri_to_object_id")
def test_link_creation_error_is_logged_not_raised(mock_resolve, listener):
    """Errors during link creation are logged but do not propagate."""
    mock_resolve.side_effect = lambda hook, uri: uri
    listener.hook.create_lineage_link.side_effect = InformaticaEDCError("network fail")
    task = DummyTask(inlets=["in1"], outlets=["out1"])
    ti = DummyTaskInstance(task)

    listener.on_task_instance_running(previous_state=None, task_instance=ti)
    listener.on_task_instance_success(previous_state=None, task_instance=ti)  # must not raise

    calls = listener.log.exception.call_args_list
    assert any("Failed to create lineage link from" in str(call) for call, *_ in calls)


@patch("airflow.providers.informatica.plugins.listener._resolve_uri_to_object_id")
def test_cache_consumed_exactly_once(mock_resolve, listener):
    """Cache entry is popped by success; a second success call is a no-op."""
    mock_resolve.side_effect = lambda hook, uri: uri
    listener.hook.create_lineage_link.return_value = {}
    task = DummyTask(inlets=["in1"], outlets=["out1"])
    ti = DummyTaskInstance(task)

    listener.on_task_instance_running(previous_state=None, task_instance=ti)
    listener.on_task_instance_success(previous_state=None, task_instance=ti)
    listener.hook.create_lineage_link.reset_mock()

    listener.on_task_instance_success(previous_state=None, task_instance=ti)  # no cache
    listener.hook.create_lineage_link.assert_not_called()


@patch("airflow.providers.informatica.plugins.listener._resolve_uri_to_object_id")
def test_cache_key_includes_dag_id_and_try_number(mock_resolve, listener):
    """Cache key should keep entries distinct across DAGs and retries."""
    mock_resolve.side_effect = lambda hook, uri: uri
    listener.hook.create_lineage_link.return_value = {}

    dag_a_task = DummyTask(inlets=["in_a"], outlets=["out_a"], dag_id="dag_a")
    dag_b_task = DummyTask(inlets=["in_b"], outlets=["out_b"], dag_id="dag_b")

    ti_dag_a_try_1 = DummyTaskInstance(
        dag_a_task,
        task_id="shared_task",
        run_id="shared_run",
        map_index=0,
        try_number=1,
    )
    ti_dag_b_try_1 = DummyTaskInstance(
        dag_b_task,
        task_id="shared_task",
        run_id="shared_run",
        map_index=0,
        try_number=1,
    )
    ti_dag_a_try_2 = DummyTaskInstance(
        dag_a_task,
        task_id="shared_task",
        run_id="shared_run",
        map_index=0,
        try_number=2,
    )

    listener.on_task_instance_running(previous_state=None, task_instance=ti_dag_a_try_1)
    listener.on_task_instance_running(previous_state=None, task_instance=ti_dag_b_try_1)
    listener.on_task_instance_running(previous_state=None, task_instance=ti_dag_a_try_2)

    assert len(listener._resolved_cache) == 3

    listener.on_task_instance_success(previous_state=None, task_instance=ti_dag_a_try_1)
    assert len(listener._resolved_cache) == 2

    listener.on_task_instance_success(previous_state=None, task_instance=ti_dag_b_try_1)
    assert len(listener._resolved_cache) == 1

    listener.on_task_instance_success(previous_state=None, task_instance=ti_dag_a_try_2)
    assert len(listener._resolved_cache) == 0
    assert listener.hook.create_lineage_link.call_count == 3


# ---------------------------------------------------------------------------
# Auto-lineage tests
# ---------------------------------------------------------------------------


class _SQLTask:
    """Task with a sql attribute and no manual inlets/outlets."""

    def __init__(self, sql, conn_id="postgres_default"):
        self.sql = sql
        self.conn_id = conn_id
        self.inlets = []
        self.outlets = []
        self.params = {}


class _SQLTaskInstance:
    def __init__(self, task, task_id="sql_task", run_id="run1", map_index=-1):
        self.task = task
        self.task_id = task_id
        self.run_id = run_id
        self.map_index = map_index


@pytest.fixture
def auto_listener():
    lsnr = InformaticaListener()
    lsnr.hook = MagicMock(spec_set=["find_object_id", "create_lineage_link"])
    lsnr.log = MagicMock(spec=logging.Logger)
    return lsnr


@patch("airflow.providers.informatica.plugins.listener.is_operator_disabled", return_value=False)
@patch("airflow.providers.informatica.plugins.listener.auto_lineage_enabled", return_value=True)
@patch("airflow.providers.informatica.plugins.listener.is_task_auto_lineage_disabled", return_value=False)
@patch("airflow.providers.informatica.plugins.listener.get_resolver")
def test_auto_lineage_fires_when_no_manual_inlets(
    mock_get_resolver, mock_task_disabled, mock_auto_enabled, mock_op_disabled, auto_listener
):
    mock_resolver = MagicMock()
    mock_resolver.resolve.return_value = (
        [TableRef(table="src")],
        [TableRef(table="dst")],
    )
    mock_get_resolver.return_value = mock_resolver
    auto_listener.hook.find_object_id.side_effect = lambda cat, schema, tbl: f"DB://schema/{tbl}"
    auto_listener.hook.create_lineage_link.return_value = {}

    task = _SQLTask(sql="INSERT INTO dst SELECT * FROM src")
    ti = _SQLTaskInstance(task)

    auto_listener.on_task_instance_running(previous_state=None, task_instance=ti)
    auto_listener.hook.create_lineage_link.assert_not_called()

    auto_listener.on_task_instance_success(previous_state=None, task_instance=ti)
    auto_listener.hook.create_lineage_link.assert_called_once_with("DB://schema/src", "DB://schema/dst")


@patch("airflow.providers.informatica.plugins.listener.is_operator_disabled", return_value=False)
@patch("airflow.providers.informatica.plugins.listener.auto_lineage_enabled", return_value=True)
@patch("airflow.providers.informatica.plugins.listener.get_resolver")
def test_manual_inlets_take_priority_over_auto_lineage(
    mock_get_resolver, mock_auto_enabled, mock_op_disabled, auto_listener
):
    auto_listener.hook.create_lineage_link.return_value = {}

    with patch(
        "airflow.providers.informatica.plugins.listener._resolve_uri_to_object_id",
        side_effect=lambda hook, uri: uri,
    ):
        task = DummyTask(inlets=["edc://manual_in"], outlets=["edc://manual_out"])
        ti = DummyTaskInstance(task)
        auto_listener.on_task_instance_running(previous_state=None, task_instance=ti)
        auto_listener.on_task_instance_success(previous_state=None, task_instance=ti)

    mock_get_resolver.assert_not_called()
    auto_listener.hook.create_lineage_link.assert_called_once_with("edc://manual_in", "edc://manual_out")


@patch("airflow.providers.informatica.plugins.listener.is_operator_disabled", return_value=True)
def test_disabled_operator_skips_all_lineage(mock_op_disabled, auto_listener):
    task = DummyTask(inlets=["edc://in"], outlets=["edc://out"])
    ti = DummyTaskInstance(task)

    auto_listener.on_task_instance_running(previous_state=None, task_instance=ti)
    auto_listener.on_task_instance_success(previous_state=None, task_instance=ti)

    auto_listener.hook.find_object_id.assert_not_called()
    auto_listener.hook.create_lineage_link.assert_not_called()


@patch("airflow.providers.informatica.plugins.listener.is_operator_disabled", return_value=False)
@patch("airflow.providers.informatica.plugins.listener.auto_lineage_enabled", return_value=False)
@patch("airflow.providers.informatica.plugins.listener.get_resolver")
def test_auto_lineage_disabled_globally_skips_resolver(
    mock_get_resolver, mock_auto_enabled, mock_op_disabled, auto_listener
):
    task = _SQLTask(sql="SELECT * FROM t")
    ti = _SQLTaskInstance(task)

    auto_listener.on_task_instance_running(previous_state=None, task_instance=ti)

    mock_get_resolver.assert_not_called()


@patch("airflow.providers.informatica.plugins.listener.is_operator_disabled", return_value=False)
@patch("airflow.providers.informatica.plugins.listener.auto_lineage_enabled", return_value=True)
@patch("airflow.providers.informatica.plugins.listener.is_task_auto_lineage_disabled", return_value=True)
@patch("airflow.providers.informatica.plugins.listener.get_resolver")
def test_per_task_disable_skips_resolver(
    mock_get_resolver, mock_task_disabled, mock_auto_enabled, mock_op_disabled, auto_listener
):
    task = _SQLTask(sql="SELECT * FROM t")
    ti = _SQLTaskInstance(task)

    auto_listener.on_task_instance_running(previous_state=None, task_instance=ti)

    mock_get_resolver.assert_not_called()


@patch("airflow.providers.informatica.plugins.listener.is_operator_disabled", return_value=False)
@patch("airflow.providers.informatica.plugins.listener.auto_lineage_enabled", return_value=True)
@patch("airflow.providers.informatica.plugins.listener.is_task_auto_lineage_disabled", return_value=False)
@patch("airflow.providers.informatica.plugins.listener.get_resolver")
def test_auto_lineage_no_lineage_when_resolver_returns_none(
    mock_get_resolver, mock_task_disabled, mock_auto_enabled, mock_op_disabled, auto_listener
):
    mock_resolver = MagicMock()
    mock_resolver.resolve.return_value = None
    mock_get_resolver.return_value = mock_resolver

    task = _SQLTask(sql="SELECT 1")
    ti = _SQLTaskInstance(task)

    auto_listener.on_task_instance_running(previous_state=None, task_instance=ti)
    auto_listener.on_task_instance_success(previous_state=None, task_instance=ti)

    auto_listener.hook.create_lineage_link.assert_not_called()


@patch("airflow.providers.informatica.plugins.listener.is_operator_disabled", return_value=False)
@patch("airflow.providers.informatica.plugins.listener.auto_lineage_enabled", return_value=True)
@patch("airflow.providers.informatica.plugins.listener.is_task_auto_lineage_disabled", return_value=False)
@patch("airflow.providers.informatica.plugins.listener.get_resolver")
def test_auto_lineage_raises_when_table_not_found(
    mock_get_resolver, mock_task_disabled, mock_auto_enabled, mock_op_disabled, auto_listener
):
    """If auto-resolved table is not in EDC, running hook raises to fail the task."""
    mock_resolver = MagicMock()
    mock_resolver.resolve.return_value = ([TableRef(table="missing")], [TableRef(table="dst")])
    mock_get_resolver.return_value = mock_resolver
    auto_listener.hook.find_object_id.return_value = None  # not found

    task = _SQLTask(sql="INSERT INTO dst SELECT * FROM missing")
    ti = _SQLTaskInstance(task)

    with pytest.raises(InformaticaLineageResolutionError, match="Could not resolve EDC object for table"):
        auto_listener.on_task_instance_running(previous_state=None, task_instance=ti)

    auto_listener.hook.create_lineage_link.assert_not_called()


@patch("airflow.providers.informatica.plugins.listener.is_operator_disabled", return_value=False)
@patch("airflow.providers.informatica.plugins.listener.auto_lineage_enabled", return_value=True)
@patch("airflow.providers.informatica.plugins.listener.is_task_auto_lineage_disabled", return_value=False)
@patch("airflow.providers.informatica.plugins.listener.get_resolver")
def test_auto_lineage_raises_when_edc_error(
    mock_get_resolver, mock_task_disabled, mock_auto_enabled, mock_op_disabled, auto_listener
):
    """EDC errors during auto-lineage resolution are wrapped and re-raised."""
    mock_resolver = MagicMock()
    mock_resolver.resolve.return_value = ([TableRef(table="src")], [TableRef(table="dst")])
    mock_get_resolver.return_value = mock_resolver
    auto_listener.hook.find_object_id.side_effect = InformaticaEDCError("connection refused")

    task = _SQLTask(sql="INSERT INTO dst SELECT * FROM src")
    ti = _SQLTaskInstance(task)

    with pytest.raises(InformaticaLineageResolutionError, match="EDC error"):
        auto_listener.on_task_instance_running(previous_state=None, task_instance=ti)

    auto_listener.hook.create_lineage_link.assert_not_called()
