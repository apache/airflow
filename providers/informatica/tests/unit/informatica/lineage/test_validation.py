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

from unittest.mock import MagicMock, patch

import pytest

from airflow.providers.informatica.extractors.informatica import InformaticaLineageExtractor
from airflow.providers.informatica.hooks.edc import InformaticaEDCError
from airflow.providers.informatica.lineage.sql_parser import TableRef
from airflow.providers.informatica.lineage.validation import (
    _PRE_EXECUTE_CACHE_MAX,
    InformaticaLineageResolutionError,
    _pre_execute_cache,
    _store_pre_execute_result,
    pop_pre_execute_result,
    resolve_informatica_lineage,
    resolve_table_refs,
    resolve_uri_to_object_id,
    resolve_uris,
    validate_informatica_lineage,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class _DummyTask:
    def __init__(self, inlets=None, outlets=None, dag_id="dag"):
        self.inlets = inlets or []
        self.outlets = outlets or []
        self.params = {}
        self.dag_id = dag_id


class _DummyTaskInstance:
    def __init__(self, task, task_id="dummy", run_id="run1", map_index=-1, dag_id=None, try_number=None):
        self.task = task
        self.task_id = task_id
        self.run_id = run_id
        self.map_index = map_index
        self.dag_id = dag_id if dag_id is not None else getattr(task, "dag_id", None)
        self.try_number = try_number


class _SQLTask:
    def __init__(self, sql, conn_id="postgres_default"):
        self.sql = sql
        self.conn_id = conn_id
        self.inlets = []
        self.outlets = []
        self.params = {}


@pytest.fixture
def hook():
    return MagicMock(spec=InformaticaLineageExtractor)


# ---------------------------------------------------------------------------
# resolve_uri_to_object_id
# ---------------------------------------------------------------------------


def test_resolve_uri_to_object_id_success(hook):
    hook.get_object.return_value = {"id": "EDC://db/schema/tbl"}
    result = resolve_uri_to_object_id(hook, "EDC://db/schema/tbl")
    assert result == "EDC://db/schema/tbl"
    hook.get_object.assert_called_once_with("EDC://db/schema/tbl")


def test_resolve_uri_to_object_id_raises_on_edc_error(hook):
    hook.get_object.side_effect = InformaticaEDCError("timeout")
    with pytest.raises(InformaticaLineageResolutionError, match="Failed to resolve"):
        resolve_uri_to_object_id(hook, "bad://uri")


def test_resolve_uri_to_object_id_raises_when_no_id(hook):
    hook.get_object.return_value = {"name": "tbl"}
    with pytest.raises(InformaticaLineageResolutionError, match="Could not resolve"):
        resolve_uri_to_object_id(hook, "EDC://db/schema/tbl")


# ---------------------------------------------------------------------------
# resolve_uris
# ---------------------------------------------------------------------------


def test_resolve_uris_string_items(hook):
    hook.get_object.side_effect = lambda uri: {"id": uri}
    result = resolve_uris(hook, ["uri1", "uri2"], "inlet", "t1")
    assert result == [("uri1", "uri1"), ("uri2", "uri2")]


def test_resolve_uris_dict_items(hook):
    hook.get_object.side_effect = lambda uri: {"id": uri}
    result = resolve_uris(hook, [{"dataset_uri": "u1"}], "outlet", "t1")
    assert result == [("u1", "u1")]


def test_resolve_uris_asset_object(hook):
    hook.get_object.side_effect = lambda uri: {"id": uri}

    class FakeAsset:
        uri = "asset://uri"

    result = resolve_uris(hook, [FakeAsset()], "inlet", "t1")
    assert result == [("asset://uri", "asset://uri")]


def test_resolve_uris_invalid_type_raises(hook):
    with pytest.raises(InformaticaLineageResolutionError, match="Invalid inlet entry"):
        resolve_uris(hook, [123], "inlet", "t1")


def test_resolve_uris_propagates_resolution_error(hook):
    hook.get_object.side_effect = InformaticaEDCError("fail")
    with pytest.raises(InformaticaLineageResolutionError):
        resolve_uris(hook, ["bad"], "inlet", "t1")


# ---------------------------------------------------------------------------
# resolve_table_refs
# ---------------------------------------------------------------------------


def test_resolve_table_refs_success(hook):
    hook.find_object_id.return_value = "EDC://obj"
    refs = [TableRef(table="tbl", schema="sch", database="cat")]
    result = resolve_table_refs(hook, refs, "t1")
    assert result == [("cat/sch/tbl", "EDC://obj")]


def test_resolve_table_refs_raises_when_not_found(hook):
    hook.find_object_id.return_value = None
    refs = [TableRef(table="missing")]
    with pytest.raises(InformaticaLineageResolutionError, match="Could not resolve"):
        resolve_table_refs(hook, refs, "t1")


def test_resolve_table_refs_wraps_edc_error(hook):
    hook.find_object_id.side_effect = InformaticaEDCError("conn refused")
    refs = [TableRef(table="tbl")]
    with pytest.raises(InformaticaLineageResolutionError, match="EDC error"):
        resolve_table_refs(hook, refs, "t1")


# ---------------------------------------------------------------------------
# resolve_informatica_lineage
# ---------------------------------------------------------------------------


@patch("airflow.providers.informatica.lineage.validation.is_operator_disabled", return_value=True)
def test_resolve_lineage_disabled_operator(mock_disabled):
    task = _DummyTask(inlets=["in1"])
    result = resolve_informatica_lineage(task, "t1", hook=MagicMock())
    assert result == ([], [])


@patch("airflow.providers.informatica.lineage.validation.resolve_uri_to_object_id")
@patch("airflow.providers.informatica.lineage.validation.is_operator_disabled", return_value=False)
def test_resolve_lineage_manual_inlets(mock_disabled, mock_resolve):
    mock_resolve.side_effect = lambda hook, uri: uri
    hook = MagicMock(spec=InformaticaLineageExtractor)
    task = _DummyTask(inlets=["in1"], outlets=["out1"])

    valid_inlets, valid_outlets = resolve_informatica_lineage(task, "t1", hook=hook)
    assert valid_inlets == [("in1", "in1")]
    assert valid_outlets == [("out1", "out1")]


@patch("airflow.providers.informatica.lineage.validation.is_operator_disabled", return_value=False)
@patch("airflow.providers.informatica.lineage.validation.auto_lineage_enabled", return_value=True)
@patch("airflow.providers.informatica.lineage.validation.is_task_auto_lineage_disabled", return_value=False)
@patch("airflow.providers.informatica.lineage.validation.get_resolver")
def test_resolve_lineage_auto(mock_get_resolver, mock_task_dis, mock_auto, mock_op_dis):
    mock_resolver = MagicMock()
    mock_resolver.resolve.return_value = ([TableRef(table="src")], [TableRef(table="dst")])
    mock_get_resolver.return_value = mock_resolver
    hook = MagicMock(spec=InformaticaLineageExtractor)
    hook.find_object_id.side_effect = lambda c, s, t: f"EDC://{t}"

    task = _SQLTask(sql="INSERT INTO dst SELECT * FROM src")
    valid_inlets, valid_outlets = resolve_informatica_lineage(task, "t1", hook=hook)

    assert valid_inlets == [("//src", "EDC://src")]
    assert valid_outlets == [("//dst", "EDC://dst")]


@patch("airflow.providers.informatica.lineage.validation.is_operator_disabled", return_value=False)
@patch("airflow.providers.informatica.lineage.validation.auto_lineage_enabled", return_value=False)
def test_resolve_lineage_no_inlets_no_auto(mock_auto, mock_op_dis):
    task = _DummyTask()
    result = resolve_informatica_lineage(task, "t1", hook=MagicMock())
    assert result == ([], [])


# ---------------------------------------------------------------------------
# validate_informatica_lineage (pre_execute callable)
# ---------------------------------------------------------------------------


@patch("airflow.providers.informatica.lineage.validation.resolve_informatica_lineage")
def test_validate_pre_execute_success(mock_resolve):
    mock_resolve.return_value = ([("in1", "id_in")], [("out1", "id_out")])
    task = _DummyTask(inlets=["in1"], outlets=["out1"])
    ti = _DummyTaskInstance(task, task_id="t1", run_id="r1", try_number=1)
    context = {"task_instance": ti}

    validate_informatica_lineage(context)

    mock_resolve.assert_called_once_with(task, "t1")
    key = ("dag", "r1", "t1", -1, 1)
    result = pop_pre_execute_result(key)
    assert result == ([("in1", "id_in")], [("out1", "id_out")])


@patch("airflow.providers.informatica.lineage.validation.resolve_informatica_lineage")
def test_validate_pre_execute_raises_on_error(mock_resolve):
    mock_resolve.side_effect = InformaticaLineageResolutionError("not found")
    task = _DummyTask(inlets=["bad"])
    ti = _DummyTaskInstance(task)
    context = {"task_instance": ti}

    with pytest.raises(InformaticaLineageResolutionError, match="not found"):
        validate_informatica_lineage(context)


def test_validate_pre_execute_noop_when_no_ti():
    validate_informatica_lineage({})


def test_validate_pre_execute_noop_when_no_task():
    ti = _DummyTaskInstance(task=None, task_id="t1")
    ti.task = None
    validate_informatica_lineage({"task_instance": ti})


# ---------------------------------------------------------------------------
# Cache management helpers
# ---------------------------------------------------------------------------


def test_pop_pre_execute_result_returns_none_for_missing_key():
    result = pop_pre_execute_result(("no", "such", "key"))
    assert result is None


@patch("airflow.providers.informatica.lineage.validation.resolve_informatica_lineage")
def test_validate_pre_execute_overwrites_duplicate_key(mock_resolve):
    """A second validate call for the same TI overwrites the cached result."""
    task = _DummyTask(inlets=["in1"], outlets=["out1"])
    ti = _DummyTaskInstance(task, task_id="t1", run_id="r1", try_number=1)
    context = {"task_instance": ti}
    key = ("dag", "r1", "t1", -1, 1)

    mock_resolve.return_value = ([("in1", "id1")], [("out1", "id2")])
    validate_informatica_lineage(context)

    mock_resolve.return_value = ([("in_new", "id3")], [("out_new", "id4")])
    validate_informatica_lineage(context)

    result = pop_pre_execute_result(key)
    assert result == ([("in_new", "id3")], [("out_new", "id4")])


def test_store_pre_execute_result_evicts_oldest_when_full():
    """Cache evicts the oldest entry when _PRE_EXECUTE_CACHE_MAX is reached."""
    _pre_execute_cache.clear()
    try:
        for i in range(_PRE_EXECUTE_CACHE_MAX):
            _store_pre_execute_result(("dag", "run", f"task_{i}", -1, 1), ([], []))

        assert len(_pre_execute_cache) == _PRE_EXECUTE_CACHE_MAX

        # One more should evict task_0
        _store_pre_execute_result(("dag", "run", "task_overflow", -1, 1), ([], []))
        assert len(_pre_execute_cache) == _PRE_EXECUTE_CACHE_MAX
        assert ("dag", "run", "task_0", -1, 1) not in _pre_execute_cache
        assert ("dag", "run", "task_overflow", -1, 1) in _pre_execute_cache
    finally:
        _pre_execute_cache.clear()


@patch("airflow.providers.informatica.lineage.validation.resolve_informatica_lineage")
def test_validate_pre_execute_uses_ti_key(mock_resolve):
    """Context with 'ti' key (alternative to 'task_instance')."""
    mock_resolve.return_value = ([], [])
    task = _DummyTask()
    ti = _DummyTaskInstance(task, task_id="alt", run_id="r2", try_number=2)
    context = {"ti": ti}

    validate_informatica_lineage(context)

    mock_resolve.assert_called_once_with(task, "alt")
    key = ("dag", "r2", "alt", -1, 2)
    assert key in _pre_execute_cache
    _pre_execute_cache.pop(key)
