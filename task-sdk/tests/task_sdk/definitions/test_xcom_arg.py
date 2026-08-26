#
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

from collections.abc import Callable
from unittest import mock

import pytest
import structlog

from airflow.sdk import TaskInstanceState
from airflow.sdk.bases.xcom import BaseXCom
from airflow.sdk.definitions._internal.expandinput import DictOfListsExpandInput
from airflow.sdk.definitions.dag import DAG
from airflow.sdk.definitions.xcom_arg import PlainXComArg
from airflow.sdk.exceptions import AirflowSkipException, ErrorType
from airflow.sdk.execution_time.comms import (
    ErrorResponse,
    GetXCom,
    GetXComBatch,
    XComBatchResult,
    XComBatchResultItem,
    XComResult,
    XComSequenceSliceResult,
)
from airflow.sdk.execution_time.lazy_sequence import LazyXComSequence
from airflow.sdk.serde import deserialize, serialize

log = structlog.get_logger(__name__)

RunTI = Callable[[DAG, str, int], TaskInstanceState]


def test_xcom_map(run_ti: RunTI, mock_supervisor_comms):
    results = set()
    with DAG("test") as dag:

        @dag.task
        def push():
            return ["a", "b", "c"]

        @dag.task
        def pull(value):
            results.add(value)

        pull.expand_kwargs(push().map(lambda v: {"value": v * 2}))

    # The function passed to "map" is *NOT* a task.
    assert set(dag.task_dict) == {"push", "pull"}

    # Mock xcom result from push task
    mock_supervisor_comms.send.return_value = XComResult(key=BaseXCom.XCOM_RETURN_KEY, value=["a", "b", "c"])

    for map_index in range(3):
        assert run_ti(dag, "pull", map_index) == TaskInstanceState.SUCCESS

    assert results == {"aa", "bb", "cc"}


def test_xcom_map_transform_to_none(run_ti: RunTI, mock_supervisor_comms):
    results = set()

    with DAG("test") as dag:

        @dag.task()
        def push():
            return ["a", "b", "c"]

        @dag.task()
        def pull(value):
            results.add(value)

        def c_to_none(v):
            if v == "c":
                return None
            return v

        pull.expand(value=push().map(c_to_none))

    # Mock xcom result from push task
    mock_supervisor_comms.send.return_value = XComResult(key=BaseXCom.XCOM_RETURN_KEY, value=["a", "b", "c"])

    # Run "pull". This should automatically convert "c" to None.
    for map_index in range(3):
        assert run_ti(dag, "pull", map_index) == TaskInstanceState.SUCCESS

    assert results == {"a", "b", None}


def test_xcom_convert_to_kwargs_fails_task(run_ti: RunTI, mock_supervisor_comms, caplog):
    results = set()

    with DAG("test") as dag:

        @dag.task()
        def push():
            return ["a", "b", "c"]

        @dag.task()
        def pull(value):
            results.add(value)

        def c_to_none(v):
            if v == "c":
                return None
            return {"value": v}

        pull.expand_kwargs(push().map(c_to_none))

    # Mock xcom result from push task
    mock_supervisor_comms.send.return_value = XComResult(key=BaseXCom.XCOM_RETURN_KEY, value=["a", "b", "c"])

    # The first two "pull" tis should succeed.
    for map_index in range(2):
        assert run_ti(dag, "pull", map_index) == TaskInstanceState.SUCCESS

    # Clear captured logs from the above
    caplog.clear()

    # But the third one fails because the map() result cannot be used as kwargs.
    assert run_ti(dag, "pull", 2) == TaskInstanceState.FAILED

    assert {
        "event": "Task failed with exception",
        "log_level": "error",
        "exception": [
            {
                "exc_notes": [],
                "exc_type": "ValueError",
                "exc_value": "expand_kwargs() expects a list[dict], not list[None]",
                "frames": mock.ANY,
                "is_cause": False,
                "is_group": False,
                "exceptions": [],
                "syntax_error": None,
            }
        ],
    } in caplog


def test_xcom_map_error_fails_task(mock_supervisor_comms, run_ti, caplog):
    with DAG("test") as dag:

        @dag.task()
        def push():
            return ["a", "b", "c"]

        @dag.task()
        def pull(value):
            print(value)

        def does_not_work_with_c(v):
            if v == "c":
                raise RuntimeError("nope")
            return {"value": v * 2}

        pull.expand_kwargs(push().map(does_not_work_with_c))

    # Mock xcom result from push task
    mock_supervisor_comms.send.return_value = XComResult(key=BaseXCom.XCOM_RETURN_KEY, value=["a", "b", "c"])
    # The third one (for "c") will fail.
    assert run_ti(dag, "pull", 2) == TaskInstanceState.FAILED

    assert {
        "event": "Task failed with exception",
        "log_level": "error",
        "timestamp": mock.ANY,
        "exception": [
            {
                "exc_notes": [],
                "exc_type": "RuntimeError",
                "exc_value": "nope",
                "frames": mock.ANY,
                "is_cause": False,
                "is_group": False,
                "exceptions": [],
                "syntax_error": None,
            }
        ],
    } in caplog


def test_xcom_map_nest(mock_supervisor_comms, run_ti):
    results = set()

    with DAG("test") as dag:

        @dag.task()
        def push():
            return ["a", "b", "c"]

        @dag.task()
        def pull(value):
            results.add(value)

        converted = push().map(lambda v: v * 2).map(lambda v: {"value": v})
        pull.expand_kwargs(converted)

    # Mock xcom result from push task
    mock_supervisor_comms.send.return_value = XComResult(key=BaseXCom.XCOM_RETURN_KEY, value=["a", "b", "c"])

    # Now "pull" should apply the mapping functions in order.
    for map_index in range(3):
        assert run_ti(dag, "pull", map_index) == TaskInstanceState.SUCCESS
    assert results == {"aa", "bb", "cc"}


def test_xcom_map_zip_nest(mock_supervisor_comms, run_ti):
    results = set()

    with DAG("test") as dag:

        @dag.task
        def push_letters():
            return ["a", "b", "c", "d"]

        @dag.task
        def push_numbers():
            return [1, 2, 3, 4]

        @dag.task
        def pull(value):
            results.add(value)

        doubled = push_numbers().map(lambda v: v * 2)
        combined = doubled.zip(push_letters())

        def convert_zipped(zipped):
            letter, number = zipped
            return letter * number

        pull.expand(value=combined.map(convert_zipped))

    def xcom_get(msg):
        if not isinstance(msg, GetXCom):
            return mock.DEFAULT
        if msg.task_id == "push_letters":
            value = push_letters.function()
            return XComResult(key=BaseXCom.XCOM_RETURN_KEY, value=value)
        if msg.task_id == "push_numbers":
            value = push_numbers.function()
            return XComResult(key=BaseXCom.XCOM_RETURN_KEY, value=value)
        return mock.DEFAULT

    mock_supervisor_comms.send.side_effect = xcom_get

    # Run "pull".
    for map_index in range(4):
        assert run_ti(dag, "pull", map_index) == TaskInstanceState.SUCCESS

    assert results == {"aa", "bbbb", "cccccc", "dddddddd"}


def test_xcom_map_raise_to_skip(run_ti, mock_supervisor_comms):
    result = []

    with DAG("test") as dag:

        @dag.task()
        def push():
            return ["a", "b", "c"]

        @dag.task()
        def forward(value):
            result.append(value)

        def skip_c(v):
            if v == "c":
                raise AirflowSkipException()
            return {"value": v}

        forward.expand_kwargs(push().map(skip_c))

    # Mock xcom result from push task
    mock_supervisor_comms.send.return_value = XComResult(key=BaseXCom.XCOM_RETURN_KEY, value=["a", "b", "c"])

    # Run "forward". This should automatically skip "c".
    states = [run_ti(dag, "forward", map_index) for map_index in range(3)]

    assert states == [TaskInstanceState.SUCCESS, TaskInstanceState.SUCCESS, TaskInstanceState.SKIPPED]

    assert result == ["a", "b"]


def test_xcom_concat(run_ti, mock_supervisor_comms):
    from airflow.sdk.definitions.xcom_arg import _ConcatResult

    agg_results = set()
    all_results = None

    with DAG("test") as dag:

        @dag.task
        def push_letters():
            return ["a", "b", "c"]

        @dag.task
        def push_numbers():
            return [1, 2]

        @dag.task
        def pull_one(value):
            agg_results.add(value)

        @dag.task
        def pull_all(value):
            assert isinstance(value, _ConcatResult)
            assert value[0] == "a"
            assert value[1] == "b"
            assert value[2] == "c"
            assert value[3] == 1
            assert value[4] == 2
            with pytest.raises(IndexError):
                value[5]
            assert value[-5] == "a"
            assert value[-4] == "b"
            assert value[-3] == "c"
            assert value[-2] == 1
            assert value[-1] == 2
            with pytest.raises(IndexError):
                value[-6]
            nonlocal all_results
            all_results = list(value)

        pushed_values = push_letters().concat(push_numbers())

        pull_one.expand(value=pushed_values)
        pull_all(pushed_values)

    def xcom_get(msg):
        if not isinstance(msg, GetXCom):
            return mock.DEFAULT
        if msg.task_id == "push_letters":
            value = push_letters.function()
            return XComResult(key=BaseXCom.XCOM_RETURN_KEY, value=value)
        if msg.task_id == "push_numbers":
            value = push_numbers.function()
            return XComResult(key=BaseXCom.XCOM_RETURN_KEY, value=value)
        return mock.DEFAULT

    mock_supervisor_comms.send.side_effect = xcom_get

    # Run "pull_one" and "pull_all".
    assert run_ti(dag, "pull_all", -1) == TaskInstanceState.SUCCESS
    assert all_results == ["a", "b", "c", 1, 2]

    states = [run_ti(dag, "pull_one", map_index) for map_index in range(5)]
    assert states == [TaskInstanceState.SUCCESS] * 5
    assert agg_results == {"a", "b", "c", 1, 2}


class TestPlainXComArgResolveMappedGroup:
    """Resolving a task inside a mapped task group from a task outside that group.

    Regression tests for #69036 and #48005: the combined return value of a
    mapped task group must always serialize to a list (one element per
    expansion), even when the group expanded only once or every expansion
    returned ``None``. Previously this case was routed through ``xcom_pull``
    pulling all map indexes, which collapsed a single value to a bare scalar
    and an empty set of values to ``None``. ``resolve`` stays lazy and serde
    materialises the sequence only when the value is actually returned.
    """

    @staticmethod
    def _make_ti(*, computed):
        ti = mock.MagicMock()
        ti._upstream_map_indexes = None
        ti._cached_template_context = {"expanded_ti_count": 1}
        ti.run_id = "run-1"
        ti.get_relevant_upstream_map_indexes.return_value = computed
        return ti

    @staticmethod
    def _make_arg():
        operator = mock.MagicMock()
        operator.is_mapped = False
        operator.task_id = "do_something"
        operator.dag_id = "test_dag"
        operator.get_closest_mapped_task_group.return_value = mock.MagicMock()
        return PlainXComArg(operator=operator, key="test")

    @pytest.mark.parametrize(
        ("root", "expected"),
        [
            pytest.param(["14"], ["14"], id="single-expansion-stays-a-list"),
            pytest.param([], [], id="all-none-expansions-give-empty-list"),
            pytest.param(["a", "b"], ["a", "b"], id="multiple-expansions"),
        ],
    )
    def test_resolve_stays_lazy_and_serializes_as_list(self, root, expected, mock_supervisor_comms):
        mock_supervisor_comms.send.return_value = XComSequenceSliceResult(root=root)

        arg = self._make_arg()
        ti = self._make_ti(computed=None)

        resolved = arg.resolve({"ti": ti})

        assert isinstance(resolved, LazyXComSequence)
        ti.xcom_pull.assert_not_called()
        # The lazy sequence materializes to a plain list only when actually serialized.
        assert deserialize(serialize(resolved)) == expected

    def test_resolve_uses_xcom_pull_for_specific_index(self):
        arg = self._make_arg()
        ti = self._make_ti(computed=0)
        ti.xcom_pull.return_value = "value-0"

        resolved = arg.resolve({"ti": ti})

        assert resolved == "value-0"
        ti.xcom_pull.assert_called_once()
        assert ti.xcom_pull.call_args.kwargs["map_indexes"] == 0


def test_expand_batches_plain_xcom_args_into_one_call(run_ti: RunTI, mock_supervisor_comms):
    """Multiple plain, non-mapped-upstream XComArg kwargs resolve via a single GetXComBatch."""
    results = []

    with DAG("test") as dag:

        @dag.task
        def push_a():
            return ["a"]

        @dag.task
        def push_b():
            return ["b"]

        @dag.task
        def push_c():
            return ["c"]

        @dag.task
        def pull(x, y, z):
            results.append((x, y, z))

        pull.expand(x=push_a(), y=push_b(), z=push_c())

    calls = {"GetXComBatch": 0, "GetXCom": 0}
    values = {"push_a": ["a"], "push_b": ["b"], "push_c": ["c"]}

    def comms(msg):
        if isinstance(msg, GetXComBatch):
            calls["GetXComBatch"] += 1
            return XComBatchResult(
                items=[
                    XComBatchResultItem(
                        task_id=item.task_id,
                        key=item.key,
                        map_index=-1,
                        found=True,
                        value=values[item.task_id],
                    )
                    for item in msg.items
                ]
            )
        if isinstance(msg, GetXCom):
            calls["GetXCom"] += 1
        return mock.DEFAULT

    mock_supervisor_comms.send.side_effect = comms

    assert run_ti(dag, "pull", 0) == TaskInstanceState.SUCCESS
    assert calls == {"GetXComBatch": 1, "GetXCom": 0}
    assert results == [("a", "b", "c")]


def test_expand_batch_falls_back_on_old_server(run_ti: RunTI, mock_supervisor_comms):
    """When the API server doesn't support batch lookups, kwargs resolve individually instead."""
    results = []

    with DAG("test") as dag:

        @dag.task
        def push_a():
            return ["a"]

        @dag.task
        def push_b():
            return ["b"]

        @dag.task
        def pull(x, y):
            results.append((x, y))

        pull.expand(x=push_a(), y=push_b())

    calls = {"GetXComBatch": 0, "GetXCom": 0}
    values = {"push_a": ["a"], "push_b": ["b"]}

    def comms(msg):
        if isinstance(msg, GetXComBatch):
            calls["GetXComBatch"] += 1
            return ErrorResponse(error=ErrorType.XCOM_BATCH_NOT_SUPPORTED, detail={})
        if isinstance(msg, GetXCom):
            calls["GetXCom"] += 1
            return XComResult(key=BaseXCom.XCOM_RETURN_KEY, value=values[msg.task_id])
        return mock.DEFAULT

    mock_supervisor_comms.send.side_effect = comms

    assert run_ti(dag, "pull", 0) == TaskInstanceState.SUCCESS
    assert calls == {"GetXComBatch": 1, "GetXCom": 2}
    assert results == [("a", "b")]


def test_expand_batch_skipped_for_custom_xcom_backend(run_ti: RunTI, mock_supervisor_comms):
    """A custom XCom backend may bypass the batch endpoint's semantics, so batching is skipped."""
    import airflow.sdk.execution_time.xcom as xcom_module

    results = []

    with DAG("test") as dag:

        @dag.task
        def push_a():
            return ["a"]

        @dag.task
        def push_b():
            return ["b"]

        @dag.task
        def pull(x, y):
            results.append((x, y))

        pull.expand(x=push_a(), y=push_b())

    calls = {"GetXComBatch": 0, "GetXCom": 0}
    values = {"push_a": ["a"], "push_b": ["b"]}

    def comms(msg):
        if isinstance(msg, GetXComBatch):
            calls["GetXComBatch"] += 1
        if isinstance(msg, GetXCom):
            calls["GetXCom"] += 1
            return XComResult(key=BaseXCom.XCOM_RETURN_KEY, value=values[msg.task_id])
        return mock.DEFAULT

    mock_supervisor_comms.send.side_effect = comms

    class CustomXCom(BaseXCom):
        pass

    with mock.patch.object(xcom_module, "XCom", CustomXCom):
        assert run_ti(dag, "pull", 0) == TaskInstanceState.SUCCESS

    assert calls == {"GetXComBatch": 0, "GetXCom": 2}
    assert results == [("a", "b")]


class TestDictOfListsExpandInputBatching:
    """Unit-level tests for _batch_resolve_plain_xcom_args, below the full task-run scaffold."""

    @staticmethod
    def _make_plain_arg(task_id: str, *, mapped_task_group: bool = False) -> PlainXComArg:
        operator = mock.MagicMock()
        operator.is_mapped = False
        operator.task_id = task_id
        operator.dag_id = "test_dag"
        operator.get_closest_mapped_task_group.return_value = mock.MagicMock() if mapped_task_group else None
        return PlainXComArg(operator=operator, key=BaseXCom.XCOM_RETURN_KEY)

    def test_single_eligible_arg_is_not_batched(self):
        """Nothing to batch with, so the caller's normal per-item path handles it instead."""
        expand_input = DictOfListsExpandInput({"x": self._make_plain_arg("push_a")})
        ti = mock.MagicMock(dag_id="test_dag")
        ti.xcom_pull_batch.side_effect = AssertionError("should not batch a single kwarg")

        assert expand_input._batch_resolve_plain_xcom_args({"ti": ti}) == {}

    def test_mixed_eligibility_only_batches_plain_args(self):
        """A mapped-task-group XComArg alongside plain ones: only the plain ones batch."""
        expand_input = DictOfListsExpandInput(
            {
                "x": self._make_plain_arg("push_a"),
                "y": self._make_plain_arg("push_b"),
                "z": self._make_plain_arg("push_c", mapped_task_group=True),
            }
        )
        ti = mock.MagicMock(dag_id="test_dag")
        ti.xcom_pull_batch.return_value = XComBatchResult(
            items=[
                XComBatchResultItem(
                    task_id="push_a", key=BaseXCom.XCOM_RETURN_KEY, map_index=-1, found=True, value="a"
                ),
                XComBatchResultItem(
                    task_id="push_b", key=BaseXCom.XCOM_RETURN_KEY, map_index=-1, found=True, value="b"
                ),
            ]
        )

        results = expand_input._batch_resolve_plain_xcom_args({"ti": ti})

        assert results == {"x": "a", "y": "b"}
        ti.xcom_pull_batch.assert_called_once()
        assert len(ti.xcom_pull_batch.call_args.args[0]) == 2
