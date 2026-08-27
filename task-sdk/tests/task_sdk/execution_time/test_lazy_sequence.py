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

from unittest.mock import AsyncMock, Mock, call, patch

import pytest

import airflow
from airflow.sdk.bases.xcom import BaseXCom
from airflow.sdk.exceptions import ErrorType
from airflow.sdk.execution_time.comms import (
    ErrorResponse,
    GetXComCount,
    GetXComSequenceItem,
    GetXComSequenceSlice,
    XComCountResponse,
    XComSequenceIndexResult,
    XComSequenceSliceResult,
)
from airflow.sdk.execution_time.lazy_sequence import LazyXComSequence, XComIterable
from airflow.sdk.execution_time.xcom import XCom, resolve_xcom_backend

from tests_common.test_utils.config import conf_vars


@pytest.fixture
def mock_operator():
    return Mock(spec=["dag_id", "task_id"], dag_id="dag", task_id="task")


@pytest.fixture
def mock_xcom_arg(mock_operator):
    return Mock(spec=["operator", "key"], operator=mock_operator, key=BaseXCom.XCOM_RETURN_KEY)


@pytest.fixture
def mock_ti():
    return Mock(spec=["run_id"], run_id="run")


@pytest.fixture
def lazy_sequence(mock_xcom_arg, mock_ti):
    return LazyXComSequence(mock_xcom_arg, mock_ti)


class CustomXCom(BaseXCom):
    @classmethod
    def deserialize_value(cls, xcom):
        return f"Made with CustomXCom: {xcom.value}"


def test_len(mock_supervisor_comms, lazy_sequence):
    mock_supervisor_comms.send.return_value = XComCountResponse(len=3)
    assert len(lazy_sequence) == 3
    mock_supervisor_comms.send.assert_called_once_with(
        msg=GetXComCount(key=BaseXCom.XCOM_RETURN_KEY, dag_id="dag", task_id="task", run_id="run"),
    )


def test_iter(mock_supervisor_comms, lazy_sequence):
    it = iter(lazy_sequence)

    mock_supervisor_comms.send.side_effect = [
        XComSequenceIndexResult(root="f"),
        ErrorResponse(error=ErrorType.XCOM_NOT_FOUND, detail={"oops": "sorry!"}),
    ]
    assert list(it) == ["f"]
    mock_supervisor_comms.send.assert_has_calls(
        [
            call(
                msg=GetXComSequenceItem(
                    key=BaseXCom.XCOM_RETURN_KEY,
                    dag_id="dag",
                    task_id="task",
                    run_id="run",
                    offset=0,
                ),
            ),
            call(
                msg=GetXComSequenceItem(
                    key=BaseXCom.XCOM_RETURN_KEY,
                    dag_id="dag",
                    task_id="task",
                    run_id="run",
                    offset=1,
                ),
            ),
        ]
    )


def test_getitem_index(mock_supervisor_comms, lazy_sequence):
    mock_supervisor_comms.send.return_value = XComSequenceIndexResult(root="f")
    assert lazy_sequence[4] == "f"
    mock_supervisor_comms.send.assert_called_once_with(
        GetXComSequenceItem(
            key=BaseXCom.XCOM_RETURN_KEY,
            dag_id="dag",
            task_id="task",
            run_id="run",
            offset=4,
        ),
    )


@conf_vars({("core", "xcom_backend"): "task_sdk.execution_time.test_lazy_sequence.CustomXCom"})
def test_getitem_calls_correct_deserialise(monkeypatch, mock_supervisor_comms, lazy_sequence):
    mock_supervisor_comms.send.return_value = XComSequenceIndexResult(root="some-value")

    xcom = resolve_xcom_backend()
    assert xcom.__name__ == "CustomXCom"
    monkeypatch.setattr(airflow.sdk.execution_time.xcom, "XCom", xcom)

    assert lazy_sequence[4] == "Made with CustomXCom: some-value"
    mock_supervisor_comms.send.assert_called_once_with(
        GetXComSequenceItem(
            key=BaseXCom.XCOM_RETURN_KEY,
            dag_id="dag",
            task_id="task",
            run_id="run",
            offset=4,
        ),
    )


def test_getitem_indexerror(mock_supervisor_comms, lazy_sequence):
    mock_supervisor_comms.send.return_value = ErrorResponse(
        error=ErrorType.XCOM_NOT_FOUND,
        detail={"oops": "sorry!"},
    )
    with pytest.raises(IndexError) as ctx:
        lazy_sequence[4]
    assert ctx.value.args == (4,)
    mock_supervisor_comms.send.assert_called_once_with(
        GetXComSequenceItem(
            key=BaseXCom.XCOM_RETURN_KEY,
            dag_id="dag",
            task_id="task",
            run_id="run",
            offset=4,
        ),
    )


def test_getitem_slice(mock_supervisor_comms, lazy_sequence):
    mock_supervisor_comms.send.return_value = XComSequenceSliceResult(root=[6, 4, 1])
    assert lazy_sequence[:5] == [6, 4, 1]
    mock_supervisor_comms.send.assert_called_once_with(
        GetXComSequenceSlice(
            key=BaseXCom.XCOM_RETURN_KEY,
            dag_id="dag",
            task_id="task",
            run_id="run",
            start=None,
            stop=5,
            step=None,
        ),
    )


class TestXComIterable:
    def make_iterable(self, length: int = 0, map_index: int | None = None) -> XComIterable:
        return XComIterable(task_id="task", dag_id="dag", run_id="run", map_index=map_index, length=length)

    # ------------------------------------------------------------------
    # append
    # ------------------------------------------------------------------

    @patch.object(XCom, "set")
    def test_append_calls_xcom_set_with_correct_key(self, mock_set):
        iterable = self.make_iterable()
        iterable.append("value1")
        mock_set.assert_called_once_with(
            key=f"{BaseXCom.XCOM_RETURN_KEY}_0",
            value="value1",
            dag_id="dag",
            task_id="task",
            run_id="run",
            map_index=None,
        )

    @patch.object(XCom, "set")
    def test_append_uses_sequential_keys(self, mock_set):
        iterable = self.make_iterable()
        iterable.append("a")
        iterable.append("b")
        iterable.append("c")
        keys = [c.kwargs["key"] for c in mock_set.call_args_list]
        assert keys == [
            f"{BaseXCom.XCOM_RETURN_KEY}_0",
            f"{BaseXCom.XCOM_RETURN_KEY}_1",
            f"{BaseXCom.XCOM_RETURN_KEY}_2",
        ]

    @patch.object(XCom, "set")
    def test_append_increments_index_and_length(self, mock_set):
        iterable = self.make_iterable()
        iterable.append("a")
        iterable.append("b")
        assert iterable.index == 2
        assert iterable.length == 2

    @patch.object(XCom, "set", side_effect=RuntimeError("oops"))
    def test_append_increments_index_even_on_error(self, mock_set):
        """The finally block must advance index/length even when XCom.set raises."""
        iterable = self.make_iterable()
        with pytest.raises(RuntimeError, match="oops"):
            iterable.append("value")
        assert iterable.index == 1
        assert iterable.length == 1

    # ------------------------------------------------------------------
    # aappend
    # ------------------------------------------------------------------

    @pytest.mark.asyncio
    @patch.object(XCom, "aset", new_callable=AsyncMock)
    async def test_aappend_calls_xcom_aset_with_correct_key(self, mock_aset):
        iterable = self.make_iterable()
        await iterable.aappend("value1")
        mock_aset.assert_called_once_with(
            key=f"{BaseXCom.XCOM_RETURN_KEY}_0",
            value="value1",
            dag_id="dag",
            task_id="task",
            run_id="run",
            map_index=None,
        )

    @pytest.mark.asyncio
    @patch.object(XCom, "aset", new_callable=AsyncMock)
    async def test_aappend_uses_sequential_keys(self, mock_aset):
        iterable = self.make_iterable()
        await iterable.aappend("a")
        await iterable.aappend("b")
        keys = [c.kwargs["key"] for c in mock_aset.call_args_list]
        assert keys == [
            f"{BaseXCom.XCOM_RETURN_KEY}_0",
            f"{BaseXCom.XCOM_RETURN_KEY}_1",
        ]

    @pytest.mark.asyncio
    @patch.object(XCom, "aset", new_callable=AsyncMock, side_effect=RuntimeError("oops"))
    async def test_aappend_increments_index_even_on_error(self, mock_aset):
        """The finally block must advance index/length even when XCom.aset raises."""
        iterable = self.make_iterable()
        with pytest.raises(RuntimeError, match="oops"):
            await iterable.aappend("value")
        assert iterable.index == 1
        assert iterable.length == 1

    # ------------------------------------------------------------------
    # serialize / deserialize / index reset
    # ------------------------------------------------------------------

    def test_serialize_returns_expected_dict(self):
        iterable = self.make_iterable(length=3, map_index=1)
        assert iterable.serialize() == {
            "task_id": "task",
            "dag_id": "dag",
            "run_id": "run",
            "map_index": 1,
            "length": 3,
        }

    def test_deserialize_restores_fields(self):
        data = {"task_id": "task", "dag_id": "dag", "run_id": "run", "map_index": 2, "length": 5}
        iterable = XComIterable.deserialize(data, version=1)
        assert iterable.task_id == "task"
        assert iterable.dag_id == "dag"
        assert iterable.run_id == "run"
        assert iterable.map_index == 2
        assert iterable.length == 5

    def test_index_starts_at_length_after_construction(self):
        """Creating an XComIterable with length>0 must start index at length, not 0."""
        iterable = self.make_iterable(length=3)
        assert iterable.index == 3

    @patch.object(XCom, "set")
    def test_deserialize_then_append_continues_from_correct_index(self, mock_set):
        """After round-tripping through serialize/deserialize, append must not overwrite existing entries."""
        data = {"task_id": "task", "dag_id": "dag", "run_id": "run", "map_index": None, "length": 3}
        iterable = XComIterable.deserialize(data, version=1)
        iterable.append("new_value")
        assert mock_set.call_args.kwargs["key"] == f"{BaseXCom.XCOM_RETURN_KEY}_3"

    # ------------------------------------------------------------------
    # flatten
    # ------------------------------------------------------------------

    @patch.object(XCom, "get_one")
    def test_flatten_expands_list_items(self, mock_get_one):
        """Items that are lists are expanded into individual elements."""
        mock_get_one.side_effect = [["a", "b"], ["c", "d"]]
        iterable = self.make_iterable(length=2)
        assert list(iterable.flatten()) == ["a", "b", "c", "d"]

    @patch.object(XCom, "get_one")
    def test_flatten_expands_tuple_items(self, mock_get_one):
        """Items that are tuples are expanded into individual elements."""
        mock_get_one.side_effect = [("a", "b"), ("c",)]
        iterable = self.make_iterable(length=2)
        assert list(iterable.flatten()) == ["a", "b", "c"]

    @patch.object(XCom, "get_one")
    def test_flatten_expands_set_items(self, mock_get_one):
        """Items that are sets are expanded into individual elements."""
        mock_get_one.side_effect = [{42}]
        iterable = self.make_iterable(length=1)
        assert list(iterable.flatten()) == [42]

    @patch.object(XCom, "get_one")
    def test_flatten_expands_generator_items(self, mock_get_one):
        """Items that are generators are expanded into individual elements."""
        mock_get_one.side_effect = [iter([1, 2, 3])]
        iterable = self.make_iterable(length=1)
        assert list(iterable.flatten()) == [1, 2, 3]

    @patch.object(XCom, "get_one")
    def test_flatten_passes_through_string_items(self, mock_get_one):
        """Strings are not iterated — they are yielded as a single item."""
        mock_get_one.side_effect = ["hello", "world"]
        iterable = self.make_iterable(length=2)
        assert list(iterable.flatten()) == ["hello", "world"]

    @patch.object(XCom, "get_one")
    def test_flatten_passes_through_bytes_items(self, mock_get_one):
        """Bytes are not iterated — they are yielded as a single item."""
        mock_get_one.side_effect = [b"hello"]
        iterable = self.make_iterable(length=1)
        assert list(iterable.flatten()) == [b"hello"]

    @patch.object(XCom, "get_one")
    def test_flatten_passes_through_non_iterable_items(self, mock_get_one):
        """Scalar (non-iterable) items are yielded unchanged."""
        mock_get_one.side_effect = [1, 2.0, True]
        iterable = self.make_iterable(length=3)
        assert list(iterable.flatten()) == [1, 2.0, True]

    @patch.object(XCom, "get_one")
    def test_flatten_handles_mixed_items(self, mock_get_one):
        """Mixed collection and scalar items are each handled correctly."""
        mock_get_one.side_effect = [["a", "b"], "c", ("d",), 5]
        iterable = self.make_iterable(length=4)
        assert list(iterable.flatten()) == ["a", "b", "c", "d", 5]

    def test_flatten_on_empty_iterable_yields_nothing(self):
        iterable = self.make_iterable(length=0)
        assert list(iterable.flatten()) == []
