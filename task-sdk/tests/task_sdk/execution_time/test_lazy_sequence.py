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

from unittest.mock import AsyncMock, Mock, call

import pytest

import airflow
from airflow.sdk.bases.xcom import BaseXCom
from airflow.sdk.execution_time.comms import (
    GetXComCount,
    GetXComSequenceSlice,
    XComCountResponse,
    XComSequenceSliceResult,
)
from airflow.sdk.execution_time.lazy_sequence import XCOM_SEQUENCE_CHUNK_SIZE, LazyXComSequence
from airflow.sdk.execution_time.xcom import resolve_xcom_backend

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


def _slice(start: int, stop: int | None = None) -> GetXComSequenceSlice:
    """The chunk request a read at ``start`` sends: ``XCOM_SEQUENCE_CHUNK_SIZE`` items from there."""
    return GetXComSequenceSlice(
        key=BaseXCom.XCOM_RETURN_KEY,
        dag_id="dag",
        task_id="task",
        run_id="run",
        start=start,
        stop=start + XCOM_SEQUENCE_CHUNK_SIZE if stop is None else stop,
        step=None,
    )


@pytest.mark.asyncio
async def test_aget(mock_supervisor_comms, lazy_sequence):
    mock_supervisor_comms.asend = AsyncMock(return_value=XComSequenceSliceResult(root=["f", "g"]))

    assert await lazy_sequence.aget(1) == "f"

    mock_supervisor_comms.asend.assert_awaited_once_with(_slice(1))
    mock_supervisor_comms.send.assert_not_called()


@pytest.mark.asyncio
async def test_aget_out_of_range_raises_index_error(mock_supervisor_comms, lazy_sequence):
    mock_supervisor_comms.asend = AsyncMock(return_value=XComSequenceSliceResult(root=[]))

    with pytest.raises(IndexError):
        await lazy_sequence.aget(3)


@pytest.mark.asyncio
async def test_aget_serves_the_held_chunk_without_a_request(mock_supervisor_comms, lazy_sequence):
    mock_supervisor_comms.asend = AsyncMock(return_value=XComSequenceSliceResult(root=["a", "b", "c"]))

    assert [await lazy_sequence.aget(index) for index in (0, 2, 1)] == ["a", "c", "b"]

    mock_supervisor_comms.asend.assert_awaited_once_with(_slice(0))


@pytest.mark.asyncio
async def test_aget_negative_index_counts_from_the_cached_length(mock_supervisor_comms, lazy_sequence):
    mock_supervisor_comms.asend = AsyncMock(
        side_effect=[XComCountResponse(len=3), XComSequenceSliceResult(root=["h"])]
    )

    assert await lazy_sequence.aget(-1) == "h"
    with pytest.raises(IndexError):
        await lazy_sequence.aget(-4)

    assert mock_supervisor_comms.asend.await_args_list[1].args[0] == _slice(2)
    assert mock_supervisor_comms.asend.await_count == 2  # the length is cached
    mock_supervisor_comms.send.assert_not_called()


@pytest.mark.asyncio
async def test_aiter(mock_supervisor_comms, lazy_sequence):
    """``async for`` reads chunk by chunk through ``asend`` and never through the blocking ``send``."""
    mock_supervisor_comms.asend = AsyncMock(
        side_effect=[XComSequenceSliceResult(root=["f", "g"]), XComSequenceSliceResult(root=[])]
    )

    assert [item async for item in lazy_sequence] == ["f", "g"]

    assert [call.args[0].start for call in mock_supervisor_comms.asend.await_args_list] == [0, 2]
    mock_supervisor_comms.send.assert_not_called()


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
        XComSequenceSliceResult(root=["f"]),
        XComSequenceSliceResult(root=[]),
    ]
    assert list(it) == ["f"]
    mock_supervisor_comms.send.assert_has_calls([call(msg=_slice(0)), call(msg=_slice(1))])


def test_iter_reads_one_request_per_chunk(mock_supervisor_comms, lazy_sequence):
    """A full chunk means there may be more: the next read fetches from where it ended."""
    first = list(range(XCOM_SEQUENCE_CHUNK_SIZE))
    mock_supervisor_comms.send.side_effect = [
        XComSequenceSliceResult(root=first),
        XComSequenceSliceResult(root=[XCOM_SEQUENCE_CHUNK_SIZE, XCOM_SEQUENCE_CHUNK_SIZE + 1]),
        XComSequenceSliceResult(root=[]),
    ]

    # a for loop, not list(): list() asks __len__ for a size hint first, which is a count request
    assert [item for item in lazy_sequence] == list(range(XCOM_SEQUENCE_CHUNK_SIZE + 2))

    assert [c.args[0].start for c in mock_supervisor_comms.send.call_args_list] == [
        0,
        XCOM_SEQUENCE_CHUNK_SIZE,
        XCOM_SEQUENCE_CHUNK_SIZE + 2,
    ]


def test_getitem_index(mock_supervisor_comms, lazy_sequence):
    mock_supervisor_comms.send.return_value = XComSequenceSliceResult(root=["f"])
    assert lazy_sequence[4] == "f"
    mock_supervisor_comms.send.assert_called_once_with(_slice(4))


def test_getitem_jump_back_fetches_again(mock_supervisor_comms, lazy_sequence):
    mock_supervisor_comms.send.side_effect = [
        XComSequenceSliceResult(root=["e", "f"]),
        XComSequenceSliceResult(root=["a", "b", "c", "d", "e", "f"]),
    ]
    assert lazy_sequence[4] == "e"
    assert lazy_sequence[5] == "f"  # held
    assert lazy_sequence[0] == "a"  # behind the held chunk: one more request
    assert lazy_sequence[3] == "d"  # held again
    assert [c.args[0].start for c in mock_supervisor_comms.send.call_args_list] == [4, 0]


def test_getitem_negative_index_counts_from_the_cached_length(mock_supervisor_comms, lazy_sequence):
    mock_supervisor_comms.send.side_effect = [XComCountResponse(len=3), XComSequenceSliceResult(root=["h"])]
    assert lazy_sequence[-1] == "h"
    with pytest.raises(IndexError):
        lazy_sequence[-4]
    assert mock_supervisor_comms.send.call_args_list[1] == call(_slice(2))
    assert mock_supervisor_comms.send.call_count == 2


@conf_vars({("core", "xcom_backend"): "task_sdk.execution_time.test_lazy_sequence.CustomXCom"})
def test_getitem_calls_correct_deserialise(monkeypatch, mock_supervisor_comms, lazy_sequence):
    mock_supervisor_comms.send.return_value = XComSequenceSliceResult(root=["some-value"])

    xcom = resolve_xcom_backend()
    assert xcom.__name__ == "CustomXCom"
    monkeypatch.setattr(airflow.sdk.execution_time.xcom, "XCom", xcom)

    assert lazy_sequence[4] == "Made with CustomXCom: some-value"
    mock_supervisor_comms.send.assert_called_once_with(_slice(4))


def test_getitem_indexerror(mock_supervisor_comms, lazy_sequence):
    mock_supervisor_comms.send.return_value = XComSequenceSliceResult(root=[])
    with pytest.raises(IndexError) as ctx:
        lazy_sequence[4]
    assert ctx.value.args == (4,)
    mock_supervisor_comms.send.assert_called_once_with(_slice(4))


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


@conf_vars({("core", "xcom_sequence_chunk_size"): "2"})
def test_chunk_size_comes_from_config(mock_supervisor_comms, mock_xcom_arg, mock_ti):
    lazy_sequence = LazyXComSequence(mock_xcom_arg, mock_ti)
    assert lazy_sequence.chunk_size == 2

    mock_supervisor_comms.send.side_effect = [
        XComSequenceSliceResult(root=["a", "b"]),
        XComSequenceSliceResult(root=["c"]),
        XComSequenceSliceResult(root=[]),
    ]
    assert [item for item in lazy_sequence] == ["a", "b", "c"]
    assert [c.args[0].stop - c.args[0].start for c in mock_supervisor_comms.send.call_args_list] == [2, 2, 2]


def test_chunk_size_can_be_given(mock_supervisor_comms, mock_xcom_arg, mock_ti):
    lazy_sequence = LazyXComSequence(mock_xcom_arg, mock_ti, chunk_size=3)
    mock_supervisor_comms.send.return_value = XComSequenceSliceResult(root=["b", "c", "d"])  # items 1..3
    assert lazy_sequence[1] == "b"
    assert lazy_sequence[3] == "d"
    mock_supervisor_comms.send.assert_called_once_with(_slice(1, stop=4))
