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

import asyncio
import threading
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


def _answering(values: list, *, failures: list[Exception] | None = None) -> AsyncMock:
    """
    An ``asend`` that answers slice requests from ``values``.

    It answers after one loop turn, so the reads of the other indexed tasks start while it is in
    flight, as under ``imap_unordered``; ``failures`` are raised first, one per request.
    """
    pending = list(failures or [])

    async def asend(msg):
        await asyncio.sleep(0)
        if pending:
            raise pending.pop()
        return XComSequenceSliceResult(root=values[msg.start : msg.stop])

    return AsyncMock(side_effect=asend)


def _requested_starts(mock_supervisor_comms) -> list[int]:
    return [call.args[0].start for call in mock_supervisor_comms.asend.await_args_list]


@pytest.mark.asyncio
async def test_aget(mock_supervisor_comms, lazy_sequence):
    """An index is read with the chunk that holds it, chunks starting at multiples of the chunk size."""
    mock_supervisor_comms.asend = AsyncMock(return_value=XComSequenceSliceResult(root=["e", "f"]))

    assert await lazy_sequence.aget(1) == "f"

    mock_supervisor_comms.asend.assert_awaited_once_with(_slice(0))
    mock_supervisor_comms.send.assert_not_called()


@pytest.mark.asyncio
async def test_aget_from_indexed_tasks_at_the_same_time_answers_each_with_its_own_index(
    mock_supervisor_comms, mock_xcom_arg, mock_ti
):
    """
    Three reads in flight, two of them in the chunk after the first one's.

    Each read in flight used to send its own request from its own index and hold what it got, so
    the indexed tasks of an iteration sent one request per index at the start of every chunk; a
    window noted before the request and filled after it, as the 3.3 plugin had, even answered a
    read with another index's item. Chunks shared by their start are read once and answer in place.
    """
    sequence = LazyXComSequence(mock_xcom_arg, mock_ti, chunk_size=4)
    mock_supervisor_comms.asend = _answering(list(range(10)))

    assert await asyncio.gather(sequence.aget(1), sequence.aget(5), sequence.aget(6)) == [1, 5, 6]

    assert _requested_starts(mock_supervisor_comms) == [0, 4]


@pytest.mark.asyncio
async def test_aget_from_the_indexed_tasks_of_one_chunk_sends_its_request_once(
    mock_supervisor_comms, mock_xcom_arg, mock_ti
):
    sequence = LazyXComSequence(mock_xcom_arg, mock_ti, chunk_size=4)
    mock_supervisor_comms.asend = _answering(list(range(10)))

    assert await asyncio.gather(*(sequence.aget(index) for index in range(4))) == [0, 1, 2, 3]

    assert _requested_starts(mock_supervisor_comms) == [0]


@pytest.mark.asyncio
async def test_aget_lets_the_chunk_reads_go_as_the_indexed_tasks_move_on(
    mock_supervisor_comms, mock_xcom_arg, mock_ti
):
    """The chunk the indexed tasks use now and the next one are held; an older one is read again."""
    sequence = LazyXComSequence(mock_xcom_arg, mock_ti, chunk_size=4)
    mock_supervisor_comms.asend = _answering(list(range(12)))

    assert [await sequence.aget(index) for index in (0, 4, 5, 8, 0)] == [0, 4, 5, 8, 0]

    assert _requested_starts(mock_supervisor_comms) == [0, 4, 8, 0]


@pytest.mark.asyncio
async def test_aget_reads_a_chunk_again_after_its_read_failed(mock_supervisor_comms, mock_xcom_arg, mock_ti):
    sequence = LazyXComSequence(mock_xcom_arg, mock_ti, chunk_size=4)
    mock_supervisor_comms.asend = _answering([0, 1], failures=[RuntimeError("supervisor went away")])

    with pytest.raises(RuntimeError):
        await sequence.aget(0)
    assert await sequence.aget(0) == 0

    assert _requested_starts(mock_supervisor_comms) == [0, 0]


@pytest.mark.asyncio
async def test_aget_cancelled_does_not_cancel_the_read_others_share(mock_supervisor_comms, mock_xcom_arg, mock_ti):
    sequence = LazyXComSequence(mock_xcom_arg, mock_ti, chunk_size=4)
    mock_supervisor_comms.asend = _answering([0, 1])

    first = asyncio.ensure_future(sequence.aget(0))
    second = asyncio.ensure_future(sequence.aget(1))
    await asyncio.sleep(0)
    first.cancel()

    assert await second == 1
    assert _requested_starts(mock_supervisor_comms) == [0]


@pytest.mark.asyncio
async def test_aget_past_a_short_chunk_raises_index_error_without_a_request(
    mock_supervisor_comms, mock_xcom_arg, mock_ti
):
    """A chunk shorter than the chunk size is the last one: the sequence ends there."""
    sequence = LazyXComSequence(mock_xcom_arg, mock_ti, chunk_size=4)
    mock_supervisor_comms.asend = _answering([0, 1])

    assert await sequence.aget(1) == 1
    with pytest.raises(IndexError):
        await sequence.aget(2)

    assert _requested_starts(mock_supervisor_comms) == [0]


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
        side_effect=[XComCountResponse(len=3), XComSequenceSliceResult(root=["f", "g", "h"])]
    )

    assert await lazy_sequence.aget(-1) == "h"
    with pytest.raises(IndexError):
        await lazy_sequence.aget(-4)

    assert mock_supervisor_comms.asend.await_args_list[1].args[0] == _slice(0)
    assert mock_supervisor_comms.asend.await_count == 2  # the length is cached
    mock_supervisor_comms.send.assert_not_called()


@pytest.mark.asyncio
async def test_aiter(mock_supervisor_comms, lazy_sequence):
    """``async for`` reads chunk by chunk through ``asend`` and never through the blocking ``send``."""
    mock_supervisor_comms.asend = AsyncMock(return_value=XComSequenceSliceResult(root=["f", "g"]))

    assert [item async for item in lazy_sequence] == ["f", "g"]

    # the chunk is shorter than the chunk size, so the end needs no further request
    assert _requested_starts(mock_supervisor_comms) == [0]
    mock_supervisor_comms.send.assert_not_called()


@pytest.mark.asyncio
async def test_async_reads_ask_for_the_previous_iteration(mock_supervisor_comms, mock_xcom_arg, mock_ti):
    """Inside a loop, the async reads ask for the previous iteration's values, as the sync ones do."""
    sequence = LazyXComSequence(mock_xcom_arg, mock_ti, previous_iteration=True)
    mock_supervisor_comms.asend = AsyncMock(
        side_effect=[XComCountResponse(len=2), XComSequenceSliceResult(root=["f", "g"])]
    )

    assert await sequence.alen() == 2
    assert await sequence.aget(0) == "f"

    assert [request.previous_iteration for (request,), _ in mock_supervisor_comms.asend.await_args_list] == [
        True,
        True,
    ]


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


class LoadValueXCom(BaseXCom):
    @classmethod
    def load_value(cls, stored):
        return f"loaded {stored}"


@pytest.mark.parametrize(
    ("index", "response", "expected"),
    [
        # An index is read with the chunk that starts at it.
        pytest.param(4, XComSequenceSliceResult(root=["ref-4"]), "loaded ref-4", id="index"),
        pytest.param(
            slice(0, 2),
            XComSequenceSliceResult(root=["ref-0", "ref-1"]),
            ["loaded ref-0", "loaded ref-1"],
            id="slice",
        ),
    ],
)
def test_getitem_loads_through_load_value(
    index, response, expected, monkeypatch, mock_supervisor_comms, lazy_sequence
):
    mock_supervisor_comms.send.return_value = response
    monkeypatch.setattr(airflow.sdk.execution_time.xcom, "XCom", LoadValueXCom)

    assert lazy_sequence[index] == expected


@pytest.mark.asyncio
async def test_aget_loads_through_load_value(monkeypatch, mock_supervisor_comms, lazy_sequence):
    mock_supervisor_comms.asend = AsyncMock(return_value=XComSequenceSliceResult(root=["ref-0"]))
    monkeypatch.setattr(airflow.sdk.execution_time.xcom, "XCom", LoadValueXCom)

    assert await lazy_sequence.aget(0) == "loaded ref-0"


@pytest.mark.asyncio
async def test_aget_loads_off_the_event_loop_thread(monkeypatch, mock_supervisor_comms, lazy_sequence):
    """The backend may download the value, so the load must not hold the loop the other reads share."""
    loaded_on = []

    class RecordingXCom(BaseXCom):
        @classmethod
        def load_value(cls, stored):
            loaded_on.append(threading.current_thread())
            return stored

    mock_supervisor_comms.asend = AsyncMock(return_value=XComSequenceSliceResult(root=["ref-0"]))
    monkeypatch.setattr(airflow.sdk.execution_time.xcom, "XCom", RecordingXCom)

    assert await lazy_sequence.aget(0) == "ref-0"
    assert loaded_on
    assert loaded_on[0] is not threading.current_thread()


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
