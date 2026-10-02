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

from unittest import mock
from unittest.mock import AsyncMock, patch

import pytest

from airflow.sdk.bases.xcom import BaseXCom, XComIterable
from airflow.sdk.execution_time.comms import (
    DeleteXCom,
    GetXCom,
    GetXComByKeys,
    GetXComSequenceSlice,
    XComResult,
    XComSequenceIndexResult,
    XComSequenceSliceResult,
)
from airflow.sdk.execution_time.xcom import XCom
from airflow.sdk.types import TaskInstanceKey


class TestBaseXCom:
    @pytest.mark.parametrize(
        "map_index",
        [
            pytest.param(None, id="map_index_none"),
            pytest.param(-1, id="map_index_negative_one"),
            pytest.param(0, id="map_index_zero"),
            pytest.param(5, id="map_index_positive"),
        ],
    )
    def test_delete_includes_map_index_in_delete_xcom_message(self, map_index, mock_supervisor_comms):
        """Test that BaseXCom.delete properly passes map_index to the DeleteXCom message."""
        with mock.patch.object(
            BaseXCom, "_get_xcom_db_ref", return_value=XComResult(key="test_key", value="test_value")
        ) as mock_get_ref:
            with mock.patch.object(BaseXCom, "purge") as mock_purge:
                BaseXCom.delete(
                    key="test_key",
                    task_id="test_task",
                    dag_id="test_dag",
                    run_id="test_run",
                    map_index=map_index,
                )

            mock_get_ref.assert_called_once_with(
                key="test_key",
                dag_id="test_dag",
                task_id="test_task",
                run_id="test_run",
                map_index=map_index,
            )

            # Verify purge was called
            mock_purge.assert_called_once()

            # Verify DeleteXCom message was sent with map_index
            mock_supervisor_comms.send.assert_called_once()
            sent_message = mock_supervisor_comms.send.call_args[0][0]

            assert isinstance(sent_message, DeleteXCom)
            assert sent_message.key == "test_key"
            assert sent_message.dag_id == "test_dag"
            assert sent_message.task_id == "test_task"
            assert sent_message.run_id == "test_run"
            assert sent_message.map_index == map_index

    @pytest.mark.asyncio
    async def test_aget_one_returns_value(self, mock_supervisor_comms):
        """aget_one awaits asend and returns the deserialized value."""
        mock_supervisor_comms.asend = mock.AsyncMock(
            return_value=XComResult(key="test_key", value="test_value")
        )

        result = await BaseXCom.aget_one(
            key="test_key",
            dag_id="test_dag",
            task_id="test_task",
            run_id="test_run",
            map_index=0,
        )

        assert result == "test_value"
        mock_supervisor_comms.asend.assert_called_once_with(
            GetXCom(
                key="test_key",
                dag_id="test_dag",
                task_id="test_task",
                run_id="test_run",
                map_index=0,
                include_prior_dates=False,
            )
        )
        mock_supervisor_comms.send.assert_not_called()

    @pytest.mark.asyncio
    async def test_aget_one_returns_none_when_not_found(self, mock_supervisor_comms):
        """aget_one returns None when XCom value is not found."""
        mock_supervisor_comms.asend = mock.AsyncMock(return_value=XComResult(key="test_key", value=None))

        result = await BaseXCom.aget_one(
            key="test_key",
            dag_id="test_dag",
            task_id="test_task",
            run_id="test_run",
        )

        assert result is None

    @pytest.mark.asyncio
    async def test_aget_one_with_include_prior_dates(self, mock_supervisor_comms):
        """aget_one passes include_prior_dates parameter correctly."""
        mock_supervisor_comms.asend = mock.AsyncMock(
            return_value=XComResult(key="test_key", value="prior_value")
        )

        result = await BaseXCom.aget_one(
            key="test_key",
            dag_id="test_dag",
            task_id="test_task",
            run_id="test_run",
            include_prior_dates=True,
        )

        assert result == "prior_value"
        mock_supervisor_comms.asend.assert_called_once_with(
            GetXCom(
                key="test_key",
                dag_id="test_dag",
                task_id="test_task",
                run_id="test_run",
                map_index=None,
                include_prior_dates=True,
            )
        )

    @pytest.mark.asyncio
    async def test_aget_one_raises_on_invalid_response(self, mock_supervisor_comms):
        """aget_one raises TypeError when receiving unexpected response type."""
        mock_supervisor_comms.asend = mock.AsyncMock(return_value="invalid_response")

        with pytest.raises(TypeError, match="Expected XComResult"):
            await BaseXCom.aget_one(
                key="test_key",
                dag_id="test_dag",
                task_id="test_task",
                run_id="test_run",
            )

    @pytest.mark.asyncio
    async def test_aget_all_returns_values(self, mock_supervisor_comms):
        """aget_all awaits asend and returns deserialized values from all map indexes."""
        mock_supervisor_comms.asend = mock.AsyncMock(
            return_value=XComSequenceSliceResult(root=["value1", "value2", "value3"])
        )

        result = await BaseXCom.aget_all(
            key="test_key",
            dag_id="test_dag",
            task_id="test_task",
            run_id="test_run",
        )

        assert result == ["value1", "value2", "value3"]
        mock_supervisor_comms.asend.assert_called_once_with(
            msg=GetXComSequenceSlice(
                key="test_key",
                dag_id="test_dag",
                task_id="test_task",
                run_id="test_run",
                start=None,
                stop=None,
                step=None,
                include_prior_dates=False,
            )
        )
        mock_supervisor_comms.send.assert_not_called()

    @pytest.mark.asyncio
    async def test_aget_all_returns_none_when_empty(self, mock_supervisor_comms):
        """aget_all returns None when no XCom values are found."""
        mock_supervisor_comms.asend = mock.AsyncMock(return_value=XComSequenceSliceResult(root=[]))

        result = await BaseXCom.aget_all(
            key="test_key",
            dag_id="test_dag",
            task_id="test_task",
            run_id="test_run",
        )

        assert result is None

    @pytest.mark.asyncio
    async def test_aget_all_with_include_prior_dates(self, mock_supervisor_comms):
        """aget_all passes include_prior_dates parameter correctly."""
        mock_supervisor_comms.asend = mock.AsyncMock(
            return_value=XComSequenceSliceResult(root=["prior_value"])
        )

        result = await BaseXCom.aget_all(
            key="test_key",
            dag_id="test_dag",
            task_id="test_task",
            run_id="test_run",
            include_prior_dates=True,
        )

        assert result == ["prior_value"]
        mock_supervisor_comms.asend.assert_called_once_with(
            msg=GetXComSequenceSlice(
                key="test_key",
                dag_id="test_dag",
                task_id="test_task",
                run_id="test_run",
                start=None,
                stop=None,
                step=None,
                include_prior_dates=True,
            )
        )

    @pytest.mark.asyncio
    async def test_aget_all_raises_on_invalid_response(self, mock_supervisor_comms):
        """aget_all raises TypeError when receiving unexpected response type."""
        mock_supervisor_comms.asend = mock.AsyncMock(return_value="invalid_response")

        with pytest.raises(TypeError, match="Expected XComSequenceSliceResult"):
            await BaseXCom.aget_all(
                key="test_key",
                dag_id="test_dag",
                task_id="test_task",
                run_id="test_run",
            )

    @pytest.mark.asyncio
    async def test_aget_value_calls_aget_one(self, mock_supervisor_comms):
        """aget_value delegates to aget_one with ti_key fields."""
        mock_supervisor_comms.asend = mock.AsyncMock(
            return_value=XComResult(key="test_key", value="test_value")
        )

        ti_key = TaskInstanceKey(
            dag_id="test_dag",
            task_id="test_task",
            run_id="test_run",
            map_index=2,
        )

        result = await BaseXCom.aget_value(ti_key=ti_key, key="test_key")

        assert result == "test_value"
        mock_supervisor_comms.asend.assert_called_once_with(
            GetXCom(
                key="test_key",
                dag_id="test_dag",
                task_id="test_task",
                run_id="test_run",
                map_index=2,
                include_prior_dates=False,
            )
        )


class TestXComIterable:
    def make_iterable(self, length: int = 0, map_index: int | None = None) -> XComIterable:
        return XComIterable(task_id="task", dag_id="dag", run_id="run", map_index=map_index, length=length)

    def test_iter_fetches_every_value_with_one_request(self, mock_supervisor_comms):
        iterable = self.make_iterable(length=3, map_index=5)
        mock_supervisor_comms.send.return_value = XComSequenceSliceResult(root=["a", "b", "c"])

        assert list(iterable) == ["a", "b", "c"]

        mock_supervisor_comms.send.assert_called_once_with(
            GetXComByKeys(
                keys=[f"{BaseXCom.XCOM_RETURN_KEY}_{i}" for i in range(3)],
                dag_id="dag",
                run_id="run",
                task_id="task",
                map_index=5,
            )
        )

    def test_iter_on_an_unmapped_producer_asks_for_map_index_minus_one(self, mock_supervisor_comms):
        mock_supervisor_comms.send.return_value = XComSequenceSliceResult(root=["a"])
        assert list(self.make_iterable(length=1)) == ["a"]
        assert mock_supervisor_comms.send.call_args.args[0].map_index == -1

    def test_iter_on_an_empty_iterable_sends_nothing(self, mock_supervisor_comms):
        assert list(self.make_iterable(length=0)) == []
        mock_supervisor_comms.send.assert_not_called()

    def test_iter_makes_one_request_however_far_it_is_consumed(self, mock_supervisor_comms):
        mock_supervisor_comms.send.return_value = XComSequenceSliceResult(root=["a", "b", "c"])
        it = iter(self.make_iterable(length=3))
        assert next(it) == "a"
        mock_supervisor_comms.send.assert_called_once()

    def test_iter_rejects_an_unexpected_response(self, mock_supervisor_comms):
        mock_supervisor_comms.send.return_value = XComSequenceIndexResult(root="oops")
        with pytest.raises(TypeError, match="Expected XComSequenceSliceResult"):
            list(self.make_iterable(length=2))

    def test_getitem_slice_fetches_the_wanted_values_with_one_request(self, mock_supervisor_comms):
        iterable = self.make_iterable(length=3)
        mock_supervisor_comms.send.return_value = XComSequenceSliceResult(root=["a", "c"])

        assert iterable[::2] == ["a", "c"]

        assert mock_supervisor_comms.send.call_args.args[0].keys == [
            f"{BaseXCom.XCOM_RETURN_KEY}_0",
            f"{BaseXCom.XCOM_RETURN_KEY}_2",
        ]

    def test_getitem_empty_slice_sends_nothing(self, mock_supervisor_comms):
        assert self.make_iterable(length=3)[5:2] == []
        mock_supervisor_comms.send.assert_not_called()

    @patch.object(XCom, "get_one", return_value="val")
    def test_getitem_single_index_is_one_read(self, mock_get_one, mock_supervisor_comms):
        assert self.make_iterable(length=3)[1] == "val"
        mock_get_one.assert_called_once_with(
            key=f"{BaseXCom.XCOM_RETURN_KEY}_1", dag_id="dag", task_id="task", run_id="run", map_index=None
        )
        mock_supervisor_comms.send.assert_not_called()

    def test_has_no_append(self):
        """The consumer-facing Sequence is read-only: nothing on it mutates the underlying XComs."""
        iterable = self.make_iterable(length=1)
        assert not hasattr(iterable, "append")
        assert not hasattr(iterable, "aappend")

    def test_serialize_returns_expected_dict(self):
        iterable = self.make_iterable(length=3, map_index=1)
        assert iterable.serialize() == {
            "task_id": "task",
            "dag_id": "dag",
            "run_id": "run",
            "map_index": 1,
            "length": 3,
            "skipped": [],
        }

    def test_deserialize_restores_fields(self):
        data = {"task_id": "task", "dag_id": "dag", "run_id": "run", "map_index": 2, "length": 5}
        iterable = XComIterable.deserialize(data, version=1)
        assert iterable.task_id == "task"
        assert iterable.dag_id == "dag"
        assert iterable.run_id == "run"
        assert iterable.map_index == 2
        assert iterable.length == 5
        assert iterable.skipped == []
        assert len(iterable) == 5

    def test_skipped_indices_round_trip(self):
        iterable = XComIterable(task_id="task", dag_id="dag", run_id="run", length=4, skipped=[3, 1])
        restored = XComIterable.deserialize(iterable.serialize(), version=1)
        assert restored.skipped == [1, 3]
        assert len(restored) == 2

    @pytest.mark.asyncio
    @patch.object(XCom, "aget_one", new_callable=AsyncMock, return_value="value-1")
    async def test_aget_calls_xcom_aget_one_with_indexed_key(self, mock_aget_one):
        iterable = self.make_iterable(length=2, map_index=3)
        assert await iterable.aget(1) == "value-1"
        mock_aget_one.assert_awaited_once_with(
            key=f"{BaseXCom.XCOM_RETURN_KEY}_1",
            dag_id="dag",
            task_id="task",
            run_id="run",
            map_index=3,
        )

    @pytest.mark.asyncio
    @pytest.mark.parametrize("index", [-3, 2])
    @patch.object(XCom, "aget_one", new_callable=AsyncMock)
    async def test_aget_out_of_range_raises_index_error_without_fetching(self, mock_aget_one, index):
        iterable = self.make_iterable(length=2)
        with pytest.raises(IndexError):
            await iterable.aget(index)
        mock_aget_one.assert_not_awaited()

    @pytest.mark.asyncio
    @patch.object(XCom, "aget_one", new_callable=AsyncMock, return_value="last")
    async def test_aget_negative_index_counts_from_the_end(self, mock_aget_one):
        iterable = self.make_iterable(length=3)
        assert await iterable.aget(-1) == "last"
        assert mock_aget_one.await_args.kwargs["key"] == f"{BaseXCom.XCOM_RETURN_KEY}_2"

    @pytest.mark.asyncio
    @patch.object(XCom, "get_one")
    @patch.object(XCom, "aget_one", new_callable=AsyncMock)
    async def test_async_iteration_reads_every_item_in_order_through_aget_one(
        self, mock_aget_one, mock_get_one
    ):
        """``async for`` never touches the synchronous ``get_one``, so it is safe on the task's event loop."""
        mock_aget_one.side_effect = self._pages_by_key(["a", "b", "c"])
        iterable = self.make_iterable(length=3)
        assert [item async for item in iterable] == ["a", "b", "c"]
        assert [call.kwargs["key"] for call in mock_aget_one.await_args_list] == [
            f"{BaseXCom.XCOM_RETURN_KEY}_{index}" for index in range(3)
        ]
        mock_get_one.assert_not_called()

    @pytest.mark.asyncio
    @patch.object(XCom, "aget_one", new_callable=AsyncMock)
    async def test_async_iteration_on_empty_iterable_yields_nothing(self, mock_aget_one):
        iterable = self.make_iterable(length=0)
        assert [item async for item in iterable] == []
        mock_aget_one.assert_not_awaited()

    @staticmethod
    def _pages_by_key(pages: list) -> object:
        """
        Build a get_one side_effect that maps each page's index-suffixed key to its page.

        Unlike a plain list side_effect (consumed once and then exhausted), this can be called any
        number of times for the same key, mirroring how a real XCom backend is queried by key and
        does not get "used up".
        """

        def _get_one(*args, **kwargs):
            index = int(kwargs["key"].rsplit("_", 1)[-1])
            page = pages[index]
            return page() if callable(page) else page

        return _get_one

    @patch.object(XCom, "get_one")
    def test_negative_indices_count_from_the_end(self, mock_get_one):
        """The Sequence contract holds: ``[-1]`` is the last element."""
        mock_get_one.side_effect = self._pages_by_key([["a", "b"], ["c"]])
        iterable = self.make_iterable(length=2)

        assert iterable[-1] == ["c"]
        assert iterable[-2] == ["a", "b"]

    @patch.object(XCom, "get_one")
    def test_negative_indices_out_of_range_raise(self, mock_get_one):
        mock_get_one.side_effect = self._pages_by_key([["a", "b"], ["c"]])
        iterable = self.make_iterable(length=2)

        with pytest.raises(IndexError):
            iterable[-3]

    @staticmethod
    def _values_by_index(values: dict[int, str]) -> object:
        """A get_one/aget_one side_effect that returns the value pushed under each index-suffixed key."""

        def _get_one(*args, **kwargs):
            return values[int(kwargs["key"].rsplit("_", 1)[-1])]

        return _get_one

    def make_skipping_iterable(self) -> XComIterable:
        """Five input items, of which indices 0, 2 and 3 were skipped: only 1 and 4 hold a value."""
        return XComIterable(task_id="task", dag_id="dag", run_id="run", length=5, skipped=[0, 2, 3])

    @patch.object(XCom, "get_by_keys")
    @patch.object(XCom, "get_one")
    def test_skipped_indices_are_left_out(self, mock_get_one, mock_get_by_keys):
        values = {1: "one", 4: "four"}
        mock_get_one.side_effect = self._values_by_index(values)
        mock_get_by_keys.side_effect = lambda *, keys, **kwargs: [
            values[int(key.rsplit("_", 1)[-1])] for key in keys
        ]
        iterable = self.make_skipping_iterable()

        assert len(iterable) == 2
        assert list(iterable) == ["one", "four"]
        assert iterable[0] == "one"
        assert iterable[1] == "four"
        assert iterable[-1] == "four"
        assert iterable[-2] == "one"
        assert iterable[::-1] == ["four", "one"]
        with pytest.raises(IndexError):
            iterable[2]
        with pytest.raises(IndexError):
            iterable[-3]
        requested = {call.kwargs["key"] for call in mock_get_one.call_args_list} | {
            key for call in mock_get_by_keys.call_args_list for key in call.kwargs["keys"]
        }
        assert sorted(requested) == [f"{BaseXCom.XCOM_RETURN_KEY}_1", f"{BaseXCom.XCOM_RETURN_KEY}_4"]

    @pytest.mark.asyncio
    @patch.object(XCom, "aget_one", new_callable=AsyncMock)
    async def test_skipped_indices_are_left_out_when_read_asynchronously(self, mock_aget_one):
        mock_aget_one.side_effect = self._values_by_index({1: "one", 4: "four"})
        iterable = self.make_skipping_iterable()

        assert await iterable.alen() == 2
        assert [item async for item in iterable] == ["one", "four"]
        assert await iterable.aget(-1) == "four"

    def test_every_index_skipped_is_empty(self):
        iterable = XComIterable(task_id="task", dag_id="dag", run_id="run", length=2, skipped=[0, 1])
        assert len(iterable) == 0
        assert list(iterable) == []
