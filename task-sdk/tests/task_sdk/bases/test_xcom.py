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

from airflow.sdk.bases.xcom import BaseXCom, XComIterable, flattened_count
from airflow.sdk.execution_time.comms import (
    DeleteXCom,
    GetXCom,
    GetXComSequenceSlice,
    XComResult,
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
    def make_iterable(
        self, length: int = 0, map_index: int | None = None, flattened_length: int | None = None
    ) -> XComIterable:
        return XComIterable(
            task_id="task",
            dag_id="dag",
            run_id="run",
            map_index=map_index,
            length=length,
            flattened_length=flattened_length,
        )

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
            "flattened_length": None,
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
        assert iterable.flattened_length is None  # pushed by a producer that did not tally it

    def test_flattened_length_round_trips_and_reaches_the_flattened_view(self):
        iterable = self.make_iterable(length=2, flattened_length=7)
        restored = XComIterable.deserialize(iterable.serialize(), version=1)
        assert restored.flattened_length == 7
        assert len(restored.flatten()) == 7  # no page read needed

    @pytest.mark.parametrize(
        ("value", "expected"),
        [(["a", "b"], 2), ([], 0), ("str", 1), (b"raw", 1), (7, 1), ([["x", "y"], ("z",)], 3), ({"k": 1}, 1)],
    )
    def test_flattened_count_matches_what_flatten_yields(self, value, expected):
        assert flattened_count(value) == expected

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

    @pytest.mark.asyncio
    @patch.object(XCom, "get_one")
    @patch.object(XCom, "aget_one", new_callable=AsyncMock)
    async def test_flatten_async_iteration_expands_pages_through_aget_one(self, mock_aget_one, mock_get_one):
        mock_aget_one.side_effect = self._pages_by_key([["a", "b"], "c", ("d",), [["e"]]])
        flattened = self.make_iterable(length=4).flatten()
        assert [item async for item in flattened] == ["a", "b", "c", "d", "e"]
        assert len(flattened) == 5  # counted on the loop through aget_one, then cached
        assert mock_aget_one.await_count == 8  # one counting pass and one reading pass over 4 pages
        mock_get_one.assert_not_called()

    @pytest.mark.asyncio
    @patch.object(XCom, "aget_one", new_callable=AsyncMock)
    async def test_flatten_with_a_tallied_length_reads_each_page_once(self, mock_aget_one):
        """The producer's tally spares the counting pass: a sequential read fetches every page exactly once."""
        mock_aget_one.side_effect = self._pages_by_key([["a", "b"], [], ["c"]])
        flattened = self.make_iterable(length=3, flattened_length=3).flatten()
        assert [item async for item in flattened] == ["a", "b", "c"]
        assert mock_aget_one.await_count == 3
        assert [await flattened.aget(index) for index in range(3)] == ["a", "b", "c"]
        assert mock_aget_one.await_count == 6  # the jump back to index 0 restarts from the first page

    @pytest.mark.asyncio
    @patch.object(XCom, "aget_one", new_callable=AsyncMock)
    async def test_flatten_aget_indexes_flattened_items_not_pages(self, mock_aget_one):
        mock_aget_one.side_effect = self._pages_by_key([["a", "b"], ["c"]])
        flattened = self.make_iterable(length=2).flatten()
        assert await flattened.aget(2) == "c"
        assert await flattened.aget(-1) == "c"
        with pytest.raises(IndexError):
            await flattened.aget(3)

    @patch.object(XCom, "get_one")
    def test_flatten_expands_list_items(self, mock_get_one):
        """Items that are lists are expanded into individual elements."""
        mock_get_one.side_effect = self._pages_by_key([["a", "b"], ["c", "d"]])
        iterable = self.make_iterable(length=2)
        assert list(iterable.flatten()) == ["a", "b", "c", "d"]

    @patch.object(XCom, "get_one")
    def test_flatten_expands_tuple_items(self, mock_get_one):
        """Items that are tuples are expanded into individual elements."""
        mock_get_one.side_effect = self._pages_by_key([("a", "b"), ("c",)])
        iterable = self.make_iterable(length=2)
        assert list(iterable.flatten()) == ["a", "b", "c"]

    @patch.object(XCom, "get_one")
    def test_flatten_expands_set_items(self, mock_get_one):
        """Items that are sets are expanded into individual elements."""
        mock_get_one.side_effect = self._pages_by_key([{42}])
        iterable = self.make_iterable(length=1)
        assert list(iterable.flatten()) == [42]

    @patch.object(XCom, "get_one")
    def test_flatten_expands_generator_items(self, mock_get_one):
        """Items that are generators are expanded into individual elements."""
        mock_get_one.side_effect = self._pages_by_key([lambda: iter([1, 2, 3])])
        iterable = self.make_iterable(length=1)
        assert list(iterable.flatten()) == [1, 2, 3]

    @patch.object(XCom, "get_one")
    def test_flatten_passes_through_string_items(self, mock_get_one):
        """Strings are not iterated — they are yielded as a single item."""
        mock_get_one.side_effect = self._pages_by_key(["hello", "world"])
        iterable = self.make_iterable(length=2)
        assert list(iterable.flatten()) == ["hello", "world"]

    @patch.object(XCom, "get_one")
    def test_flatten_passes_through_bytes_items(self, mock_get_one):
        """Bytes are not iterated — they are yielded as a single item."""
        mock_get_one.side_effect = self._pages_by_key([b"hello"])
        iterable = self.make_iterable(length=1)
        assert list(iterable.flatten()) == [b"hello"]

    @patch.object(XCom, "get_one")
    def test_flatten_passes_through_non_iterable_items(self, mock_get_one):
        """Scalar (non-iterable) items are yielded unchanged."""
        mock_get_one.side_effect = self._pages_by_key([1, 2.0, True])
        iterable = self.make_iterable(length=3)
        assert list(iterable.flatten()) == [1, 2.0, True]

    @patch.object(XCom, "get_one")
    def test_flatten_handles_mixed_items(self, mock_get_one):
        """Mixed collection and scalar items are each handled correctly."""
        mock_get_one.side_effect = self._pages_by_key([["a", "b"], "c", ("d",), 5])
        iterable = self.make_iterable(length=4)
        assert list(iterable.flatten()) == ["a", "b", "c", "d", 5]

    def test_flatten_on_empty_iterable_yields_nothing(self):
        iterable = self.make_iterable(length=0)
        assert list(iterable.flatten()) == []

    @staticmethod
    def _pages_by_key(pages: list) -> object:
        """Build a get_one side_effect that maps each page's index-suffixed key to its page.

        Unlike a plain list side_effect (consumed once and then exhausted), this can be called
        any number of times for the same key — mirroring how a real XCom backend is queried by
        key and does not get "used up". This is required because ``list()``/``tuple()`` call
        ``__len__`` as a size hint before calling ``__iter__``, and since FlattenedXComIterable's
        ``__len__`` walks the stream to discover the count when it is not yet cached, any bulk
        consumer ends up walking (and re-fetching) pages twice — once for the hint, once for the
        real iteration — even though a plain ``for`` loop only walks once.

        A page may be a zero-arg callable instead of a plain value, in which case it is invoked
        fresh on every access — required for one-shot values like generators, which would
        otherwise appear exhausted on the second (real) walk after already being drained by the
        first (size-hint) walk.
        """

        def _get_one(*args, **kwargs):
            index = int(kwargs["key"].rsplit("_", 1)[-1])
            page = pages[index]
            return page() if callable(page) else page

        return _get_one

    @patch.object(XCom, "get_one")
    def test_negative_indices_count_from_the_end_on_both_iterables(self, mock_get_one):
        """Both classes honour the Sequence contract: ``[-1]`` is the last element of each view."""
        mock_get_one.side_effect = self._pages_by_key([["a", "b"], ["c"]])
        iterable = self.make_iterable(length=2)

        assert iterable[-1] == ["c"]
        assert iterable[-2] == ["a", "b"]
        assert iterable.flatten()[-1] == "c"
        assert iterable.flatten()[-3] == "a"

    @patch.object(XCom, "get_one")
    def test_negative_indices_out_of_range_raise_on_both_iterables(self, mock_get_one):
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

    @patch.object(XCom, "get_one")
    def test_skipped_indices_are_left_out(self, mock_get_one):
        mock_get_one.side_effect = self._values_by_index({1: "one", 4: "four"})
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
        assert sorted({call.kwargs["key"] for call in mock_get_one.call_args_list}) == [
            f"{BaseXCom.XCOM_RETURN_KEY}_1",
            f"{BaseXCom.XCOM_RETURN_KEY}_4",
        ]

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
        with pytest.raises(IndexError):
            iterable.flatten()[-4]

    @pytest.mark.asyncio
    @patch.object(XCom, "get_one")
    @patch.object(XCom, "aget_one", new_callable=AsyncMock)
    async def test_flattened_aget_negative_index_counts_from_the_end_without_sync_reads(
        self, mock_aget_one, mock_get_one
    ):
        mock_aget_one.side_effect = self._pages_by_key([["a", "b"], ["c"]])
        flattened = self.make_iterable(length=2).flatten()

        assert await flattened.aget(-1) == "c"
        assert await flattened.aget(-3) == "a"
        with pytest.raises(IndexError):
            await flattened.aget(-4)
        mock_get_one.assert_not_called()

    @patch.object(XCom, "get_one")
    def test_flatten_len_counts_flattened_items_not_pages(self, mock_get_one):
        """len() of a flattened iterable must count individual flattened items, not the raw
        pages XComIterable stores — a page can expand to any number of items."""
        mock_get_one.side_effect = [["a", "b"], ["c", "d", "e"]]
        flattened = self.make_iterable(length=2).flatten()
        assert len(flattened) == 5

    @patch.object(XCom, "get_one")
    def test_flatten_getitem_indexes_flattened_items_not_pages(self, mock_get_one):
        """Indexing a flattened iterable must address individual flattened items, not the raw
        pages XComIterable stores."""
        mock_get_one.side_effect = self._pages_by_key([["a", "b"], ["c", "d", "e"]])
        flattened = self.make_iterable(length=2).flatten()
        assert [flattened[i] for i in range(len(flattened))] == ["a", "b", "c", "d", "e"]

    @patch.object(XCom, "get_one")
    def test_flatten_getitem_slice_returns_flattened_items(self, mock_get_one):
        mock_get_one.side_effect = self._pages_by_key([["a", "b"], ["c", "d", "e"]])
        flattened = self.make_iterable(length=2).flatten()
        assert flattened[1:4] == ["b", "c", "d"]

    @patch.object(XCom, "get_one")
    def test_flatten_getitem_out_of_range_raises_index_error(self, mock_get_one):
        mock_get_one.side_effect = [["a", "b"]]
        flattened = self.make_iterable(length=1).flatten()
        with pytest.raises(IndexError):
            flattened[5]

    @patch.object(XCom, "get_one")
    def test_flatten_len_caches_total_length_without_materializing_items(self, mock_get_one):
        """len() walks the flattened stream once and caches only the resulting integer, so a
        repeated len() call does not re-fetch every page from XCom."""
        mock_get_one.side_effect = self._pages_by_key([["a", "b"], ["c", "d", "e"]])
        flattened = self.make_iterable(length=2).flatten()
        assert len(flattened) == 5
        assert len(flattened) == 5
        assert mock_get_one.call_count == 2
        assert flattened.flattened_length == 5

    @patch.object(XCom, "get_one")
    def test_flatten_getitem_holds_one_page_at_a_time(self, mock_get_one):
        """
        Reads keep only the last page fetched, so a sequential read fetches each page once, a read
        within the held page costs nothing, and a jump backwards restarts from the first page.
        Memory stays bounded by one page instead of the whole iterable.
        """
        mock_get_one.side_effect = self._pages_by_key([["a", "b"], ["c", "d", "e"]])
        flattened = self.make_iterable(length=2, flattened_length=5).flatten()
        assert flattened[0] == "a"
        assert flattened[1] == "b"
        assert mock_get_one.call_count == 1
        assert flattened[4] == "e"
        assert mock_get_one.call_count == 2
        assert flattened[2] == "c"
        assert mock_get_one.call_count == 2
        assert flattened[0] == "a"
        assert mock_get_one.call_count == 3
        assert flattened._items == ["a", "b"]

    @patch.object(XCom, "get_one")
    def test_flatten_len_before_iteration_does_not_retain_items(self, mock_get_one):
        """Calling len() before any iteration must not keep yielded items in memory — only the
        resulting count is cached, so repeated flattening of a large iterable stays bounded."""
        mock_get_one.side_effect = self._pages_by_key([["a", "b"], ["c", "d", "e"]])
        flattened = self.make_iterable(length=2).flatten()
        assert flattened.flattened_length is None
        assert len(flattened) == 5
        # Only the integer count is cached; no page is held after counting.
        assert flattened.flattened_length == 5
        assert flattened._items == []

    @patch.object(XCom, "get_one")
    def test_flatten_list_of_an_untallied_iterable_counts_then_reads(self, mock_get_one):
        """
        ``list()`` calls ``__len__`` as a size hint *before* iterating (a CPython optimization for
        any sized+iterable object), so an iterable without a tallied length is read twice here:
        once to count, once for the items — 4 fetches for 2 pages. A producer's tally, or a plain
        ``for`` loop, reads each page once.
        """
        mock_get_one.side_effect = self._pages_by_key([["a", "b"], ["c", "d", "e"]])
        flattened = self.make_iterable(length=2).flatten()
        assert list(flattened) == ["a", "b", "c", "d", "e"]
        assert flattened.flattened_length == 5
        assert len(flattened) == 5
        # len() must not trigger another full walk since the length is already cached.
        assert mock_get_one.call_count == 4

    @patch.object(XCom, "get_one")
    def test_flatten_reads_only_the_pages_that_exist(self, mock_get_one):
        """A flattened view of an iterable with skipped iterations expands the other pages only."""
        mock_get_one.side_effect = self._values_by_index({1: ["a", "b"], 4: ["c"]})
        flattened = self.make_skipping_iterable().flatten()

        assert len(flattened) == 3
        assert list(flattened) == ["a", "b", "c"]
        assert flattened.skipped == [0, 2, 3]
