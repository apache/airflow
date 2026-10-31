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

import hashlib
import threading
import uuid
from typing import Any
from unittest import mock
from unittest.mock import AsyncMock, patch

import pytest

from airflow.sdk import BaseOperator
from airflow.sdk.bases.xcom import (
    BaseXCom,
    XComCoordinate,
    XComIterable,
    _current_trigger_ti,
    _is_legacy_xcom_backend,
)
from airflow.sdk.execution_time.comms import (
    DeleteXCom,
    GetXCom,
    GetXComSequenceSlice,
    SetXCom,
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

    def test_delete_does_not_purge_when_delete_is_rejected(self, mock_supervisor_comms):
        mock_supervisor_comms.send.side_effect = [
            XComResult(key="key", value="remote://original"),
            RuntimeError("DELETE rejected: 410"),
        ]

        with mock.patch.object(BaseXCom, "purge", autospec=True) as purge:
            with pytest.raises(RuntimeError, match="DELETE rejected: 410"):
                BaseXCom.delete(key="key", task_id="task", dag_id="dag", run_id="run")

        purge.assert_not_called()
        assert mock_supervisor_comms.send.call_args_list == [
            mock.call(GetXCom(key="key", task_id="task", dag_id="dag", run_id="run")),
            mock.call(DeleteXCom(key="key", task_id="task", dag_id="dag", run_id="run")),
        ]

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

    def test_an_iterable_in_a_loop_keeps_the_pass_it_was_pushed_in_through_serialization(self):
        """A reader outside the loop, or in a later pass, gets the pointer with the producer's pass."""
        region_id = uuid.UUID(int=3)
        iterable = XComIterable(
            task_id="task", dag_id="dag", run_id="run", length=2, region_id=region_id, region_index=1
        )

        restored = XComIterable.deserialize(iterable.serialize(), version=1)

        assert (restored.region_id, restored.region_index) == (region_id, 1)
        assert self.make_iterable(length=2).serialize().keys() == {
            "task_id",
            "dag_id",
            "run_id",
            "map_index",
            "length",
            "skipped",
        }

    @pytest.mark.parametrize("in_a_loop", [False, True])
    @pytest.mark.asyncio
    @patch.object(XCom, "get_one")
    @patch.object(XCom, "aget_one", new_callable=AsyncMock)
    async def test_item_reads_name_the_pass_the_items_were_pushed_in(
        self, mock_aget_one, mock_get_one, in_a_loop
    ):
        """Only an iterable pushed in a loop scopes its reads, so a backend outside one gets the old arguments."""
        region = {"region_id": uuid.UUID(int=3), "region_index": 1} if in_a_loop else {}
        iterable = XComIterable(task_id="task", dag_id="dag", run_id="run", length=2, **region)

        iterable[0]
        await iterable.aget(1)

        scope = {f"_{name}": value for name, value in region.items()}
        assert mock_get_one.call_args.kwargs == {
            "key": "return_value_0",
            "dag_id": "dag",
            "task_id": "task",
            "run_id": "run",
            "map_index": None,
            **scope,
        }
        assert mock_aget_one.call_args.kwargs == {**mock_get_one.call_args.kwargs, "key": "return_value_1"}

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


class LegacyXCom(BaseXCom):
    @staticmethod
    def serialize_value(value, *, key=None, task_id=None, dag_id=None, run_id=None, map_index=None):
        return f"legacy://{dag_id}/{run_id}/{task_id}/{map_index}/{key}"

    @staticmethod
    def deserialize_value(result):
        return f"deserialized {result.value}"


class StoreXCom(BaseXCom):
    @classmethod
    def store_value(cls, coordinate, key, value):
        return f"store://{coordinate.ti_id}/{key}"

    @classmethod
    def load_value(cls, stored):
        return f"loaded {stored}"


class CompatXCom(BaseXCom):
    store_value = StoreXCom.__dict__["store_value"]
    load_value = StoreXCom.__dict__["load_value"]
    serialize_value = LegacyXCom.__dict__["serialize_value"]
    deserialize_value = LegacyXCom.__dict__["deserialize_value"]


class SubclassOverridingOldHooks(StoreXCom):
    serialize_value = LegacyXCom.__dict__["serialize_value"]
    deserialize_value = LegacyXCom.__dict__["deserialize_value"]


class StoreMixin:
    @classmethod
    def store_value(cls, coordinate, key, value):
        return "mixin"


@pytest.mark.parametrize(
    ("backend", "expected"),
    [
        pytest.param(BaseXCom, False, id="base"),
        pytest.param(
            type("ReadOnly", (BaseXCom,), {"deserialize_value": staticmethod(str)}), False, id="read-only"
        ),
        pytest.param(LegacyXCom, True, id="serialize-only"),
        pytest.param(type("LegacySubclass", (LegacyXCom,), {}), True, id="inherits-serialize-only"),
        pytest.param(StoreXCom, False, id="store"),
        pytest.param(CompatXCom, False, id="store-and-serialize-in-one-class"),
        pytest.param(SubclassOverridingOldHooks, True, id="serialize-overridden-below-store"),
        pytest.param(
            type("StoreBelowLegacy", (LegacyXCom,), {"store_value": StoreXCom.__dict__["store_value"]}),
            False,
            id="store-overridden-below-serialize",
        ),
        pytest.param(type("MixinStore", (StoreMixin, LegacyXCom), {}), False, id="store-from-mixin"),
    ],
)
def test_is_legacy_xcom_backend(backend, expected):
    assert _is_legacy_xcom_backend(backend) is expected


class TestStoreAndLoadValue:
    @pytest.mark.parametrize(
        ("backend", "expected_value"),
        [
            pytest.param(StoreXCom, "store://{ti_id}/key", id="store"),
            pytest.param(CompatXCom, "store://{ti_id}/key", id="store-and-serialize-in-one-class"),
            pytest.param(LegacyXCom, "legacy://test_dag/test_run/task/-1/key", id="legacy"),
            pytest.param(
                SubclassOverridingOldHooks,
                "legacy://test_dag/test_run/task/-1/key",
                id="serialize-overridden-below-store",
            ),
        ],
    )
    @pytest.mark.parametrize("use_async", [False, True])
    @pytest.mark.asyncio
    async def test_push_stores_value_for_the_task_instance(
        self, backend, expected_value, use_async, create_runtime_ti, mock_supervisor_comms
    ):
        ti = create_runtime_ti(task=BaseOperator(task_id="task"))
        mock_supervisor_comms.asend = mock.AsyncMock()

        if use_async:
            await backend._apush(ti, "key", 1, dag_result=True)
            sent = mock_supervisor_comms.asend.call_args.args[0]
        else:
            backend._push(ti, "key", 1, dag_result=True)
            sent = mock_supervisor_comms.send.call_args.args[0]

        assert sent == SetXCom(
            key="key",
            value=expected_value.format(ti_id=ti.id),
            dag_id="test_dag",
            task_id="task",
            run_id="test_run",
            map_index=-1,
            dag_result=True,
        )

    @pytest.mark.parametrize(
        ("region_id", "region_index"),
        [
            pytest.param(None, None, id="no-region"),
            pytest.param(uuid.UUID(int=0), 2, id="top-level-region"),
            pytest.param(uuid.uuid4(), 3, id="loop-region"),
        ],
    )
    @pytest.mark.parametrize("use_async", [False, True])
    @pytest.mark.asyncio
    async def test_push_passes_the_task_instance_and_its_region_to_store_value(
        self, region_id, region_index, use_async, create_runtime_ti, mock_supervisor_comms
    ):
        ti = create_runtime_ti(task=BaseOperator(task_id="task"), map_index=2)
        ti.region_id, ti.region_index = region_id, region_index
        mock_supervisor_comms.asend = mock.AsyncMock()

        with mock.patch.object(StoreXCom, "store_value", autospec=True, return_value="stored") as store_value:
            if use_async:
                await StoreXCom._apush(ti, "key", 1)
            else:
                StoreXCom._push(ti, "key", 1)

        in_region = region_id not in (None, uuid.UUID(int=0))
        store_value.assert_called_once_with(
            XComCoordinate(
                "test_dag",
                "test_run",
                "task",
                ti.id,
                region_id if in_region else None,
                region_index if in_region else None,
            ),
            "key",
            1,
        )
        sent = (mock_supervisor_comms.asend if use_async else mock_supervisor_comms.send).call_args.args[0]
        assert sent.map_index == 2

    @pytest.mark.parametrize("operation", ["aset", "apush", "aget_one", "aget_all"])
    @pytest.mark.asyncio
    async def test_async_paths_call_the_backend_hooks_off_the_calling_thread(
        self, operation, create_runtime_ti, mock_supervisor_comms
    ):
        threads: list[int] = []

        class RecordingXCom(BaseXCom):
            @classmethod
            def store_value(cls, coordinate, key, value):
                threads.append(threading.get_ident())
                return "stored"

            @classmethod
            def load_value(cls, stored):
                threads.append(threading.get_ident())
                return stored

        ti = create_runtime_ti(task=BaseOperator(task_id="task"))
        coordinates = {"key": "key", "dag_id": "dag", "task_id": "task", "run_id": "run"}
        reply = (
            XComSequenceSliceResult(root=["ref"])
            if operation == "aget_all"
            else XComResult(key="key", value="ref")
        )
        mock_supervisor_comms.asend = mock.AsyncMock(return_value=reply)

        match operation:
            case "aset":
                await RecordingXCom.aset(value=1, **coordinates)
            case "apush":
                await RecordingXCom._apush(ti, "key", 1)
            case "aget_one":
                await RecordingXCom.aget_one(**coordinates)
            case "aget_all":
                await RecordingXCom.aget_all(**coordinates)

        assert threads
        assert threading.get_ident() not in threads

    def test_super_store_value_keeps_the_value_in_the_metadata_database(self):
        coordinate = XComCoordinate("dag", "run", "task")

        assert BaseXCom.store_value(coordinate, "key", {"a": 1}) == {"a": 1}

    @pytest.mark.parametrize(
        ("backend", "expected_value"),
        [
            pytest.param(StoreXCom, "store://None/key", id="store_value"),
            pytest.param(CompatXCom, "store://None/key", id="store-and-serialize-in-one-class"),
            pytest.param(LegacyXCom, "legacy://dag/run/task/3/key", id="serialize_value"),
        ],
    )
    @pytest.mark.parametrize("use_async", [False, True])
    @pytest.mark.asyncio
    async def test_set_by_coordinates_goes_through_the_backend_without_a_task_instance_id(
        self, backend, expected_value, use_async, mock_supervisor_comms
    ):
        mock_supervisor_comms.asend = mock.AsyncMock()
        coordinates = dict(key="key", value=1, dag_id="dag", task_id="task", run_id="run", map_index=3)

        if use_async:
            await backend.aset(**coordinates)
            sent = mock_supervisor_comms.asend.call_args.args[0]
        else:
            backend.set(**coordinates)
            sent = mock_supervisor_comms.send.call_args.args[0]

        assert sent == SetXCom(
            key="key", value=expected_value, dag_id="dag", task_id="task", run_id="run", map_index=3
        )

    @pytest.mark.parametrize(
        ("owner", "overrides", "expected_id"),
        [
            pytest.param("task", {}, "{ti_id}", id="running-task-own-coordinates"),
            pytest.param("task", {"task_id": "other"}, "None", id="running-task-another-task"),
            pytest.param("task", {"map_index": 2}, "None", id="running-task-another-map-index"),
            pytest.param("task", {"dag_id": "other_dag"}, "None", id="running-task-another-dag"),
            pytest.param("task", {"run_id": "other_run"}, "None", id="running-task-another-run"),
            pytest.param("trigger", {}, "{ti_id}", id="trigger-own-coordinates"),
            pytest.param("trigger", {"task_id": "other"}, "None", id="trigger-another-task"),
            pytest.param(None, {}, "None", id="no-owner"),
        ],
    )
    @pytest.mark.parametrize("use_async", [False, True])
    @pytest.mark.asyncio
    async def test_set_passes_the_task_instance_id_only_for_the_writers_own_coordinates(
        self, owner, overrides, expected_id, use_async, create_runtime_ti, mock_supervisor_comms
    ):
        ti = create_runtime_ti(task=BaseOperator(task_id="task"))
        mock_supervisor_comms.asend = mock.AsyncMock()
        context = [{"ti": ti}] if owner == "task" else []
        coordinates: dict[str, Any] = {
            "dag_id": "test_dag",
            "run_id": "test_run",
            "task_id": "task",
            "map_index": -1,
            **overrides,
        }
        token = _current_trigger_ti.set(ti if owner == "trigger" else None)
        try:
            with mock.patch("airflow.sdk.definitions._internal.contextmanager._CURRENT_CONTEXT", context):
                if use_async:
                    await StoreXCom.aset(key="key", value=1, **coordinates)
                    sent = mock_supervisor_comms.asend.call_args.args[0]
                else:
                    StoreXCom.set(key="key", value=1, **coordinates)
                    sent = mock_supervisor_comms.send.call_args.args[0]
        finally:
            _current_trigger_ti.reset(token)

        assert sent.value == f"store://{expected_id.format(ti_id=ti.id)}/key"

    @pytest.mark.parametrize(
        ("backend", "prefix"),
        [
            pytest.param(StoreXCom, "loaded", id="load_value"),
            pytest.param(SubclassOverridingOldHooks, "deserialized", id="deserialize-overridden-below-load"),
        ],
    )
    @pytest.mark.parametrize("use_async", [False, True])
    @pytest.mark.asyncio
    async def test_get_one_loads_the_stored_value(self, backend, prefix, use_async, mock_supervisor_comms):
        response = XComResult(key="key", value="ref")
        coordinates = dict(key="key", dag_id="dag", task_id="task", run_id="run")

        if use_async:
            mock_supervisor_comms.asend = mock.AsyncMock(return_value=response)
            result = await backend.aget_one(**coordinates)
        else:
            mock_supervisor_comms.send.return_value = response
            result = backend.get_one(**coordinates)

        assert result == f"{prefix} ref"

    @pytest.mark.parametrize(
        ("backend", "prefix"),
        [
            pytest.param(StoreXCom, "loaded", id="load_value"),
            pytest.param(SubclassOverridingOldHooks, "deserialized", id="deserialize-overridden-below-load"),
        ],
    )
    @pytest.mark.parametrize("use_async", [False, True])
    @pytest.mark.asyncio
    async def test_get_all_loads_each_stored_value(self, backend, prefix, use_async, mock_supervisor_comms):
        response = XComSequenceSliceResult(root=["a", "b"])
        coordinates = dict(key="key", dag_id="dag", task_id="task", run_id="run")

        if use_async:
            mock_supervisor_comms.asend = mock.AsyncMock(return_value=response)
            result = await backend.aget_all(**coordinates)
        else:
            mock_supervisor_comms.send.return_value = response
            result = backend.get_all(**coordinates)

        assert result == [f"{prefix} a", f"{prefix} b"]


class TestXComCoordinate:
    def test_build_path_for_a_task_instance_reuses_its_path_for_a_key(self):
        ti_id = uuid.uuid4()
        coordinate = XComCoordinate("dag", "run", "task", ti_id)

        path = coordinate.build_path("s3://bucket/xcom", "key", suffix=".gz")

        assert path == f"s3://bucket/xcom/dag/run/task/ti-{ti_id}/key.gz"
        assert coordinate.build_path("s3://bucket/xcom/", "key", suffix=".gz") == path

    def test_build_path_without_a_task_instance_is_unique_per_write(self):
        coordinate = XComCoordinate("dag", "run", "task")

        first = coordinate.build_path("p", "key")
        second = coordinate.build_path("p", "key")

        head, instance, name = first.rsplit("/", 2)
        assert (head, name) == ("p/dag/run/task", "key")
        assert uuid.UUID(instance.removeprefix("write-"))
        assert first != second

    @pytest.mark.parametrize(
        ("key", "expected"),
        [
            pytest.param("a/key", "a%2Fkey", id="quoted"),
            pytest.param(".", "%2E", id="dot"),
            pytest.param("..", "%2E%2E", id="dots"),
            pytest.param("k" * 300, hashlib.sha256(b"k" * 300).hexdigest(), id="too-long-to-quote"),
            pytest.param("/" * 100, hashlib.sha256(b"/" * 100).hexdigest(), id="quoting-makes-it-too-long"),
            pytest.param("a" * 200, "a" * 200, id="at-the-limit"),
            pytest.param("a" * 201, hashlib.sha256(b"a" * 201).hexdigest(), id="just-over-the-limit"),
        ],
    )
    def test_build_path_names_the_key_safely(self, key, expected):
        path = XComCoordinate("dag", "run", "task", uuid.uuid4()).build_path("p", key)

        assert path.rsplit("/", 1)[1] == expected

    @pytest.mark.parametrize("dag_id", ["d" * 100, "é" * 100])
    def test_build_path_shortens_the_readable_parts_only_when_it_must(self, dag_id):
        ti_id = uuid.uuid4()
        coordinate = XComCoordinate(dag_id, "r" * 100, "t" * 100, ti_id)
        whole = coordinate.build_path("prefix", "key")

        shortened = coordinate.build_path("prefix", "key", max_length=len(whole.encode()) - 20)

        assert len(shortened.encode()) <= len(whole.encode()) - 20
        assert shortened.startswith("prefix/")
        assert shortened.endswith(f"/ti-{ti_id}/key")
        assert coordinate.build_path("prefix", "key", max_length=len(whole.encode())) == whole

    def test_build_path_shortens_the_longest_part_first(self):
        ti_id = uuid.uuid4()
        coordinate = XComCoordinate("d" * 10, "r" * 100, "t" * 10, ti_id)
        whole = coordinate.build_path("p", "key")

        shortened = coordinate.build_path("p", "key", max_length=len(whole) - 20)

        assert shortened == f"p/{'d' * 10}/{'r' * 80}/{'t' * 10}/ti-{ti_id}/key"

    def test_build_path_raises_when_the_fixed_parts_do_not_fit(self):
        coordinate = XComCoordinate("dag", "run", "task", uuid.uuid4())

        with pytest.raises(ValueError, match="Cannot fit"):
            coordinate.build_path("p" * 300, "key", max_length=100)
