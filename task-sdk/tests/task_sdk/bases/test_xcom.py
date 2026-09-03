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
    def test_append_does_not_increment_index_on_error(self, mock_set):
        """index/length must not advance when XCom.set raises, to avoid phantom entries."""
        iterable = self.make_iterable()
        with pytest.raises(RuntimeError, match="oops"):
            iterable.append("value")
        assert iterable.index == 0
        assert iterable.length == 0

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
    async def test_aappend_does_not_increment_index_on_error(self, mock_aset):
        """index/length must not advance when XCom.aset raises, to avoid phantom entries."""
        iterable = self.make_iterable()
        with pytest.raises(RuntimeError, match="oops"):
            await iterable.aappend("value")
        assert iterable.index == 0
        assert iterable.length == 0

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
