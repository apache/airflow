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

import pytest

from airflow.sdk.bases.xcom import BaseXCom
from airflow.sdk.execution_time.comms import (
    DeleteXCom,
    GetXCom,
    GetXComSequenceSlice,
    XComResult,
    XComSequenceSliceResult,
)
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
