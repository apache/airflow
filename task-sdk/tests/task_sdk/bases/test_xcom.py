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
from airflow.sdk.execution_time.comms import DeleteXCom, XComResult, XComSequenceSliceResult


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

    def test_get_all_returns_empty_list_when_no_values_found(self, mock_supervisor_comms):
        """Test that BaseXCom.get_all returns an empty list instead of None when no values are found."""
        mock_supervisor_comms.send.return_value = XComSequenceSliceResult(root=[])

        result = BaseXCom.get_all(
            key="test_key",
            dag_id="test_dag",
            task_id="test_task",
            run_id="test_run",
        )

        assert result == []
        assert isinstance(result, list)
