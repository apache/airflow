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

from collections import defaultdict
from collections.abc import Callable
from unittest import mock

from airflow.sdk import DAG, TaskInstanceState
from airflow.sdk.bases.xcom import BaseXCom
from airflow.sdk.execution_time.comms import (
    GetTICount,
    GetXCom,
    GetXComSequenceSlice,
    TICount,
    XComResult,
    XComSequenceSliceResult,
)

RunTI = Callable[[DAG, str, int], TaskInstanceState]


class TestPartitionedOperator:
    def test_partition_iterate(self, run_ti: RunTI, mock_supervisor_comms):
        """Test a partitioned task which iterates on it's set of values."""
        outputs = defaultdict(list)
        numbers = list(range(10))

        with DAG(dag_id="product_same") as dag:

            @dag.task
            def emit_numbers():
                return numbers

            @dag.task
            def show(number, **context):
                map_index = str(context["ti"].map_index)
                outputs[map_index].append(number)
                return number

            emit_task = emit_numbers()
            show.partition(size=2).iterate(number=emit_task)

        def mock_comms(msg):
            if isinstance(msg, GetXCom):
                if msg.task_id == "emit_numbers":
                    return XComResult(key=BaseXCom.XCOM_RETURN_KEY, value=numbers)
            elif isinstance(msg, GetXComSequenceSlice):
                if msg.task_id == "emit_numbers":
                    return XComSequenceSliceResult(root=numbers)
            elif isinstance(msg, GetTICount):
                if msg.task_ids and msg.task_ids[0] == "show":
                    return TICount(count=2)
                return TICount(count=1)
            return mock.DEFAULT

        mock_supervisor_comms.send.side_effect = mock_comms

        states = [run_ti(dag, "show", map_index) for map_index in range(2)]
        assert states == [TaskInstanceState.SUCCESS] * 2
        assert set(outputs["0"]) == {0, 2, 4, 6, 8}
        assert set(outputs["1"]) == {1, 3, 5, 7, 9}
