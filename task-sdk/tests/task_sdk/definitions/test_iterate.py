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
"""Tests for the ``.iterate()`` / ``.iterate_kwargs()`` entry points on partials and decorated tasks."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any
from unittest import mock

import pytest

from airflow.sdk import DAG, TaskInstanceState
from airflow.sdk.bases.xcom import BaseXCom
from airflow.sdk.execution_time.comms import (
    GetTICount,
    GetXCom,
    GetXComSequenceSlice,
    SetXCom,
    TICount,
    XComResult,
    XComSequenceSliceResult,
)

RunTI = Callable[[DAG, str, int], TaskInstanceState]


class TestIterate:
    def test_iterate_task_with_dict_return_annotation_pushes_whole_results(
        self, run_ti: RunTI, mock_supervisor_comms
    ):
        """A Mapping return annotation makes @task infer multiple_outputs=True. The runner must not
        apply that to an iterated task: its return value is the XComIterable aggregate, which is not a
        dict, and every sub-task result is pushed whole rather than fanned out by key."""
        items = [{"dag_id": "a", "n": 1}, {"dag_id": "b", "n": 2}, {"dag_id": "c", "n": 3}]

        with DAG(dag_id="iterate_dict_return") as dag:

            @dag.task
            def list_items():
                return items

            @dag.task
            def enrich(item: dict) -> dict:
                return {"dag_id": item["dag_id"], "n": item["n"] * 2}

            enrich.iterate(item=list_items())

        assert enrich.multiple_outputs is True

        def mock_comms(msg):
            if isinstance(msg, GetXCom):
                if msg.task_id == "list_items":
                    return XComResult(key=BaseXCom.XCOM_RETURN_KEY, value=items)
            elif isinstance(msg, GetXComSequenceSlice):
                if msg.task_id == "list_items":
                    return XComSequenceSliceResult(root=items)
            elif isinstance(msg, GetTICount):
                return TICount(count=1)
            return mock.DEFAULT

        mock_supervisor_comms.send.side_effect = mock_comms

        # Sub-task results are pushed through the async send, the aggregate through the sync one.
        mock_supervisor_comms.asend.reset_mock()
        assert run_ti(dag, "enrich", -1) == TaskInstanceState.SUCCESS
        pushed: dict[str, Any] = {
            msg.key: msg.value
            for call in [*mock_supervisor_comms.send.mock_calls, *mock_supervisor_comms.asend.mock_calls]
            if isinstance(msg := (call.kwargs.get("msg") or call.args[0]), SetXCom)
            and msg.task_id == "enrich"
        }

        assert pushed[BaseXCom.XCOM_RETURN_KEY]["__classname__"] == "airflow.sdk.bases.xcom.XComIterable"
        assert pushed[BaseXCom.XCOM_RETURN_KEY]["__data__"]["map_index"] == -1
        assert not {"dag_id", "n"} & pushed.keys()

        sub_results = [
            value for key, value in pushed.items() if key.startswith(f"{BaseXCom.XCOM_RETURN_KEY}_")
        ]
        assert sorted(sub_results, key=lambda r: r["dag_id"]) == [
            {"dag_id": "a", "n": 2},
            {"dag_id": "b", "n": 4},
            {"dag_id": "c", "n": 6},
        ]

    def test_decorated_iterate_validates_as_iterate_not_expand(self):
        """The decorated ``.iterate()`` names itself in its errors and, like the classic path, does not
        impose ``.expand()``'s mappable-type rule: a scalar is a valid one-item input to iterate over."""
        with DAG(dag_id="test_decorated_iterate_validation") as dag:

            @dag.task
            def show(number):
                return number

            with pytest.raises(TypeError, match=r"iterate\(\) got an unexpected keyword argument 'bogus'"):
                show.iterate(bogus=1)
            with pytest.raises(ValueError, match=r"cannot call iterate\(\) on task context variable 'ti'"):
                show.iterate(ti=1)
            with pytest.raises(ValueError, match=r"expand\(\) got an unexpected type 'int'"):
                show.expand(number=5)
            show.iterate(number=5)

    def test_iterate_marks_partial_as_expanded(self, recwarn):
        """Test that .iterate() (like .expand()) flags the OperatorPartial as consumed, so
        OperatorPartial.__del__ does not spuriously warn "Task ... was never mapped!" once the
        partial and its resulting operator are garbage collected."""
        from airflow.providers.standard.operators.empty import EmptyOperator
        from airflow.sdk.definitions._internal.expandinput import DictOfListsExpandInput

        with DAG(dag_id="test_iterate_expand_called"):
            partial = EmptyOperator.partial(task_id="test_task")
            partial._iterate(DictOfListsExpandInput({"retry_delay": [1, 2]}), strict=False)

            assert partial._expand_called is True

        del partial
        assert not any("was never mapped" in str(w.message) for w in recwarn.list)
