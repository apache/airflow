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


class TestBatchedOperator:
    def test_batch_iterate(self, run_ti: RunTI, mock_supervisor_comms):
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
            show.batch(size=2).iterate(number=emit_task)

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

    @pytest.mark.parametrize("batch_size", [None, 2])
    def test_iterate_task_with_dict_return_annotation_pushes_whole_results(
        self, batch_size, run_ti: RunTI, mock_supervisor_comms
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

            target = enrich if batch_size is None else enrich.batch(size=batch_size)
            target.iterate(item=list_items())

        assert enrich.multiple_outputs is True

        def mock_comms(msg):
            if isinstance(msg, GetXCom):
                if msg.task_id == "list_items":
                    return XComResult(key=BaseXCom.XCOM_RETURN_KEY, value=items)
            elif isinstance(msg, GetXComSequenceSlice):
                if msg.task_id == "list_items":
                    return XComSequenceSliceResult(root=items)
            elif isinstance(msg, GetTICount):
                if msg.task_ids and msg.task_ids[0] == "enrich":
                    return TICount(count=2)
                return TICount(count=1)
            return mock.DEFAULT

        mock_supervisor_comms.send.side_effect = mock_comms

        map_indexes = [-1] if batch_size is None else [0, 1]
        pushed: dict[int, dict[str, Any]] = {}
        for map_index in map_indexes:
            # Sub-task results are pushed through the async send, the aggregate through the sync one.
            mock_supervisor_comms.asend.reset_mock()
            assert run_ti(dag, "enrich", map_index) == TaskInstanceState.SUCCESS
            pushed[map_index] = {
                msg.key: msg.value
                for call in [*mock_supervisor_comms.send.mock_calls, *mock_supervisor_comms.asend.mock_calls]
                if isinstance(msg := (call.kwargs.get("msg") or call.args[0]), SetXCom)
                and msg.task_id == "enrich"
            }

        for map_index, xcoms in pushed.items():
            assert xcoms[BaseXCom.XCOM_RETURN_KEY]["__classname__"] == "airflow.sdk.bases.xcom.XComIterable"
            assert xcoms[BaseXCom.XCOM_RETURN_KEY]["__data__"]["map_index"] == map_index
            assert not {"dag_id", "n"} & xcoms.keys()

        sub_results = [
            value
            for xcoms in pushed.values()
            for key, value in xcoms.items()
            if key.startswith(f"{BaseXCom.XCOM_RETURN_KEY}_")
        ]
        assert sorted(sub_results, key=lambda r: r["dag_id"]) == [
            {"dag_id": "a", "n": 2},
            {"dag_id": "b", "n": 4},
            {"dag_id": "c", "n": 6},
        ]

    @pytest.mark.parametrize(
        ("fan_out", "expected_mapped_length"),
        [
            pytest.param(lambda t, arg: t.expand(**arg), 3000, id="expand"),
            pytest.param(lambda t, arg: t.iterate(**arg), None, id="iterate"),
            pytest.param(lambda t, arg: t.batch(size=10).iterate(**arg), None, id="batch-iterate"),
        ],
    )
    def test_upstream_of_batched_iterate_is_not_length_tagged(
        self, fan_out, expected_mapped_length, run_ti: RunTI, mock_supervisor_comms
    ):
        """An upstream push is tagged with mapped_length only when a downstream's instance count depends
        on it. A batched iterate always creates batch_size instances, so its upstream must push untagged
        or the API server would reject any input longer than core.max_map_length."""
        ids = [str(i) for i in range(3000)]

        with DAG(dag_id="length_tagging") as dag:

            @dag.task
            def get_ids() -> list[str]:
                return ids

            @dag.task
            def get_relations(object_id: str):
                return object_id

            fan_out(get_relations, {"object_id": get_ids()})

        assert run_ti(dag, "get_ids", -1) == TaskInstanceState.SUCCESS
        pushes = [
            msg
            for call in [*mock_supervisor_comms.send.mock_calls, *mock_supervisor_comms.asend.mock_calls]
            if isinstance(msg := (call.kwargs.get("msg") or call.args[0]), SetXCom)
            and msg.task_id == "get_ids"
        ]
        assert [push.mapped_length for push in pushes] == [expected_mapped_length]

    @pytest.mark.parametrize(
        ("batch_size", "expand_size"),
        [
            (5, 10),  # Batched: size=5 for 10 items
            (3, 3),  # Batched: size=3 for 3 items
            (4, 20),  # Batched: size=4 for 20 items
            (0, 5),  # Non-batched: the internal size=0 sentinel used by .iterate()/.expand()
        ],
    )
    def test_batch_size_preserved_through_lifecycle(self, batch_size, expand_size):
        from airflow.providers.standard.operators.empty import EmptyOperator
        from airflow.sdk.definitions._internal.expandinput import DictOfListsExpandInput
        from airflow.sdk.definitions.iterableoperator import IterableOperator, MappedIterableOperator
        from airflow.serialization.serialized_objects import OperatorSerialization

        with DAG(dag_id=f"test_batch_{batch_size}") as dag:
            op = EmptyOperator.partial(task_id="test_task", dag=dag)._batch(size=batch_size)

            expand_input = DictOfListsExpandInput({"retry_delay": list(range(expand_size))})
            iterable_op = op._iterate(expand_input, strict=False)

            # Check if batched or non-batched
            if batch_size > 1:
                assert isinstance(iterable_op, MappedIterableOperator)
                mapped_op = iterable_op.delegate
                assert iterable_op.batch_size == batch_size

                # 1. Verify batch_size is in partial_kwargs
                assert "batch_size" in mapped_op.partial_kwargs
                assert mapped_op.partial_kwargs["batch_size"] == batch_size

                # 2. Verify batch_size is serialized (not excluded)
                serialized = OperatorSerialization.serialize_mapped_operator(mapped_op)
                assert "partial_kwargs" in serialized
                assert "batch_size" in serialized["partial_kwargs"]
                assert serialized["partial_kwargs"]["batch_size"] == batch_size

                # 3. Verify batch_size survives deserialization
                deserialized_op = OperatorSerialization.deserialize_operator(serialized)
                assert "batch_size" in deserialized_op.partial_kwargs
                assert deserialized_op.partial_kwargs["batch_size"] == batch_size

                # 4. Verify batch_size is removed before operator instantiation (only for batched)
                unmapped = iterable_op.unmap({"retry_delay": 1})
                # Verify unmapped task doesn't have batch_size attribute
                assert not hasattr(unmapped, "batch_size")
            else:
                assert isinstance(iterable_op, IterableOperator)

    @pytest.mark.parametrize("batch_size", [0, 3])
    def test_iterate_marks_partial_as_expanded(self, batch_size, recwarn):
        """Test that .iterate() (unlike .expand()'s dedicated OperatorPartial._expand(), which sets
        _expand_called explicitly) also flags the OperatorPartial as consumed, so
        OperatorPartial.__del__ does not spuriously warn "Task ... was never mapped!" once the
        partial and its resulting operator are garbage collected."""
        from airflow.providers.standard.operators.empty import EmptyOperator
        from airflow.sdk.definitions._internal.expandinput import DictOfListsExpandInput

        with DAG(dag_id=f"test_iterate_expand_called_{batch_size}"):
            partial = EmptyOperator.partial(task_id="test_task")
            expand_input = DictOfListsExpandInput({"retry_delay": [1, 2]})
            partial._batch(size=batch_size)._iterate(expand_input, strict=False)

            assert partial._expand_called is True

        del partial
        assert not any("was never mapped" in str(w.message) for w in recwarn.list)

    def test_mapped_iterable_operator_retries_preserved(self):
        """Ensure MappedIterableOperator delegates retries to and from the wrapped operator, so that
        Airflow's standard retry mechanism (applied to the whole IterableOperator) sees the same
        retries the user configured on the delegate."""
        from airflow.providers.standard.operators.empty import EmptyOperator
        from airflow.sdk.definitions._internal.expandinput import DictOfListsExpandInput
        from airflow.sdk.definitions.iterableoperator import MappedIterableOperator

        with DAG(dag_id="test_mapped_iterable_retries") as dag:
            expand_input = DictOfListsExpandInput({"retry_delay": [1.0, 2.0]})
            iterable_op = (
                EmptyOperator.partial(task_id="test_task", dag=dag, retries=3)
                .batch(size=2)
                ._iterate(expand_input, strict=False)
            )

            assert isinstance(iterable_op, MappedIterableOperator)
            assert iterable_op.retries == 3

            iterable_op.retries = 5
            mapped_op = iterable_op.delegate
            assert mapped_op.retries == 5

            unmapped = mapped_op.unmap({"retry_delay": 1.0})
            assert unmapped.retries == 5


@pytest.mark.parametrize("size", [-1, 0, 1])
def test_batch_rejects_sizes_below_two(size):
    from airflow.providers.standard.operators.empty import EmptyOperator

    with DAG(dag_id="test_batch_size_rejected"):
        with pytest.raises(ValueError, match=f"batch size must be at least 2, got {size}"):
            EmptyOperator.partial(task_id="test_task").batch(size=size)
