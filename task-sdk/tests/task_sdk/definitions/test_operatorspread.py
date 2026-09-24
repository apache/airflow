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


class TestPartialOperatorSpread:
    def test_spread_iterate(self, run_ti: RunTI, mock_supervisor_comms):
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
            show.spread(across=2).iterate(number=emit_task)

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

    def test_spread_iterate_task_with_dict_return_annotation_pushes_whole_results(
        self, run_ti: RunTI, mock_supervisor_comms
    ):
        """Spread counterpart of the test in test_iterate.py: each of the spread task's instances pushes
        the XComIterable aggregate and its sub-task results whole, never fanned out by key."""
        spread_across = 2
        items = [{"dag_id": "a", "n": 1}, {"dag_id": "b", "n": 2}, {"dag_id": "c", "n": 3}]

        with DAG(dag_id="iterate_dict_return") as dag:

            @dag.task
            def list_items():
                return items

            @dag.task
            def enrich(item: dict) -> dict:
                return {"dag_id": item["dag_id"], "n": item["n"] * 2}

            enrich.spread(across=spread_across).iterate(item=list_items())

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

        map_indexes = [0, 1]
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
            pytest.param(lambda t, arg: t.spread(across=10).iterate(**arg), None, id="spread-iterate"),
        ],
    )
    def test_upstream_of_spread_iterate_is_not_length_tagged(
        self, fan_out, expected_mapped_length, run_ti: RunTI, mock_supervisor_comms
    ):
        """An upstream push is tagged with mapped_length only when a downstream's instance count depends
        on it. A spread iterate always creates ``across`` instances, so its upstream must push untagged
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
        ("spread_across", "expand_size"),
        [
            (5, 10),  # Spread across 5 for 10 items
            (3, 3),  # Spread across 3 for 3 items
            (4, 20),  # Spread across 4 for 20 items
            (0, 5),  # Not spread: a plain .iterate()
        ],
    )
    def test_spread_across_preserved_through_lifecycle(self, spread_across, expand_size):
        from airflow.providers.standard.operators.empty import EmptyOperator
        from airflow.sdk.definitions._internal.expandinput import DictOfListsExpandInput
        from airflow.sdk.definitions.iterableoperator import IterableOperator, MappedIterableOperator
        from airflow.serialization.serialized_objects import OperatorSerialization

        with DAG(dag_id=f"test_spread_{spread_across}") as dag:
            partial = EmptyOperator.partial(task_id="test_task", dag=dag)
            op = partial if spread_across == 0 else partial.spread(across=spread_across)

            expand_input = DictOfListsExpandInput({"retry_delay": list(range(expand_size))})
            iterable_op = op._iterate(expand_input, strict=False)

            # Check if spread or not
            if spread_across > 1:
                assert isinstance(iterable_op, MappedIterableOperator)
                mapped_op = iterable_op.delegate
                assert iterable_op.spread_across == spread_across

                # 1. Verify spread_across is in partial_kwargs
                assert "spread_across" in mapped_op.partial_kwargs
                assert mapped_op.partial_kwargs["spread_across"] == spread_across

                # 2. Verify spread_across is serialized (not excluded)
                serialized = OperatorSerialization.serialize_mapped_operator(mapped_op)
                assert "partial_kwargs" in serialized
                assert "spread_across" in serialized["partial_kwargs"]
                assert serialized["partial_kwargs"]["spread_across"] == spread_across

                # 3. Verify spread_across survives deserialization
                deserialized_op = OperatorSerialization.deserialize_operator(serialized)
                assert "spread_across" in deserialized_op.partial_kwargs
                assert deserialized_op.partial_kwargs["spread_across"] == spread_across

                # 4. Verify spread_across is removed before operator instantiation (only when spread)
                unmapped = iterable_op.unmap({"retry_delay": 1})
                # Verify unmapped task doesn't have spread_across attribute
                assert not hasattr(unmapped, "spread_across")
            else:
                assert isinstance(iterable_op, IterableOperator)

    def test_spread_iterate_marks_partial_as_expanded(self, recwarn):
        """Spread counterpart of the test in test_iterate.py: .spread().iterate() also flags the
        OperatorPartial as consumed, so OperatorPartial.__del__ does not warn "Task ... was never
        mapped!" once the partial and its resulting operator are garbage collected."""
        from airflow.providers.standard.operators.empty import EmptyOperator
        from airflow.sdk.definitions._internal.expandinput import DictOfListsExpandInput

        with DAG(dag_id="test_spread_iterate_expand_called"):
            partial = EmptyOperator.partial(task_id="test_task")
            expand_input = DictOfListsExpandInput({"retry_delay": [1, 2]})
            partial.spread(across=3)._iterate(expand_input, strict=False)

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
                .spread(across=2)
                ._iterate(expand_input, strict=False)
            )

            assert isinstance(iterable_op, MappedIterableOperator)
            assert iterable_op.retries == 3

            iterable_op.retries = 5
            mapped_op = iterable_op.delegate
            assert mapped_op.retries == 5

            unmapped = mapped_op.unmap({"retry_delay": 1.0})
            assert unmapped.retries == 5


@pytest.mark.parametrize("across", [-1, 0, 1])
def test_spread_rejects_across_below_two(across):
    from airflow.providers.standard.operators.empty import EmptyOperator

    with DAG(dag_id="test_spread_across_rejected"):
        with pytest.raises(ValueError, match=f"across must be at least 2, got {across}"):
            EmptyOperator.partial(task_id="test_task").spread(across=across)


def test_spread_takes_across_as_a_keyword_only():
    """``spread(17)`` reads as a chunk length as easily as a task instance count, and ``size=`` was the
    rejected spelling of the draft: both fail at parse time instead of being accepted silently."""
    from airflow.providers.standard.operators.empty import EmptyOperator

    with DAG(dag_id="test_spread_keyword_only"):
        with pytest.raises(TypeError, match="positional argument"):
            EmptyOperator.partial(task_id="test_task").spread(17)
        with pytest.raises(TypeError, match="unexpected keyword argument 'size'"):
            EmptyOperator.partial(task_id="test_task").spread(size=17)


class TestRuntimeSpreadAcross:
    """``.spread(across=<XComArg>)``: the instance count comes from an upstream task's return value."""

    @staticmethod
    def _dag(across_value: Any = 3) -> DAG:
        with DAG(dag_id="runtime_spread_across") as dag:

            @dag.task
            def count_instances():
                return across_value

            @dag.task
            def get_ids() -> list[str]:
                return [str(i) for i in range(10)]

            @dag.task
            def get_relations(object_id: str):
                return object_id

            get_relations.spread(across=count_instances()).iterate(object_id=get_ids())
        return dag

    def test_wiring(self):
        from airflow.sdk.definitions.iterableoperator import MappedIterableOperator, is_spread_across_source
        from airflow.sdk.definitions.xcom_arg import XComArg

        dag = self._dag()
        spread = dag.task_dict["get_relations"]
        assert isinstance(spread, MappedIterableOperator)
        assert isinstance(spread.spread_across, XComArg)
        assert isinstance(spread.partial_kwargs["spread_across"], XComArg)
        # The counting task is an ordinary upstream, never a mapped dependency: neither upstream may
        # be length-tagged as a list, the counting task is tagged with its integer instead.
        assert spread.upstream_task_ids == {"count_instances", "get_ids"}
        assert list(spread.iter_mapped_dependencies()) == []
        assert is_spread_across_source(dag.task_dict["count_instances"])
        assert not is_spread_across_source(dag.task_dict["get_ids"])

    def test_rejects_an_across_the_scheduler_cannot_look_up(self):
        from airflow.sdk.definitions.xcom_arg import XComArg

        with DAG(dag_id="runtime_spread_across_validation") as dag:

            @dag.task
            def count_instances():
                return 3

            @dag.task
            def per_item(x):
                return x

            @dag.task
            def work(value):
                return value

            across = count_instances()
            with pytest.raises(TypeError, match="must be a plain XComArg, not MapXComArg"):
                work.spread(across=across.map(lambda v: v))
            with pytest.raises(
                ValueError, match="must be the return value of 'count_instances', not its 'other' XCom"
            ):
                work.spread(across=XComArg(across.operator, key="other"))
            with pytest.raises(ValueError, match="cannot come from mapped task 'per_item'"):
                work.spread(across=per_item.expand(x=[1, 2]))

    def test_resolved_before_unmap(self):
        from airflow.sdk.definitions._internal.expandinput import SpreadExpandInput
        from airflow.sdk.definitions.xcom_arg import PlainXComArg, XComArg

        spread = self._dag().task_dict["get_relations"]
        with pytest.raises(RuntimeError, match="not resolved yet"):
            spread.unmap({})

        context = {"ti": mock.Mock(map_index=1)}
        with mock.patch.object(PlainXComArg, "resolve", return_value=3):
            spread._resolve_spread_across(context)
        assert spread.spread_across == 3
        # partial_kwargs is what gets serialized and keeps the reference.
        assert isinstance(spread.partial_kwargs["spread_across"], XComArg)

        expand_input = spread.unmap({}).expand_input
        assert isinstance(expand_input, SpreadExpandInput)
        assert expand_input.across == 3

    def test_unmapped_instance_keeps_the_downstream_tasks(self):
        """A spread instance whose iterations partly skip must find the task's downstream tasks to skip."""
        from airflow.sdk import BaseOperator
        from airflow.sdk.definitions.iterableoperator import IterableOperator

        with DAG("spread_downstream") as dag:

            @dag.task
            def work(value):
                return value

            spread = work.spread(across=2).iterate(value=[1, 2, 3])
            spread >> BaseOperator(task_id="downstream")

        unmapped = dag.task_dict["work"].unmap({})
        assert isinstance(unmapped, IterableOperator)
        assert unmapped.downstream_task_ids == {"downstream"}
        assert [task.task_id for task in unmapped.downstream_list] == ["downstream"]

    @pytest.mark.parametrize(
        ("values", "map_index", "expected"),
        [
            pytest.param([1, 2], 3, TaskInstanceState.SUCCESS, id="empty-share-of-a-short-input"),
            pytest.param([], 0, TaskInstanceState.SKIPPED, id="empty-input"),
        ],
    )
    def test_only_an_empty_input_skips_a_spread_instance(
        self, values, map_index, expected, run_ti: RunTI, mock_supervisor_comms
    ):
        """
        An instance left without items by a short input succeeds with nothing, as it did before
        ``.spread()`` knew skips: the task has items, just not for it, and skipping it would skip
        an ``all_success`` downstream task. Only an input with no items at all skips it, as it
        skips a mapped task.
        """
        with DAG("spread_empty_share") as dag:

            @dag.task
            def work(value):
                return value

            work.spread(across=4).iterate(value=values)

        assert run_ti(dag, "work", map_index) == expected

    @pytest.mark.parametrize("value", ["three", 1, 0, -1, None])
    def test_resolving_an_unusable_across_fails(self, value):
        from airflow.sdk.definitions.xcom_arg import PlainXComArg

        spread = self._dag().task_dict["get_relations"]
        with mock.patch.object(PlainXComArg, "resolve", return_value=value):
            with pytest.raises(ValueError, match="must be an integer of at least 2"):
                spread._resolve_spread_across({"ti": mock.Mock(map_index=0)})

    @staticmethod
    def _pushes(mock_supervisor_comms) -> list[SetXCom]:
        return [
            msg
            for call in [*mock_supervisor_comms.send.mock_calls, *mock_supervisor_comms.asend.mock_calls]
            if isinstance(msg := (call.kwargs.get("msg") or call.args[0]), SetXCom)
        ]

    def test_across_upstream_is_tagged_with_its_value_and_input_is_not(
        self, run_ti: RunTI, mock_supervisor_comms
    ):
        """The scheduler only reads metadata, so the count reaches it as the mapped_length of the counting
        task's push (the XCom row's mapped_length), while the input list stays untagged as for any spread iterate."""
        dag = self._dag()

        assert run_ti(dag, "count_instances", -1) == TaskInstanceState.SUCCESS
        assert [(push.task_id, push.mapped_length) for push in self._pushes(mock_supervisor_comms)] == [
            ("count_instances", 3)
        ]
        assert run_ti(dag, "get_ids", -1) == TaskInstanceState.SUCCESS
        assert [(push.task_id, push.mapped_length) for push in self._pushes(mock_supervisor_comms)] == [
            ("get_ids", None)
        ]

    @pytest.mark.parametrize("value", ["three", 1, 0, -1, None])
    def test_across_upstream_fails_on_an_unusable_value(self, value, run_ti: RunTI, mock_supervisor_comms):
        """0 leaves nothing to run and 1 is what .iterate() already is: both mean the counting task is wrong."""
        dag = self._dag(across_value=value)

        assert run_ti(dag, "count_instances", -1) == TaskInstanceState.FAILED
        assert not self._pushes(mock_supervisor_comms)


def _spread_a_skip_capable_operator(name: str, decorated: bool) -> None:
    from airflow.providers.standard.operators.python import BranchPythonOperator, ShortCircuitOperator
    from airflow.sdk import task

    def decide(x):
        return x

    if decorated:
        getattr(task, name)(decide).spread(across=2).iterate(x=[1, 2])
    elif name == "short_circuit":
        ShortCircuitOperator.partial(task_id=name).spread(across=2).iterate(python_callable=[lambda: True])
    else:
        BranchPythonOperator.partial(task_id=name).spread(across=2).iterate(python_callable=[lambda: True])


@pytest.mark.parametrize("name", ["short_circuit", "branch"])
@pytest.mark.parametrize("decorated", [False, True], ids=["classic", "decorated"])
def test_spread_refuses_operators_that_skip_downstream(decorated, name):
    """A spread instance iterates too, so a skip-capable operator is refused as by ``.iterate()``."""
    with DAG(f"spread_rejects_{decorated}_{name}"):
        with pytest.raises(TypeError, match="can skip downstream tasks and cannot be iterated"):
            _spread_a_skip_capable_operator(name, decorated)
