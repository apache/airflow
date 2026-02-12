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

from dataclasses import dataclass

import pytest

from airflow._shared.timezones import timezone
from airflow.models.taskgroupinstance import (
    collect_task_group_tis,
    iter_retryable_task_group_ids,
    iter_retryable_task_groups,
    iter_task_group_ancestors,
    iter_task_group_ids,
    should_task_group_retry,
)
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import DAG, TaskGroup
from airflow.sdk.definitions.taskgroup import evaluate_task_group_retry_condition
from airflow.utils.state import State, TaskInstanceState

from tests_common.test_utils.dag import create_scheduler_dag


@dataclass
class _DummyTI:
    task_id: str
    state: TaskInstanceState


def test_iter_task_group_helpers_with_nested_groups():
    with DAG(dag_id="test_iter_tg_helpers", schedule=None, start_date=timezone.datetime(2024, 1, 1)):
        with TaskGroup(group_id="outer", retries=0) as outer:
            with TaskGroup(group_id="inner", retries=2) as inner:
                EmptyOperator(task_id="task")

    assert [group.group_id for group in iter_task_group_ancestors(inner)] == [inner.group_id, outer.group_id]
    assert [group.group_id for group in iter_retryable_task_groups(inner)] == [inner.group_id]
    assert list(iter_task_group_ids(inner)) == [inner.group_id, outer.group_id]
    assert list(iter_retryable_task_group_ids(inner)) == [inner.group_id]


def test_iter_task_group_helpers_with_serialized_groups():
    with DAG(
        dag_id="test_iter_serialized_tg_helpers", schedule=None, start_date=timezone.datetime(2024, 1, 1)
    ):
        with TaskGroup(group_id="outer", retries=1) as outer:
            with TaskGroup(group_id="inner", retries=2) as inner:
                EmptyOperator(task_id="task")

    serialized_dag = create_scheduler_dag(inner.dag)
    serialized_inner = serialized_dag.task_group.get_task_group_dict()[inner.group_id]

    assert [group.group_id for group in iter_task_group_ancestors(serialized_inner)] == [
        inner.group_id,
        outer.group_id,
    ]
    assert [group.group_id for group in iter_retryable_task_groups(serialized_inner)] == [
        inner.group_id,
        outer.group_id,
    ]
    assert list(iter_task_group_ids(serialized_inner)) == [inner.group_id, outer.group_id]
    assert list(iter_retryable_task_group_ids(serialized_inner)) == [inner.group_id, outer.group_id]


def test_collect_task_group_tis_returns_expected_order():
    with DAG(dag_id="test_collect_tg_tis", schedule=None, start_date=timezone.datetime(2024, 1, 1)):
        with TaskGroup(group_id="group", retries=1) as group:
            task1 = EmptyOperator(task_id="task1")
            task2 = EmptyOperator(task_id="task2")

    ti_1_1 = _DummyTI(task_id=task1.task_id, state=TaskInstanceState.SUCCESS)
    ti_1_2 = _DummyTI(task_id=task1.task_id, state=TaskInstanceState.FAILED)
    ti_2_1 = _DummyTI(task_id=task2.task_id, state=TaskInstanceState.SUCCESS)

    tis_by_task_id = {
        task1.task_id: [ti_1_1, ti_1_2],
        task2.task_id: [ti_2_1],
    }
    assert collect_task_group_tis(group, tis_by_task_id) == [ti_1_1, ti_1_2, ti_2_1]


def test_collect_task_group_tis_returns_none_when_task_is_missing():
    with DAG(dag_id="test_collect_tg_tis_missing", schedule=None, start_date=timezone.datetime(2024, 1, 1)):
        with TaskGroup(group_id="group", retries=1) as group:
            task1 = EmptyOperator(task_id="task1")
            EmptyOperator(task_id="task2")

    tis_by_task_id = {
        task1.task_id: [_DummyTI(task_id=task1.task_id, state=TaskInstanceState.SUCCESS)],
    }
    assert collect_task_group_tis(group, tis_by_task_id) is None


def test_should_task_group_retry_supports_default_and_all_failed():
    with DAG(dag_id="test_should_retry_any_failed", schedule=None, start_date=timezone.datetime(2024, 1, 1)):
        with TaskGroup(group_id="group_any", retries=1) as group_any:
            EmptyOperator(task_id="task")

    one_failed = [
        _DummyTI(task_id="task1", state=TaskInstanceState.SUCCESS),
        _DummyTI(task_id="task2", state=TaskInstanceState.FAILED),
    ]
    assert should_task_group_retry(task_group=group_any, task_instances=one_failed) is True

    with DAG(dag_id="test_should_retry_all_failed", schedule=None, start_date=timezone.datetime(2024, 1, 1)):
        with TaskGroup(group_id="group_all", retries=1, retry_condition="all_failed") as group_all:
            EmptyOperator(task_id="task")

    assert should_task_group_retry(task_group=group_all, task_instances=one_failed) is False
    all_failed = [
        _DummyTI(task_id="task1", state=TaskInstanceState.FAILED),
        _DummyTI(task_id="task2", state=TaskInstanceState.UPSTREAM_FAILED),
    ]
    assert should_task_group_retry(task_group=group_all, task_instances=all_failed) is True


def test_should_task_group_retry_supports_custom_callable():
    callback_args = {}

    def retry_condition(task_instances, task_group_id, ti):
        callback_args["task_group_id"] = task_group_id
        callback_args["ti"] = ti
        failed = sum(task_instance.state in State.failed_states for task_instance in task_instances)
        return failed >= 2

    with DAG(dag_id="test_should_retry_custom", schedule=None, start_date=timezone.datetime(2024, 1, 1)):
        with TaskGroup(group_id="group_custom", retries=1, retry_condition=retry_condition) as group_custom:
            EmptyOperator(task_id="task")

    marker_ti = object()
    task_instances = [
        _DummyTI(task_id="task1", state=TaskInstanceState.SUCCESS),
        _DummyTI(task_id="task2", state=TaskInstanceState.FAILED),
        _DummyTI(task_id="task3", state=TaskInstanceState.UPSTREAM_FAILED),
    ]
    assert (
        should_task_group_retry(task_group=group_custom, task_instances=task_instances, ti=marker_ti) is True
    )
    assert callback_args["task_group_id"] == group_custom.group_id
    assert callback_args["ti"] is marker_ti


@pytest.mark.parametrize(
    ("condition", "states", "expected"),
    [
        pytest.param("any_failed", ["success", "failed"], True, id="any_failed-one_fails"),
        pytest.param("any_failed", ["success", "success"], False, id="any_failed-none_fails"),
        pytest.param("all_failed", ["failed", "upstream_failed"], True, id="all_failed-all_fail"),
        pytest.param("all_failed", ["success", "failed"], False, id="all_failed-partial"),
        pytest.param(None, ["success", "failed"], True, id="default-any_failed"),
        pytest.param(None, ["success", "success"], False, id="default-none_fails"),
    ],
)
def test_sdk_and_core_retry_condition_agree(condition, states, expected):
    """Both SDK and core retry condition implementations must produce identical results."""
    tis_for_core = [_DummyTI(task_id=f"t{i}", state=TaskInstanceState(s)) for i, s in enumerate(states)]

    @dataclass
    class _SDKStyleTI:
        state: str
        task_id: str = ""

    tis_for_sdk = [_SDKStyleTI(state=s, task_id=f"t{i}") for i, s in enumerate(states)]

    kwargs = {}
    if condition is not None:
        kwargs["retry_condition"] = condition

    with DAG(dag_id="test_cross_validate", schedule=None, start_date=timezone.datetime(2024, 1, 1)):
        with TaskGroup(group_id="grp", retries=1, **kwargs) as group:
            EmptyOperator(task_id="task")

    core_result = should_task_group_retry(task_group=group, task_instances=tis_for_core)
    sdk_result = evaluate_task_group_retry_condition(group.retry_condition, tis_for_sdk, task_group=group)
    assert core_result == expected
    assert sdk_result == expected, (
        f"SDK and core disagree: core={core_result}, sdk={sdk_result} "
        f"for condition={condition!r}, states={states}"
    )
