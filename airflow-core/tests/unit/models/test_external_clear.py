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

from datetime import timedelta

import pytest

from airflow._shared.timezones import timezone
from airflow.models.dagbag import DBDagBag
from airflow.models.external_clear import (
    EXTERNAL_CLEAR_TARGET_XCOM_KEY as CORE_RECORD_KEY,
    ExternalClearDepthExceeded,
    find_external_clear_task_instances,
)
from airflow.models.xcom import XComModel
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import ExternalClearTarget
from airflow.sdk.definitions.external_clear import EXTERNAL_CLEAR_TARGET_XCOM_KEY
from airflow.utils.state import TaskInstanceState

pytestmark = pytest.mark.db_test

DATE = timezone.datetime(2026, 10, 1)


def _record(ti, target: ExternalClearTarget, session) -> None:
    """Store a target the way the execution API stores the task runner's push."""
    XComModel.set_for_attempt(
        task_instance_id=ti.id,
        key=EXTERNAL_CLEAR_TARGET_XCOM_KEY,
        value=target.serialize(),
        serialize=False,
        session=session,
    )
    session.flush()


def _keys(tis) -> set[tuple[str, str, str]]:
    return {(ti.dag_id, ti.run_id, ti.task_id) for ti in tis}


def _make_child(dag_maker, dag_id: str, *logical_dates):
    """A Dag ``prep >> wait >> load`` with one run per logical date."""
    with dag_maker(dag_id):
        EmptyOperator(task_id="prep") >> EmptyOperator(task_id="wait") >> EmptyOperator(task_id="load")
    return [dag_maker.create_dagrun(run_id=f"run_{d:%H}", logical_date=d) for d in logical_dates]


def _make_parent(dag_maker, dag_id: str, logical_date):
    with dag_maker(dag_id):
        EmptyOperator(task_id="mark")
    return dag_maker.create_dagrun(logical_date=logical_date).get_task_instance("mark")


def test_follows_target_into_downstream_of_target_task(dag_maker, session):
    (child_run,) = _make_child(dag_maker, "child", DATE)
    mark = _make_parent(dag_maker, "parent", DATE)
    _record(mark, ExternalClearTarget(dag_id="child", task_id="wait", logical_date=DATE), session)

    found = find_external_clear_task_instances([mark], dag_bag=DBDagBag(), session=session)

    assert _keys(found) == {("child", child_run.run_id, "wait"), ("child", child_run.run_id, "load")}


def test_uses_recorded_logical_date_not_the_parent_run_date(dag_maker, session):
    parent_date = DATE + timedelta(hours=5)
    midnight_run, _same_hour_run = _make_child(dag_maker, "child", DATE, parent_date)
    mark = _make_parent(dag_maker, "parent", parent_date)
    _record(mark, ExternalClearTarget(dag_id="child", task_id="wait", logical_date=DATE), session)

    found = find_external_clear_task_instances([mark], dag_bag=DBDagBag(), session=session)

    assert {ti.run_id for ti in found} == {midnight_run.run_id}


def test_follows_targets_recorded_by_the_target_dag(dag_maker, session):
    (grandchild_run,) = _make_child(dag_maker, "grandchild", DATE)
    (child_run,) = _make_child(dag_maker, "child", DATE)
    _record(
        child_run.get_task_instance("load"),
        ExternalClearTarget(dag_id="grandchild", task_id="wait", logical_date=DATE),
        session,
    )
    mark = _make_parent(dag_maker, "parent", DATE)
    _record(mark, ExternalClearTarget(dag_id="child", task_id="wait", logical_date=DATE), session)

    found = find_external_clear_task_instances([mark], dag_bag=DBDagBag(), session=session)

    assert ("grandchild", grandchild_run.run_id, "load") in _keys(found)
    assert len(found) == 4


def test_raises_when_targets_go_deeper_than_first_recursion_depth(dag_maker, session):
    _make_child(dag_maker, "grandchild", DATE)
    (child_run,) = _make_child(dag_maker, "child", DATE)
    _record(
        child_run.get_task_instance("load"),
        ExternalClearTarget(dag_id="grandchild", task_id="wait", logical_date=DATE),
        session,
    )
    mark = _make_parent(dag_maker, "parent", DATE)
    _record(
        mark,
        ExternalClearTarget(dag_id="child", task_id="wait", logical_date=DATE, recursion_depth=1),
        session,
    )

    with pytest.raises(ExternalClearDepthExceeded, match="more than 1 levels"):
        find_external_clear_task_instances([mark], dag_bag=DBDagBag(), session=session)


def test_stops_on_a_cycle_back_to_already_selected_task_instances(dag_maker, session):
    (child_run,) = _make_child(dag_maker, "child", DATE)
    mark = _make_parent(dag_maker, "parent", DATE)
    _record(mark, ExternalClearTarget(dag_id="child", task_id="wait", logical_date=DATE), session)
    _record(
        child_run.get_task_instance("load"),
        ExternalClearTarget(dag_id="parent", task_id="mark", logical_date=DATE),
        session,
    )

    found = find_external_clear_task_instances([mark], dag_bag=DBDagBag(), session=session)

    assert _keys(found) == {("child", child_run.run_id, "wait"), ("child", child_run.run_id, "load")}


def test_task_without_a_recorded_target_pulls_in_nothing(dag_maker, session):
    _make_child(dag_maker, "child", DATE)
    mark = _make_parent(dag_maker, "parent", DATE)

    assert find_external_clear_task_instances([mark], dag_bag=DBDagBag(), session=session) == []


def test_only_failed_keeps_successful_target_task_instances(dag_maker, session):
    (child_run,) = _make_child(dag_maker, "child", DATE)
    child_run.get_task_instance("wait", session=session).state = TaskInstanceState.SUCCESS
    child_run.get_task_instance("load", session=session).state = TaskInstanceState.FAILED
    session.flush()
    mark = _make_parent(dag_maker, "parent", DATE)
    _record(mark, ExternalClearTarget(dag_id="child", task_id="wait", logical_date=DATE), session)

    found = find_external_clear_task_instances([mark], dag_bag=DBDagBag(), only_failed=True, session=session)

    assert _keys(found) == {("child", child_run.run_id, "load")}


def test_core_reads_the_key_the_task_sdk_writes():
    assert CORE_RECORD_KEY == EXTERNAL_CLEAR_TARGET_XCOM_KEY
