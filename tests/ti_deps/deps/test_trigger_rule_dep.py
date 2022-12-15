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

from datetime import datetime
from typing import Iterator

import pytest

from airflow.models.baseoperator import BaseOperator
from airflow.models.dagrun import DagRun
from airflow.models.taskinstance import TaskInstance
from airflow.operators.empty import EmptyOperator
from airflow.ti_deps.dep_context import DepContext
from airflow.ti_deps.deps.trigger_rule_dep import TriggerRuleDep, _UpstreamTIStates
from airflow.utils.state import DagRunState, TaskInstanceState
from airflow.utils.trigger_rule import TriggerRule


@pytest.fixture
def get_task_instance(monkeypatch, session, dag_maker):
    def _get_task_instance(
        trigger_rule: TriggerRule = TriggerRule.ALL_SUCCESS,
        *,
        success: int | list[str] = 0,
        skipped: int | list[str] = 0,
        failed: int | list[str] = 0,
        upstream_failed: int | list[str] = 0,
        removed: int | list[str] = 0,
        done: int = 0,
    ):
        with dag_maker(session=session):
            task = BaseOperator(
                task_id="test_task",
                trigger_rule=trigger_rule,
                start_date=datetime(2015, 1, 1),
            )
            for upstreams in (success, skipped, failed, upstream_failed, removed, done):
                if not isinstance(upstreams, int):
                    [EmptyOperator(task_id=task_id) for task_id in upstreams] >> task
        dr = dag_maker.create_dagrun()
        ti = dr.task_instances[0]
        ti.task = task

        fake_upstream_states = _UpstreamTIStates(
            success=(success if isinstance(success, int) else len(success)),
            skipped=(skipped if isinstance(skipped, int) else len(skipped)),
            failed=(failed if isinstance(failed, int) else len(failed)),
            upstream_failed=(upstream_failed if isinstance(upstream_failed, int) else len(upstream_failed)),
            removed=(removed if isinstance(removed, int) else len(removed)),
            done=done,
        )
        monkeypatch.setattr(_UpstreamTIStates, "calculate", lambda *_: fake_upstream_states)

        return ti

    return _get_task_instance


@pytest.fixture
def get_mapped_task_dagrun(session, dag_maker):
    def _get_dagrun(trigger_rule=TriggerRule.ALL_SUCCESS, state=TaskInstanceState.SUCCESS):
        from airflow.decorators import task

        @task
        def do_something(i):
            return 1

        @task(trigger_rule=trigger_rule)
        def do_something_else(i):
            return 1

        with dag_maker(dag_id="test_dag"):
            nums = do_something.expand(i=[i + 1 for i in range(5)])
            do_something_else.expand(i=nums)

        dr = dag_maker.create_dagrun()

        ti = dr.get_task_instance("do_something_else", session=session)
        ti.map_index = 0
        for map_index in range(1, 5):
            ti = TaskInstance(ti.task, run_id=dr.run_id, map_index=map_index)
            ti.dag_run = dr
            session.add(ti)
        session.flush()
        tis = dr.get_task_instances()
        for ti in tis:
            if ti.task_id == "do_something":
                if ti.map_index > 2:
                    ti.state = TaskInstanceState.REMOVED
                else:
                    ti.state = state
                session.merge(ti)
        session.commit()
        return dr, ti.task

    return _get_dagrun


class TestTriggerRuleDep:
    def test_no_upstream_tasks(self, get_task_instance):
        """
        If the TI has no upstream TIs then there is nothing to check and the dep is passed
        """
        ti = get_task_instance(TriggerRule.ALL_DONE)
        assert TriggerRuleDep().is_met(ti=ti)

    def test_always_tr(self, get_task_instance):
        """
        The always trigger rule should always pass this dep
        """
        ti = get_task_instance(TriggerRule.ALWAYS)
        assert TriggerRuleDep().is_met(ti=ti)

    def test_one_success_tr_success(self, session, get_task_instance):
        """
        One-success trigger rule success
        """
        ti = get_task_instance(
            TriggerRule.ONE_SUCCESS,
            success=1,
            skipped=2,
            failed=3,
            removed=0,
            upstream_failed=2,
            done=2,
        )
        dep_statuses = tuple(
            TriggerRuleDep()._evaluate_trigger_rule(
                ti=ti,
                dep_context=DepContext(flag_upstream_failed=False),
                session=session,
            )
        )
        assert len(dep_statuses) == 0

    def test_one_success_tr_failure(self, session, get_task_instance):
        """
        One-success trigger rule failure
        """
        ti = get_task_instance(
            TriggerRule.ONE_SUCCESS,
            success=0,
            skipped=2,
            failed=2,
            removed=0,
            upstream_failed=2,
            done=2,
        )
        dep_statuses = tuple(
            TriggerRuleDep()._evaluate_trigger_rule(
                ti=ti,
                dep_context=DepContext(flag_upstream_failed=False),
                session=session,
            )
        )
        assert len(dep_statuses) == 1
        assert not dep_statuses[0].passed

    def test_one_failure_tr_failure(self, session, get_task_instance):
        """
        One-failure trigger rule failure
        """
        ti = get_task_instance(
            TriggerRule.ONE_FAILED,
            success=2,
            skipped=0,
            failed=0,
            removed=0,
            upstream_failed=0,
            done=2,
        )
        dep_statuses = tuple(
            TriggerRuleDep()._evaluate_trigger_rule(
                ti=ti,
                dep_context=DepContext(flag_upstream_failed=False),
                session=session,
            )
        )
        assert len(dep_statuses) == 1
        assert not dep_statuses[0].passed

    def test_one_failure_tr_success(self, session, get_task_instance):
        """
        One-failure trigger rule success
        """
        ti = get_task_instance(
            TriggerRule.ONE_FAILED,
            success=0,
            skipped=2,
            failed=2,
            removed=0,
            upstream_failed=0,
            done=2,
        )
        dep_statuses = tuple(
            TriggerRuleDep()._evaluate_trigger_rule(
                ti=ti,
                dep_context=DepContext(flag_upstream_failed=False),
                session=session,
            )
        )
        assert len(dep_statuses) == 0

    def test_one_failure_tr_success_no_failed(self, session, get_task_instance):
        """
        One-failure trigger rule success
        """
        ti = get_task_instance(
            TriggerRule.ONE_FAILED,
            success=0,
            skipped=2,
            failed=0,
            removed=0,
            upstream_failed=2,
            done=2,
        )
        dep_statuses = tuple(
            TriggerRuleDep()._evaluate_trigger_rule(
                ti=ti,
                dep_context=DepContext(flag_upstream_failed=False),
                session=session,
            )
        )
        assert len(dep_statuses) == 0

    def test_one_done_tr_success(self, session, get_task_instance):
        """
        One-done trigger rule success
        """
        ti = get_task_instance(
            TriggerRule.ONE_DONE,
            success=2,
            skipped=0,
            failed=0,
            removed=0,
            upstream_failed=0,
            done=2,
        )
        dep_statuses = tuple(
            TriggerRuleDep()._evaluate_trigger_rule(
                ti=ti,
                dep_context=DepContext(flag_upstream_failed=False),
                session=session,
            )
        )
        assert len(dep_statuses) == 0

    def test_one_done_tr_success_with_failed(self, session, get_task_instance):
        """
        One-done trigger rule success
        """
        ti = get_task_instance(
            TriggerRule.ONE_DONE,
            success=0,
            skipped=0,
            failed=2,
            removed=0,
            upstream_failed=0,
            done=2,
        )
        dep_statuses = tuple(
            TriggerRuleDep()._evaluate_trigger_rule(
                ti=ti,
                dep_context=DepContext(flag_upstream_failed=False),
                session=session,
            )
        )
        assert len(dep_statuses) == 0

    def test_one_done_tr_skip(self, session, get_task_instance):
        """
        One-done trigger rule skip
        """
        ti = get_task_instance(
            TriggerRule.ONE_DONE,
            success=0,
            skipped=2,
            failed=0,
            removed=0,
            upstream_failed=0,
            done=2,
        )
        dep_statuses = tuple(
            TriggerRuleDep()._evaluate_trigger_rule(
                ti=ti,
                dep_context=DepContext(flag_upstream_failed=False),
                session=session,
            )
        )
        assert len(dep_statuses) == 1
        assert not dep_statuses[0].passed

    def test_one_done_tr_upstream_failed(self, session, get_task_instance):
        """
        One-done trigger rule upstream_failed
        """
        ti = get_task_instance(
            TriggerRule.ONE_DONE,
            success=0,
            skipped=0,
            failed=0,
            removed=0,
            upstream_failed=2,
            done=2,
        )
        dep_statuses = tuple(
            TriggerRuleDep()._evaluate_trigger_rule(
                ti=ti,
                dep_context=DepContext(flag_upstream_failed=False),
                session=session,
            )
        )
        assert len(dep_statuses) == 1
        assert not dep_statuses[0].passed

    def test_all_success_tr_success(self, session, get_task_instance):
        """
        All-success trigger rule success
        """
        ti = get_task_instance(
            TriggerRule.ALL_SUCCESS,
            success=["FakeTaskID"],
            skipped=0,
            failed=0,
            removed=0,
            upstream_failed=0,
            done=1,
        )
        dep_statuses = tuple(
            TriggerRuleDep()._evaluate_trigger_rule(
                ti=ti,
                dep_context=DepContext(flag_upstream_failed=False),
                session=session,
            )
        )
        assert len(dep_statuses) == 0

    def test_all_success_tr_failure(self, session, get_task_instance):
        """
        All-success trigger rule failure
        """
        ti = get_task_instance(
            TriggerRule.ALL_SUCCESS,
            success=["FakeTaskID"],
            skipped=0,
            failed=["OtherFakeTaskID"],
            removed=0,
            upstream_failed=0,
            done=2,
        )
        dep_statuses = tuple(
            TriggerRuleDep()._evaluate_trigger_rule(
                ti=ti,
                dep_context=DepContext(flag_upstream_failed=False),
                session=session,
            )
        )
        assert len(dep_statuses) == 1
        assert not dep_statuses[0].passed

    @pytest.mark.parametrize(
        "flag_upstream_failed, expected_ti_state",
        [(True, TaskInstanceState.SKIPPED), (False, None)],
    )
    def test_all_success_tr_skip(self, session, get_task_instance, flag_upstream_failed, expected_ti_state):
        """
        All-success trigger rule fails when some upstream tasks are skipped.
        """
        ti = get_task_instance(
            TriggerRule.ALL_SUCCESS,
            success=["FakeTaskID"],
            skipped=["OtherFakeTaskID"],
            failed=0,
            removed=0,
            upstream_failed=0,
            done=2,
        )
        dep_statuses = tuple(
            TriggerRuleDep()._evaluate_trigger_rule(
                ti=ti,
                dep_context=DepContext(flag_upstream_failed=flag_upstream_failed),
                session=session,
            )
        )
        assert len(dep_statuses) == 1
        assert not dep_statuses[0].passed
        assert ti.state == expected_ti_state

    @pytest.mark.parametrize("flag_upstream_failed", [True, False])
    def test_none_failed_tr_success(self, session, get_task_instance, flag_upstream_failed):
        """
        All success including skip trigger rule success
        """
        ti = get_task_instance(
            TriggerRule.NONE_FAILED,
            success=["FakeTaskID"],
            skipped=["OtherFakeTaskID"],
            failed=0,
            removed=0,
            upstream_failed=0,
            done=2,
        )
        dep_statuses = tuple(
            TriggerRuleDep()._evaluate_trigger_rule(
                ti=ti,
                dep_context=DepContext(flag_upstream_failed=flag_upstream_failed),
                session=session,
            )
        )
        assert len(dep_statuses) == 0
        assert ti.state is None

    def test_none_failed_tr_failure(self, session, get_task_instance):
        """
        All success including skip trigger rule failure
        """
        ti = get_task_instance(
            TriggerRule.NONE_FAILED,
            success=["FakeTaskID"],
            skipped=["OtherFakeTaskID"],
            failed=["FailedFakeTaskID"],
            removed=0,
            upstream_failed=0,
            done=3,
        )
        dep_statuses = tuple(
            TriggerRuleDep()._evaluate_trigger_rule(
                ti=ti,
                dep_context=DepContext(flag_upstream_failed=False),
                session=session,
            )
        )
        assert len(dep_statuses) == 1
        assert not dep_statuses[0].passed

    def test_none_failed_min_one_success_tr_success(self, session, get_task_instance):
        """
        All success including skip trigger rule success
        """
        ti = get_task_instance(
            TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
            success=["FakeTaskID"],
            skipped=["OtherFakeTaskID"],
            failed=0,
            removed=0,
            upstream_failed=0,
            done=2,
        )
        dep_statuses = tuple(
            TriggerRuleDep()._evaluate_trigger_rule(
                ti=ti,
                dep_context=DepContext(flag_upstream_failed=False),
                session=session,
            )
        )
        assert len(dep_statuses) == 0

    def test_none_failed_min_one_success_tr_skipped(self, session, get_task_instance):
        """
        All success including all upstream skips trigger rule success
        """
        ti = get_task_instance(
            TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
            success=0,
            skipped=["FakeTaskID", "OtherFakeTaskID"],
            failed=0,
            removed=0,
            upstream_failed=0,
            done=2,
        )
        dep_statuses = tuple(
            TriggerRuleDep()._evaluate_trigger_rule(
                ti=ti,
                dep_context=DepContext(flag_upstream_failed=True),
                session=session,
            )
        )
        assert len(dep_statuses) == 0
        assert ti.state == TaskInstanceState.SKIPPED

    def test_none_failed_min_one_success_tr_failure(self, session, get_task_instance):
        """
        All success including skip trigger rule failure
        """
        ti = get_task_instance(
            TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
            success=["FakeTaskID"],
            skipped=["OtherFakeTaskID"],
            failed=["FailedFakeTaskID"],
            removed=0,
            upstream_failed=0,
            done=3,
        )
        dep_statuses = tuple(
            TriggerRuleDep()._evaluate_trigger_rule(
                ti=ti,
                dep_context=DepContext(flag_upstream_failed=False),
                session=session,
            )
        )
        assert len(dep_statuses) == 1
        assert not dep_statuses[0].passed

    def test_all_failed_tr_success(self, session, get_task_instance):
        """
        All-failed trigger rule success
        """
        ti = get_task_instance(
            TriggerRule.ALL_FAILED,
            success=0,
            skipped=0,
            failed=["FakeTaskID", "OtherFakeTaskID"],
            removed=0,
            upstream_failed=0,
            done=2,
        )
        dep_statuses = tuple(
            TriggerRuleDep()._evaluate_trigger_rule(
                ti=ti,
                dep_context=DepContext(flag_upstream_failed=False),
                session=session,
            )
        )
        assert len(dep_statuses) == 0

    def test_all_failed_tr_failure(self, session, get_task_instance):
        """
        All-failed trigger rule failure
        """
        ti = get_task_instance(
            TriggerRule.ALL_FAILED,
            success=["FakeTaskID", "OtherFakeTaskID"],
            skipped=0,
            failed=0,
            removed=0,
            upstream_failed=0,
            done=2,
        )
        dep_statuses = tuple(
            TriggerRuleDep()._evaluate_trigger_rule(
                ti=ti,
                dep_context=DepContext(flag_upstream_failed=False),
                session=session,
            )
        )
        assert len(dep_statuses) == 1
        assert not dep_statuses[0].passed

    def test_all_done_tr_success(self, session, get_task_instance):
        """
        All-done trigger rule success
        """
        ti = get_task_instance(
            TriggerRule.ALL_DONE,
            success=["FakeTaskID", "OtherFakeTaskID"],
            skipped=0,
            failed=0,
            removed=0,
            upstream_failed=0,
            done=2,
        )
        dep_statuses = tuple(
            TriggerRuleDep()._evaluate_trigger_rule(
                ti=ti,
                dep_context=DepContext(flag_upstream_failed=False),
                session=session,
            )
        )
        assert len(dep_statuses) == 0

    def test_all_skipped_tr_failure(self, session, get_task_instance):
        """
        All-skipped trigger rule failure
        """
        ti = get_task_instance(
            TriggerRule.ALL_SKIPPED,
            success=["FakeTaskID"],
            skipped=0,
            failed=0,
            removed=0,
            upstream_failed=0,
            done=1,
        )
        dep_statuses = tuple(
            TriggerRuleDep()._evaluate_trigger_rule(
                ti=ti,
                dep_context=DepContext(flag_upstream_failed=False),
                session=session,
            )
        )
        assert len(dep_statuses) == 1
        assert not dep_statuses[0].passed

    @pytest.mark.parametrize("flag_upstream_failed", [True, False])
    def test_all_skipped_tr_success(self, session, get_task_instance, flag_upstream_failed):
        """
        All-skipped trigger rule success
        """
        ti = get_task_instance(
            TriggerRule.ALL_SKIPPED,
            success=0,
            skipped=["FakeTaskID", "OtherFakeTaskID", "FailedFakeTaskID"],
            failed=0,
            removed=0,
            upstream_failed=0,
            done=3,
        )
        dep_statuses = tuple(
            TriggerRuleDep()._evaluate_trigger_rule(
                ti=ti,
                dep_context=DepContext(flag_upstream_failed=flag_upstream_failed),
                session=session,
            )
        )
        assert len(dep_statuses) == 0

    def test_all_done_tr_failure(self, session, get_task_instance):
        """
        All-done trigger rule failure
        """
        ti = get_task_instance(
            TriggerRule.ALL_DONE,
            success=["FakeTaskID"],
            skipped=0,
            failed=0,
            removed=0,
            upstream_failed=0,
            done=1,
        )
        EmptyOperator(task_id="OtherFakeTeakID", dag=ti.task.dag) >> ti.task  # An unfinished upstream.

        dep_statuses = tuple(
            TriggerRuleDep()._evaluate_trigger_rule(
                ti=ti,
                dep_context=DepContext(flag_upstream_failed=False),
                session=session,
            )
        )
        assert len(dep_statuses) == 1
        assert not dep_statuses[0].passed

    @pytest.mark.parametrize("flag_upstream_failed", [True, False])
    def test_none_skipped_tr_success(self, session, get_task_instance, flag_upstream_failed):
        """
        None-skipped trigger rule success
        """
        ti = get_task_instance(
            TriggerRule.NONE_SKIPPED,
            success=["FakeTaskID", "OtherFakeTaskID"],
            skipped=0,
            failed=["FailedFakeTaskID"],
            removed=0,
            upstream_failed=0,
            done=3,
        )
        dep_statuses = tuple(
            TriggerRuleDep()._evaluate_trigger_rule(
                ti=ti,
                dep_context=DepContext(flag_upstream_failed=flag_upstream_failed),
                session=session,
            )
        )
        assert len(dep_statuses) == 0

    @pytest.mark.parametrize("flag_upstream_failed", [True, False])
    def test_none_skipped_tr_failure(self, session, get_task_instance, flag_upstream_failed):
        """
        None-skipped trigger rule failure
        """
        ti = get_task_instance(
            TriggerRule.NONE_SKIPPED,
            success=["FakeTaskID"],
            skipped=["SkippedTaskID"],
            failed=0,
            removed=0,
            upstream_failed=0,
            done=2,
        )
        dep_statuses = tuple(
            TriggerRuleDep()._evaluate_trigger_rule(
                ti=ti,
                dep_context=DepContext(flag_upstream_failed=flag_upstream_failed),
                session=session,
            )
        )
        assert len(dep_statuses) == 1
        assert not dep_statuses[0].passed

    def test_none_skipped_tr_failure_empty(self, session, get_task_instance):
        """
        None-skipped trigger rule fails until all upstream tasks have completed execution
        """
        ti = get_task_instance(
            TriggerRule.NONE_SKIPPED,
            success=0,
            skipped=0,
            failed=0,
            removed=0,
            upstream_failed=0,
            done=0,
        )
        EmptyOperator(task_id="FakeTeakID", dag=ti.task.dag) >> ti.task  # An unfinished upstream.

        dep_statuses = tuple(
            TriggerRuleDep()._evaluate_trigger_rule(
                ti=ti,
                dep_context=DepContext(flag_upstream_failed=False),
                session=session,
            )
        )
        assert len(dep_statuses) == 1
        assert not dep_statuses[0].passed

    def test_unknown_tr(self, session, get_task_instance):
        """
        Unknown trigger rules should cause this dep to fail
        """
        ti = get_task_instance(
            TriggerRule.DUMMY,
            success=1,
            skipped=0,
            failed=0,
            removed=0,
            upstream_failed=0,
            done=1,
        )
        ti.task.trigger_rule = "Unknown Trigger Rule"

        dep_statuses = tuple(
            TriggerRuleDep()._evaluate_trigger_rule(
                ti=ti,
                dep_context=DepContext(flag_upstream_failed=False),
                session=session,
            )
        )
        assert len(dep_statuses) == 1
        assert not dep_statuses[0].passed

    def test_UpstreamTIStates(self, session, dag_maker):
        """
        this test tests the helper class '_UpstreamTIStates' as a unit and inside update_state
        """
        with dag_maker(session=session):
            op1 = EmptyOperator(task_id="op1")
            op2 = EmptyOperator(task_id="op2")
            op3 = EmptyOperator(task_id="op3")
            op4 = EmptyOperator(task_id="op4")
            op5 = EmptyOperator(task_id="op5", trigger_rule=TriggerRule.ONE_FAILED)

            op1 >> (op2, op3) >> op4
            (op2, op3, op4) >> op5

        dr = dag_maker.create_dagrun()
        tis = {ti.task_id: ti for ti in dr.task_instances}

        tis["op1"].state = TaskInstanceState.SUCCESS
        tis["op2"].state = TaskInstanceState.FAILED
        tis["op3"].state = TaskInstanceState.SUCCESS
        tis["op4"].state = TaskInstanceState.SUCCESS
        tis["op5"].state = TaskInstanceState.SUCCESS

        def _get_finished_tis(task_id: str) -> Iterator[TaskInstance]:
            return (ti for ti in tis.values() if ti.task_id in tis[task_id].task.upstream_task_ids)

        # check handling with cases that tasks are triggered from backfill with no finished tasks
        assert _UpstreamTIStates.calculate(_get_finished_tis("op2")) == (1, 0, 0, 0, 0, 1)
        assert _UpstreamTIStates.calculate(_get_finished_tis("op4")) == (1, 0, 1, 0, 0, 2)
        assert _UpstreamTIStates.calculate(_get_finished_tis("op5")) == (2, 0, 1, 0, 0, 3)

        dr.update_state(session=session)
        assert dr.state == DagRunState.SUCCESS

    def test_mapped_task_upstream_removed_with_all_success_trigger_rules(
        self,
        monkeypatch,
        session,
        get_mapped_task_dagrun,
    ):
        """
        Test ALL_SUCCESS trigger rule with mapped task upstream removed
        """
        dr, task = get_mapped_task_dagrun()

        # ti with removed upstream ti
        ti = dr.get_task_instance(task_id="do_something_else", map_index=3, session=session)
        ti.task = task

        upstream_states = _UpstreamTIStates(
            success=3,
            skipped=0,
            failed=0,
            removed=2,
            upstream_failed=0,
            done=5,
        )
        monkeypatch.setattr(_UpstreamTIStates, "calculate", lambda *_: upstream_states)

        dep_statuses = tuple(
            TriggerRuleDep()._evaluate_trigger_rule(
                ti=ti,
                # Marks the task as removed if upstream is removed.
                dep_context=DepContext(flag_upstream_failed=True),
                session=session,
            )
        )
        assert len(dep_statuses) == 0
        assert ti.state == TaskInstanceState.REMOVED

    def test_mapped_task_upstream_removed_with_all_failed_trigger_rules(
        self,
        monkeypatch,
        session,
        get_mapped_task_dagrun,
    ):
        """
        Test ALL_FAILED trigger rule with mapped task upstream removed
        """

        dr, task = get_mapped_task_dagrun(trigger_rule=TriggerRule.ALL_FAILED, state=TaskInstanceState.FAILED)

        # ti with removed upstream ti
        ti = dr.get_task_instance(task_id="do_something_else", map_index=3, session=session)
        ti.task = task

        upstream_states = _UpstreamTIStates(
            success=0,
            skipped=0,
            failed=3,
            removed=2,
            upstream_failed=0,
            done=5,
        )
        monkeypatch.setattr(_UpstreamTIStates, "calculate", lambda *_: upstream_states)

        dep_statuses = tuple(
            TriggerRuleDep()._evaluate_trigger_rule(
                ti=ti,
                dep_context=DepContext(flag_upstream_failed=False),
                session=session,
            )
        )

        assert len(dep_statuses) == 0

    @pytest.mark.parametrize(
        "trigger_rule",
        [TriggerRule.NONE_FAILED, TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS],
    )
    def test_mapped_task_upstream_removed_with_none_failed_trigger_rules(
        self,
        monkeypatch,
        session,
        get_mapped_task_dagrun,
        trigger_rule,
    ):
        """
        Test NONE_FAILED trigger rule with mapped task upstream removed
        """
        dr, task = get_mapped_task_dagrun(trigger_rule=trigger_rule)

        # ti with removed upstream ti
        ti = dr.get_task_instance(task_id="do_something_else", map_index=3, session=session)
        ti.task = task

        upstream_states = _UpstreamTIStates(
            success=3,
            skipped=0,
            failed=0,
            removed=2,
            upstream_failed=0,
            done=5,
        )
        monkeypatch.setattr(_UpstreamTIStates, "calculate", lambda *_: upstream_states)

        dep_statuses = tuple(
            TriggerRuleDep()._evaluate_trigger_rule(
                ti=ti,
                dep_context=DepContext(flag_upstream_failed=False),
                session=session,
            )
        )

        assert len(dep_statuses) == 0


def test_upstream_in_mapped_group_triggers_only_relevant(dag_maker, session):
    from airflow.decorators import task, task_group

    with dag_maker(session=session):

        @task
        def t(x):
            return x

        @task_group
        def tg(x):
            t1 = t.override(task_id="t1")(x=x)
            return t.override(task_id="t2")(x=t1)

        t2 = tg.expand(x=[1, 2, 3])
        t.override(task_id="t3")(x=t2)

    dr: DagRun = dag_maker.create_dagrun()

    def _one_scheduling_decision_iteration() -> dict[tuple[str, int], TaskInstance]:
        decision = dr.task_instance_scheduling_decisions(session=session)
        return {(ti.task_id, ti.map_index): ti for ti in decision.schedulable_tis}

    # Initial decision.
    tis = _one_scheduling_decision_iteration()
    assert sorted(tis) == [("tg.t1", 0), ("tg.t1", 1), ("tg.t1", 2)]

    # After running the first t1, the first t2 becomes immediately available.
    tis["tg.t1", 0].run()
    tis = _one_scheduling_decision_iteration()
    assert sorted(tis) == [("tg.t1", 1), ("tg.t1", 2), ("tg.t2", 0)]

    # Similarly for the subsequent t2 instances.
    tis["tg.t1", 2].run()
    tis = _one_scheduling_decision_iteration()
    assert sorted(tis) == [("tg.t1", 1), ("tg.t2", 0), ("tg.t2", 2)]

    # But running t2 partially does not make t3 available.
    tis["tg.t1", 1].run()
    tis["tg.t2", 0].run()
    tis["tg.t2", 2].run()
    tis = _one_scheduling_decision_iteration()
    assert sorted(tis) == [("tg.t2", 1)]

    # Only after all t2 instances are run does t3 become available.
    tis["tg.t2", 1].run()
    tis = _one_scheduling_decision_iteration()
    assert sorted(tis) == [("t3", -1)]
