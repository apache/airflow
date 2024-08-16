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
from typing import TYPE_CHECKING, Iterator
from unittest import mock
from unittest.mock import Mock

import pytest

from airflow.decorators import task, task_group
from airflow.models.baseoperator import BaseOperator
from airflow.models.taskinstance import TaskInstance
from airflow.operators.empty import EmptyOperator
from airflow.ti_deps.dep_context import DepContext
from airflow.ti_deps.deps.trigger_rule_dep import TriggerRuleDep, _UpstreamTIStates
from airflow.utils.state import DagRunState, TaskInstanceState
from airflow.utils.trigger_rule import TriggerRule

pytestmark = [pytest.mark.db_test, pytest.mark.skip_if_database_isolation_mode]

if TYPE_CHECKING:
    from sqlalchemy.orm.session import Session

    from airflow.models.dagrun import DagRun

SKIPPED = TaskInstanceState.SKIPPED
UPSTREAM_FAILED = TaskInstanceState.UPSTREAM_FAILED
REMOVED = TaskInstanceState.REMOVED
SUCCESS = TaskInstanceState.SUCCESS
FAILED = TaskInstanceState.FAILED


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
        skipped_setup: int = 0,
        success_setup: int = 0,
        normal_tasks: list[str] | None = None,
        setup_tasks: list[str] | None = None,
    ):
        with dag_maker(session=session):
            task = BaseOperator(
                task_id="test_task",
                trigger_rule=trigger_rule,
                start_date=datetime(2015, 1, 1),
            )
            for task_id in normal_tasks or []:
                EmptyOperator(task_id=task_id) >> task
            for task_id in setup_tasks or []:
                EmptyOperator(task_id=task_id).as_setup() >> task
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
            skipped_setup=skipped_setup,
            success_setup=success_setup,
        )
        monkeypatch.setattr(_UpstreamTIStates, "calculate", lambda *_: fake_upstream_states)

        return ti

    return _get_task_instance


@pytest.fixture
def get_mapped_task_dagrun(session, dag_maker):
    def _get_dagrun(trigger_rule=TriggerRule.ALL_SUCCESS, state=SUCCESS, add_setup_tasks: bool = False):
        from airflow.decorators import task

        @task
        def setup_1(i):
            return 1

        @task
        def setup_2(i):
            return 1

        @task
        def setup_3(i):
            return 1

        @task
        def do_something(i):
            return 1

        @task(trigger_rule=trigger_rule)
        def do_something_else(i):
            return 1

        with dag_maker(dag_id="test_dag"):
            nums = do_something.expand(i=[i + 1 for i in range(5)])
            do_something_else.expand(i=nums)
            if add_setup_tasks:
                setup_nums = setup_1.expand(i=[i + 1 for i in range(5)])
                setup_more_nums = setup_2.expand(i=setup_nums)
                setup_other_nums = setup_3.expand(i=setup_more_nums)
                setup_more_nums.as_setup() >> nums
                setup_nums.as_setup() >> nums
                setup_other_nums.as_setup() >> nums

        dr = dag_maker.create_dagrun()

        def _expand_tasks(task_instance: str, upstream: str) -> BaseOperator | None:
            ti = dr.get_task_instance(task_instance, session=session)
            ti.map_index = 0
            for map_index in range(1, 5):
                ti = TaskInstance(ti.task, run_id=dr.run_id, map_index=map_index)
                session.add(ti)
                ti.dag_run = dr
            session.flush()
            tis = dr.get_task_instances(session=session)
            for ti in tis:
                if ti.task_id == upstream:
                    if ti.map_index > 2:
                        ti.state = REMOVED
                    else:
                        ti.state = state
                    session.merge(ti)
            return ti.task

        do_task = _expand_tasks("do_something_else", "do_something")
        if add_setup_tasks:
            _expand_tasks("setup_2", "setup_1")
            setup_task = _expand_tasks("setup_3", "setup_2")
        else:
            setup_task = None
        session.commit()
        return dr, do_task, setup_task

    return _get_dagrun


class TestTriggerRuleDep:
    def test_no_upstream_tasks(self, session, get_task_instance):
        """
        If the TI has no upstream TIs then there is nothing to check and the dep is passed
        """
        ti = get_task_instance(TriggerRule.ALL_DONE)
        dep_statuses = tuple(
            TriggerRuleDep().get_dep_statuses(ti=ti, dep_context=DepContext(), session=session)
        )
        assert len(dep_statuses) == 1
        assert dep_statuses[0].passed
        assert dep_statuses[0].reason == "The task instance did not have any upstream tasks."

    def test_always_tr(self, session, get_task_instance):
        """
        The always trigger rule should always pass this dep
        """
        ti = get_task_instance(TriggerRule.ALWAYS, normal_tasks=["a"])

        dep_statuses = tuple(
            TriggerRuleDep().get_dep_statuses(ti=ti, dep_context=DepContext(), session=session)
        )
        assert len(dep_statuses) == 1
        assert dep_statuses[0].passed
        assert dep_statuses[0].reason == "The task had a always trigger rule set."

    @pytest.mark.parametrize("flag_upstream_failed, expected_ti_state", [(True, SKIPPED), (False, None)])
    def test_one_success_tr_success(
        self, session, get_task_instance, flag_upstream_failed, expected_ti_state
    ):
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
        _test_trigger_rule(
            ti=ti,
            session=session,
            flag_upstream_failed=flag_upstream_failed,
            expected_ti_state=expected_ti_state,
        )

    @pytest.mark.parametrize(
        "flag_upstream_failed, expected_ti_state", [(True, UPSTREAM_FAILED), (False, None)]
    )
    def test_one_success_tr_failure(
        self, session, get_task_instance, flag_upstream_failed, expected_ti_state
    ):
        """
        One-success trigger rule failure
        """
        ti = get_task_instance(
            TriggerRule.ONE_SUCCESS,
            success=0,
            skipped=1,
            failed=1,
            removed=1,
            upstream_failed=1,
            done=4,
        )
        _test_trigger_rule(
            ti=ti,
            session=session,
            flag_upstream_failed=flag_upstream_failed,
            expected_reason="requires one upstream task success, but none were found.",
            expected_ti_state=expected_ti_state,
        )

    @pytest.mark.parametrize("flag_upstream_failed, expected_ti_state", [(True, SKIPPED), (False, None)])
    def test_one_success_tr_failure_all_skipped(
        self, session, get_task_instance, flag_upstream_failed, expected_ti_state
    ):
        """
        One-success trigger rule failure and all are skipped
        """
        ti = get_task_instance(
            TriggerRule.ONE_SUCCESS,
            success=0,
            skipped=2,
            failed=0,
            removed=0,
            upstream_failed=0,
            done=2,
        )
        _test_trigger_rule(
            ti=ti,
            session=session,
            flag_upstream_failed=flag_upstream_failed,
            expected_reason="requires one upstream task success, but none were found.",
            expected_ti_state=expected_ti_state,
        )

    @pytest.mark.parametrize("flag_upstream_failed, expected_ti_state", [(True, SKIPPED), (False, None)])
    def test_one_failure_tr_failure(
        self, session, get_task_instance, flag_upstream_failed, expected_ti_state
    ):
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
        _test_trigger_rule(
            ti=ti,
            session=session,
            flag_upstream_failed=flag_upstream_failed,
            expected_reason="requires one upstream task failure, but none were found.",
            expected_ti_state=expected_ti_state,
        )

    @pytest.mark.parametrize("flag_upstream_failed", [True, False])
    def test_one_failure_tr_success(self, session, get_task_instance, flag_upstream_failed):
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
        _test_trigger_rule(ti=ti, session=session, flag_upstream_failed=flag_upstream_failed)

    @pytest.mark.parametrize("flag_upstream_failed", [True, False])
    def test_one_failure_tr_success_no_failed(self, session, get_task_instance, flag_upstream_failed):
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
        _test_trigger_rule(ti=ti, session=session, flag_upstream_failed=flag_upstream_failed)

    @pytest.mark.parametrize("flag_upstream_failed", [True, False])
    def test_one_done_tr_success(self, session, get_task_instance, flag_upstream_failed):
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
        _test_trigger_rule(ti=ti, session=session, flag_upstream_failed=flag_upstream_failed)

    @pytest.mark.parametrize("flag_upstream_failed", [True, False])
    def test_one_done_tr_success_with_failed(self, session, get_task_instance, flag_upstream_failed):
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
        _test_trigger_rule(ti=ti, session=session, flag_upstream_failed=flag_upstream_failed)

    @pytest.mark.parametrize("flag_upstream_failed, expected_ti_state", [(True, SKIPPED), (False, None)])
    def test_one_done_tr_skip(self, session, get_task_instance, flag_upstream_failed, expected_ti_state):
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
        _test_trigger_rule(
            ti=ti,
            session=session,
            flag_upstream_failed=flag_upstream_failed,
            expected_reason="requires at least one upstream task failure or success but none",
            expected_ti_state=expected_ti_state,
        )

    @pytest.mark.parametrize("flag_upstream_failed, expected_ti_state", [(True, SKIPPED), (False, None)])
    def test_one_done_tr_upstream_failed(
        self, session, get_task_instance, flag_upstream_failed, expected_ti_state
    ):
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
        _test_trigger_rule(
            ti=ti,
            session=session,
            flag_upstream_failed=flag_upstream_failed,
            expected_reason="requires at least one upstream task failure or success but none",
            expected_ti_state=expected_ti_state,
        )

    @pytest.mark.parametrize("flag_upstream_failed", [True, False])
    def test_all_success_tr_success(self, session, get_task_instance, flag_upstream_failed):
        """
        All-success trigger rule success
        """
        ti = get_task_instance(
            TriggerRule.ALL_SUCCESS,
            success=1,
            skipped=0,
            failed=0,
            removed=0,
            upstream_failed=0,
            done=1,
            normal_tasks=["FakeTaskID"],
        )
        _test_trigger_rule(ti=ti, session=session, flag_upstream_failed=flag_upstream_failed)

    @pytest.mark.parametrize(
        "flag_upstream_failed, expected_ti_state", [(True, UPSTREAM_FAILED), (False, None)]
    )
    def test_all_success_tr_failure(
        self, session, get_task_instance, flag_upstream_failed, expected_ti_state
    ):
        """
        All-success trigger rule failure
        """
        ti = get_task_instance(
            TriggerRule.ALL_SUCCESS,
            success=1,
            skipped=0,
            failed=1,
            removed=0,
            upstream_failed=0,
            done=2,
            normal_tasks=["FakeTaskID", "OtherFakeTaskID"],
        )
        _test_trigger_rule(
            ti=ti,
            session=session,
            flag_upstream_failed=flag_upstream_failed,
            expected_reason="requires all upstream tasks to have succeeded, but found 1",
            expected_ti_state=expected_ti_state,
        )

    @pytest.mark.parametrize("flag_upstream_failed, expected_ti_state", [(True, SKIPPED), (False, None)])
    def test_all_success_tr_skip(self, session, get_task_instance, flag_upstream_failed, expected_ti_state):
        """
        All-success trigger rule fails when some upstream tasks are skipped.
        """
        ti = get_task_instance(
            TriggerRule.ALL_SUCCESS,
            success=1,
            skipped=1,
            failed=0,
            removed=0,
            upstream_failed=0,
            done=2,
            normal_tasks=["FakeTaskID", "OtherFakeTaskID"],
        )
        _test_trigger_rule(
            ti=ti,
            session=session,
            flag_upstream_failed=flag_upstream_failed,
            expected_reason="requires all upstream tasks to have succeeded, but found 1",
            expected_ti_state=expected_ti_state,
        )

    @pytest.mark.parametrize("flag_upstream_failed", [True, False])
    def test_all_success_tr_skip_wait_for_past_depends_before_skipping(
        self, session, get_task_instance, flag_upstream_failed
    ):
        """
        All-success trigger rule fails when some upstream tasks are skipped. The state of the ti
        should not be set to SKIPPED when flag_upstream_failed is True and
        wait_for_past_depends_before_skipping is True and the past depends are not met.
        """
        ti = get_task_instance(
            TriggerRule.ALL_SUCCESS,
            success=1,
            skipped=1,
            failed=0,
            removed=0,
            upstream_failed=0,
            done=2,
            normal_tasks=["FakeTaskID", "OtherFakeTaskID"],
        )
        ti.task.xcom_pull.return_value = None
        xcom_mock = Mock(return_value=None)
        with mock.patch("airflow.models.taskinstance.TaskInstance.xcom_pull", xcom_mock):
            _test_trigger_rule(
                ti=ti,
                session=session,
                flag_upstream_failed=flag_upstream_failed,
                wait_for_past_depends_before_skipping=True,
                expected_reason=(
                    "Task should be skipped but the past depends are not met"
                    if flag_upstream_failed
                    else "requires all upstream tasks to have succeeded, but found 1"
                ),
            )

    @pytest.mark.parametrize("flag_upstream_failed, expected_ti_state", [(True, SKIPPED), (False, None)])
    def test_all_success_tr_skip_wait_for_past_depends_before_skipping_past_depends_met(
        self, session, get_task_instance, flag_upstream_failed, expected_ti_state
    ):
        """
        All-success trigger rule fails when some upstream tasks are skipped. The state of the ti
        should be set to SKIPPED when flag_upstream_failed is True and
        wait_for_past_depends_before_skipping is True and the past depends are met.
        """
        ti = get_task_instance(
            TriggerRule.ALL_SUCCESS,
            success=1,
            skipped=1,
            failed=0,
            removed=0,
            upstream_failed=0,
            done=2,
            normal_tasks=["FakeTaskID", "OtherFakeTaskID"],
        )
        ti.task.xcom_pull.return_value = None
        xcom_mock = Mock(return_value=True)
        with mock.patch("airflow.models.taskinstance.TaskInstance.xcom_pull", xcom_mock):
            _test_trigger_rule(
                ti=ti,
                session=session,
                flag_upstream_failed=flag_upstream_failed,
                wait_for_past_depends_before_skipping=True,
                expected_ti_state=expected_ti_state,
                expected_reason="requires all upstream tasks to have succeeded, but found 1",
            )

    @pytest.mark.parametrize("flag_upstream_failed", [True, False])
    def test_none_failed_tr_success(self, session, get_task_instance, flag_upstream_failed):
        """
        None failed trigger rule success
        """
        ti = get_task_instance(
            TriggerRule.NONE_FAILED,
            success=1,
            skipped=1,
            failed=0,
            removed=0,
            upstream_failed=0,
            done=2,
            normal_tasks=["FakeTaskID", "OtherFakeTaskID"],
        )
        _test_trigger_rule(ti=ti, session=session, flag_upstream_failed=flag_upstream_failed)

    @pytest.mark.parametrize(
        "flag_upstream_failed, expected_ti_state", [(True, UPSTREAM_FAILED), (False, None)]
    )
    def test_none_failed_tr_failure(
        self, session, get_task_instance, flag_upstream_failed, expected_ti_state
    ):
        """
        None failed trigger rule failure
        """
        ti = get_task_instance(
            TriggerRule.NONE_FAILED,
            success=1,
            skipped=1,
            failed=1,
            removed=0,
            upstream_failed=0,
            done=3,
            normal_tasks=["FakeTaskID", "OtherFakeTaskID", "FailedFakeTaskID"],
        )
        _test_trigger_rule(
            ti=ti,
            session=session,
            flag_upstream_failed=flag_upstream_failed,
            expected_reason="all upstream tasks to have succeeded or been skipped, but found 1",
            expected_ti_state=expected_ti_state,
        )

    @pytest.mark.parametrize(
        "flag_upstream_failed, expected_ti_state", [(True, UPSTREAM_FAILED), (False, None)]
    )
    def test_none_failed_tr_failure_with_upstream_failure(
        self, session, get_task_instance, flag_upstream_failed, expected_ti_state
    ):
        """
        None failed skip trigger rule failure
        """
        ti = get_task_instance(
            TriggerRule.NONE_FAILED,
            success=1,
            skipped=1,
            failed=0,
            removed=0,
            upstream_failed=1,
            done=3,
            normal_tasks=["FakeTaskID", "OtherFakeTaskID", "FailedFakeTaskID"],
        )
        _test_trigger_rule(
            ti=ti,
            session=session,
            flag_upstream_failed=flag_upstream_failed,
            expected_reason="all upstream tasks to have succeeded or been skipped, but found 1",
            expected_ti_state=expected_ti_state,
        )

    @pytest.mark.parametrize("flag_upstream_failed", [True, False])
    def test_none_failed_min_one_success_tr_success(self, session, get_task_instance, flag_upstream_failed):
        """
        None failed min one success trigger rule success
        """
        ti = get_task_instance(
            TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
            success=1,
            skipped=1,
            failed=0,
            removed=0,
            upstream_failed=0,
            done=2,
            normal_tasks=["FakeTaskID", "OtherFakeTaskID"],
        )
        _test_trigger_rule(ti=ti, session=session, flag_upstream_failed=flag_upstream_failed)

    @pytest.mark.parametrize("flag_upstream_failed, expected_ti_state", [(True, SKIPPED), (False, None)])
    def test_none_failed_min_one_success_tr_skipped(
        self, session, get_task_instance, flag_upstream_failed, expected_ti_state
    ):
        """
        None failed min one success trigger rule success with all skipped
        """
        ti = get_task_instance(
            TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
            success=0,
            skipped=2,
            failed=0,
            removed=0,
            upstream_failed=0,
            done=2,
            normal_tasks=["FakeTaskID", "OtherFakeTaskID"],
        )
        _test_trigger_rule(
            ti=ti,
            session=session,
            flag_upstream_failed=flag_upstream_failed,
            expected_ti_state=expected_ti_state,
        )

    @pytest.mark.parametrize(
        "flag_upstream_failed, expected_ti_state", [(True, UPSTREAM_FAILED), (False, None)]
    )
    def test_none_failed_min_one_success_tr_failure(
        self, session, get_task_instance, flag_upstream_failed, expected_ti_state
    ):
        """
        None failed min one success trigger rule failure due to single failure
        """
        ti = get_task_instance(
            TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
            success=1,
            skipped=1,
            failed=1,
            removed=0,
            upstream_failed=0,
            done=3,
            normal_tasks=["FakeTaskID", "OtherFakeTaskID", "FailedFakeTaskID"],
        )
        _test_trigger_rule(
            ti=ti,
            session=session,
            flag_upstream_failed=flag_upstream_failed,
            expected_reason="all upstream tasks to have succeeded or been skipped, but found 1",
            expected_ti_state=expected_ti_state,
        )

    @pytest.mark.parametrize(
        "flag_upstream_failed, expected_ti_state", [(True, UPSTREAM_FAILED), (False, None)]
    )
    def test_none_failed_min_one_success_tr_upstream_failure(
        self, session, get_task_instance, flag_upstream_failed, expected_ti_state
    ):
        """
        None failed min one success trigger rule failure due to single upstream failure
        """
        ti = get_task_instance(
            TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
            success=1,
            skipped=1,
            failed=0,
            removed=0,
            upstream_failed=1,
            done=3,
            normal_tasks=["FakeTaskID", "OtherFakeTaskID", "FailedFakeTaskID"],
        )
        _test_trigger_rule(
            ti=ti,
            session=session,
            flag_upstream_failed=flag_upstream_failed,
            expected_reason="all upstream tasks to have succeeded or been skipped, but found 1",
            expected_ti_state=expected_ti_state,
        )

    @pytest.mark.parametrize("flag_upstream_failed", [True, False])
    def test_all_failed_tr_success(self, session, get_task_instance, flag_upstream_failed):
        """
        All-failed trigger rule success
        """
        ti = get_task_instance(
            TriggerRule.ALL_FAILED,
            success=0,
            skipped=0,
            failed=2,
            removed=0,
            upstream_failed=0,
            done=2,
            normal_tasks=["FakeTaskID", "OtherFakeTaskID"],
        )
        _test_trigger_rule(ti=ti, session=session, flag_upstream_failed=flag_upstream_failed)

    @pytest.mark.parametrize("flag_upstream_failed, expected_ti_state", [(True, SKIPPED), (False, None)])
    def test_all_failed_tr_failure(self, session, get_task_instance, flag_upstream_failed, expected_ti_state):
        """
        All-failed trigger rule failure
        """
        ti = get_task_instance(
            TriggerRule.ALL_FAILED,
            success=2,
            skipped=0,
            failed=0,
            removed=0,
            upstream_failed=0,
            done=2,
            normal_tasks=["FakeTaskID", "OtherFakeTaskID"],
        )
        _test_trigger_rule(
            ti=ti,
            session=session,
            flag_upstream_failed=flag_upstream_failed,
            expected_reason="requires all upstream tasks to have failed, but found 2",
            expected_ti_state=expected_ti_state,
        )

    @pytest.mark.parametrize("flag_upstream_failed", [True, False])
    def test_all_done_tr_success(self, session, get_task_instance, flag_upstream_failed):
        """
        All-done trigger rule success
        """
        ti = get_task_instance(
            TriggerRule.ALL_DONE,
            success=2,
            skipped=0,
            failed=0,
            removed=0,
            upstream_failed=0,
            done=2,
            normal_tasks=["FakeTaskID", "OtherFakeTaskID"],
        )
        _test_trigger_rule(ti=ti, session=session, flag_upstream_failed=flag_upstream_failed)

    @pytest.mark.parametrize(
        "task_cfg, states, exp_reason, exp_state",
        [
            pytest.param(
                dict(work=2, setup=0),
                dict(success=2, done=2),
                None,
                None,
                id="no setups",
            ),
            pytest.param(
                dict(work=2, setup=1),
                dict(success=2, done=2),
                "but found 1 task(s) that were not done",
                None,
                id="setup not done",
            ),
            pytest.param(
                dict(work=2, setup=1),
                dict(success=2, done=3),
                "requires at least one upstream setup task be successful",
                UPSTREAM_FAILED,
                id="setup failed",
            ),
            pytest.param(
                dict(work=2, setup=2),
                dict(success=2, done=4, success_setup=1),
                None,
                None,
                id="one setup failed one success",
            ),
            pytest.param(
                dict(work=2, setup=2),
                dict(success=2, done=3, success_setup=1),
                "found 1 task(s) that were not done",
                None,
                id="one setup success one running",
            ),
            pytest.param(
                dict(work=2, setup=1),
                dict(success=2, done=3, failed=1),
                "requires at least one upstream setup task be successful",
                UPSTREAM_FAILED,
                id="setup failed",
            ),
            pytest.param(
                dict(work=2, setup=2),
                dict(success=2, done=4, failed=1, skipped_setup=1),
                "requires at least one upstream setup task be successful",
                UPSTREAM_FAILED,
                id="one setup failed one skipped",
            ),
            pytest.param(
                dict(work=2, setup=2),
                dict(success=2, done=4, failed=0, skipped_setup=2),
                "requires at least one upstream setup task be successful",
                SKIPPED,
                id="two setups both skipped",
            ),
            pytest.param(
                dict(work=2, setup=1),
                dict(success=3, done=3, success_setup=1),
                None,
                None,
                id="all success",
            ),
            pytest.param(
                dict(work=2, setup=1),
                dict(success=1, done=3, success_setup=1),
                None,
                None,
                id="work failed",
            ),
            pytest.param(
                dict(work=2, setup=1),
                dict(success=2, done=3, skipped_setup=1),
                "requires at least one upstream setup task be successful",
                SKIPPED,
                id="one setup; skipped",
            ),
        ],
    )
    @pytest.mark.parametrize("flag_upstream_failed", [True, False])
    def test_teardown_tr_not_all_done(
        self, task_cfg, states, exp_reason, exp_state, session, get_task_instance, flag_upstream_failed
    ):
        """
        All-done trigger rule success
        """
        ti = get_task_instance(
            TriggerRule.ALL_DONE_SETUP_SUCCESS,
            **states,
            normal_tasks=[f"w{x}" for x in range(task_cfg["work"])],
            setup_tasks=[f"s{x}" for x in range(task_cfg["setup"])],
        )
        _test_trigger_rule(
            ti=ti,
            session=session,
            flag_upstream_failed=flag_upstream_failed,
            expected_reason=exp_reason,
            expected_ti_state=exp_state if exp_state and flag_upstream_failed else None,
        )

    @pytest.mark.parametrize("flag_upstream_failed, expected_ti_state", [(True, SKIPPED), (False, None)])
    def test_all_skipped_tr_failure(
        self, session, get_task_instance, flag_upstream_failed, expected_ti_state
    ):
        """
        All-skipped trigger rule failure
        """
        ti = get_task_instance(
            TriggerRule.ALL_SKIPPED,
            success=1,
            skipped=0,
            failed=0,
            removed=0,
            upstream_failed=0,
            done=1,
            normal_tasks=["FakeTaskID"],
        )
        _test_trigger_rule(
            ti=ti,
            session=session,
            flag_upstream_failed=flag_upstream_failed,
            expected_reason="requires all upstream tasks to have been skipped, but found 1",
            expected_ti_state=expected_ti_state,
        )

    @pytest.mark.parametrize("flag_upstream_failed, expected_ti_state", [(True, SKIPPED), (False, None)])
    def test_all_skipped_tr_failure_upstream_failed(
        self, session, get_task_instance, flag_upstream_failed, expected_ti_state
    ):
        """
        All-skipped trigger rule failure if an upstream task is in a `upstream_failed` state
        """
        ti = get_task_instance(
            TriggerRule.ALL_SKIPPED,
            success=0,
            skipped=0,
            failed=0,
            removed=0,
            upstream_failed=1,
            done=1,
            normal_tasks=["FakeTaskID"],
        )
        _test_trigger_rule(
            ti=ti,
            session=session,
            flag_upstream_failed=flag_upstream_failed,
            expected_reason="requires all upstream tasks to have been skipped, but found 1",
            expected_ti_state=expected_ti_state,
        )

    @pytest.mark.parametrize("flag_upstream_failed", [True, False])
    def test_all_skipped_tr_success(self, session, get_task_instance, flag_upstream_failed):
        """
        All-skipped trigger rule success
        """
        ti = get_task_instance(
            TriggerRule.ALL_SKIPPED,
            success=0,
            skipped=3,
            failed=0,
            removed=0,
            upstream_failed=0,
            done=3,
            normal_tasks=["FakeTaskID", "OtherFakeTaskID", "FailedFakeTaskID"],
        )
        _test_trigger_rule(ti=ti, session=session, flag_upstream_failed=flag_upstream_failed)

    @pytest.mark.parametrize("flag_upstream_failed", [True, False])
    def test_all_done_tr_failure(self, session, get_task_instance, flag_upstream_failed):
        """
        All-done trigger rule failure
        """
        ti = get_task_instance(
            TriggerRule.ALL_DONE,
            success=1,
            skipped=0,
            failed=0,
            removed=0,
            upstream_failed=0,
            done=1,
            normal_tasks=["FakeTaskID"],
        )
        EmptyOperator(task_id="OtherFakeTeakID", dag=ti.task.dag) >> ti.task  # An unfinished upstream.

        _test_trigger_rule(
            ti=ti,
            session=session,
            flag_upstream_failed=flag_upstream_failed,
            expected_reason="requires all upstream tasks to have completed, but found 1",
        )

    @pytest.mark.parametrize("flag_upstream_failed", [True, False])
    def test_none_skipped_tr_success(self, session, get_task_instance, flag_upstream_failed):
        """
        None-skipped trigger rule success
        """
        ti = get_task_instance(
            TriggerRule.NONE_SKIPPED,
            success=2,
            skipped=0,
            failed=1,
            removed=0,
            upstream_failed=0,
            done=3,
            normal_tasks=["FakeTaskID", "OtherFakeTaskID", "FailedFakeTaskID"],
        )
        _test_trigger_rule(ti=ti, session=session, flag_upstream_failed=flag_upstream_failed)

    @pytest.mark.parametrize("flag_upstream_failed, expected_ti_state", [(True, SKIPPED), (False, None)])
    def test_none_skipped_tr_failure(
        self, session, get_task_instance, flag_upstream_failed, expected_ti_state
    ):
        """
        None-skipped trigger rule failure
        """
        ti = get_task_instance(
            TriggerRule.NONE_SKIPPED,
            success=1,
            skipped=1,
            failed=0,
            removed=0,
            upstream_failed=0,
            done=2,
            normal_tasks=["FakeTaskID", "SkippedTaskID"],
        )
        _test_trigger_rule(
            ti=ti,
            session=session,
            flag_upstream_failed=flag_upstream_failed,
            expected_ti_state=expected_ti_state,
            expected_reason="requires all upstream tasks to not have been skipped, but found 1",
        )

    @pytest.mark.parametrize("flag_upstream_failed", [True, False])
    def test_none_skipped_tr_failure_empty(self, session, get_task_instance, flag_upstream_failed):
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

        _test_trigger_rule(
            ti=ti,
            session=session,
            flag_upstream_failed=flag_upstream_failed,
            expected_reason="requires all upstream tasks to not have been skipped, but found 0",
        )

    @pytest.mark.parametrize("flag_upstream_failed", [True, False])
    def test_unknown_tr(self, session, get_task_instance, flag_upstream_failed):
        """
        Unknown trigger rules should cause this dep to fail
        """
        ti = get_task_instance(
            TriggerRule.ALWAYS,
            success=1,
            skipped=0,
            failed=0,
            removed=0,
            upstream_failed=0,
            done=1,
        )
        ti.task.trigger_rule = "Unknown Trigger Rule"

        _test_trigger_rule(
            ti=ti,
            session=session,
            flag_upstream_failed=flag_upstream_failed,
            expected_reason="No strategy to evaluate trigger rule 'Unknown Trigger Rule'.",
        )

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

        tis["op1"].state = SUCCESS
        tis["op2"].state = FAILED
        tis["op3"].state = SUCCESS
        tis["op4"].state = SUCCESS
        tis["op5"].state = SUCCESS

        def _get_finished_tis(task_id: str) -> Iterator[TaskInstance]:
            return (ti for ti in tis.values() if ti.task_id in tis[task_id].task.upstream_task_ids)

        # check handling with cases that tasks are triggered from backfill with no finished tasks
        assert _UpstreamTIStates.calculate(_get_finished_tis("op2")) == (1, 0, 0, 0, 0, 1, 0, 0)
        assert _UpstreamTIStates.calculate(_get_finished_tis("op4")) == (1, 0, 1, 0, 0, 2, 0, 0)
        assert _UpstreamTIStates.calculate(_get_finished_tis("op5")) == (2, 0, 1, 0, 0, 3, 0, 0)

        dr.update_state(session=session)
        assert dr.state == DagRunState.SUCCESS

    @pytest.mark.parametrize("flag_upstream_failed, expected_ti_state", [(True, REMOVED), (False, None)])
    def test_mapped_task_upstream_removed_with_all_success_trigger_rules(
        self,
        monkeypatch,
        session,
        get_mapped_task_dagrun,
        flag_upstream_failed,
        expected_ti_state,
    ):
        """
        Test ALL_SUCCESS trigger rule with mapped task upstream removed
        """
        dr, task, _ = get_mapped_task_dagrun()

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
            skipped_setup=0,
            success_setup=0,
        )
        monkeypatch.setattr(_UpstreamTIStates, "calculate", lambda *_: upstream_states)

        _test_trigger_rule(
            ti=ti,
            session=session,
            flag_upstream_failed=flag_upstream_failed,
            expected_ti_state=expected_ti_state,
        )

    @pytest.mark.parametrize("flag_upstream_failed", [True, False])
    def test_mapped_task_upstream_removed_with_all_failed_trigger_rules(
        self,
        monkeypatch,
        session,
        get_mapped_task_dagrun,
        flag_upstream_failed,
    ):
        """
        Test ALL_FAILED trigger rule with mapped task upstream removed
        """

        dr, task, _ = get_mapped_task_dagrun(trigger_rule=TriggerRule.ALL_FAILED, state=FAILED)

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
            skipped_setup=0,
            success_setup=0,
        )
        monkeypatch.setattr(_UpstreamTIStates, "calculate", lambda *_: upstream_states)

        _test_trigger_rule(ti=ti, session=session, flag_upstream_failed=flag_upstream_failed)

    @pytest.mark.flaky(reruns=5)
    @pytest.mark.parametrize(
        "trigger_rule", [TriggerRule.NONE_FAILED, TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS]
    )
    @pytest.mark.parametrize("flag_upstream_failed", [True, False])
    def test_mapped_task_upstream_removed_with_none_failed_trigger_rules(
        self,
        monkeypatch,
        session,
        get_mapped_task_dagrun,
        trigger_rule,
        flag_upstream_failed,
    ):
        """
        Test NONE_FAILED trigger rule with mapped task upstream removed
        """
        dr, task, _ = get_mapped_task_dagrun(trigger_rule=trigger_rule)

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
            skipped_setup=0,
            success_setup=0,
        )
        monkeypatch.setattr(_UpstreamTIStates, "calculate", lambda *_: upstream_states)

        _test_trigger_rule(ti=ti, session=session, flag_upstream_failed=flag_upstream_failed)


def test_upstream_in_mapped_group_triggers_only_relevant(dag_maker, session):
    from airflow.decorators import task, task_group

    with dag_maker(session=session):

        @task
        def t(x):
            return x

        @task_group
        def tg1(x):
            t1 = t.override(task_id="t1")(x=x)
            return t.override(task_id="t2")(x=t1)

        t2 = tg1.expand(x=[1, 2, 3])

        @task_group
        def tg2(x):
            return t.override(task_id="t3")(x=t2)

        vals2 = tg2.expand(x=[4, 5, 6])
        t.override(task_id="t4")(x=vals2)

    dr: DagRun = dag_maker.create_dagrun()

    def _one_scheduling_decision_iteration() -> dict[tuple[str, int], TaskInstance]:
        decision = dr.task_instance_scheduling_decisions(session=session)
        return {(ti.task_id, ti.map_index): ti for ti in decision.schedulable_tis}

    # Initial decision.
    tis = _one_scheduling_decision_iteration()
    assert sorted(tis) == [("tg1.t1", 0), ("tg1.t1", 1), ("tg1.t1", 2)]

    # After running the first t1, the first t2 becomes immediately available.
    tis["tg1.t1", 0].run()
    tis = _one_scheduling_decision_iteration()
    assert sorted(tis) == [("tg1.t1", 1), ("tg1.t1", 2), ("tg1.t2", 0)]

    # Similarly for the subsequent t2 instances.
    tis["tg1.t1", 2].run()
    tis = _one_scheduling_decision_iteration()
    assert sorted(tis) == [("tg1.t1", 1), ("tg1.t2", 0), ("tg1.t2", 2)]

    # But running t2 partially does not make t3 available.
    tis["tg1.t1", 1].run()
    tis["tg1.t2", 0].run()
    tis["tg1.t2", 2].run()
    tis = _one_scheduling_decision_iteration()
    assert sorted(tis) == [("tg1.t2", 1)]

    # Only after all t2 instances are run does t3 become available.
    tis["tg1.t2", 1].run()
    tis = _one_scheduling_decision_iteration()
    assert sorted(tis) == [("tg2.t3", 0), ("tg2.t3", 1), ("tg2.t3", 2)]

    # But running t3 partially does not make t4 available.
    tis["tg2.t3", 0].run()
    tis["tg2.t3", 2].run()
    tis = _one_scheduling_decision_iteration()
    assert sorted(tis) == [("tg2.t3", 1)]

    # Only after all t3 instances are run does t4 become available.
    tis["tg2.t3", 1].run()
    tis = _one_scheduling_decision_iteration()
    assert sorted(tis) == [("t4", -1)]


def test_upstream_in_mapped_group_when_mapped_tasks_list_is_empty(dag_maker, session):
    from airflow.decorators import task, task_group

    with dag_maker(session=session):

        @task
        def t(x):
            return x

        @task_group
        def tg(x):
            t1 = t.override(task_id="t1")(x=x)
            return t.override(task_id="t2")(x=t1)

        t2 = tg.expand(x=[])
        t.override(task_id="t3")(x=t2)

    dr: DagRun = dag_maker.create_dagrun()

    def _one_scheduling_decision_iteration() -> dict[tuple[str, int], TaskInstance]:
        decision = dr.task_instance_scheduling_decisions(session=session)
        return {(ti.task_id, ti.map_index): ti for ti in decision.schedulable_tis}

    # should return an empty dict
    tis = _one_scheduling_decision_iteration()
    assert tis == {}


@pytest.mark.parametrize("flag_upstream_failed", [True, False])
def test_mapped_task_check_before_expand(dag_maker, session, flag_upstream_failed):
    """
    t3 depends on t2, which depends on t1 for expansion. Since t1 has not yet run, t2 has not expanded yet,
    and we need to guarantee this lack of expansion does not fail the dependency-checking logic.
    """
    with dag_maker(session=session):

        @task
        def t(x):
            return x

        @task_group
        def tg(a):
            b = t.override(task_id="t2")(a)
            c = t.override(task_id="t3")(b)
            return c

        tg.expand(a=t([1, 2, 3]))

    dr: DagRun = dag_maker.create_dagrun()

    _test_trigger_rule(
        ti=next(ti for ti in dr.task_instances if ti.task_id == "tg.t3" and ti.map_index == -1),
        session=session,
        flag_upstream_failed=flag_upstream_failed,
        expected_reason="requires all upstream tasks to have succeeded, but found 1",
    )


@pytest.mark.parametrize("flag_upstream_failed, expected_ti_state", [(True, SKIPPED), (False, None)])
def test_mapped_task_group_finished_upstream_before_expand(
    dag_maker, session, flag_upstream_failed, expected_ti_state
):
    """
    t3 depends on t2, which was skipped before it was expanded. We need to guarantee this lack of expansion
    does not fail the dependency-checking logic.
    """
    with dag_maker(session=session):

        @task
        def t(x):
            return x

        @task_group
        def tg(x):
            return t.override(task_id="t3")(x=x)

        t.override(task_id="t2").expand(x=t.override(task_id="t1")([1, 2])) >> tg.expand(x=[1, 2])

    dr: DagRun = dag_maker.create_dagrun()
    tis = {ti.task_id: ti for ti in dr.get_task_instances(session=session)}
    tis["t2"].set_state(SKIPPED, session=session)
    session.flush()
    _test_trigger_rule(
        ti=tis["tg.t3"],
        session=session,
        flag_upstream_failed=flag_upstream_failed,
        expected_reason="requires all upstream tasks to have succeeded, but found 1",
        expected_ti_state=expected_ti_state,
    )


class TestTriggerRuleDepSetupConstraint:
    @staticmethod
    def get_ti(dr, task_id):
        return next(ti for ti in dr.task_instances if ti.task_id == task_id)

    def get_dep_statuses(self, dr, task_id, flag_upstream_failed=False, session=None):
        return list(
            TriggerRuleDep()._get_dep_statuses(
                ti=self.get_ti(dr, task_id),
                dep_context=DepContext(flag_upstream_failed=flag_upstream_failed),
                session=session,
            )
        )

    def test_setup_constraint_blocks_execution(self, dag_maker, session):
        with dag_maker(session=session):

            @task
            def t1():
                return 1

            @task
            def t2():
                return 2

            @task
            def t3():
                return 3

            t1_task = t1()
            t2_task = t2()
            t3_task = t3()
            t1_task >> t2_task >> t3_task
            t1_task.as_setup()
        dr = dag_maker.create_dagrun()

        # setup constraint is not applied to t2 because it has a direct setup
        # so even though the setup is not done, the check passes
        # but trigger rule fails because the normal trigger rule dep behavior
        statuses = self.get_dep_statuses(dr, "t2", session=session)
        assert len(statuses) == 1
        assert statuses[0].passed is False
        assert statuses[0].reason.startswith("Task's trigger rule 'all_success' requires all upstream tasks")

        # t3 has an indirect setup so the setup check fails
        # trigger rule also fails
        statuses = self.get_dep_statuses(dr, "t3", session=session)
        assert len(statuses) == 2
        assert statuses[0].passed is False
        assert statuses[0].reason.startswith("All setup tasks must complete successfully")
        assert statuses[1].passed is False
        assert statuses[1].reason.startswith("Task's trigger rule 'all_success' requires all upstream tasks")

    @pytest.mark.parametrize(
        "setup_state, expected", [(None, None), ("failed", "upstream_failed"), ("skipped", "skipped")]
    )
    def test_setup_constraint_changes_state_appropriately(self, dag_maker, session, setup_state, expected):
        with dag_maker(session=session):

            @task
            def t1():
                return 1

            @task
            def t2():
                return 2

            @task
            def t3():
                return 3

            t1_task = t1()
            t2_task = t2()
            t3_task = t3()
            t1_task >> t2_task >> t3_task
            t1_task.as_setup()
        dr = dag_maker.create_dagrun()

        # if the setup fails then now, in processing the trigger rule dep, the ti states
        # will be updated
        if setup_state:
            self.get_ti(dr, "t1").state = setup_state
        session.commit()
        (status,) = self.get_dep_statuses(dr, "t2", flag_upstream_failed=True, session=session)
        assert status.passed is False
        # t2 fails on the non-setup-related trigger rule constraint since it has
        # a direct setup
        assert status.reason.startswith("Task's trigger rule 'all_success' requires")
        assert self.get_ti(dr, "t2").state == expected
        assert self.get_ti(dr, "t3").state is None  # hasn't been evaluated yet

        # unlike t2, t3 fails on the setup constraint, and the normal trigger rule
        # constraint is not actually evaluated, since it ain't gonna run anyway
        if setup_state is None:
            # when state is None, setup constraint doesn't mutate ti state, so we get
            # two failure reasons -- setup constraint and trigger rule
            (status, _) = self.get_dep_statuses(dr, "t3", flag_upstream_failed=True, session=session)
        else:
            (status,) = self.get_dep_statuses(dr, "t3", flag_upstream_failed=True, session=session)
        assert status.reason.startswith("All setup tasks must complete successfully")
        assert self.get_ti(dr, "t3").state == expected

    @pytest.mark.parametrize(
        "setup_state, expected", [(None, None), ("failed", "upstream_failed"), ("skipped", "skipped")]
    )
    def test_setup_constraint_will_fail_or_skip_fast(self, dag_maker, session, setup_state, expected):
        """
        When a setup fails or skips, the tasks that depend on it will immediately fail or skip
        and not, for example, wait for all setups to complete before determining what is
        the appropriate state.  This is a bit of a race condition, but it's consistent
        with the behavior for many-to-one direct upstream task relationships, and it's
        required if you want to fail fast.

        So in this test we verify that if even one setup is failed or skipped, the
        state will propagate to the in-scope work tasks.
        """
        with dag_maker(session=session):

            @task
            def s1():
                return 1

            @task
            def s2():
                return 1

            @task
            def w1():
                return 2

            @task
            def w2():
                return 3

            s1 = s1().as_setup()
            s2 = s2().as_setup()
            [s1, s2] >> w1() >> w2()
        dr = dag_maker.create_dagrun()

        # if the setup fails then now, in processing the trigger rule dep, the ti states
        # will be updated
        if setup_state:
            self.get_ti(dr, "s2").state = setup_state
        session.commit()
        (status,) = self.get_dep_statuses(dr, "w1", flag_upstream_failed=True, session=session)
        assert status.passed is False
        # t2 fails on the non-setup-related trigger rule constraint since it has
        # a direct setup
        assert status.reason.startswith("Task's trigger rule 'all_success' requires")
        assert self.get_ti(dr, "w1").state == expected
        assert self.get_ti(dr, "w2").state is None  # hasn't been evaluated yet

        # unlike t2, t3 fails on the setup constraint, and the normal trigger rule
        # constraint is not actually evaluated, since it ain't gonna run anyway
        if setup_state is None:
            # when state is None, setup constraint doesn't mutate ti state, so we get
            # two failure reasons -- setup constraint and trigger rule
            (status, _) = self.get_dep_statuses(dr, "w2", flag_upstream_failed=True, session=session)
        else:
            (status,) = self.get_dep_statuses(dr, "w2", flag_upstream_failed=True, session=session)
        assert status.reason.startswith("All setup tasks must complete successfully")
        assert self.get_ti(dr, "w2").state == expected


@pytest.mark.parametrize(
    "map_index, flag_upstream_failed, expected_ti_state",
    [(2, True, None), (3, True, REMOVED), (4, True, REMOVED), (3, False, None)],
)
def test_setup_constraint_mapped_task_upstream_removed_and_success(
    dag_maker,
    session,
    get_mapped_task_dagrun,
    map_index,
    flag_upstream_failed,
    expected_ti_state,
):
    """
    Dynamically mapped setup task with successful and removed upstream tasks. Expect rule to be
    successful. State is set to REMOVED for map index >= n success
    """
    dr, _, setup_task = get_mapped_task_dagrun(add_setup_tasks=True)

    ti = dr.get_task_instance(task_id="setup_3", map_index=map_index, session=session)
    ti.task = setup_task

    _test_trigger_rule(
        ti=ti,
        session=session,
        flag_upstream_failed=flag_upstream_failed,
        expected_ti_state=expected_ti_state,
    )


@pytest.mark.parametrize(
    "flag_upstream_failed, wait_for_past_depends_before_skipping, past_depends_met, expected_ti_state, expect_failure",
    [
        (False, True, True, None, False),
        (False, True, False, None, False),
        (False, False, False, None, False),
        (False, False, True, None, False),
        (True, False, False, SKIPPED, False),
        (True, False, True, SKIPPED, False),
        (True, True, False, None, True),
        (True, True, True, SKIPPED, False),
    ],
)
def test_setup_constraint_wait_for_past_depends_before_skipping(
    dag_maker,
    session,
    get_task_instance,
    monkeypatch,
    flag_upstream_failed,
    wait_for_past_depends_before_skipping,
    past_depends_met,
    expected_ti_state,
    expect_failure,
):
    """
    Setup task with a skipped upstream task.
    * If flag_upstream_failed is False then do not expect either a failure nor a modified state.
    * If flag_upstream_failed is True and wait_for_past_depends_before_skipping is False then expect the
      state to be set to SKIPPED but no failure.
    * If both flag_upstream_failed and wait_for_past_depends_before_skipping are True then if the past
      depends are met the state is expected to be SKIPPED and no failure, otherwise the state is not
      expected to change but the trigger rule should fail.
    """
    ti = get_task_instance(
        trigger_rule=TriggerRule.ALL_DONE,
        success=1,
        skipped=1,
        failed=0,
        removed=0,
        upstream_failed=0,
        done=2,
        setup_tasks=["FakeTaskID", "OtherFakeTaskID"],
    )

    ti.task.xcom_pull.return_value = None
    xcom_mock = Mock(return_value=True if past_depends_met else None)
    with mock.patch("airflow.models.taskinstance.TaskInstance.xcom_pull", xcom_mock):
        _test_trigger_rule(
            ti=ti,
            session=session,
            flag_upstream_failed=flag_upstream_failed,
            wait_for_past_depends_before_skipping=wait_for_past_depends_before_skipping,
            expected_ti_state=expected_ti_state,
            expected_reason=(
                "Task should be skipped but the past depends are not met" if expect_failure else ""
            ),
        )


@pytest.mark.parametrize("flag_upstream_failed, expected_ti_state", [(True, SKIPPED), (False, None)])
def test_setup_mapped_task_group_finished_upstream_before_expand(
    dag_maker, session, flag_upstream_failed, expected_ti_state
):
    """
    t3 indirectly depends on t1, which was skipped before it was expanded. We need to guarantee this lack of
    expansion does not fail the dependency-checking logic.
    """
    with dag_maker(session=session):

        @task(trigger_rule=TriggerRule.ALL_DONE)
        def t(x):
            return x

        @task_group
        def tg(x):
            return t.override(task_id="t3")(x=x)

        vals = t.override(task_id="t1")([1, 2]).as_setup()
        t.override(task_id="t2").expand(x=vals).as_setup() >> tg.expand(x=[1, 2]).as_setup()

    dr: DagRun = dag_maker.create_dagrun()

    tis = {ti.task_id: ti for ti in dr.get_task_instances(session=session)}
    tis["t1"].set_state(SKIPPED, session=session)
    tis["t2"].set_state(SUCCESS, session=session)
    session.flush()
    _test_trigger_rule(
        ti=tis["tg.t3"],
        session=session,
        flag_upstream_failed=flag_upstream_failed,
        expected_reason="All setup tasks must complete successfully.",
        expected_ti_state=expected_ti_state,
    )


def _test_trigger_rule(
    ti: TaskInstance,
    session: Session,
    flag_upstream_failed: bool,
    wait_for_past_depends_before_skipping: bool = False,
    expected_reason: str = "",
    expected_ti_state: TaskInstanceState | None = None,
) -> None:
    assert ti.state is None
    dep_statuses = tuple(
        TriggerRuleDep()._evaluate_trigger_rule(
            ti=ti,
            dep_context=DepContext(
                flag_upstream_failed=flag_upstream_failed,
                wait_for_past_depends_before_skipping=wait_for_past_depends_before_skipping,
            ),
            session=session,
        )
    )
    if expected_reason:
        assert len(dep_statuses) == 1
        assert not dep_statuses[0].passed
        assert expected_reason in dep_statuses[0].reason
    else:
        assert not dep_statuses
    assert ti.state == expected_ti_state
