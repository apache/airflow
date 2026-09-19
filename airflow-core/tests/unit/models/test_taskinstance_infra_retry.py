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

from types import SimpleNamespace
from typing import TYPE_CHECKING
from unittest import mock

import pytest

from airflow._shared.state import TaskFailureKind
from airflow.models.taskinstance import TaskInstance, _maybe_use_infra_retry, clear_task_instances
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.utils.state import TaskInstanceState

from tests_common.test_utils.config import conf_vars

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from tests_common.pytest_plugin import DagMaker

pytestmark = pytest.mark.db_test

INFRA_RETRIES = {("core", "max_infra_retries"): "3"}


def _use_infra_retry(*, ti, task, failure_kind) -> bool:
    return _maybe_use_infra_retry(
        task_instance=ti,
        task=task,
        failure_kind=failure_kind,
        reason="PreemptionByScheduler",
    )


class TestMaybeUseInfraRetry:
    @pytest.mark.parametrize(
        ("cap", "failure_kind", "retries", "max_tries", "try_number", "expected_metric"),
        [
            (0, TaskFailureKind.INFRA, 0, 0, 1, None),
            (1, None, 0, 1, 2, None),
            (1, TaskFailureKind.APPLICATION, 0, 1, 2, None),
            (1, TaskFailureKind.TIMEOUT, 0, 1, 2, None),
            (1, TaskFailureKind.MANUAL, 0, 1, 2, None),
            (1, TaskFailureKind.INFRA, 0, 0, 1, "ti_infra_retry_granted"),
            (1, TaskFailureKind.INFRA, 0, 1, 2, "ti_infra_retry_denied"),
            (1, TaskFailureKind.INFRA, 2, 2, 2, "ti_infra_retry_denied"),
            (3, TaskFailureKind.INFRA, 2, 5, 4, "ti_infra_retry_denied"),
            (3, TaskFailureKind.INFRA, 0, 5, 2, "ti_infra_retry_denied"),
            (3, TaskFailureKind.INFRA, 5, 5, 3, "ti_infra_retry_granted"),
            (3, TaskFailureKind.INFRA, 20, 6, 4, "ti_infra_retry_denied"),
        ],
    )
    @mock.patch("airflow.models.taskinstance.stats.incr")
    def test_policy_outcome_metrics(
        self,
        mock_incr: mock.MagicMock,
        cap: int,
        failure_kind: TaskFailureKind | None,
        retries: int,
        max_tries: int,
        try_number: int,
        expected_metric: str | None,
        dag_maker: DagMaker,
        session: Session,
    ) -> None:
        with dag_maker(dag_id="infra_policy_metrics"):
            task = EmptyOperator(task_id="task", retries=retries)
        ti = dag_maker.create_dagrun().get_task_instance(task.task_id, session=session)
        assert ti is not None
        ti.task = dag_maker.serialized_dag.get_task(task.task_id)
        ti.state = TaskInstanceState.RUNNING
        ti.max_tries = max_tries
        ti.try_number = try_number
        mock_incr.reset_mock()

        with conf_vars({("core", "max_infra_retries"): str(cap)}):
            granted = _use_infra_retry(ti=ti, task=ti.task, failure_kind=failure_kind)

        assert granted is (expected_metric == "ti_infra_retry_granted")
        assert ti.max_tries == max_tries + int(granted)
        if expected_metric is None:
            mock_incr.assert_not_called()
        else:
            mock_incr.assert_called_once_with(expected_metric, tags=ti.stats_tags)

    @pytest.mark.parametrize("failure_kind", [None, *TaskFailureKind])
    @mock.patch("airflow.models.taskinstance.stats.incr")
    def test_failure_metrics_have_bounded_kind(
        self,
        mock_incr: mock.MagicMock,
        failure_kind: TaskFailureKind | None,
        dag_maker: DagMaker,
        session: Session,
    ) -> None:
        with dag_maker(dag_id="bounded_failure_metrics"):
            task = EmptyOperator(task_id="task", retries=0)
        ti = dag_maker.create_dagrun().get_task_instance(task.task_id, session=session)
        assert ti is not None
        ti.task = dag_maker.serialized_dag.get_task(task.task_id)
        ti.state = TaskInstanceState.RUNNING
        ti.try_number = 1
        session.flush()

        with conf_vars({("core", "max_infra_retries"): "0"}):
            ti.handle_failure(error="task stopped", failure_kind=failure_kind, session=session)

        expected_tags = {
            **ti.stats_tags,
            "failure_kind": failure_kind.value if failure_kind is not None else "unclassified",
        }
        mock_incr.assert_any_call("ti_failures", tags=expected_tags)
        mock_incr.assert_any_call("operator_failures", tags={**expected_tags, "operator_name": ti.operator})

    @pytest.mark.parametrize(
        "failure_kind",
        [None, TaskFailureKind.APPLICATION, TaskFailureKind.MANUAL, TaskFailureKind.TIMEOUT],
    )
    @conf_vars(INFRA_RETRIES)
    def test_only_infra_uses_extra_attempts(self, failure_kind, dag_maker, session):
        with dag_maker(dag_id=f"failure_kind_{failure_kind or 'none'}"):
            task = EmptyOperator(task_id="task", retries=0)
        ti = dag_maker.create_dagrun().get_task_instance(task.task_id, session=session)
        ti.task = task

        assert not _use_infra_retry(
            ti=ti,
            task=task,
            failure_kind=failure_kind,
        )
        assert ti.max_tries == 0

    @conf_vars(INFRA_RETRIES)
    def test_consecutive_infrastructure_replacements_reach_the_cap(self, dag_maker, session):
        with dag_maker(dag_id="infra_budget"):
            task = EmptyOperator(task_id="task", retries=0)
        ti = dag_maker.create_dagrun().get_task_instance(task.task_id, session=session)
        ti.task = task

        granted: list[bool] = []
        for attempt in range(1, 6):
            ti.try_number = attempt
            ti.state = TaskInstanceState.RUNNING
            granted.append(
                _use_infra_retry(
                    ti=ti,
                    task=task,
                    failure_kind=TaskFailureKind.INFRA,
                )
            )
        assert granted == [True, True, True, False, False]
        assert ti.max_tries == 3

    def test_zero_budget_preserves_current_behavior(self, dag_maker, session):
        with dag_maker(dag_id="zero_infra_budget"):
            task = EmptyOperator(task_id="task", retries=1)
        ti = dag_maker.create_dagrun().get_task_instance(task.task_id, session=session)
        ti.task = task

        assert not _use_infra_retry(
            ti=ti,
            task=task,
            failure_kind=TaskFailureKind.INFRA,
        )
        assert ti.max_tries == 1

    @conf_vars({("core", "max_infra_retries"): "1"})
    def test_none_max_tries_uses_zero(self, dag_maker, session):
        with dag_maker(dag_id="none_max_tries"):
            task = EmptyOperator(task_id="task", retries=0)
        ti = dag_maker.create_dagrun().get_task_instance(task.task_id, session=session)
        ti.task = task
        ti.max_tries = None
        ti.try_number = 1
        ti.state = TaskInstanceState.RUNNING

        assert _use_infra_retry(
            ti=ti,
            task=task,
            failure_kind=TaskFailureKind.INFRA,
        )
        assert ti.max_tries == 1

    @conf_vars({("core", "max_infra_retries"): "1"})
    def test_retry_increase_cannot_reopen_prior_attempt_ceiling(self, dag_maker, session):
        with dag_maker(dag_id="retries_changed"):
            task = EmptyOperator(task_id="task", retries=0)
        ti = dag_maker.create_dagrun().get_task_instance(task.task_id, session=session)
        ti.task = task
        ti.try_number = 1
        ti.state = TaskInstanceState.RUNNING

        assert _use_infra_retry(
            ti=ti,
            task=task,
            failure_kind=TaskFailureKind.INFRA,
        )

        task.retries = 2
        ti.try_number = 2

        assert not _use_infra_retry(
            ti=ti,
            task=task,
            failure_kind=TaskFailureKind.INFRA,
        )
        assert ti.max_tries == 1

    @conf_vars(INFRA_RETRIES)
    def test_clear_cannot_reopen_prior_attempt_ceiling(self, dag_maker, session):
        with dag_maker(dag_id="infra_budget_clear"):
            task = EmptyOperator(task_id="task", retries=2)
        ti = dag_maker.create_dagrun().get_task_instance(task.task_id, session=session)
        ti.task = task

        for attempt in (1, 2):
            ti.try_number = attempt
            ti.state = TaskInstanceState.RUNNING
            assert _use_infra_retry(
                ti=ti,
                task=task,
                failure_kind=TaskFailureKind.INFRA,
            )
        ti.state = TaskInstanceState.FAILED
        ti.try_number = 3
        session.flush()

        clear_task_instances([ti], session=session)
        ti.task = task
        ti.try_number = 4
        ti.state = TaskInstanceState.RUNNING

        assert ti.max_tries == 5
        assert not _use_infra_retry(
            ti=ti,
            task=task,
            failure_kind=TaskFailureKind.INFRA,
        )

    @conf_vars({("core", "max_infra_retries"): "1"})
    def test_clear_consumes_ceiling_without_an_infrastructure_grant(self, dag_maker, session):
        with dag_maker(dag_id="clear_without_infra_grant"):
            task = EmptyOperator(task_id="task", retries=2)
        ti = dag_maker.create_dagrun().get_task_instance(task.task_id, session=session)
        ti.task = dag_maker.serialized_dag.get_task(task.task_id)
        ti.state = TaskInstanceState.RUNNING
        ti.try_number = 1
        session.flush()
        ti.handle_failure(
            error="application failure", failure_kind=TaskFailureKind.APPLICATION, session=session
        )
        assert ti.max_tries == 2

        clear_task_instances(tis=[ti], session=session)
        assert ti.max_tries == 3
        ti.state = TaskInstanceState.RUNNING
        ti.try_number = 2
        ti.task = dag_maker.serialized_dag.get_task(task.task_id)

        assert not _use_infra_retry(ti=ti, task=ti.task, failure_kind=TaskFailureKind.INFRA)
        assert ti.max_tries == 3

    @pytest.mark.parametrize(
        ("state", "try_number"),
        [
            (TaskInstanceState.QUEUED, 0),
            (TaskInstanceState.QUEUED, 1),
            (TaskInstanceState.SCHEDULED, 1),
            (TaskInstanceState.RESTARTING, 1),
            (TaskInstanceState.UP_FOR_RETRY, 1),
            (TaskInstanceState.FAILED, 1),
            (TaskInstanceState.SUCCESS, 1),
        ],
    )
    @conf_vars(INFRA_RETRIES)
    @mock.patch("airflow.models.taskinstance.stats.incr")
    def test_unstarted_or_handled_attempt_has_no_policy_decision(
        self, mock_incr, state, try_number, dag_maker, session
    ):
        with dag_maker(dag_id="handled_infra_attempt"):
            task = EmptyOperator(task_id="task", retries=0)
        ti = dag_maker.create_dagrun().get_task_instance(task.task_id, session=session)
        ti.task = task
        ti.state = state
        ti.try_number = try_number
        mock_incr.reset_mock()

        assert not _use_infra_retry(ti=ti, task=task, failure_kind=TaskFailureKind.INFRA)
        assert ti.max_tries == 0
        mock_incr.assert_not_called()


class TestIsEligibleToRetryUsesMaxTries:
    @staticmethod
    def _eligible(*, max_tries: int | None, try_number: int, state: TaskInstanceState | None = None) -> bool:
        stub = SimpleNamespace(state=state, max_tries=max_tries, try_number=try_number)
        return TaskInstance.is_eligible_to_retry(stub)  # type: ignore[arg-type]

    @pytest.mark.parametrize(
        ("max_tries", "try_number", "expected"),
        [
            (None, 1, False),
            (0, 1, False),
            (1, 1, True),
            (2, 2, True),
            (2, 3, False),
        ],
    )
    def test_eligibility_uses_effective_max_tries(self, max_tries, try_number, expected):
        assert self._eligible(max_tries=max_tries, try_number=try_number) is expected

    def test_restarting_is_always_eligible(self):
        assert (
            self._eligible(
                max_tries=0,
                try_number=9,
                state=TaskInstanceState.RESTARTING,
            )
            is True
        )
