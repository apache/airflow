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

from datetime import timedelta
from unittest.mock import ANY, Mock, patch

import pytest

from airflow._shared.timezones.timezone import convert_to_utc, datetime
from airflow.sdk import DAG, BaseOperator
from airflow.ti_deps.dep_context import DepContext
from airflow.ti_deps.deps.prev_dagrun_dep import PrevDagrunDep
from airflow.utils.state import DagRunState, TaskInstanceState
from airflow.utils.types import DagRunTriggeredByType, DagRunType

from tests_common.test_utils.dag import create_scheduler_dag, sync_dag_to_db
from tests_common.test_utils.db import clear_db_runs

pytestmark = pytest.mark.db_test

START_DATE = convert_to_utc(datetime(2016, 1, 1))


class TestPrevDagrunDep:
    def teardown_method(self):
        clear_db_runs()

    def test_first_task_run_of_new_task(self, testing_dag_bundle):
        """
        The first task run of a new task in an old DAG should pass if the task has
        ignore_first_depends_on_past set to True.
        """
        dag = DAG("test_dag", schedule=timedelta(days=1), start_date=START_DATE)
        old_task = BaseOperator(
            task_id="test_task",
            dag=dag,
            depends_on_past=True,
            start_date=START_DATE,
            wait_for_downstream=False,
        )
        scheduler_dag = sync_dag_to_db(dag)
        # Old DAG run will include only TaskInstance of old_task
        scheduler_dag.create_dagrun(
            run_id="old_run",
            state=TaskInstanceState.SUCCESS,
            logical_date=old_task.start_date,
            run_type=DagRunType.SCHEDULED,
            data_interval=(old_task.start_date, old_task.start_date),
            run_after=old_task.start_date,
            triggered_by=DagRunTriggeredByType.TEST,
        )

        new_task = BaseOperator(
            task_id="new_task",
            dag=dag,
            depends_on_past=True,
            ignore_first_depends_on_past=True,
            start_date=old_task.start_date,
        )

        # New DAG run will include 1st TaskInstance of new_task
        logical_date = convert_to_utc(datetime(2016, 1, 2))
        dr = create_scheduler_dag(dag).create_dagrun(
            run_id="new_run",
            state=DagRunState.RUNNING,
            logical_date=logical_date,
            run_type=DagRunType.SCHEDULED,
            data_interval=(logical_date, logical_date),
            run_after=logical_date,
            triggered_by=DagRunTriggeredByType.TEST,
        )

        ti = dr.get_task_instance(new_task.task_id)
        ti.task = new_task

        dep_context = DepContext(ignore_depends_on_past=False)
        dep = PrevDagrunDep()

        with patch.object(dep, "_has_any_prior_tis", Mock(return_value=False)) as mock_has_any_prior_tis:
            assert dep.is_met(ti=ti, dep_context=dep_context)
            mock_has_any_prior_tis.assert_called_once_with(ti, session=ANY)


@pytest.mark.parametrize(
    "kwargs",
    [
        # If the task does not set depends_on_past, the previous dagrun should
        # be ignored, even though previous_ti would otherwise fail the dep.
        # wait_for_past_depends_before_skipping is False, past_depends_met xcom should not be sent
        pytest.param(
            dict(
                depends_on_past=False,
                wait_for_past_depends_before_skipping=False,
                wait_for_downstream=False,  # wait_for_downstream=True overrides depends_on_past=False.
                prev_tis=[Mock(state=None, **{"are_dependents_done.return_value": False})],
                context_ignore_depends_on_past=False,
                expected_dep_met=True,
                past_depends_met_xcom_sent=False,
            ),
            id="not_depends_on_past",
        ),
        # If the task does not set depends_on_past, the previous dagrun should
        # be ignored, even though previous_ti would otherwise fail the dep.
        # wait_for_past_depends_before_skipping is True, past_depends_met xcom should be sent
        pytest.param(
            dict(
                depends_on_past=False,
                wait_for_past_depends_before_skipping=True,
                wait_for_downstream=False,  # wait_for_downstream=True overrides depends_on_past=False.
                prev_tis=[Mock(state=None, **{"are_dependents_done.return_value": False})],
                context_ignore_depends_on_past=False,
                expected_dep_met=True,
                past_depends_met_xcom_sent=True,
            ),
            id="not_depends_on_past_with_wait",
        ),
        # If the context overrides depends_on_past, the dep should be met even
        # though there is no previous_ti which would normally fail the dep.
        # wait_for_past_depends_before_skipping is False, past_depends_met xcom should not be sent
        pytest.param(
            dict(
                depends_on_past=True,
                wait_for_past_depends_before_skipping=False,
                wait_for_downstream=False,
                prev_tis=[
                    Mock(state=TaskInstanceState.SUCCESS, **{"are_dependents_done.return_value": True})
                ],
                context_ignore_depends_on_past=True,
                expected_dep_met=True,
                past_depends_met_xcom_sent=False,
            ),
            id="context_ignore_depends_on_past",
        ),
        # If the context overrides depends_on_past, the dep should be met even
        # though there is no previous_ti which would normally fail the dep.
        # wait_for_past_depends_before_skipping is True, past_depends_met xcom should be sent
        pytest.param(
            dict(
                depends_on_past=True,
                wait_for_past_depends_before_skipping=True,
                wait_for_downstream=False,
                prev_tis=[
                    Mock(state=TaskInstanceState.SUCCESS, **{"are_dependents_done.return_value": True})
                ],
                context_ignore_depends_on_past=True,
                expected_dep_met=True,
                past_depends_met_xcom_sent=True,
            ),
            id="context_ignore_depends_on_past_with_wait",
        ),
        # The first task run should pass since it has no previous dagrun.
        # wait_for_past_depends_before_skipping is False, past_depends_met xcom should not be sent
        pytest.param(
            dict(
                depends_on_past=True,
                wait_for_past_depends_before_skipping=False,
                wait_for_downstream=False,
                prev_tis=[],
                context_ignore_depends_on_past=False,
                expected_dep_met=True,
                past_depends_met_xcom_sent=False,
            ),
            id="first_task_run",
        ),
        # The first task run should pass since it has no previous dagrun.
        # wait_for_past_depends_before_skipping is True, past_depends_met xcom should be sent
        pytest.param(
            dict(
                depends_on_past=True,
                wait_for_past_depends_before_skipping=True,
                wait_for_downstream=False,
                prev_tis=[],
                context_ignore_depends_on_past=False,
                expected_dep_met=True,
                past_depends_met_xcom_sent=True,
            ),
            id="first_task_run_wait",
        ),
        # Previous TI did not complete execution. This dep should fail.
        pytest.param(
            dict(
                depends_on_past=True,
                wait_for_past_depends_before_skipping=False,
                wait_for_downstream=False,
                prev_tis=[Mock(state=None, **{"are_dependents_done.return_value": True})],
                context_ignore_depends_on_past=False,
                expected_dep_met=False,
                past_depends_met_xcom_sent=False,
            ),
            id="prev_ti_bad_state",
        ),
        # Previous TI specified to wait for the downstream tasks of the previous
        # dagrun. It should fail this dep if the previous TI's downstream TIs
        # are not done.
        pytest.param(
            dict(
                depends_on_past=True,
                wait_for_past_depends_before_skipping=False,
                wait_for_downstream=True,
                prev_tis=[
                    Mock(state=TaskInstanceState.SUCCESS, **{"are_dependents_done.return_value": False})
                ],
                context_ignore_depends_on_past=False,
                expected_dep_met=False,
                past_depends_met_xcom_sent=False,
            ),
            id="failed_wait_for_downstream",
        ),
        # All the conditions for the dep are met.
        # wait_for_past_depends_before_skipping is False, past_depends_met xcom should not be sent
        pytest.param(
            dict(
                depends_on_past=True,
                wait_for_past_depends_before_skipping=False,
                wait_for_downstream=True,
                prev_tis=[
                    Mock(state=TaskInstanceState.SUCCESS, **{"are_dependents_done.return_value": True})
                ],
                context_ignore_depends_on_past=False,
                expected_dep_met=True,
                past_depends_met_xcom_sent=False,
            ),
            id="all_met",
        ),
        # All the conditions for the dep are met
        # wait_for_past_depends_before_skipping is True, past_depends_met xcom should be sent
        pytest.param(
            dict(
                depends_on_past=True,
                wait_for_past_depends_before_skipping=True,
                wait_for_downstream=True,
                prev_tis=[
                    Mock(state=TaskInstanceState.SUCCESS, **{"are_dependents_done.return_value": True})
                ],
                context_ignore_depends_on_past=False,
                expected_dep_met=True,
                past_depends_met_xcom_sent=True,
            ),
            id="all_met_with_wait",
        ),
    ],
)
@patch("airflow.models.dagrun.DagRun.get_previous_scheduled_dagrun")
@patch("airflow.models.dagrun.DagRun.get_previous_dagrun")
def test_dagrun_dep(mock_get_previous_dagrun, mock_get_previous_scheduled_dagrun, kwargs):
    depends_on_past = kwargs["depends_on_past"]
    wait_for_past_depends_before_skipping = kwargs["wait_for_past_depends_before_skipping"]
    wait_for_downstream = kwargs["wait_for_downstream"]
    prev_tis = kwargs["prev_tis"]
    context_ignore_depends_on_past = kwargs["context_ignore_depends_on_past"]
    expected_dep_met = kwargs["expected_dep_met"]
    past_depends_met_xcom_sent = kwargs["past_depends_met_xcom_sent"]
    task = BaseOperator(
        task_id="test_task",
        dag=DAG("test_dag", schedule=timedelta(days=1), start_date=datetime(2016, 1, 1)),
        depends_on_past=depends_on_past,
        start_date=datetime(2016, 1, 1),
        wait_for_downstream=wait_for_downstream,
    )
    if prev_tis:
        prev_dagrun = Mock(logical_date=datetime(2016, 1, 2))
    else:
        prev_dagrun = None
    mock_get_previous_scheduled_dagrun.return_value = prev_dagrun
    mock_get_previous_dagrun.return_value = prev_dagrun
    dagrun = Mock(
        **{
            "get_previous_dagrun.return_value": prev_dagrun,
            "backfill_id": None,
            "logical_date": datetime(2016, 1, 3),
            "dag_id": "test_dag",
        },
    )
    ti = Mock(
        task=task,
        task_id=task.task_id,
        **{"get_dagrun.return_value": dagrun, "xcom_push.return_value": None},
    )
    dep_context = DepContext(
        ignore_depends_on_past=context_ignore_depends_on_past,
        wait_for_past_depends_before_skipping=wait_for_past_depends_before_skipping,
    )

    unsuccessful_tis_count = sum(
        int(ti.state not in {TaskInstanceState.SUCCESS, TaskInstanceState.SKIPPED}) for ti in prev_tis
    )

    mock_has_tis = Mock(return_value=bool(prev_tis))
    mock_has_any_prior_tis = Mock(return_value=bool(prev_tis))
    mock_count_unsuccessful_tis = Mock(return_value=unsuccessful_tis_count)
    mock_has_unsuccessful_dependants = Mock(return_value=any(not ti.are_dependents_done() for ti in prev_tis))

    dep = PrevDagrunDep()
    with patch.multiple(
        dep,
        _has_tis=mock_has_tis,
        _has_any_prior_tis=mock_has_any_prior_tis,
        _count_unsuccessful_tis=mock_count_unsuccessful_tis,
        _has_unsuccessful_dependants=mock_has_unsuccessful_dependants,
    ):
        actual_dep_met = dep.is_met(ti=ti, dep_context=dep_context)

        mock_has_any_prior_tis.assert_not_called()
        if depends_on_past and not context_ignore_depends_on_past and prev_tis:
            mock_has_tis.assert_called_once_with(prev_dagrun, "test_task", session=ANY)
            mock_count_unsuccessful_tis.assert_called_once_with(prev_dagrun, "test_task", session=ANY)
        else:
            mock_has_tis.assert_not_called()
            mock_count_unsuccessful_tis.assert_not_called()
        if depends_on_past and not context_ignore_depends_on_past and prev_tis and not unsuccessful_tis_count:
            mock_has_unsuccessful_dependants.assert_called_once_with(prev_dagrun, task, session=ANY)
        else:
            mock_has_unsuccessful_dependants.assert_not_called()

    assert actual_dep_met == expected_dep_met
    if past_depends_met_xcom_sent:
        ti.xcom_push.assert_called_with(key="past_depends_met", value=True)
    else:
        ti.xcom_push.assert_not_called()


class TestDependsOnPreviousTasks:
    """Tests for the depends_on_previous_tasks feature."""

    @patch("airflow.models.dagrun.DagRun.get_previous_scheduled_dagrun")
    @patch("airflow.models.dagrun.DagRun.get_previous_dagrun")
    def test_not_set_passes(self, mock_get_previous_dagrun, mock_get_previous_scheduled_dagrun):
        """When depends_on_previous_tasks is not set, the dep should pass."""
        task = Mock(
            spec=BaseOperator,
            task_id="test_task",
            depends_on_past=False,
            depends_on_previous_tasks=None,
            dag=Mock(catchup=False),
            start_date=None,
        )
        dagrun = Mock(backfill_id=None, logical_date=datetime(2016, 1, 3), dag_id="test_dag")
        ti = Mock(task=task, task_id="test_task", **{"get_dagrun.return_value": dagrun})

        dep_context = DepContext(ignore_depends_on_past=False)
        dep = PrevDagrunDep()
        assert dep.is_met(ti=ti, dep_context=dep_context)

    @patch("airflow.models.dagrun.DagRun.get_previous_scheduled_dagrun")
    @patch("airflow.models.dagrun.DagRun.get_previous_dagrun")
    def test_all_succeeded_passes(self, mock_get_previous_dagrun, mock_get_previous_scheduled_dagrun):
        """When all listed tasks succeeded in prev run, the dep should pass."""
        task = Mock(
            spec=BaseOperator,
            task_id="C",
            depends_on_past=False,
            depends_on_previous_tasks=["D", "E"],
            dag=Mock(catchup=False),
            start_date=None,
        )
        prev_dagrun = Mock(logical_date=datetime(2016, 1, 2))
        mock_get_previous_dagrun.return_value = prev_dagrun
        dagrun = Mock(backfill_id=None, logical_date=datetime(2016, 1, 3), dag_id="test_dag")
        ti = Mock(
            task=task,
            task_id="C",
            **{"get_dagrun.return_value": dagrun, "xcom_push.return_value": None},
        )

        dep_context = DepContext(ignore_depends_on_past=False)
        dep = PrevDagrunDep()
        with patch.multiple(
            dep,
            _has_tis=Mock(return_value=True),
            _count_unsuccessful_tis=Mock(return_value=0),
        ):
            assert dep.is_met(ti=ti, dep_context=dep_context)

    @patch("airflow.models.dagrun.DagRun.get_previous_scheduled_dagrun")
    @patch("airflow.models.dagrun.DagRun.get_previous_dagrun")
    def test_one_failed_blocks(self, mock_get_previous_dagrun, mock_get_previous_scheduled_dagrun):
        """When one listed task failed in prev run, the dep should fail."""
        task = Mock(
            spec=BaseOperator,
            task_id="C",
            depends_on_past=False,
            depends_on_previous_tasks=["D", "E"],
            dag=Mock(catchup=False),
            start_date=None,
        )
        prev_dagrun = Mock(logical_date=datetime(2016, 1, 2))
        mock_get_previous_dagrun.return_value = prev_dagrun
        dagrun = Mock(backfill_id=None, logical_date=datetime(2016, 1, 3), dag_id="test_dag")
        ti = Mock(
            task=task,
            task_id="C",
            **{"get_dagrun.return_value": dagrun, "xcom_push.return_value": None},
        )

        dep_context = DepContext(ignore_depends_on_past=False)
        dep = PrevDagrunDep()

        def count_unsuccessful(dagrun, task_id, *, session):
            return 1 if task_id == "D" else 0

        with patch.multiple(
            dep,
            _has_tis=Mock(return_value=True),
            _count_unsuccessful_tis=count_unsuccessful,
        ):
            assert not dep.is_met(ti=ti, dep_context=dep_context)

    @patch("airflow.models.dagrun.DagRun.get_previous_scheduled_dagrun")
    @patch("airflow.models.dagrun.DagRun.get_previous_dagrun")
    def test_first_run_passes(self, mock_get_previous_dagrun, mock_get_previous_scheduled_dagrun):
        """No previous dagrun (first run) should pass."""
        task = Mock(
            spec=BaseOperator,
            task_id="C",
            depends_on_past=False,
            depends_on_previous_tasks=["D"],
            dag=Mock(catchup=False),
            start_date=None,
        )
        mock_get_previous_dagrun.return_value = None
        dagrun = Mock(backfill_id=None, logical_date=datetime(2016, 1, 3), dag_id="test_dag")
        ti = Mock(
            task=task,
            task_id="C",
            **{"get_dagrun.return_value": dagrun, "xcom_push.return_value": None},
        )

        dep_context = DepContext(ignore_depends_on_past=False)
        dep = PrevDagrunDep()
        assert dep.is_met(ti=ti, dep_context=dep_context)

    @patch("airflow.models.dagrun.DagRun.get_previous_scheduled_dagrun")
    @patch("airflow.models.dagrun.DagRun.get_previous_dagrun")
    def test_context_ignore_bypasses(self, mock_get_previous_dagrun, mock_get_previous_scheduled_dagrun):
        """ignore_depends_on_past=True should bypass depends_on_previous_tasks check."""
        task = Mock(
            spec=BaseOperator,
            task_id="C",
            depends_on_past=False,
            depends_on_previous_tasks=["D"],
            dag=Mock(catchup=False),
            start_date=None,
        )
        dagrun = Mock(backfill_id=None, logical_date=datetime(2016, 1, 3), dag_id="test_dag")
        ti = Mock(
            task=task,
            task_id="C",
            **{"get_dagrun.return_value": dagrun, "xcom_push.return_value": None},
        )

        dep_context = DepContext(ignore_depends_on_past=True)
        dep = PrevDagrunDep()
        assert dep.is_met(ti=ti, dep_context=dep_context)

    @patch("airflow.models.dagrun.DagRun.get_previous_scheduled_dagrun")
    @patch("airflow.models.dagrun.DagRun.get_previous_dagrun")
    def test_skipped_is_ok(self, mock_get_previous_dagrun, mock_get_previous_scheduled_dagrun):
        """SKIPPED tasks in prev run should be treated as successful."""
        task = Mock(
            spec=BaseOperator,
            task_id="C",
            depends_on_past=False,
            depends_on_previous_tasks=["D"],
            dag=Mock(catchup=False),
            start_date=None,
        )
        prev_dagrun = Mock(logical_date=datetime(2016, 1, 2))
        mock_get_previous_dagrun.return_value = prev_dagrun
        dagrun = Mock(backfill_id=None, logical_date=datetime(2016, 1, 3), dag_id="test_dag")
        ti = Mock(
            task=task,
            task_id="C",
            **{"get_dagrun.return_value": dagrun, "xcom_push.return_value": None},
        )

        dep_context = DepContext(ignore_depends_on_past=False)
        dep = PrevDagrunDep()
        # _count_unsuccessful_tis returns 0 for SKIPPED tasks (they're in _SUCCESSFUL_STATES)
        with patch.multiple(
            dep,
            _has_tis=Mock(return_value=True),
            _count_unsuccessful_tis=Mock(return_value=0),
        ):
            assert dep.is_met(ti=ti, dep_context=dep_context)

    @patch("airflow.models.dagrun.DagRun.get_previous_scheduled_dagrun")
    @patch("airflow.models.dagrun.DagRun.get_previous_dagrun")
    def test_task_not_in_prev_run_fails(self, mock_get_previous_dagrun, mock_get_previous_scheduled_dagrun):
        """A listed task not found in the previous dagrun should fail."""
        task = Mock(
            spec=BaseOperator,
            task_id="C",
            depends_on_past=False,
            depends_on_previous_tasks=["D"],
            dag=Mock(catchup=False),
            start_date=None,
        )
        prev_dagrun = Mock(logical_date=datetime(2016, 1, 2))
        mock_get_previous_dagrun.return_value = prev_dagrun
        dagrun = Mock(backfill_id=None, logical_date=datetime(2016, 1, 3), dag_id="test_dag")
        ti = Mock(
            task=task,
            task_id="C",
            **{"get_dagrun.return_value": dagrun, "xcom_push.return_value": None},
        )

        dep_context = DepContext(ignore_depends_on_past=False)
        dep = PrevDagrunDep()
        with patch.multiple(
            dep,
            _has_tis=Mock(return_value=False),
            _count_unsuccessful_tis=Mock(return_value=0),
        ):
            assert not dep.is_met(ti=ti, dep_context=dep_context)


class TestDependsOnPreviousTasksValidation:
    """Tests for mutual exclusivity validation."""

    def test_conflicts_with_depends_on_past(self):
        """Setting both depends_on_previous_tasks and depends_on_past should raise."""
        with pytest.raises(ValueError, match="cannot be combined"):
            BaseOperator(
                task_id="test",
                depends_on_past=True,
                depends_on_previous_tasks=["A"],
            )

    def test_conflicts_with_wait_for_downstream(self):
        """Setting both depends_on_previous_tasks and wait_for_downstream should raise."""
        with pytest.raises(ValueError, match="cannot be combined"):
            BaseOperator(
                task_id="test",
                wait_for_downstream=True,
                depends_on_previous_tasks=["A"],
            )

    def test_standalone_is_valid(self):
        """Setting only depends_on_previous_tasks should be valid."""
        dag = DAG("test_dag", schedule=timedelta(days=1), start_date=datetime(2016, 1, 1))
        task = BaseOperator(
            task_id="test",
            dag=dag,
            depends_on_previous_tasks=["A", "B"],
        )
        assert task.depends_on_previous_tasks == ["A", "B"]
        assert task.depends_on_past is False
        assert task.wait_for_downstream is False
