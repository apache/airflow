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

from unittest.mock import ANY, Mock, patch

import pytest

from airflow.models.baseoperator import BaseOperator
from airflow.models.dag import DAG
from airflow.ti_deps.dep_context import DepContext
from airflow.ti_deps.deps.prev_dagrun_dep import PrevDagrunDep
from airflow.utils.state import DagRunState, TaskInstanceState
from airflow.utils.timezone import convert_to_utc, datetime
from airflow.utils.types import DagRunType
from tests.test_utils.db import clear_db_runs

pytestmark = pytest.mark.db_test


class TestPrevDagrunDep:
    def teardown_method(self):
        clear_db_runs()

    def test_first_task_run_of_new_task(self):
        """
        The first task run of a new task in an old DAG should pass if the task has
        ignore_first_depends_on_past set to True.
        """
        dag = DAG("test_dag")
        old_task = BaseOperator(
            task_id="test_task",
            dag=dag,
            depends_on_past=True,
            start_date=convert_to_utc(datetime(2016, 1, 1)),
            wait_for_downstream=False,
        )
        # Old DAG run will include only TaskInstance of old_task
        dag.create_dagrun(
            run_id="old_run",
            state=TaskInstanceState.SUCCESS,
            execution_date=old_task.start_date,
            run_type=DagRunType.SCHEDULED,
        )

        new_task = BaseOperator(
            task_id="new_task",
            dag=dag,
            depends_on_past=True,
            ignore_first_depends_on_past=True,
            start_date=old_task.start_date,
        )

        # New DAG run will include 1st TaskInstance of new_task
        dr = dag.create_dagrun(
            run_id="new_run",
            state=DagRunState.RUNNING,
            execution_date=convert_to_utc(datetime(2016, 1, 2)),
            run_type=DagRunType.SCHEDULED,
        )

        ti = dr.get_task_instance(new_task.task_id)
        ti.task = new_task

        dep_context = DepContext(ignore_depends_on_past=False)
        dep = PrevDagrunDep()

        with patch.object(dep, "_has_any_prior_tis", Mock(return_value=False)) as mock_has_any_prior_tis:
            assert dep.is_met(ti=ti, dep_context=dep_context)
            mock_has_any_prior_tis.assert_called_once_with(ti, session=ANY)


@pytest.mark.parametrize(
    (
        "depends_on_past",
        "wait_for_past_depends_before_skipping",
        "wait_for_downstream",
        "prev_tis",
        "context_ignore_depends_on_past",
        "expected_dep_met",
        "past_depends_met_xcom_sent",
    ),
    [
        # If the task does not set depends_on_past, the previous dagrun should
        # be ignored, even though previous_ti would otherwise fail the dep.
        # wait_for_past_depends_before_skipping is False, past_depends_met xcom should not be sent
        pytest.param(
            False,
            False,
            False,  # wait_for_downstream=True overrides depends_on_past=False.
            [Mock(state=None, **{"are_dependents_done.return_value": False})],
            False,
            True,
            False,
            id="not_depends_on_past",
        ),
        # If the task does not set depends_on_past, the previous dagrun should
        # be ignored, even though previous_ti would otherwise fail the dep.
        # wait_for_past_depends_before_skipping is True, past_depends_met xcom should be sent
        pytest.param(
            False,
            True,
            False,  # wait_for_downstream=True overrides depends_on_past=False.
            [Mock(state=None, **{"are_dependents_done.return_value": False})],
            False,
            True,
            True,
            id="not_depends_on_past",
        ),
        # If the context overrides depends_on_past, the dep should be met even
        # though there is no previous_ti which would normally fail the dep.
        # wait_for_past_depends_before_skipping is False, past_depends_met xcom should not be sent
        pytest.param(
            True,
            False,
            False,
            [Mock(state=TaskInstanceState.SUCCESS, **{"are_dependents_done.return_value": True})],
            True,
            True,
            False,
            id="context_ignore_depends_on_past",
        ),
        # If the context overrides depends_on_past, the dep should be met even
        # though there is no previous_ti which would normally fail the dep.
        # wait_for_past_depends_before_skipping is True, past_depends_met xcom should be sent
        pytest.param(
            True,
            True,
            False,
            [Mock(state=TaskInstanceState.SUCCESS, **{"are_dependents_done.return_value": True})],
            True,
            True,
            True,
            id="context_ignore_depends_on_past",
        ),
        # The first task run should pass since it has no previous dagrun.
        # wait_for_past_depends_before_skipping is False, past_depends_met xcom should not be sent
        pytest.param(True, False, False, [], False, True, False, id="first_task_run"),
        # The first task run should pass since it has no previous dagrun.
        # wait_for_past_depends_before_skipping is True, past_depends_met xcom should be sent
        pytest.param(True, True, False, [], False, True, True, id="first_task_run_wait"),
        # Previous TI did not complete execution. This dep should fail.
        pytest.param(
            True,
            False,
            False,
            [Mock(state=None, **{"are_dependents_done.return_value": True})],
            False,
            False,
            False,
            id="prev_ti_bad_state",
        ),
        # Previous TI specified to wait for the downstream tasks of the previous
        # dagrun. It should fail this dep if the previous TI's downstream TIs
        # are not done.
        pytest.param(
            True,
            False,
            True,
            [Mock(state=TaskInstanceState.SUCCESS, **{"are_dependents_done.return_value": False})],
            False,
            False,
            False,
            id="failed_wait_for_downstream",
        ),
        # All the conditions for the dep are met.
        # wait_for_past_depends_before_skipping is False, past_depends_met xcom should not be sent
        pytest.param(
            True,
            False,
            True,
            [Mock(state=TaskInstanceState.SUCCESS, **{"are_dependents_done.return_value": True})],
            False,
            True,
            False,
            id="all_met",
        ),
        # All the conditions for the dep are met
        # wait_for_past_depends_before_skipping is False, past_depends_met xcom should not be sent
        pytest.param(
            True,
            True,
            True,
            [Mock(state=TaskInstanceState.SUCCESS, **{"are_dependents_done.return_value": True})],
            False,
            True,
            True,
            id="all_met",
        ),
    ],
)
@patch("airflow.models.dagrun.DagRun.get_previous_scheduled_dagrun")
def test_dagrun_dep(
    mock_get_previous_scheduled_dagrun,
    depends_on_past,
    wait_for_past_depends_before_skipping,
    wait_for_downstream,
    prev_tis,
    context_ignore_depends_on_past,
    expected_dep_met,
    past_depends_met_xcom_sent,
):
    task = BaseOperator(
        task_id="test_task",
        dag=DAG("test_dag"),
        depends_on_past=depends_on_past,
        start_date=datetime(2016, 1, 1),
        wait_for_downstream=wait_for_downstream,
    )
    if prev_tis:
        prev_dagrun = Mock(execution_date=datetime(2016, 1, 2))
    else:
        prev_dagrun = None
    mock_get_previous_scheduled_dagrun.return_value = prev_dagrun
    dagrun = Mock(
        **{
            "get_previous_dagrun.return_value": prev_dagrun,
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
