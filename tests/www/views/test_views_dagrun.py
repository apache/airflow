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

import pytest

from airflow.models import DagBag, DagRun, TaskInstance
from airflow.utils import timezone
from airflow.utils.session import create_session
from airflow.utils.types import DagRunTriggeredByType, DagRunType

from tests_common.test_utils.www import (
    check_content_in_response,
    check_content_not_in_response,
)

pytestmark = pytest.mark.db_test


@pytest.fixture(scope="module", autouse=True)
def _init_blank_dagrun():
    """Make sure there are no runs before we test anything.

    This really shouldn't be needed, but tests elsewhere leave the db dirty.
    """
    with create_session() as session:
        session.query(DagRun).delete()
        session.query(TaskInstance).delete()


@pytest.fixture(autouse=True)
def _reset_dagrun():
    yield
    with create_session() as session:
        session.query(DagRun).delete()
        session.query(TaskInstance).delete()


@pytest.fixture
def running_dag_run(session):
    dag = DagBag().get_dag("example_bash_operator")
    logical_date = timezone.datetime(2016, 1, 9)
    dr = dag.create_dagrun(
        state="running",
        logical_date=logical_date,
        data_interval=(logical_date, logical_date),
        run_id="test_dag_runs_action",
        run_type=DagRunType.MANUAL,
        session=session,
        run_after=logical_date,
        triggered_by=DagRunTriggeredByType.TEST,
    )
    session.add(dr)
    tis = [
        TaskInstance(dag.get_task("runme_0"), run_id=dr.run_id, state="success"),
        TaskInstance(dag.get_task("runme_1"), run_id=dr.run_id, state="failed"),
    ]
    session.bulk_save_objects(tis)
    session.commit()
    return dr


@pytest.fixture
def completed_dag_run_with_missing_task(session):
    dag = DagBag().get_dag("example_bash_operator")
    logical_date = timezone.datetime(2016, 1, 9)
    dr = dag.create_dagrun(
        state="success",
        logical_date=logical_date,
        data_interval=(logical_date, logical_date),
        run_id="test_dag_runs_action",
        run_type=DagRunType.MANUAL,
        session=session,
        run_after=logical_date,
        triggered_by=DagRunTriggeredByType.TEST,
    )
    session.add(dr)
    tis = [
        TaskInstance(dag.get_task("runme_0"), run_id=dr.run_id, state="success"),
        TaskInstance(dag.get_task("runme_1"), run_id=dr.run_id, state="success"),
        TaskInstance(dag.get_task("also_run_this"), run_id=dr.run_id, state="success"),
        TaskInstance(dag.get_task("run_after_loop"), run_id=dr.run_id, state="success"),
        TaskInstance(dag.get_task("this_will_skip"), run_id=dr.run_id, state="success"),
        TaskInstance(dag.get_task("run_this_last"), run_id=dr.run_id, state="success"),
    ]
    session.bulk_save_objects(tis)
    session.commit()
    return dag, dr


@pytest.mark.parametrize(
    "action, expected_ti_states, expected_message",
    [
        (
            "clear",
            {None},
            "1 dag runs and 2 task instances were cleared",
        ),
        (
            "set_success",
            {"success"},
            "1 dag runs and 1 task instances were set to success",
        ),
        (
            "set_failed",
            {"success", "failed"},  # The success ti is not set to failed.
            "1 dag runs and 0 task instances were set to failed",
        ),
        (
            "set_running",
            {"success", "failed"},  # Unchanged.
            "1 dag runs were set to running",
        ),
        (
            "set_queued",
            {"success", "failed"},
            "1 dag runs were set to queued",
        ),
    ],
    ids=["clear", "success", "failed", "running", "queued"],
)
def test_set_dag_runs_action(
    session,
    admin_client,
    running_dag_run,
    action,
    expected_ti_states,
    expected_message,
):
    resp = admin_client.post(
        "/dagrun/action_post",
        data={"action": action, "rowid": [running_dag_run.id]},
        follow_redirects=True,
    )
    check_content_in_response(expected_message, resp)
    assert {ti.state for ti in session.query(TaskInstance).all()} == expected_ti_states


@pytest.mark.parametrize(
    "action, expected_message",
    [
        ("clear", "Failed to clear state"),
        ("set_success", "Failed to set state"),
        ("set_failed", "Failed to set state"),
        ("set_running", "Failed to set state"),
        ("set_queued", "Failed to set state"),
    ],
    ids=["clear", "success", "failed", "running", "queued"],
)
def test_set_dag_runs_action_fails(admin_client, action, expected_message):
    resp = admin_client.post(
        "/dagrun/action_post",
        data={"action": action, "rowid": ["0"]},
        follow_redirects=True,
    )
    check_content_in_response(expected_message, resp)


def test_muldelete_dag_runs_action(session, admin_client, running_dag_run):
    dag_run_id = running_dag_run.id
    resp = admin_client.post(
        "/dagrun/action_post",
        data={"action": "muldelete", "rowid": [dag_run_id]},
        follow_redirects=True,
    )
    assert resp.status_code == 200
    assert session.query(TaskInstance).count() == 0  # associated TIs are deleted
    assert session.query(DagRun).filter(DagRun.id == dag_run_id).count() == 0


def test_dag_runs_queue_new_tasks_action(session, admin_client, completed_dag_run_with_missing_task):
    dag, dag_run = completed_dag_run_with_missing_task
    resp = admin_client.post(
        "/dagrun_queued",
        data={"dag_id": dag.dag_id, "dag_run_id": dag_run.run_id, "confirmed": False},
    )

    check_content_in_response("runme_2", resp)
    check_content_not_in_response("runme_1", resp)
    assert resp.status_code == 200


@pytest.fixture
def dag_run_with_all_done_task(session):
    """Creates a DAG run for example_bash_decorator with tasks in various states and an ALL_DONE task not yet run."""
    dag = DagBag().get_dag("example_bash_decorator")

    # Re-sync the DAG to the DB
    dag.sync_to_db()

    logical_date = timezone.datetime(2016, 1, 9)
    dr = dag.create_dagrun(
        state="running",
        logical_date=logical_date,
        data_interval=(logical_date, logical_date),
        run_id="test_dagrun_failed",
        run_type=DagRunType.MANUAL,
        session=session,
        run_after=logical_date,
        triggered_by=DagRunTriggeredByType.TEST,
    )

    # Create task instances in various states to test the ALL_DONE trigger rule
    tis = [
        # runme_loop tasks
        TaskInstance(dag.get_task("runme_0"), run_id=dr.run_id, state="success"),
        TaskInstance(dag.get_task("runme_1"), run_id=dr.run_id, state="failed"),
        TaskInstance(dag.get_task("runme_2"), run_id=dr.run_id, state="running"),
        # Other tasks before run_this_last
        TaskInstance(dag.get_task("run_after_loop"), run_id=dr.run_id, state="success"),
        TaskInstance(dag.get_task("also_run_this"), run_id=dr.run_id, state="success"),
        TaskInstance(dag.get_task("also_run_this_again"), run_id=dr.run_id, state="skipped"),
        TaskInstance(dag.get_task("this_will_skip"), run_id=dr.run_id, state="running"),
        # The task with trigger_rule=ALL_DONE
        TaskInstance(dag.get_task("run_this_last"), run_id=dr.run_id, state=None),
    ]
    session.bulk_save_objects(tis)
    session.commit()

    return dag, dr


def test_dagrun_failed(session, admin_client, dag_run_with_all_done_task):
    """Test marking a dag run as failed with a task having trigger_rule='all_done'"""
    dag, dr = dag_run_with_all_done_task

    # Verify task instances were created
    task_instances = (
        session.query(TaskInstance)
        .filter(TaskInstance.dag_id == dr.dag_id, TaskInstance.run_id == dr.run_id)
        .all()
    )
    assert len(task_instances) > 0

    resp = admin_client.post(
        "/dagrun_failed",
        data={"dag_id": dr.dag_id, "dag_run_id": dr.run_id, "confirmed": "true"},
        follow_redirects=True,
    )

    assert resp.status_code == 200

    with create_session() as session:
        updated_dr = (
            session.query(DagRun).filter(DagRun.dag_id == dr.dag_id, DagRun.run_id == dr.run_id).first()
        )
        assert updated_dr.state == "failed"

        task_instances = (
            session.query(TaskInstance)
            .filter(TaskInstance.dag_id == dr.dag_id, TaskInstance.run_id == dr.run_id)
            .all()
        )

        done_states = {"success", "failed", "skipped", "upstream_failed"}
        for ti in task_instances:
            assert ti.state in done_states
