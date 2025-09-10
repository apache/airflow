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

from typing import TYPE_CHECKING
from unittest import mock

import pytest

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

from airflow.jobs.triggerer_job_runner import TriggererJobRunner
from airflow.models import DagModel, DagRun, TaskInstance, Trigger
from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.utils import timezone
from airflow.utils.state import DagRunState, State as TaskInstanceState
from airflow.utils.types import DagRunType
from tests.test_utils.db import clear_db_dags, clear_db_runs, clear_db_serialized_dags


@pytest.fixture(autouse=True)
def clean_db():
    """Clean up database before and after each test."""
    clear_db_runs()
    clear_db_dags()
    clear_db_serialized_dags()
    yield
    clear_db_runs()
    clear_db_dags()
    clear_db_serialized_dags()


def test_cleanup_invalid_triggers_paused_dag(session: Session):
    """Test that triggers for paused DAGs are cleaned up."""
    # Create a DAG and a task instance with a trigger
    dag_id = "test_cleanup_invalid_triggers_paused_dag"
    with DAG(dag_id=dag_id, start_date=timezone.utcnow(), schedule=None) as _:
        task = EmptyOperator(task_id="dummy")

    # Create a DAG run
    dag_run = DagRun(
        dag_id=dag_id,
        run_id=f"test_run_{dag_id}",
        run_type=DagRunType.MANUAL,
        execution_date=timezone.utcnow(),
        start_date=timezone.utcnow(),
        state=DagRunState.RUNNING,
        external_trigger=True,
    )
    session.add(dag_run)
    session.flush()
    # Create a task instance with a trigger
    ti = TaskInstance(task=task, run_id=dag_run.run_id)
    ti.state = TaskInstanceState.DEFERRED
    trigger = Trigger(
        classpath="airflow.triggers.temporal.TriggerWithStatus",
        kwargs={},
        created_date=timezone.utcnow(),
    )
    session.add(trigger)
    session.flush()
    ti.trigger_id = trigger.id
    session.add(ti)
    session.commit()

    # Create a triggerer job runner with a properly configured mock job
    mock_job = mock.MagicMock()
    mock_job.id = 1
    mock_job.job_type = "TriggererJob"  # Set the expected job_type
    job = TriggererJobRunner(job=mock_job)

    # Assign the trigger to the triggerer
    trigger.triggerer_id = job.job.id
    session.merge(trigger)
    session.commit()

    # Mark DAG as paused after assigning the trigger
    dag_model = DagModel(dag_id=dag_id, is_paused=True, is_active=True)
    session.add(dag_model)
    session.flush()

    # Run cleanup with session
    job._cleanup_invalid_triggers(session=session)

    # Refresh the session to get the latest state from the database
    session.expire_all()

    # Verify the trigger was unassigned and task instance was marked as failed
    ti = session.query(TaskInstance).filter(TaskInstance.dag_id == dag_id).one()
    print(f"TaskInstance state after cleanup: {ti.state}")
    print(f"TaskInstance end_date: {ti.end_date}")
    print(f"TaskInstance trigger_id: {ti.trigger_id}")

    # Check if the trigger was unassigned
    trigger = session.get(Trigger, ti.trigger_id) if ti.trigger_id else None
    print(f"Trigger after cleanup: {trigger}")
    if trigger:
        print(f"Trigger triggerer_id: {trigger.triggerer_id}")

    # For paused DAGs, the task instance should remain DEFERRED
    # and keep the trigger_id for reassignment when DAG is unpaused
    assert ti.state == TaskInstanceState.DEFERRED
    assert ti.trigger_id is not None, "trigger_id should be preserved for paused DAGs"

    # The trigger should be unassigned from the triggerer
    trigger = session.get(Trigger, ti.trigger_id)
    assert trigger is not None
    assert trigger.triggerer_id is None, "Trigger should be unassigned from triggerer"


def test_cleanup_invalid_triggers_deactivated_dag(session: Session):
    """Test that triggers for deactivated DAGs are cleaned up."""
    # Create a DAG and a task instance with a trigger
    dag_id = "test_cleanup_invalid_triggers_deactivated_dag"
    with DAG(dag_id=dag_id, start_date=timezone.utcnow(), schedule=None) as _:
        task = EmptyOperator(task_id="dummy")

    # Create a DAG run
    dag_run = DagRun(
        dag_id=dag_id,
        run_id=f"test_run_{dag_id}",
        run_type=DagRunType.MANUAL,
        execution_date=timezone.utcnow(),
        start_date=timezone.utcnow(),
        state=DagRunState.RUNNING,
        external_trigger=True,
    )
    session.add(dag_run)
    session.flush()
    # Create a task instance with a trigger
    ti = TaskInstance(task=task, run_id=dag_run.run_id)
    ti.state = TaskInstanceState.DEFERRED
    trigger = Trigger(
        classpath="airflow.triggers.temporal.TriggerWithStatus",
        kwargs={},
        created_date=timezone.utcnow(),
    )
    session.add(trigger)
    session.flush()
    ti.trigger_id = trigger.id
    session.add(ti)
    session.commit()

    # Create a triggerer job runner with a properly configured mock job
    mock_job = mock.MagicMock()
    mock_job.id = 1
    mock_job.job_type = "TriggererJob"  # Set the expected job_type
    job = TriggererJobRunner(job=mock_job)

    # Assign the trigger to the triggerer
    trigger.triggerer_id = job.job.id
    session.merge(trigger)
    session.commit()

    # Mark DAG as deactivated after assigning the trigger
    dag_model = DagModel(dag_id=dag_id, is_paused=False, is_active=False)
    session.add(dag_model)
    session.flush()

    # Run cleanup with session
    job._cleanup_invalid_triggers(session=session)

    # Refresh the session to get the latest state from the database
    session.expire_all()

    # Verify the trigger was unassigned and task instance was marked as failed
    ti = session.query(TaskInstance).filter(TaskInstance.dag_id == dag_id).one()
    print(f"TaskInstance state after cleanup: {ti.state}")
    print(f"TaskInstance end_date: {ti.end_date}")
    print(f"TaskInstance trigger_id: {ti.trigger_id}")

    # Check if the trigger was unassigned
    trigger = session.get(Trigger, ti.trigger_id) if ti.trigger_id else None
    print(f"Trigger after cleanup: {trigger}")
    if trigger:
        print(f"Trigger triggerer_id: {trigger.triggerer_id}")

    # For deactivated DAGs, the task instance should be marked as FAILED
    assert ti.state == TaskInstanceState.FAILED
    assert ti.trigger_id is None


def test_get_sorted_triggers_excludes_paused_dags(session: Session):
    """Test that get_sorted_triggers excludes triggers from paused DAGs."""
    from airflow.models.trigger import Trigger

    # Create a DAG and a task instance with a trigger
    dag_id = "test_get_sorted_triggers_excludes_paused_dags"
    with DAG(dag_id=dag_id, start_date=timezone.utcnow(), schedule=None) as _:
        task = EmptyOperator(task_id="dummy")

    # Create a DAG run
    dag_run = DagRun(
        dag_id=dag_id,
        run_id=f"test_run_{dag_id}",
        run_type=DagRunType.MANUAL,
        execution_date=timezone.utcnow(),
        start_date=timezone.utcnow(),
        state=DagRunState.RUNNING,
        external_trigger=True,
    )
    session.add(dag_run)
    session.flush()

    # Create a task instance with a trigger
    ti = TaskInstance(task=task, run_id=dag_run.run_id)
    ti.state = TaskInstanceState.DEFERRED
    trigger = Trigger(
        classpath="airflow.triggers.temporal.TriggerWithStatus",
        kwargs={},
        created_date=timezone.utcnow(),
    )
    session.add(trigger)
    session.flush()
    ti.trigger_id = trigger.id
    session.add(ti)
    session.commit()

    # Mark DAG as paused
    dag_model = DagModel(dag_id=dag_id, is_paused=True, is_active=True)
    session.add(dag_model)
    session.commit()

    # Create a triggerer job runner with a properly configured mock job
    mock_job = mock.MagicMock()
    mock_job.id = 1
    mock_job.job_type = "TriggererJob"  # Set the expected job_type
    job = TriggererJobRunner(job=mock_job)

    # Get sorted triggers - should exclude the paused DAG's trigger
    triggers = job.get_sorted_triggers(session, 10)
    assert len(triggers) == 0


def test_get_sorted_triggers_excludes_deactivated_dags(session: Session):
    """Test that get_sorted_triggers excludes triggers from deactivated DAGs."""
    from airflow.models.trigger import Trigger

    # Create a DAG and a task instance with a trigger
    dag_id = "test_get_sorted_triggers_excludes_deactivated_dags"
    with DAG(dag_id=dag_id, start_date=timezone.utcnow(), schedule=None) as _:
        task = EmptyOperator(task_id="dummy")

    # Create a DAG run
    dag_run = DagRun(
        dag_id=dag_id,
        run_id=f"test_run_{dag_id}",
        run_type=DagRunType.MANUAL,
        execution_date=timezone.utcnow(),
        start_date=timezone.utcnow(),
        state=DagRunState.RUNNING,
        external_trigger=True,
    )
    session.add(dag_run)
    session.flush()

    # Create a task instance with a trigger
    ti = TaskInstance(task=task, run_id=dag_run.run_id)
    ti.state = TaskInstanceState.DEFERRED
    trigger = Trigger(
        classpath="airflow.triggers.temporal.TriggerWithStatus",
        kwargs={},
        created_date=timezone.utcnow(),
    )
    session.add(trigger)
    session.flush()
    ti.trigger_id = trigger.id
    session.add(ti)
    session.commit()

    # Mark DAG as deactivated
    dag_model = DagModel(dag_id=dag_id, is_paused=False, is_active=False)
    session.add(dag_model)
    session.commit()

    # Create a triggerer job runner with a properly configured mock job
    mock_job = mock.MagicMock()
    mock_job.id = 1
    mock_job.job_type = "TriggererJob"  # Set the expected job_type
    job = TriggererJobRunner(job=mock_job)

    # Get sorted triggers - should exclude the deactivated DAG's trigger
    triggers = job.get_sorted_triggers(session, 10)
    assert len(triggers) == 0
