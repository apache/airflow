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

import pendulum
import pytest
from sqlalchemy import select

from airflow.jobs.job import Job
from airflow.jobs.scheduler_job_runner import SchedulerJobRunner
from airflow.models.dag import DagModel
from airflow.models.dagrun import DagRun
from airflow.models.serialized_dag import SerializedDagModel
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import DAG
from airflow.utils.session import create_session

from tests_common.test_utils.db import clear_db_dags, clear_db_runs, clear_db_serialized_dags

pytestmark = pytest.mark.db_test


class TestSchedulerStaleSerializedDags:
    """Test that scheduler ignores stale serialized DAGs during DagRun creation."""

    @pytest.fixture(autouse=True)
    def setup_test_cases(self, mock_executor):
        """Set up test fixtures."""
        self.null_exec = mock_executor
        clear_db_runs()
        clear_db_dags()
        clear_db_serialized_dags()
        yield
        clear_db_runs()
        clear_db_dags()
        clear_db_serialized_dags()

    @pytest.mark.need_serialized_dag
    def test_scheduler_skips_stale_serialized_dags(self, dag_maker, session):
        """
        Test that scheduler only creates DagRuns for DAGs with current SerializedDagModel records.

        Scenario:
        1. Create two DAGs (dag_a and dag_b) with SerializedDagModel records
        2. Delete SerializedDagModel for dag_b (simulating a removed DAG file)
        3. Run _create_dag_runs
        4. Assert DagRun is created only for dag_a
        5. Assert dag_b is ignored (no DagRun created)
        """
        # Create DAG A with serialization
        with dag_maker(
            dag_id="dag_a",
            schedule="@daily",
            start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
            session=session,
        ) as dag_a:
            EmptyOperator(task_id="task_a")

        dag_a_model = dag_maker.dag_model

        # Create DAG B with serialization
        with dag_maker(
            dag_id="dag_b",
            schedule="@daily",
            start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
            session=session,
        ) as dag_b:
            EmptyOperator(task_id="task_b")

        dag_b_model = dag_maker.dag_model
        session.flush()

        # Verify both SerializedDagModels exist
        serialized_dags = session.scalars(select(SerializedDagModel.dag_id)).all()
        assert set(serialized_dags) == {"dag_a", "dag_b"}

        # Simulate stale DAG: delete SerializedDagModel for dag_b
        # (as if the DAG file was removed but DagModel still exists)
        session.execute(
            select(SerializedDagModel)
            .where(SerializedDagModel.dag_id == "dag_b")
            .with_for_update()
        )
        session.query(SerializedDagModel).filter(SerializedDagModel.dag_id == "dag_b").delete()
        session.flush()

        # Verify only dag_a has SerializedDagModel now
        remaining_serialized = session.scalars(select(SerializedDagModel.dag_id)).all()
        assert remaining_serialized == ["dag_a"]

        # Create scheduler and run DagRun creation
        scheduler_job = Job(executor=self.null_exec)
        job_runner = SchedulerJobRunner(job=scheduler_job)

        with create_session() as run_session:
            # Call _create_dag_runs with both dag_models
            job_runner._create_dag_runs([dag_a_model, dag_b_model], run_session)
            run_session.flush()

        # Assert DagRun was created only for dag_a
        dag_runs = session.scalars(select(DagRun)).all()
        dag_run_ids = {dr.dag_id for dr in dag_runs}

        assert "dag_a" in dag_run_ids, "DagRun should be created for dag_a"
        assert "dag_b" not in dag_run_ids, "DagRun should NOT be created for stale dag_b"

        # Verify dag_a's DagRun was created properly
        dag_a_run = session.scalars(
            select(DagRun).where(DagRun.dag_id == "dag_a")
        ).one_or_none()
        assert dag_a_run is not None
        assert dag_a_run.creating_job_id == scheduler_job.id
