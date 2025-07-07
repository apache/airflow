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

from datetime import datetime, timezone

import pytest

from airflow.api_fastapi.common.db.dags import generate_dag_with_latest_run_query
from airflow.api_fastapi.common.parameters import SortParam
from airflow.models import DagModel
from airflow.models.dagrun import DagRun
from airflow.utils.state import DagRunState
from airflow.utils.timezone import utcnow

from tests_common.test_utils.db import clear_db_dags, clear_db_runs

pytestmark = pytest.mark.db_test


class TestGenerateDagWithLatestRunQuery:
    """Unit tests for generate_dag_with_latest_run_query function."""

    @staticmethod
    def _clear_db():
        clear_db_runs()
        clear_db_dags()

    @pytest.fixture(autouse=True)
    def setup_teardown(self):
        """Setup and teardown for each test."""
        self._clear_db()
        yield
        self._clear_db()

    @pytest.fixture
    def dag_with_queued_run(self, session):
        """Returns a DAG with a QUEUED DagRun and null start_date."""

        dag_id = "dag_with_queued_run"

        # Create DagModel
        dag_model = DagModel(
            dag_id=dag_id,
            is_stale=False,
            is_paused=False,
            fileloc="/tmp/dag.py",
        )
        session.add(dag_model)
        session.flush()

        # Create DagRun with start_date=None (QUEUED state)
        dagrun = DagRun(
            dag_id=dag_id,
            run_id="manual__queued",
            run_type="manual",
            logical_date=utcnow(),
            state=DagRunState.QUEUED,
            start_date=None,
        )
        session.add(dagrun)
        session.commit()

        return dag_model, dagrun

    @pytest.fixture
    def dag_with_running_run(self, session):
        """Returns a DAG with a RUNNING DagRun and a valid start_date."""

        dag_id = "dag_with_running_run"

        # Create DagModel
        dag_model = DagModel(
            dag_id=dag_id,
            is_stale=False,
            is_paused=False,
            fileloc="/tmp/dag2.py",
        )
        session.add(dag_model)
        session.flush()

        # Create DagRun with start_date set (RUNNING state)
        start_time = utcnow()
        dagrun = DagRun(
            dag_id=dag_id,
            run_id="manual__running",
            run_type="manual",
            logical_date=start_time,
            state=DagRunState.RUNNING,
            start_date=start_time,
        )
        session.add(dagrun)
        session.commit()

        return dag_model, dagrun

    def test_includes_queued_run_without_start_date(self, dag_with_queued_run, session):
        """DAGs with QUEUED runs and null start_date should be included when no filters are applied, and joined DagRun state must not be None."""
        dag_model, _ = dag_with_queued_run
        query = generate_dag_with_latest_run_query(
            max_run_filters=[],
            order_by=SortParam(allowed_attrs=["dag_id"], model=DagModel).set_value("dag_id"),
        )

        # Also fetch joined DagRun's state and start_date
        extended_query = query.add_columns(DagRun.state, DagRun.start_date)
        result = session.execute(extended_query).fetchall()
        dag_row = next((row for row in result if row[0].dag_id == dag_model.dag_id), None)
        assert dag_row is not None
        dagrun_state = dag_row[1]
        assert dagrun_state is not None, "Joined DagRun state must not be None"

    def test_includes_queued_run_when_ordering_by_state(
        self, dag_with_queued_run, dag_with_running_run, session
    ):
        """DAGs with QUEUED runs and null start_date, and RUNNING runs must all have joined DagRun info not None."""
        queued_dag_model, _ = dag_with_queued_run
        running_dag_model, _ = dag_with_running_run

        query = generate_dag_with_latest_run_query(
            max_run_filters=[],
            order_by=SortParam(allowed_attrs=["last_run_state"], model=DagModel).set_value("last_run_state"),
        )
        extended_query = query.add_columns(DagRun.state, DagRun.start_date)
        result = session.execute(extended_query).fetchall()

        # QUEUED DAG
        queued_row = next((row for row in result if row[0].dag_id == queued_dag_model.dag_id), None)
        assert queued_row is not None
        assert queued_row[1] is not None, "Joined DagRun state for QUEUED DAG must not be None"
        # RUNNING DAG
        running_row = next((row for row in result if row[0].dag_id == running_dag_model.dag_id), None)
        assert running_row is not None
        assert running_row[1] is not None, "Joined DagRun state for RUNNING DAG must not be None"
        assert running_row[2] is not None, "Joined DagRun start_date for RUNNING DAG must not be None"

    def test_includes_queued_run_when_ordering_by_start_date(
        self, dag_with_queued_run, dag_with_running_run, session
    ):
        """DAGs with QUEUED runs and RUNNING runs must all have joined DagRun info not None when ordering by start_date."""
        queued_dag_model, _ = dag_with_queued_run
        running_dag_model, _ = dag_with_running_run

        query = generate_dag_with_latest_run_query(
            max_run_filters=[],
            order_by=SortParam(allowed_attrs=["last_run_start_date"], model=DagModel).set_value(
                "last_run_start_date"
            ),
        )
        extended_query = query.add_columns(DagRun.state, DagRun.start_date)
        result = session.execute(extended_query).fetchall()

        # QUEUED DAG
        queued_row = next((row for row in result if row[0].dag_id == queued_dag_model.dag_id), None)
        assert queued_row is not None
        assert queued_row[1] is not None, "Joined DagRun state for QUEUED DAG must not be None"
        # RUNNING DAG
        running_row = next((row for row in result if row[0].dag_id == running_dag_model.dag_id), None)
        assert running_row is not None
        assert running_row[1] is not None, "Joined DagRun state for RUNNING DAG must not be None"
        assert running_row[2] is not None, "Joined DagRun start_date for RUNNING DAG must not be None"

    def test_latest_queued_run_without_start_date_is_included(self, session):
        """Even if the latest DagRun is QUEUED+start_date=None, joined DagRun state must not be None."""
        dag_id = "dag_with_multiple_runs"
        dag_model = DagModel(
            dag_id=dag_id,
            is_stale=False,
            is_paused=False,
            fileloc="/tmp/dag3.py",
        )
        session.add(dag_model)
        session.flush()
        older_run = DagRun(
            dag_id=dag_id,
            run_id="manual__older",
            run_type="manual",
            logical_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
            state=DagRunState.SUCCESS,
            start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
        )
        session.add(older_run)
        newer_run = DagRun(
            dag_id=dag_id,
            run_id="manual__newer_queued",
            run_type="manual",
            logical_date=utcnow(),
            state=DagRunState.QUEUED,
            start_date=None,
        )
        session.add(newer_run)
        session.commit()
        query = generate_dag_with_latest_run_query(
            max_run_filters=[],
            order_by=SortParam(allowed_attrs=["last_run_state"], model=DagModel).set_value("last_run_state"),
        )
        extended_query = query.add_columns(DagRun.state, DagRun.start_date)
        result = session.execute(extended_query).fetchall()
        dag_row = next((row for row in result if row[0].dag_id == dag_id), None)
        assert dag_row is not None
        assert dag_row[1] is not None, (
            "Even if latest DagRun is QUEUED+start_date=None, state must not be None"
        )

    def test_queued_runs_with_null_start_date_are_properly_joined(
        self, dag_with_queued_run, dag_with_running_run, session
    ):
        """
        Verifies that DAGs with null start_date are properly joined in the query.

        If a WHERE clause filters out null start_dates, these DAGs would be excluded.
        This test ensures they are still present and joined correctly.
        """

        queued_dag_model, _ = dag_with_queued_run
        running_dag_model, _ = dag_with_running_run
        query = generate_dag_with_latest_run_query(
            max_run_filters=[],
            order_by=SortParam(allowed_attrs=["last_run_state"], model=DagModel).set_value("last_run_state"),
        )
        extended_query = query.add_columns(DagRun.state, DagRun.start_date)

        result = session.execute(extended_query).fetchall()

        # Find results for each DAG
        queued_dag_result = None
        running_dag_result = None

        for row in result:
            dag_model = row[0]
            if dag_model.dag_id == queued_dag_model.dag_id:
                queued_dag_result = row
            elif dag_model.dag_id == running_dag_model.dag_id:
                running_dag_result = row

        # Assert both DAGs are present
        assert queued_dag_result is not None, f"Queued DAG {queued_dag_model.dag_id} should be in results"
        assert running_dag_result is not None, f"Running DAG {running_dag_model.dag_id} should be in results"

        # if WHERE start_date IS NOT NULL is present,
        # the queued DAG should have NO DagRun information joined (state=None, start_date=None)
        # But the running DAG should have DagRun information joined

        queued_dagrun_state = queued_dag_result[1]
        running_dagrun_state = running_dag_result[1]
        assert queued_dagrun_state is not None, (
            "Queued DAG should have DagRun state joined, but got None. "
            "This suggests the WHERE start_date IS NOT NULL condition is excluding it."
        )
        assert running_dagrun_state is not None, "Running DAG should have DagRun state joined"
