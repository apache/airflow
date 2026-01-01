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

from airflow._shared.timezones.timezone import utcnow
from airflow.api_fastapi.common.db.dags import generate_dag_with_latest_run_query
from airflow.api_fastapi.common.parameters import SortParam
from airflow.models import DagModel
from airflow.models.dagrun import DagRun
from airflow.utils.state import DagRunState

from tests_common.test_utils.db import clear_db_dag_bundles, clear_db_dags, clear_db_runs

pytestmark = pytest.mark.db_test


class TestGenerateDagWithLatestRunQuery:
    """Unit tests for generate_dag_with_latest_run_query function."""

    @staticmethod
    def _clear_db():
        clear_db_runs()
        clear_db_dags()
        clear_db_dag_bundles()

    @pytest.fixture(autouse=True)
    def setup_teardown(self):
        """Setup and teardown for each test."""
        self._clear_db()
        yield
        self._clear_db()

    @pytest.fixture
    def dag_with_queued_run(self, session, testing_dag_bundle):
        """Returns a DAG with a QUEUED DagRun and null start_date."""
        dag_id = "dag_with_queued_run"

        # Create DagModel
        dag_model = DagModel(
            dag_id=dag_id,
            bundle_name="testing",
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
            bundle_name="testing",
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
            order_by=SortParam(allowed_attrs=["dag_id"], model=DagModel).set_value(["dag_id"]),
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
            order_by=SortParam(allowed_attrs=["last_run_state"], model=DagModel).set_value(
                ["last_run_state"]
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

    def test_includes_queued_run_when_ordering_by_start_date(
        self, dag_with_queued_run, dag_with_running_run, session
    ):
        """DAGs with QUEUED runs and RUNNING runs must all have joined DagRun info not None when ordering by start_date."""
        queued_dag_model, _ = dag_with_queued_run
        running_dag_model, _ = dag_with_running_run

        query = generate_dag_with_latest_run_query(
            max_run_filters=[],
            order_by=SortParam(allowed_attrs=["last_run_start_date"], model=DagModel).set_value(
                ["last_run_start_date"]
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

    @pytest.mark.usefixtures("testing_dag_bundle")
    def test_latest_queued_run_without_start_date_is_included(self, session):
        """Even if the latest DagRun is QUEUED+start_date=None, joined DagRun state must not be None."""
        dag_id = "dag_with_multiple_runs"
        dag_model = DagModel(
            dag_id=dag_id,
            bundle_name="testing",
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
            order_by=SortParam(allowed_attrs=["last_run_state"], model=DagModel).set_value(
                ["last_run_state"]
            ),
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
            order_by=SortParam(allowed_attrs=["last_run_state"], model=DagModel).set_value(
                ["last_run_state"]
            ),
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

    @pytest.mark.usefixtures("testing_dag_bundle")
    def test_filters_by_dag_ids_when_provided(self, session):
        """
        Verify that when dag_ids is provided, only those DAGs and their runs are queried.

        This is a performance optimization: both the main DAG query and the DagRun subquery
        should only process accessible DAGs when the user has limited access.
        """
        dag_ids = ["dag_accessible_1", "dag_accessible_2", "dag_inaccessible_3"]

        for dag_id in dag_ids:
            dag_model = DagModel(
                dag_id=dag_id,
                bundle_name="testing",
                is_stale=False,
                is_paused=False,
                fileloc=f"/tmp/{dag_id}.py",
            )
            session.add(dag_model)
            session.flush()

            # Create 2 runs for each DAG
            for run_idx in range(2):
                dagrun = DagRun(
                    dag_id=dag_id,
                    run_id=f"manual__{run_idx}",
                    run_type="manual",
                    logical_date=datetime(2024, 1, 1 + run_idx, tzinfo=timezone.utc),
                    state=DagRunState.SUCCESS,
                    start_date=datetime(2024, 1, 1 + run_idx, 1, tzinfo=timezone.utc),
                )
                session.add(dagrun)
        session.commit()

        # User has access to only 2 DAGs
        accessible_dag_ids = {"dag_accessible_1", "dag_accessible_2"}

        # Query with dag_ids filter
        query_filtered = generate_dag_with_latest_run_query(
            max_run_filters=[],
            order_by=SortParam(allowed_attrs=["last_run_state"], model=DagModel).set_value(
                ["last_run_state"]
            ),
            dag_ids=accessible_dag_ids,
        )

        # Query without dag_ids filter
        query_unfiltered = generate_dag_with_latest_run_query(
            max_run_filters=[],
            order_by=SortParam(allowed_attrs=["last_run_state"], model=DagModel).set_value(
                ["last_run_state"]
            ),
        )

        result_filtered = session.execute(query_filtered.add_columns(DagRun.state)).fetchall()
        result_unfiltered = session.execute(query_unfiltered.add_columns(DagRun.state)).fetchall()

        # Filtered query should only return accessible DAGs
        filtered_dag_ids = {row[0].dag_id for row in result_filtered}
        assert filtered_dag_ids == accessible_dag_ids

        # Unfiltered query returns all DAGs
        unfiltered_dag_ids = {row[0].dag_id for row in result_unfiltered}
        assert unfiltered_dag_ids == set(dag_ids)

        # All accessible DAGs should have DagRun info
        filtered_dags_with_runs = {row[0].dag_id for row in result_filtered if row[1] is not None}
        assert filtered_dags_with_runs == accessible_dag_ids
