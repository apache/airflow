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

import pytest
from sqlalchemy import select

from airflow.models.dagrun import DagRun
from airflow.sdk import DAG
from airflow.utils.session import create_session
from airflow.utils.state import DagRunState, IntermediateTIState, State, TaskInstanceState, TerminalTIState
from airflow.utils.types import DagRunTriggeredByType, DagRunType

from tests_common.test_utils.dag import sync_dag_to_db
from unit.models import DEFAULT_DATE

pytestmark = pytest.mark.db_test


def test_dagrun_state_enum_escape(testing_dag_bundle):
    """
    Make sure DagRunState.QUEUED is converted to string 'queued' when
    referenced in DB query
    """
    with create_session() as session:
        dag = sync_dag_to_db(
            DAG(dag_id="test_dagrun_state_enum_escape", schedule=timedelta(days=1), start_date=DEFAULT_DATE),
            session=session,
        )
        dag.create_dagrun(
            run_id=dag.timetable.generate_run_id(
                run_type=DagRunType.SCHEDULED,
                run_after=DEFAULT_DATE,
                data_interval=dag.timetable.infer_manual_data_interval(run_after=DEFAULT_DATE),
            ),
            run_type=DagRunType.SCHEDULED,
            state=DagRunState.QUEUED,
            logical_date=DEFAULT_DATE,
            start_date=DEFAULT_DATE,
            data_interval=dag.timetable.infer_manual_data_interval(run_after=DEFAULT_DATE),
            session=session,
            run_after=DEFAULT_DATE,
            triggered_by=DagRunTriggeredByType.TEST,
        )

        stmt = select(DagRun.dag_id, DagRun.state, DagRun.run_type).where(
            DagRun.dag_id == dag.dag_id,
            # make sure enum value can be used in filter queries
            DagRun.state == DagRunState.QUEUED,
        )
        assert str(stmt.compile(compile_kwargs={"literal_binds": True})) == (
            "SELECT dag_run.dag_id, dag_run.state, dag_run.run_type \n"
            "FROM dag_run \n"
            "WHERE dag_run.dag_id = 'test_dagrun_state_enum_escape' AND dag_run.state = 'queued'"
        )

        rows = session.execute(stmt).all()
        assert len(rows) == 1
        assert rows[0].dag_id == dag.dag_id
        # make sure value in db is stored as `queued`, not `DagRunType.QUEUED`
        assert rows[0].state == "queued"

        session.rollback()


class TestTaskInstanceStates:
    """Test suite for validating task instance state relationships."""

    def test_failed_and_success_states_union_equals_terminal_states(self):
        """Test that union of failed_states and success_states equals TerminalTIState values."""
        # Get all terminal state values
        terminal_state_values = {state.value for state in TerminalTIState}

        # Get union of failed and success states (convert to string values)
        failed_success_union = {
            state.value for state in State.failed_states.union(State.success_states)
        }.union(
            {"removed"}
        )  # Treat removed separately since it doesn't neatly fit into the "success" or "failure" category semantically.

        assert failed_success_union == terminal_state_values, (
            f"Union of failed_states and success_states ({failed_success_union}) "
            f"does not equal TerminalTIState values ({terminal_state_values})"
        )

    def test_terminal_and_intermediate_states_union_equals_task_instance_states(self):
        """Test that union of TerminalTIState and IntermediateTIState values equals TaskInstanceState values."""
        # Get all terminal state values
        terminal_state_values = {state.value for state in TerminalTIState}

        # Get all intermediate state values
        intermediate_state_values = {state.value for state in IntermediateTIState}

        # Get union of terminal and intermediate states
        terminal_intermediate_union = terminal_state_values.union(intermediate_state_values)

        # Get all TaskInstanceState values, excluding RUNNING since it's not in terminal or intermediate
        task_instance_state_values = {state.value for state in TaskInstanceState}

        # Add RUNNING state separately since it's defined directly in TaskInstanceState
        expected_task_instance_values = terminal_intermediate_union.union({"running"})

        assert task_instance_state_values == expected_task_instance_values, (
            f"TaskInstanceState values ({task_instance_state_values}) "
            f"does not equal union of TerminalTIState and IntermediateTIState values plus RUNNING "
            f"({expected_task_instance_values})"
        )

    def test_no_overlap_between_failed_and_success_states(self):
        """Test that failed_states and success_states have no overlap."""
        overlap = State.failed_states.intersection(State.success_states)
        assert len(overlap) == 0, f"failed_states and success_states should not overlap, but found: {overlap}"

    def test_all_terminal_states_are_either_failed_or_success(self):
        """Test that every terminal state (except 'removed') is classified as either failed or success."""
        excluded_states = {"removed"}
        all_terminal_states = {
            TaskInstanceState(state.value) for state in TerminalTIState if state.value not in excluded_states
        }
        classified_states = State.failed_states.union(State.success_states)

        assert all_terminal_states == classified_states, (
            f"All terminal states ({all_terminal_states}) except excluded ones ({excluded_states}) "
            f"should be classified as either failed or success ({classified_states})"
        )
