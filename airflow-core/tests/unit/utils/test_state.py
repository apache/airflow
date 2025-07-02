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
from enum import Enum

import pytest

from airflow.models.dag import DAG
from airflow.models.dagrun import DagRun
from airflow.utils.session import create_session
from airflow.utils.state import (
    DagRunState,
    FailureTIState,
    IntermediateTIState,
    SuccessTIState,
    TaskInstanceState,
    TerminalTIState,
)
from airflow.utils.types import DagRunTriggeredByType, DagRunType

from unit.models import DEFAULT_DATE

pytestmark = pytest.mark.db_test


def test_dagrun_state_enum_escape():
    """
    Make sure DagRunState.QUEUED is converted to string 'queued' when
    referenced in DB query
    """
    with create_session() as session:
        dag = DAG(dag_id="test_dagrun_state_enum_escape", schedule=timedelta(days=1), start_date=DEFAULT_DATE)
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

        query = session.query(
            DagRun.dag_id,
            DagRun.state,
            DagRun.run_type,
        ).filter(
            DagRun.dag_id == dag.dag_id,
            # make sure enum value can be used in filter queries
            DagRun.state == DagRunState.QUEUED,
        )
        assert str(query.statement.compile(compile_kwargs={"literal_binds": True})) == (
            "SELECT dag_run.dag_id, dag_run.state, dag_run.run_type \n"
            "FROM dag_run \n"
            "WHERE dag_run.dag_id = 'test_dagrun_state_enum_escape' AND dag_run.state = 'queued'"
        )

        rows = query.all()
        assert len(rows) == 1
        assert rows[0].dag_id == dag.dag_id
        # make sure value in db is stored as `queued`, not `DagRunType.QUEUED`
        assert rows[0].state == "queued"

        session.rollback()


class TestTaskInstanceStateEnumerations:
    """Test suite to validate the relationships between TaskInstance state enumerations."""

    def _get_enum_values(self, enum_class):
        """
        Extract the underlying values from an enum, handling both direct values
        and references to other enum instances.
        """
        values = set()
        for member in enum_class:
            # Get the actual value, which might be a string or another enum instance
            value = member.value

            # If the value is an enum instance from another class, get its value
            if hasattr(value, "value") and isinstance(value, Enum):
                values.add(value.value)
            else:
                # Direct string or other primitive value
                values.add(value)

        return values

    def test_success_and_failure_states_equal_terminal_states(self):
        """Test that SuccessTIState + FailureTIState = TerminalTIState"""
        # Get all values from success and failure enumerations
        success_values = self._get_enum_values(SuccessTIState)
        failure_values = self._get_enum_values(FailureTIState)
        terminal_values = self._get_enum_values(TerminalTIState)

        # Union of success and failure state values should equal terminal state values
        combined_values = success_values.union(failure_values)

        assert combined_values == terminal_values, (
            f"Success + Failure state values don't match Terminal state values.\n"
            f"Success values: {success_values}\n"
            f"Failure values: {failure_values}\n"
            f"Combined: {combined_values}\n"
            f"Terminal values: {terminal_values}\n"
            f"Missing from combined: {terminal_values - combined_values}\n"
            f"Extra in combined: {combined_values - terminal_values}"
        )

    def test_success_and_failure_states_are_disjoint(self):
        """Test that SuccessTIState and FailureTIState have no overlap"""
        success_values = self._get_enum_values(SuccessTIState)
        failure_values = self._get_enum_values(FailureTIState)

        overlap = success_values.intersection(failure_values)

        assert len(overlap) == 0, (
            f"Success and Failure state values should not overlap.\nOverlapping values: {overlap}"
        )

    def test_terminal_and_intermediate_states_equal_all_task_states(self):
        """Test that TerminalTIState + IntermediateTIState = TaskInstanceState"""
        terminal_values = self._get_enum_values(TerminalTIState)
        intermediate_values = self._get_enum_values(IntermediateTIState)
        all_task_values = self._get_enum_values(TaskInstanceState)

        # Union of terminal and intermediate state values should equal all task state values
        combined_values = terminal_values.union(intermediate_values)

        assert combined_values == all_task_values, (
            f"Terminal + Intermediate state values don't match all Task state values.\n"
            f"Terminal values: {terminal_values}\n"
            f"Intermediate values: {intermediate_values}\n"
            f"Combined: {combined_values}\n"
            f"All task values: {all_task_values}\n"
            f"Missing from combined: {all_task_values - combined_values}\n"
            f"Extra in combined: {combined_values - all_task_values}"
        )

    def test_terminal_and_intermediate_states_are_disjoint(self):
        """Test that TerminalTIState and IntermediateTIState have no overlap"""
        terminal_values = self._get_enum_values(TerminalTIState)
        intermediate_values = self._get_enum_values(IntermediateTIState)

        overlap = terminal_values.intersection(intermediate_values)

        assert len(overlap) == 0, (
            f"Terminal and Intermediate state values should not overlap.\nOverlapping values: {overlap}"
        )

    def test_state_value_consistency(self):
        """Test that state values are consistent across enumerations"""
        # All values in SuccessTIState should also be in TerminalTIState
        success_values = self._get_enum_values(SuccessTIState)
        terminal_values = self._get_enum_values(TerminalTIState)

        for success_value in success_values:
            assert success_value in terminal_values, (
                f"Success state value '{success_value}' not found in TerminalTIState values"
            )

        # All values in FailureTIState should also be in TerminalTIState
        failure_values = self._get_enum_values(FailureTIState)

        for failure_value in failure_values:
            assert failure_value in terminal_values, (
                f"Failure state value '{failure_value}' not found in TerminalTIState values"
            )

        # All values in TerminalTIState should also be in TaskInstanceState
        all_task_values = self._get_enum_values(TaskInstanceState)

        for terminal_value in terminal_values:
            assert terminal_value in all_task_values, (
                f"Terminal state value '{terminal_value}' not found in TaskInstanceState values"
            )

        # All values in IntermediateTIState should also be in TaskInstanceState
        intermediate_values = self._get_enum_values(IntermediateTIState)

        for intermediate_value in intermediate_values:
            assert intermediate_value in all_task_values, (
                f"Intermediate state value '{intermediate_value}' not found in TaskInstanceState values"
            )
