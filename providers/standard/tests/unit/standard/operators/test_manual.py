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

from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.manual import ManualGateOperator
from airflow.utils.state import State, TaskInstanceState

from tests_common.test_utils.compat import TriggerRule, timezone
from tests_common.test_utils.db import clear_db_runs
from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_1

if AIRFLOW_V_3_0_1:
    from airflow.providers.common.compat.sdk import DownstreamTasksSkipped


DEFAULT_DATE = timezone.datetime(2016, 1, 1)


class TestManualGateOperator:
    pytestmark = [pytest.mark.db_test, pytest.mark.need_serialized_dag]

    @pytest.fixture(autouse=True)
    def setup_tests(self, dag_maker):
        self.dag_maker = dag_maker
        clear_db_runs()
        yield
        clear_db_runs()

    @staticmethod
    def assert_expected_task_states(dag_run, expected_states: dict[str, TaskInstanceState | None]) -> None:
        task_instances = {ti.task_id: ti.state for ti in dag_run.get_task_instances()}
        for task_id, expected_state in expected_states.items():
            assert task_instances[task_id] == expected_state

    @pytest.mark.parametrize(
        (
            "ignore_downstream_trigger_rules",
            "join_trigger_rule",
            "expected_skipped_tasks",
            "expected_task_states",
        ),
        [
            (
                True,
                TriggerRule.ALL_SUCCESS,
                {"optional_step", "join"},
                {"manual_gate": State.SUCCESS, "optional_step": State.SKIPPED, "join": State.SKIPPED},
            ),
            (
                False,
                TriggerRule.ALL_SUCCESS,
                {"optional_step"},
                {"manual_gate": State.SUCCESS, "optional_step": State.SKIPPED, "join": State.NONE},
            ),
            (
                False,
                TriggerRule.ALL_DONE,
                {"optional_step"},
                {"manual_gate": State.SUCCESS, "optional_step": State.SKIPPED, "join": State.SUCCESS},
            ),
        ],
    )
    def test_manual_gate_skips_optional_section(
        self,
        ignore_downstream_trigger_rules,
        join_trigger_rule,
        expected_skipped_tasks,
        expected_task_states,
    ):
        with self.dag_maker(
            "manual_gate_test",
            start_date=DEFAULT_DATE,
            serialized=True,
        ):
            manual_gate = ManualGateOperator(
                task_id="manual_gate",
                ignore_downstream_trigger_rules=ignore_downstream_trigger_rules,
            )
            optional_step = EmptyOperator(task_id="optional_step")
            join = EmptyOperator(task_id="join", trigger_rule=join_trigger_rule)

            manual_gate >> optional_step >> join

        dag_run = self.dag_maker.create_dagrun()

        if AIRFLOW_V_3_0_1:
            with pytest.raises(DownstreamTasksSkipped) as exc_info:
                self.dag_maker.run_ti("manual_gate", dag_run)

            assert set(exc_info.value.tasks) == expected_skipped_tasks
        else:
            self.dag_maker.run_ti("manual_gate", dag_run)
            self.dag_maker.run_ti("optional_step", dag_run)
            self.dag_maker.run_ti("join", dag_run)

            assert manual_gate.ignore_downstream_trigger_rules == ignore_downstream_trigger_rules
            self.assert_expected_task_states(dag_run, expected_task_states)

    def test_manual_gate_stores_skipped_tasks_for_clear_semantics(self):
        with self.dag_maker("manual_gate_xcom_test", start_date=DEFAULT_DATE, serialized=True):
            manual_gate = ManualGateOperator(task_id="manual_gate")
            optional_step = EmptyOperator(task_id="optional_step")
            manual_gate >> optional_step

        dag_run = self.dag_maker.create_dagrun()

        if AIRFLOW_V_3_0_1:
            with pytest.raises(DownstreamTasksSkipped):
                self.dag_maker.run_ti("manual_gate", dag_run)
        else:
            self.dag_maker.run_ti("manual_gate", dag_run)

        task_instances = dag_run.get_task_instances()
        gate_ti = next(ti for ti in task_instances if ti.task_id == "manual_gate")

        assert gate_ti.xcom_pull(task_ids=manual_gate.task_id, key="skipmixin_key") == {
            "skipped": ["optional_step"]
        }

    def test_manual_gate_uses_label_or_task_identity(self):
        labeled_gate = ManualGateOperator(task_id="manual_gate", label="Run optional section")
        display_name_gate = ManualGateOperator(task_id="display_gate", task_display_name="Displayed gate")
        task_id_gate = ManualGateOperator(task_id="task_id_gate")

        assert labeled_gate.manual_gate_label == "Run optional section"
        assert display_name_gate.manual_gate_label == "Displayed gate"
        assert task_id_gate.manual_gate_label == "task_id_gate"

    def test_manual_gate_noop_without_downstream(self):
        with self.dag_maker("manual_gate_noop_test", start_date=DEFAULT_DATE, serialized=True):
            manual_gate = ManualGateOperator(task_id="manual_gate")

        result = manual_gate.execute({})

        assert result == {"label": "manual_gate", "skipped_task_ids": []}
