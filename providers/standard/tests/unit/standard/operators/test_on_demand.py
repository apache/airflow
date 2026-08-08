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
from airflow.providers.standard.operators.on_demand import OnDemandSectionOperator
from airflow.utils.state import State, TaskInstanceState

from tests_common.test_utils.compat import TriggerRule, timezone
from tests_common.test_utils.db import clear_db_runs
from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_1

if AIRFLOW_V_3_0_1:
    from airflow.providers.common.compat.sdk import DownstreamTasksSkipped


DEFAULT_DATE = timezone.datetime(2016, 1, 1)


class TestOnDemandSectionOperator:
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
                {"on_demand_section": State.SUCCESS, "optional_step": State.SKIPPED, "join": State.SKIPPED},
            ),
            (
                False,
                TriggerRule.ALL_SUCCESS,
                {"optional_step"},
                {"on_demand_section": State.SUCCESS, "optional_step": State.SKIPPED, "join": State.NONE},
            ),
            (
                False,
                TriggerRule.ALL_DONE,
                {"optional_step"},
                {"on_demand_section": State.SUCCESS, "optional_step": State.SKIPPED, "join": State.SUCCESS},
            ),
        ],
    )
    def test_on_demand_section_skips_optional_section(
        self,
        ignore_downstream_trigger_rules,
        join_trigger_rule,
        expected_skipped_tasks,
        expected_task_states,
    ):
        with self.dag_maker(
            "on_demand_section_test",
            start_date=DEFAULT_DATE,
            serialized=True,
        ):
            on_demand_section = OnDemandSectionOperator(
                task_id="on_demand_section",
                ignore_downstream_trigger_rules=ignore_downstream_trigger_rules,
            )
            optional_step = EmptyOperator(task_id="optional_step")
            join = EmptyOperator(task_id="join", trigger_rule=join_trigger_rule)

            on_demand_section >> optional_step >> join

        dag_run = self.dag_maker.create_dagrun()

        if AIRFLOW_V_3_0_1:
            with pytest.raises(DownstreamTasksSkipped) as exc_info:
                self.dag_maker.run_ti("on_demand_section", dag_run)

            assert set(exc_info.value.tasks) == expected_skipped_tasks
        else:
            self.dag_maker.run_ti("on_demand_section", dag_run)
            self.dag_maker.run_ti("optional_step", dag_run)
            self.dag_maker.run_ti("join", dag_run)

            assert on_demand_section.ignore_downstream_trigger_rules == ignore_downstream_trigger_rules
            self.assert_expected_task_states(dag_run, expected_task_states)

    def test_on_demand_section_stores_skipped_tasks_for_clear_semantics(self):
        with self.dag_maker("on_demand_section_xcom_test", start_date=DEFAULT_DATE, serialized=True):
            on_demand_section = OnDemandSectionOperator(task_id="on_demand_section")
            optional_step = EmptyOperator(task_id="optional_step")
            on_demand_section >> optional_step

        dag_run = self.dag_maker.create_dagrun()

        if AIRFLOW_V_3_0_1:
            with pytest.raises(DownstreamTasksSkipped):
                self.dag_maker.run_ti("on_demand_section", dag_run)
        else:
            self.dag_maker.run_ti("on_demand_section", dag_run)

        task_instances = dag_run.get_task_instances()
        section_ti = next(ti for ti in task_instances if ti.task_id == "on_demand_section")

        assert section_ti.xcom_pull(task_ids=on_demand_section.task_id, key="skipmixin_key") == {
            "skipped": ["optional_step"]
        }

    def test_on_demand_section_uses_label_or_task_identity(self):
        labeled_section = OnDemandSectionOperator(task_id="on_demand_section", label="Run optional section")
        display_name_section = OnDemandSectionOperator(
            task_id="display_section", task_display_name="Displayed section"
        )
        task_id_section = OnDemandSectionOperator(task_id="task_id_section")

        assert labeled_section.on_demand_section_label == "Run optional section"
        assert display_name_section.on_demand_section_label == "Displayed section"
        assert task_id_section.on_demand_section_label == "task_id_section"

    def test_on_demand_section_noop_without_downstream(self):
        with self.dag_maker("on_demand_section_noop_test", start_date=DEFAULT_DATE, serialized=True):
            on_demand_section = OnDemandSectionOperator(task_id="on_demand_section")

        result = on_demand_section.execute({})

        assert result == {"label": "on_demand_section", "skipped_task_ids": []}
