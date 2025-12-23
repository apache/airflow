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

from airflow_e2e_tests.e2e_test_utils.clients import AirflowClient

DAG_IDS = [
    "example_bash_decorator",
    "example_bash_operator",
    "example_branch_datetime_operator",
    "example_branch_datetime_operator_2",
    "example_branch_datetime_operator_3",
    "example_branch_dop_operator_v3",
    "example_branch_labels",
    "example_branch_operator",
    "example_branch_python_operator_decorator",
    "example_complex",
    "example_custom_weight",
    "example_dag_decorator",
    "example_dynamic_task_mapping",
    "example_dynamic_task_mapping_with_no_taskflow_operators",
    "example_external_task_marker_parent",
    "example_nested_branch_dag",
    "example_sensor_decorator",
    "example_setup_teardown",
    "example_setup_teardown_taskflow",
    "example_short_circuit_decorator",
    "example_short_circuit_operator",
    "example_simplest_dag",
    "example_skip_dag",
    "example_task_group",
    "example_task_group_decorator",
    "example_task_mapping_second_order",
    "example_time_delta_sensor_async",
    "example_trigger_controller_dag",
    "example_trigger_target_dag",
    "example_weekday_branch_operator",
    "example_workday_timetable",
    "example_xcom",
    "example_xcom_args",
    "example_xcom_args_with_operators",
    "latest_only",
    "latest_only_with_trigger",
    "tutorial",
    "tutorial_dag",
    "tutorial_taskflow_api",
    "tutorial_taskflow_api_virtualenv",
    "tutorial_taskflow_templates",
]


class TestExampleDags:
    """Test Airflow Core example dags."""

    airflow_client = AirflowClient()

    @pytest.mark.parametrize(
        "dag_id",
        DAG_IDS,
        ids=[dag_id for dag_id in DAG_IDS],
    )
    def test_example_dags(self, dag_id):
        """Test that DAGs can be triggered and complete successfully."""

        state = self.airflow_client.trigger_dag_and_wait(dag_id)

        assert state == "success", f"DAG {dag_id} did not complete successfully. Final state: {state}"
