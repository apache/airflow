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
"""
DAG exercising the emission policy authoring API for the controls the other authoring Dags do not.

- ``t_task_events_off``: ``emit_task_events=False`` silences the task events
- ``t_task_events_beat_emit``: ``emit_task_events=True`` beats ``emit=False``
- ``t_task_full_info``: a task level ``include_full_task_info=True``
- a Dag level ``hook_lineage=False`` and ``include_full_task_info=True`` reach the tasks that exist at the call
  (``t_dag_hook_off``, ``t_dag_full_info``) and a task level call after it wins (``t_dag_hook_back_on``,
  ``t_dag_full_info_off``), while a task defined later does not inherit them (``t_after_dag_call``)
"""

from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.providers.common.compat.lineage.hook import get_hook_lineage_collector
from airflow.providers.openlineage.api.emission_policy import extend_global_openlineage_emission_policy
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator

from system.openlineage.constants import DEFAULT_DAGRUN_TIMEOUT
from system.openlineage.expected_events import get_expected_event_file_path
from system.openlineage.operator import OpenLineageTestOperator


def _register_hook_lineage():
    collector = get_hook_lineage_collector()
    collector.add_input_asset(context=None, uri="file://host1/in.txt")
    collector.add_output_asset(context=None, uri="file://host1/out.txt")


DAG_ID = "openlineage_policy_authoring_controls_dag"

with DAG(
    dagrun_timeout=DEFAULT_DAGRUN_TIMEOUT,
    dag_id=DAG_ID,
    start_date=datetime(2021, 1, 1),
    schedule=None,
    catchup=False,
    default_args={"retries": 0},
) as dag:
    t_dag_hook_off = PythonOperator(task_id="t_dag_hook_off", python_callable=_register_hook_lineage)
    t_dag_full_info = BashOperator(task_id="t_dag_full_info", bash_command="echo full_info")
    t_dag_hook_back_on = PythonOperator(task_id="t_dag_hook_back_on", python_callable=_register_hook_lineage)
    t_dag_full_info_off = BashOperator(task_id="t_dag_full_info_off", bash_command="echo full_info")
    # Dag-level flags only reach tasks that already exist, so the tasks defined below are left out of this call.
    extend_global_openlineage_emission_policy(dag, hook_lineage=False, include_full_task_info=True)
    extend_global_openlineage_emission_policy(t_dag_hook_back_on, hook_lineage=True)
    extend_global_openlineage_emission_policy(t_dag_full_info_off, include_full_task_info=False)

    t_after_dag_call = PythonOperator(task_id="t_after_dag_call", python_callable=_register_hook_lineage)
    t_task_events_off = extend_global_openlineage_emission_policy(
        PythonOperator(task_id="t_task_events_off", python_callable=_register_hook_lineage),
        emit_task_events=False,
    )
    t_task_events_beat_emit = extend_global_openlineage_emission_policy(
        PythonOperator(task_id="t_task_events_beat_emit", python_callable=_register_hook_lineage),
        emit=False,
        emit_task_events=True,
    )
    t_task_full_info = extend_global_openlineage_emission_policy(
        BashOperator(task_id="t_task_full_info", bash_command="echo full_info"),
        include_full_task_info=True,
    )
    check_events = OpenLineageTestOperator(
        task_id="check_events", file_path=get_expected_event_file_path(DAG_ID)
    )
    (
        t_dag_hook_off
        >> t_dag_full_info
        >> t_dag_hook_back_on
        >> t_dag_full_info_off
        >> t_after_dag_call
        >> t_task_events_off
        >> t_task_events_beat_emit
        >> t_task_full_info
        >> check_events
    )


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: contributing-docs/testing/system_tests.rst)
test_run = get_test_run(dag)
