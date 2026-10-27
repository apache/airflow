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
DAG exercising locked Dag + task conf rules for every control.

Each task has a locked rule, and authoring tries to give the control the opposite value, which the lock blocks:

- ``t_locked_emit``: ``emit: false``, authored ``emit=True``
- ``t_locked_task_events``: ``emit_task_events: false``, authored ``emit_task_events=True``
- ``t_locked_extract``: ``extract_operator_metadata: false``, authored ``True``
- ``t_locked_hook``: ``hook_lineage: false``, authored ``True``
- ``t_locked_full``: ``include_full_task_info: true``, authored ``False``

The Dag run START event also reports the locked tasks as not emitting in ``emits_ol_events``.

Required global conf::

    [
        {
            "scope": {"dag_id": "openlineage_policy_conf_locked_controls_dag", "task_id": "t_locked_emit"},
            "locked": true,
            "controls": {"emit": false},
        },
        {
            "scope": {
                "dag_id": "openlineage_policy_conf_locked_controls_dag",
                "task_id": "t_locked_task_events",
            },
            "locked": true,
            "controls": {"emit_task_events": false},
        },
        {
            "scope": {"dag_id": "openlineage_policy_conf_locked_controls_dag", "task_id": "t_locked_extract"},
            "locked": true,
            "controls": {"extract_operator_metadata": false},
        },
        {
            "scope": {"dag_id": "openlineage_policy_conf_locked_controls_dag", "task_id": "t_locked_hook"},
            "locked": true,
            "controls": {"hook_lineage": false},
        },
        {
            "scope": {"dag_id": "openlineage_policy_conf_locked_controls_dag", "task_id": "t_locked_full"},
            "locked": true,
            "controls": {"include_full_task_info": true},
        },
    ]
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


DAG_ID = "openlineage_policy_conf_locked_controls_dag"

with DAG(
    dagrun_timeout=DEFAULT_DAGRUN_TIMEOUT,
    dag_id=DAG_ID,
    start_date=datetime(2021, 1, 1),
    schedule=None,
    catchup=False,
    default_args={"retries": 0},
) as dag:
    t_locked_emit = extend_global_openlineage_emission_policy(
        PythonOperator(task_id="t_locked_emit", python_callable=_register_hook_lineage),
        emit=True,
    )
    t_locked_task_events = extend_global_openlineage_emission_policy(
        PythonOperator(task_id="t_locked_task_events", python_callable=_register_hook_lineage),
        emit_task_events=True,
    )
    t_locked_extract = extend_global_openlineage_emission_policy(
        PythonOperator(task_id="t_locked_extract", python_callable=_register_hook_lineage),
        extract_operator_metadata=True,
    )
    t_locked_hook = extend_global_openlineage_emission_policy(
        PythonOperator(task_id="t_locked_hook", python_callable=_register_hook_lineage),
        hook_lineage=True,
    )
    t_locked_full = extend_global_openlineage_emission_policy(
        BashOperator(task_id="t_locked_full", bash_command="echo full_info"),
        include_full_task_info=False,
    )
    check_events = OpenLineageTestOperator(
        task_id="check_events", file_path=get_expected_event_file_path(DAG_ID)
    )
    (
        t_locked_emit
        >> t_locked_task_events
        >> t_locked_extract
        >> t_locked_hook
        >> t_locked_full
        >> check_events
    )


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: contributing-docs/testing/system_tests.rst)
test_run = get_test_run(dag)
