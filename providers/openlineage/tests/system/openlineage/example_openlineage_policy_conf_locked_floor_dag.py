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
DAG exercising the floor lock of a Dag scoped conf rule.

A locked Dag rule blocks authoring for every task of the Dag, even for a task that a more specific, unlocked
rule gives another value:

- ``t_floor``: the Dag + task rule sets ``include_source_code: true``, so the source code stays, and the
  authored ``False`` is blocked by the locked Dag rule
- ``t_blocked_up``: the locked Dag rule sets ``include_source_code: false``, and the authored ``True`` is blocked

Required global conf::

    [
        {
            "scope": {"dag_id": "openlineage_policy_conf_locked_floor_dag"},
            "locked": true,
            "controls": {"include_source_code": false},
        },
        {
            "scope": {"dag_id": "openlineage_policy_conf_locked_floor_dag", "task_id": "t_floor"},
            "controls": {"include_source_code": true},
        },
    ]
"""

from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.providers.common.compat.lineage.hook import get_hook_lineage_collector
from airflow.providers.openlineage.api.emission_policy import extend_global_openlineage_emission_policy
from airflow.providers.standard.operators.python import PythonOperator

from system.openlineage.constants import DEFAULT_DAGRUN_TIMEOUT
from system.openlineage.expected_events import get_expected_event_file_path
from system.openlineage.operator import OpenLineageTestOperator


def _register_hook_lineage():
    collector = get_hook_lineage_collector()
    collector.add_input_asset(context=None, uri="file://host1/in.txt")
    collector.add_output_asset(context=None, uri="file://host1/out.txt")


DAG_ID = "openlineage_policy_conf_locked_floor_dag"

with DAG(
    dagrun_timeout=DEFAULT_DAGRUN_TIMEOUT,
    dag_id=DAG_ID,
    start_date=datetime(2021, 1, 1),
    schedule=None,
    catchup=False,
    default_args={"retries": 0},
) as dag:
    t_floor = extend_global_openlineage_emission_policy(
        PythonOperator(task_id="t_floor", python_callable=_register_hook_lineage),
        include_source_code=False,
    )
    t_blocked_up = extend_global_openlineage_emission_policy(
        PythonOperator(task_id="t_blocked_up", python_callable=_register_hook_lineage),
        include_source_code=True,
    )
    check_events = OpenLineageTestOperator(
        task_id="check_events", file_path=get_expected_event_file_path(DAG_ID)
    )
    t_floor >> t_blocked_up >> check_events


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: contributing-docs/testing/system_tests.rst)
test_run = get_test_run(dag)
