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
DAG exercising the OpenLineage emission policy authoring API.

Per-task ``hook_lineage=False`` blocks asset + SQL child events.

Also verifies precedence: ``extract_operator_metadata=False`` short-circuits the
whole extraction pipeline, so ``hook_lineage=True`` becomes inert (no asset
inputs/outputs, no SQL child events) on those tasks.
"""

from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.providers.openlineage.api.emission_policy import extend_global_openlineage_emission_policy
from airflow.providers.standard.operators.python import PythonOperator

from system.openlineage.constants import DEFAULT_DAGRUN_TIMEOUT
from system.openlineage.expected_events import get_expected_event_file_path
from system.openlineage.operator import OpenLineageTestOperator


def _register_hook_lineage():
    from airflow.providers.common.compat.lineage.hook import get_hook_lineage_collector

    collector = get_hook_lineage_collector()
    collector.add_input_asset(context=None, uri="file://host1/in1.txt")
    collector.add_input_asset(context=None, uri="file://host1/in2.txt")
    collector.add_output_asset(context=None, uri="file://host2/out1.txt")
    collector.add_output_asset(context=None, uri="file://host2/out2.txt")


def _register_sql_hook_lineage():
    from airflow.providers.common.compat.lineage.hook import get_hook_lineage_collector
    from airflow.providers.common.sql.hooks.lineage import SqlJobHookLineageExtra

    collector = get_hook_lineage_collector()
    collector.add_extra(
        context=None,
        key=SqlJobHookLineageExtra.KEY.value,
        value={SqlJobHookLineageExtra.VALUE__SQL_STATEMENT.value: "SELECT 1 AS test_col"},
    )


DAG_ID = "openlineage_policy_hook_lineage_dag"

with DAG(
    dagrun_timeout=DEFAULT_DAGRUN_TIMEOUT,
    dag_id=DAG_ID,
    start_date=datetime(2021, 1, 1),
    schedule=None,
    catchup=False,
    default_args={"retries": 0},
) as dag:
    t_with_hook = PythonOperator(task_id="t_with_hook", python_callable=_register_hook_lineage)
    t_no_hook = extend_global_openlineage_emission_policy(
        PythonOperator(task_id="t_no_hook", python_callable=_register_hook_lineage),
        hook_lineage=False,
    )
    t_with_sql = PythonOperator(task_id="t_with_sql_hook", python_callable=_register_sql_hook_lineage)
    t_no_sql = extend_global_openlineage_emission_policy(
        PythonOperator(task_id="t_no_sql_hook", python_callable=_register_sql_hook_lineage),
        hook_lineage=False,
    )
    t_hook_extract_false = extend_global_openlineage_emission_policy(
        PythonOperator(task_id="t_hook_extract_false", python_callable=_register_hook_lineage),
        extract_operator_metadata=False,
        hook_lineage=True,
    )
    t_sql_extract_false = extend_global_openlineage_emission_policy(
        PythonOperator(task_id="t_sql_extract_false", python_callable=_register_sql_hook_lineage),
        extract_operator_metadata=False,
        hook_lineage=True,
    )
    check_events = OpenLineageTestOperator(
        task_id="check_events", file_path=get_expected_event_file_path(DAG_ID)
    )
    (
        t_with_hook
        >> t_no_hook
        >> t_with_sql
        >> t_no_sql
        >> t_hook_extract_false
        >> t_sql_extract_false
        >> check_events
    )


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: contributing-docs/testing/system_tests.rst)
test_run = get_test_run(dag)
