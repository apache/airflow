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

Dag-level ``emit_dag_events=False`` suppresses DAG-run events only; task events
must remain fully intact (source code, inputs/outputs, ...). The task registers
hook lineage so the emitted task event carries real inputs/outputs to assert on.
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


DAG_ID = "openlineage_policy_dag_events_false_dag"

with DAG(
    dagrun_timeout=DEFAULT_DAGRUN_TIMEOUT,
    dag_id=DAG_ID,
    start_date=datetime(2021, 1, 1),
    schedule=None,
    catchup=False,
    default_args={"retries": 0},
) as dag:
    run = PythonOperator(task_id="run", python_callable=_register_hook_lineage)
    check_events = OpenLineageTestOperator(
        task_id="check_events", file_path=get_expected_event_file_path(DAG_ID)
    )
    run >> check_events

extend_global_openlineage_emission_policy(dag, emit_dag_events=False)


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: contributing-docs/testing/system_tests.rst)
test_run = get_test_run(dag)
