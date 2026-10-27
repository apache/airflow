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
DAG exercising a locked Dag level ``exclude_datasets`` rule.

A locked rule makes ``extend_global_openlineage_emission_policy`` ignore an authored list, whether it is set for
a task (``t_blocked``) or for the whole Dag (``t_dag_level_blocked``).

Required global conf::

    [
        {
            "scope": {"dag_id": "openlineage_policy_exclude_datasets_locked_dag"},
            "locked": true,
            "controls": {"exclude_datasets": [".*/dag_locked_.*"]},
        }
    ]
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime

from airflow import DAG
from airflow.providers.common.compat.lineage.hook import get_hook_lineage_collector
from airflow.providers.openlineage.api.emission_policy import extend_global_openlineage_emission_policy
from airflow.providers.standard.operators.python import PythonOperator

from system.openlineage.constants import DEFAULT_DAGRUN_TIMEOUT
from system.openlineage.expected_events import get_expected_event_file_path
from system.openlineage.operator import OpenLineageTestOperator


class QuietHook:
    """Hook that no rule is scoped to."""


def _register_stems(hook: object, stems: Sequence[str]):
    collector = get_hook_lineage_collector()
    for stem in stems:
        collector.add_input_asset(context=hook, uri=f"file://host1/{stem}_in.txt")
        collector.add_output_asset(context=hook, uri=f"file://host1/{stem}_out.txt")


def _blocked():
    _register_stems(QuietHook(), ["dag_locked", "authored_a"])


DAG_ID = "openlineage_policy_exclude_datasets_locked_dag"

with DAG(
    dagrun_timeout=DEFAULT_DAGRUN_TIMEOUT,
    dag_id=DAG_ID,
    start_date=datetime(2021, 1, 1),
    schedule=None,
    catchup=False,
    default_args={"retries": 0},
) as dag:
    t_blocked = extend_global_openlineage_emission_policy(
        PythonOperator(task_id="t_blocked", python_callable=_blocked),
        exclude_datasets=[".*/authored_.*"],
    )
    t_dag_level_blocked = PythonOperator(task_id="t_dag_level_blocked", python_callable=_blocked)
    extend_global_openlineage_emission_policy(dag, exclude_datasets=[".*/authored_.*"])
    check_events = OpenLineageTestOperator(
        task_id="check_events", file_path=get_expected_event_file_path(DAG_ID)
    )
    t_blocked >> t_dag_level_blocked >> check_events


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: contributing-docs/testing/system_tests.rst)
test_run = get_test_run(dag)
