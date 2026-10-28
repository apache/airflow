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
DAG exercising the OpenLineage emission policy where Airflow versions differ.

The comment above each task says which rules and authoring calls apply to it, and what its events must show.

The rules are in ``AIRFLOW__OPENLINEAGE__EMISSION_POLICY`` of ``airflow-e2e-tests/docker/openlineage.yml``.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime

from airflow import DAG
from airflow.providers.common.compat.assets import Asset, AssetAlias
from airflow.providers.common.compat.lineage.hook import get_hook_lineage_collector
from airflow.providers.common.compat.sdk import AirflowSkipException
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

from system.openlineage.constants import DEFAULT_DAGRUN_TIMEOUT
from system.openlineage.expected_events import get_expected_event_file_path
from system.openlineage.operator import OpenLineageTestOperator


class QuietHook:
    """Hook that no rule is scoped to."""


def _inlets(stems: Sequence[str]) -> list[Asset]:
    return [Asset(uri=f"file://host1/{stem}_in.txt") for stem in stems]


def _outlets(stems: Sequence[str]) -> list[Asset]:
    return [Asset(uri=f"file://host1/{stem}_out.txt") for stem in stems]


ALIAS = AssetAlias("exclude-datasets-alias")


def _runtime_outlets(outlet_events):
    for stem in ["dag_excluded_runtime", "keep"]:
        outlet_events[ALIAS].add(Asset(uri=f"file://host1/{stem}_out.txt"))


def _collector_cap():
    # The first 5 assets are excluded and the cap keeps the first 67, so the last 3 of the 70 are lost, not recovered.
    stems = [f"dag_excluded_{i:02d}" for i in range(1, 6)] + [f"cap_keep_{i:02d}" for i in range(6, 71)]
    collector = get_hook_lineage_collector()
    hook = QuietHook()
    for stem in stems:
        collector.add_input_asset(context=hook, uri=f"file://host1/{stem}_in.txt")
        collector.add_output_asset(context=hook, uri=f"file://host1/{stem}_out.txt")


def _skip():
    raise AirflowSkipException("Intentional skip")


DAG_ID = "openlineage_policy_exclude_datasets_versioned_dag"

with DAG(
    dagrun_timeout=DEFAULT_DAGRUN_TIMEOUT,
    dag_id=DAG_ID,
    start_date=datetime(2021, 1, 1),
    schedule=None,
    catchup=False,
    default_args={"retries": 0},
) as dag:
    # The Dag list drops dag_excluded from the declared outlets and from the outlets added at runtime through
    # the alias. COMPLETE keeps keep_declared and, on Airflow 3.1+, the runtime keep.
    t_runtime_outlets = PythonOperator(
        task_id="t_runtime_outlets",
        python_callable=_runtime_outlets,
        inlets=_inlets(["dag_excluded_declared", "keep_declared"]),
        outlets=[*_outlets(["dag_excluded_declared", "keep_declared"]), ALIAS],
    )
    # Registers 70 assets. On Airflow 3.2+ the collector keeps the first 67, then the Dag list drops the 5
    # dag_excluded ones, leaving 62. Before 3.2 there is no cap, leaving 65.
    t_collector_cap = PythonOperator(task_id="t_collector_cap", python_callable=_collector_cap)
    # Skips itself. The Dag list drops dag_excluded from its START and COMPLETE. Airflow 3.0 emits no COMPLETE
    # for a task that skips itself.
    t_skipped = PythonOperator(
        task_id="t_skipped",
        python_callable=_skip,
        inlets=_inlets(["dag_excluded", "keep"]),
        outlets=_outlets(["dag_excluded", "keep"]),
    )
    # Skips itself, and the conf Dag + task rule emit: false applies: no task events.
    t_skipped_emit_off = PythonOperator(task_id="t_skipped_emit_off", python_callable=_skip)
    # Skips itself, and the conf Dag + task rule include_full_task_info: true applies: bash_command is in
    # START and COMPLETE. Airflow 3.0 emits no COMPLETE.
    t_skipped_full_info = BashOperator(task_id="t_skipped_full_info", bash_command="exit 99")
    check_events = OpenLineageTestOperator(
        task_id="check_events",
        file_path=get_expected_event_file_path(DAG_ID),
        trigger_rule=TriggerRule.NONE_FAILED,
    )
    t_runtime_outlets >> t_collector_cap >> check_events
    [t_skipped, t_skipped_emit_off, t_skipped_full_info] >> check_events


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: contributing-docs/testing/system_tests.rst)
test_run = get_test_run(dag)
