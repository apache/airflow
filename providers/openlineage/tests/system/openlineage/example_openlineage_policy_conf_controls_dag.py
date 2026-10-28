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
DAG exercising every emission policy control in Dag + task and operator scoped conf rules.

Each task registers hook lineage, so its events show the datasets and the source code. Every control has a task
with its rule, next to ``t_default``, which has none.

The comment above each task says which rules and authoring calls apply to it, and what its events must show.

The Dag run START event reports in ``emits_ol_events`` which tasks emit, as the scheduler resolves the policy for
them, including authoring.

The rules are in ``AIRFLOW__OPENLINEAGE__EMISSION_POLICY`` of ``airflow-e2e-tests/docker/openlineage.yml``.
"""

from __future__ import annotations

from datetime import datetime

from openlineage.client.event_v2 import Dataset

from airflow import DAG
from airflow.providers.common.compat.assets import Asset
from airflow.providers.common.compat.lineage.hook import get_hook_lineage_collector
from airflow.providers.common.compat.sdk import TaskGroup
from airflow.providers.openlineage.api import emit_dataset_lineage, emit_query_lineage
from airflow.providers.openlineage.api.emission_policy import extend_global_openlineage_emission_policy
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.smooth import SmoothOperator

from system.openlineage.constants import DEFAULT_DAGRUN_TIMEOUT
from system.openlineage.expected_events import get_expected_event_file_path
from system.openlineage.operator import OpenLineageTestOperator


class PolicyConfControlsOperator(PythonOperator):
    """Operator with an operator scoped rule."""


class PolicyConfEmitOffOperator(PythonOperator):
    """Operator with an operator scoped ``emit: false`` rule."""


def _register_hook_lineage(*_):
    collector = get_hook_lineage_collector()
    collector.add_input_asset(context=None, uri="file://host1/in.txt")
    collector.add_output_asset(context=None, uri="file://host1/out.txt")


def _register_and_emit_lineage():
    _register_hook_lineage()
    emit_dataset_lineage(
        inputs=[Dataset(namespace="emit://source", name="in")],
        outputs=[Dataset(namespace="emit://source", name="out")],
        raise_on_error=True,
    )
    emit_query_lineage(
        inputs=[Dataset(namespace="emit://source", name="in")],
        outputs=[Dataset(namespace="emit://source", name="out")],
        raise_on_error=True,
    )


DAG_ID = "openlineage_policy_conf_controls_dag"

with DAG(
    dagrun_timeout=DEFAULT_DAGRUN_TIMEOUT,
    dag_id=DAG_ID,
    start_date=datetime(2021, 1, 1),
    schedule=None,
    catchup=False,
    default_args={"retries": 0},
) as dag:
    # No rule. START/COMPLETE carry the hook lineage datasets and the Python source code: the baseline.
    t_default = PythonOperator(task_id="t_default", python_callable=_register_hook_lineage)
    # No rule. START/COMPLETE carry the bash source code but no bash_command in the airflow task facet: the
    # baseline for t_full_info.
    t_default_bash = BashOperator(task_id="t_default_bash", bash_command="echo full_info")
    # Conf Dag + task rule extract_operator_metadata: false. START/COMPLETE carry no datasets and no source
    # code.
    t_extract_off = PythonOperator(task_id="t_extract_off", python_callable=_register_hook_lineage)
    # Conf Dag + task rule hook_lineage: false. START/COMPLETE carry no datasets, the source code stays.
    t_hook_off = PythonOperator(task_id="t_hook_off", python_callable=_register_hook_lineage)
    # Conf Dag + task rule include_full_task_info: true. START/COMPLETE carry bash_command in the airflow task
    # facet.
    t_full_info = BashOperator(task_id="t_full_info", bash_command="echo full_info")
    # Conf Dag + task rule emit: false. No task events, and its emit_dataset_lineage and emit_query_lineage
    # calls emit nothing either.
    t_emit_off = PythonOperator(task_id="t_emit_off", python_callable=_register_and_emit_lineage)
    # Conf Dag + task rule emit_task_events: false. No task events.
    t_task_events_off = PythonOperator(task_id="t_task_events_off", python_callable=_register_hook_lineage)
    # Conf Dag + task rule emit: false with emit_task_events: true. emit_task_events wins, so START/COMPLETE
    # are emitted with the datasets.
    t_task_events_beat_emit = PythonOperator(
        task_id="t_task_events_beat_emit", python_callable=_register_hook_lineage
    )
    # Conf operator rule (regex on PolicyConfControlsOperator) hook_lineage: false. The hook lineage datasets
    # are dropped, so START/COMPLETE carry the declared inlets and outlets instead. The operator has no
    # extractor, so no source code.
    t_operator_conf = PolicyConfControlsOperator(
        task_id="t_operator_conf",
        python_callable=_register_hook_lineage,
        inlets=[Asset(uri="file://host1/declared_in.txt")],
        outlets=[Asset(uri="file://host1/declared_out.txt")],
    )
    # The same operator rule, but a Dag + task rule sets hook_lineage: true and wins as the more specific
    # rule. COMPLETE carries the hook lineage datasets instead of the declared ones, START only the declared
    # ones.
    t_operator_task = PolicyConfControlsOperator(
        task_id="t_operator_task",
        python_callable=_register_hook_lineage,
        inlets=[Asset(uri="file://host1/declared_in.txt")],
        outlets=[Asset(uri="file://host1/declared_out.txt")],
    )
    # Conf Dag + task rule extract_operator_metadata: false, unlocked. The authored True wins: datasets and
    # source code are emitted.
    t_extract_overridden = extend_global_openlineage_emission_policy(
        PythonOperator(task_id="t_extract_overridden", python_callable=_register_hook_lineage),
        extract_operator_metadata=True,
    )
    # Conf Dag + task rule hook_lineage: false, unlocked. The authored True wins: the hook lineage datasets
    # are emitted.
    t_hook_overridden = extend_global_openlineage_emission_policy(
        PythonOperator(task_id="t_hook_overridden", python_callable=_register_hook_lineage),
        hook_lineage=True,
    )
    # Conf Dag + task rule emit: false, unlocked. The authored True wins: START/COMPLETE are emitted.
    t_emit_overridden = extend_global_openlineage_emission_policy(
        PythonOperator(task_id="t_emit_overridden", python_callable=_register_hook_lineage),
        emit=True,
    )
    # Conf operator rule (regex on PolicyConfEmitOffOperator) emit: false. No task events.
    t_operator_emit_off = PolicyConfEmitOffOperator(
        task_id="t_operator_emit_off", python_callable=_register_hook_lineage
    )
    # Conf operator rule with the exact class path of SmoothOperator (default match_mode) emit: false. No task
    # events.
    t_exact_operator = SmoothOperator(task_id="t_exact_operator")
    with TaskGroup("grp"):
        # Conf Dag + task rule naming the task by its full id grp.t_grouped, hook_lineage: false. No datasets,
        # the source code stays.
        t_grouped = PythonOperator(task_id="t_grouped", python_callable=_register_hook_lineage)
    # Conf Dag + task rule hook_lineage: false, applied to the one mapped instance. No datasets, the source
    # code stays.
    t_mapped = PythonOperator.partial(task_id="t_mapped", python_callable=_register_hook_lineage).expand(
        op_args=[[1]]
    )
    # Conf rule with match_mode regex on both the dag_id and the task_id, hook_lineage: false. No datasets,
    # the source code stays.
    t_regex_task = PythonOperator(task_id="t_regex_task", python_callable=_register_hook_lineage)
    # Two conf Dag + task rules, hook_lineage: true then false. The last one wins: no datasets, the source
    # code stays.
    t_conflict = PythonOperator(task_id="t_conflict", python_callable=_register_hook_lineage)
    check_events = OpenLineageTestOperator(
        task_id="check_events", file_path=get_expected_event_file_path(DAG_ID)
    )
    (
        t_default
        >> t_default_bash
        >> t_extract_off
        >> t_hook_off
        >> t_full_info
        >> t_emit_off
        >> t_task_events_off
        >> t_task_events_beat_emit
        >> t_operator_conf
        >> t_operator_task
        >> t_extract_overridden
        >> t_hook_overridden
        >> t_emit_overridden
        >> t_operator_emit_off
        >> t_exact_operator
        >> t_grouped
        >> t_mapped
        >> t_regex_task
        >> t_conflict
        >> check_events
    )


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: contributing-docs/testing/system_tests.rst)
test_run = get_test_run(dag)
