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
with its rule, next to ``t_default``, which has none:

- ``t_extract_off``: ``extract_operator_metadata: false`` leaves no datasets and no source code
- ``t_hook_off``: ``hook_lineage: false`` leaves no datasets, and the source code stays
- ``t_full_info``: ``include_full_task_info: true`` adds the operator fields, which ``t_default_bash`` does not have
- ``t_emit_off``, ``t_task_events_off``: ``emit: false`` and ``emit_task_events: false`` silence the task events
- ``t_task_events_beat_emit``: ``emit_task_events: true`` beats ``emit: false`` in the same rule
- ``t_emit_off`` also calls ``emit_dataset_lineage`` and ``emit_query_lineage``, which emit nothing either
- ``t_operator_conf``: an operator rule with ``hook_lineage: false`` leaves no datasets
- ``t_operator_task``: a Dag + task rule beats the operator rule
- ``t_operator_emit_off``: an operator rule with ``emit: false`` silences the task events
- ``t_exact_operator``: an operator rule with the exact class name of a built-in operator, which no other test uses
- ``t_extract_overridden``, ``t_hook_overridden``, ``t_emit_overridden``: authoring beats an unlocked conf rule
- ``grp.t_grouped``: a Dag + task rule names a task in a task group by its full id
- ``t_mapped``: a Dag + task rule applies to a mapped task instance
- ``t_regex_task``: a Dag + task rule with ``match_mode: regex`` matches both ids as patterns
- ``t_conflict``: of two Dag + task rules with different values, the last one wins
- ``t_skipped_emit_off``, ``t_skipped_full_info``: the events of a skipped task follow the rules too

The Dag run START event reports in ``emits_ol_events`` which tasks emit, as the scheduler resolves the policy for
them, including authoring.

Required global conf::

    [
        {
            "scope": {"dag_id": "openlineage_policy_conf_controls_dag", "task_id": "t_extract_off"},
            "controls": {"extract_operator_metadata": false},
        },
        {
            "scope": {"dag_id": "openlineage_policy_conf_controls_dag", "task_id": "t_hook_off"},
            "controls": {"hook_lineage": false},
        },
        {
            "scope": {"dag_id": "openlineage_policy_conf_controls_dag", "task_id": "t_full_info"},
            "controls": {"include_full_task_info": true},
        },
        {
            "scope": {"dag_id": "openlineage_policy_conf_controls_dag", "task_id": "t_emit_off"},
            "controls": {"emit": false},
        },
        {
            "scope": {"dag_id": "openlineage_policy_conf_controls_dag", "task_id": "t_task_events_off"},
            "controls": {"emit_task_events": false},
        },
        {
            "scope": {"dag_id": "openlineage_policy_conf_controls_dag", "task_id": "t_task_events_beat_emit"},
            "controls": {"emit": false, "emit_task_events": true},
        },
        {
            "scope": {"operator": ".*\\.PolicyConfControlsOperator"},
            "match_mode": "regex",
            "controls": {"hook_lineage": false},
        },
        {
            "scope": {"dag_id": "openlineage_policy_conf_controls_dag", "task_id": "t_operator_task"},
            "controls": {"hook_lineage": true},
        },
        {
            "scope": {"dag_id": "openlineage_policy_conf_controls_dag", "task_id": "t_extract_overridden"},
            "controls": {"extract_operator_metadata": false},
        },
        {
            "scope": {"dag_id": "openlineage_policy_conf_controls_dag", "task_id": "t_hook_overridden"},
            "controls": {"hook_lineage": false},
        },
        {
            "scope": {"dag_id": "openlineage_policy_conf_controls_dag", "task_id": "t_emit_overridden"},
            "controls": {"emit": false},
        },
        {
            "scope": {"operator": ".*\\.PolicyConfEmitOffOperator"},
            "match_mode": "regex",
            "controls": {"emit": false},
        },
        {
            "scope": {"operator": "airflow.providers.standard.operators.smooth.SmoothOperator"},
            "controls": {"emit": false},
        },
        {
            "scope": {"dag_id": "openlineage_policy_conf_controls_dag", "task_id": "grp.t_grouped"},
            "controls": {"hook_lineage": false},
        },
        {
            "scope": {"dag_id": "openlineage_policy_conf_controls_dag", "task_id": "t_mapped"},
            "controls": {"hook_lineage": false},
        },
        {
            "scope": {"dag_id": "openlineage_policy_conf_contr.*", "task_id": "t_regex_.*"},
            "match_mode": "regex",
            "controls": {"hook_lineage": false},
        },
        {
            "scope": {"dag_id": "openlineage_policy_conf_controls_dag", "task_id": "t_conflict"},
            "controls": {"hook_lineage": true},
        },
        {
            "scope": {"dag_id": "openlineage_policy_conf_controls_dag", "task_id": "t_conflict"},
            "controls": {"hook_lineage": false},
        },
        {
            "scope": {"dag_id": "openlineage_policy_conf_controls_dag", "task_id": "t_skipped_emit_off"},
            "controls": {"emit": false},
        },
        {
            "scope": {"dag_id": "openlineage_policy_conf_controls_dag", "task_id": "t_skipped_full_info"},
            "controls": {"include_full_task_info": true},
        },
    ]
"""

from __future__ import annotations

from datetime import datetime

from openlineage.client.event_v2 import Dataset

from airflow import DAG
from airflow.providers.common.compat.lineage.hook import get_hook_lineage_collector
from airflow.providers.common.compat.sdk import AirflowSkipException, TaskGroup
from airflow.providers.openlineage.api import emit_dataset_lineage, emit_query_lineage
from airflow.providers.openlineage.api.emission_policy import extend_global_openlineage_emission_policy
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.smooth import SmoothOperator
from airflow.utils.trigger_rule import TriggerRule

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


def _skip():
    raise AirflowSkipException("Intentional skip")


DAG_ID = "openlineage_policy_conf_controls_dag"

with DAG(
    dagrun_timeout=DEFAULT_DAGRUN_TIMEOUT,
    dag_id=DAG_ID,
    start_date=datetime(2021, 1, 1),
    schedule=None,
    catchup=False,
    default_args={"retries": 0},
) as dag:
    t_default = PythonOperator(task_id="t_default", python_callable=_register_hook_lineage)
    t_default_bash = BashOperator(task_id="t_default_bash", bash_command="echo full_info")
    t_extract_off = PythonOperator(task_id="t_extract_off", python_callable=_register_hook_lineage)
    t_hook_off = PythonOperator(task_id="t_hook_off", python_callable=_register_hook_lineage)
    t_full_info = BashOperator(task_id="t_full_info", bash_command="echo full_info")
    t_emit_off = PythonOperator(task_id="t_emit_off", python_callable=_register_and_emit_lineage)
    t_task_events_off = PythonOperator(task_id="t_task_events_off", python_callable=_register_hook_lineage)
    t_task_events_beat_emit = PythonOperator(
        task_id="t_task_events_beat_emit", python_callable=_register_hook_lineage
    )
    t_operator_conf = PolicyConfControlsOperator(
        task_id="t_operator_conf", python_callable=_register_hook_lineage
    )
    t_operator_task = PolicyConfControlsOperator(
        task_id="t_operator_task", python_callable=_register_hook_lineage
    )
    t_extract_overridden = extend_global_openlineage_emission_policy(
        PythonOperator(task_id="t_extract_overridden", python_callable=_register_hook_lineage),
        extract_operator_metadata=True,
    )
    t_hook_overridden = extend_global_openlineage_emission_policy(
        PythonOperator(task_id="t_hook_overridden", python_callable=_register_hook_lineage),
        hook_lineage=True,
    )
    t_emit_overridden = extend_global_openlineage_emission_policy(
        PythonOperator(task_id="t_emit_overridden", python_callable=_register_hook_lineage),
        emit=True,
    )
    t_operator_emit_off = PolicyConfEmitOffOperator(
        task_id="t_operator_emit_off", python_callable=_register_hook_lineage
    )
    t_exact_operator = SmoothOperator(task_id="t_exact_operator")
    with TaskGroup("grp"):
        t_grouped = PythonOperator(task_id="t_grouped", python_callable=_register_hook_lineage)
    t_mapped = PythonOperator.partial(task_id="t_mapped", python_callable=_register_hook_lineage).expand(
        op_args=[[1]]
    )
    t_regex_task = PythonOperator(task_id="t_regex_task", python_callable=_register_hook_lineage)
    t_conflict = PythonOperator(task_id="t_conflict", python_callable=_register_hook_lineage)
    t_skipped_emit_off = PythonOperator(task_id="t_skipped_emit_off", python_callable=_skip)
    t_skipped_full_info = BashOperator(task_id="t_skipped_full_info", bash_command="exit 99")
    check_events = OpenLineageTestOperator(
        task_id="check_events",
        file_path=get_expected_event_file_path(DAG_ID),
        trigger_rule=TriggerRule.NONE_FAILED,
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
    [t_skipped_emit_off, t_skipped_full_info] >> check_events


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: contributing-docs/testing/system_tests.rst)
test_run = get_test_run(dag)
