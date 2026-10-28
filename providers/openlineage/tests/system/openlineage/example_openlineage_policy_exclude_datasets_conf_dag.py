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
DAG exercising the OpenLineage ``exclude_datasets`` control in the ``dag_id`` conf tiers.

The most specific matching rule replaces broader lists: Dag + task + hook, Dag + task, Dag + hook, Dag, then
operator + hook, operator, hook, global. Hook and operator rules are regexes on the names of the stand-in classes
below. Every task reports an input and an output per stem, and the expected events check both.

- ``t_dag``: the Dag list replaces the global list.
- ``t_dag_hook``: a Dag + hook list replaces the Dag list.
- ``t_task``: a Dag + task list replaces the Dag + hook and Dag lists.
- ``t_task_hook``: a Dag + task + hook list replaces the Dag + task list for its hook, a second hook of the task
  gets the Dag + task list.
- ``t_task_clear``: a Dag + task ``[]`` clears the Dag list.
- ``t_task_dropped``, ``t_dropped_hook``: a hook scoped ``hook_lineage: false`` drops the datasets of one hook and
  not of another, at Dag + task + hook and at Dag + hook.
- ``t_kept_hook``: ``hook_lineage: true`` keeps all the datasets of a hook, which clears the Dag list for it.
- ``t_operator_dag``: Dag rules beat operator and operator + hook rules.
- ``t_exact_hook``: a hook scope with the exact class name of a real hook, which is the default ``match_mode``.
- ``t_same_dataset``: one dataset reported by two hooks is filtered for each hook.
- ``t_inlets``: Dag lists filter declared inlets and outlets, a Dag + hook list does not.
- ``t_all_excluded``: when every hook dataset is excluded, declared inlets and outlets are not used instead.

The rules are in ``AIRFLOW__OPENLINEAGE__EMISSION_POLICY`` of ``airflow-e2e-tests/docker/openlineage.yml``, whose
global rule, which the Dag rules here replace, belongs to ``openlineage_policy_exclude_datasets_global_dag``.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime

from airflow import DAG
from airflow.providers.common.compat.assets import Asset
from airflow.providers.common.compat.lineage.hook import get_hook_lineage_collector
from airflow.providers.standard.hooks.filesystem import FSHook
from airflow.providers.standard.operators.python import PythonOperator

from system.openlineage.constants import DEFAULT_DAGRUN_TIMEOUT
from system.openlineage.expected_events import get_expected_event_file_path
from system.openlineage.operator import OpenLineageTestOperator


class QuietHook:
    """Hook that no rule is scoped to."""


class ConfHook:
    """Hook with Dag, Dag + task and Dag + task + hook rules."""


class DroppedHook:
    """Hook whose datasets a Dag + hook rule with ``hook_lineage: false`` drops."""


class KeptHook:
    """Hook whose datasets a Dag + hook rule with ``hook_lineage: true`` keeps."""


class TaskDroppedHook:
    """Hook whose datasets a Dag + task + hook rule with ``hook_lineage: false`` drops."""


class ExcludeDatasetsConfOperator(PythonOperator):
    """Operator with operator and operator + hook rules, which the Dag rules beat."""


def _register_stems(hook: object, stems: Sequence[str]):
    collector = get_hook_lineage_collector()
    for stem in stems:
        collector.add_input_asset(context=hook, uri=f"file://host1/{stem}_in.txt")
        collector.add_output_asset(context=hook, uri=f"file://host1/{stem}_out.txt")


def _inlets(stems: Sequence[str]) -> list[Asset]:
    return [Asset(uri=f"file://host1/{stem}_in.txt") for stem in stems]


def _outlets(stems: Sequence[str]) -> list[Asset]:
    return [Asset(uri=f"file://host1/{stem}_out.txt") for stem in stems]


def _dag():
    _register_stems(QuietHook(), ["dag_excluded", "exclude_datasets_global", "keep"])


def _dag_hook():
    _register_stems(ConfHook(), ["dag_hook_excluded", "dag_excluded", "keep"])


def _task():
    _register_stems(ConfHook(), ["task_excluded_a", "dag_hook_excluded", "dag_excluded", "keep"])


def _task_hook():
    _register_stems(ConfHook(), ["task_hook_excluded", "task_excluded_a", "dag_hook_excluded", "keep_conf"])
    _register_stems(QuietHook(), ["task_excluded_b", "dag_excluded", "keep"])


def _task_clear():
    _register_stems(QuietHook(), ["dag_excluded", "keep"])


def _task_dropped():
    _register_stems(TaskDroppedHook(), ["dropped"])
    _register_stems(QuietHook(), ["dag_excluded", "keep"])


def _dropped_hook():
    _register_stems(DroppedHook(), ["dropped"])
    _register_stems(QuietHook(), ["keep"])


def _kept_hook():
    _register_stems(KeptHook(), ["dag_excluded", "keep"])


def _operator_dag():
    _register_stems(ConfHook(), ["operator_hook_excluded", "dag_hook_excluded", "keep_conf"])
    _register_stems(QuietHook(), ["operator_excluded", "dag_excluded", "keep"])


def _exact_hook():
    # The class name is all the policy reads from the hook, so skip the connection FSHook looks up when created.
    _register_stems(FSHook.__new__(FSHook), ["fs_hook_excluded", "dag_excluded", "keep"])


def _same_dataset():
    _register_stems(ConfHook(), ["dag_hook_excluded"])
    _register_stems(QuietHook(), ["dag_hook_excluded"])


def _all_excluded():
    _register_stems(QuietHook(), ["dag_excluded"])


def _do_nothing():
    pass


DAG_ID = "openlineage_policy_exclude_datasets_conf_dag"

with DAG(
    dagrun_timeout=DEFAULT_DAGRUN_TIMEOUT,
    dag_id=DAG_ID,
    start_date=datetime(2021, 1, 1),
    schedule=None,
    catchup=False,
    default_args={"retries": 0},
) as dag:
    t_dag = PythonOperator(task_id="t_dag", python_callable=_dag)
    t_dag_hook = PythonOperator(task_id="t_dag_hook", python_callable=_dag_hook)
    t_task = PythonOperator(task_id="t_task", python_callable=_task)
    t_task_hook = PythonOperator(task_id="t_task_hook", python_callable=_task_hook)
    t_task_clear = PythonOperator(task_id="t_task_clear", python_callable=_task_clear)
    t_task_dropped = PythonOperator(task_id="t_task_dropped", python_callable=_task_dropped)
    t_dropped_hook = PythonOperator(task_id="t_dropped_hook", python_callable=_dropped_hook)
    t_kept_hook = PythonOperator(task_id="t_kept_hook", python_callable=_kept_hook)
    t_operator_dag = ExcludeDatasetsConfOperator(task_id="t_operator_dag", python_callable=_operator_dag)
    t_exact_hook = PythonOperator(task_id="t_exact_hook", python_callable=_exact_hook)
    t_same_dataset = PythonOperator(task_id="t_same_dataset", python_callable=_same_dataset)
    t_inlets = PythonOperator(
        task_id="t_inlets",
        python_callable=_do_nothing,
        inlets=_inlets(["dag_excluded", "dag_hook_excluded", "keep"]),
        outlets=_outlets(["dag_excluded", "dag_hook_excluded", "keep"]),
    )
    t_all_excluded = PythonOperator(
        task_id="t_all_excluded",
        python_callable=_all_excluded,
        inlets=_inlets(["keep"]),
        outlets=_outlets(["keep"]),
    )
    check_events = OpenLineageTestOperator(
        task_id="check_events", file_path=get_expected_event_file_path(DAG_ID)
    )
    (
        t_dag
        >> t_dag_hook
        >> t_task
        >> t_task_hook
        >> t_task_clear
        >> t_task_dropped
        >> t_dropped_hook
        >> t_kept_hook
        >> t_operator_dag
        >> t_exact_hook
        >> t_same_dataset
        >> t_inlets
        >> t_all_excluded
        >> check_events
    )


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: contributing-docs/testing/system_tests.rst)
test_run = get_test_run(dag)
