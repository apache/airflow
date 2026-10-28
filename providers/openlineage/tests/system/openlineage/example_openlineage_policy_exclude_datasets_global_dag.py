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
DAG exercising the OpenLineage ``exclude_datasets`` control in the conf tiers that have no ``dag_id``.

Hook, operator and operator + hook rules are regexes on the names of the stand-in classes below, which nothing
else uses, so they cannot affect another test. The one global rule matches a dataset name no other test uses.
Every task reports an input and an output per stem, and the expected events check both.

The most specific matching rule replaces broader lists: operator + hook, operator, hook, global.

The comment above each task says which rules and authoring calls apply to it, and what its events must show.

The rules are in ``AIRFLOW__OPENLINEAGE__EMISSION_POLICY`` of ``airflow-e2e-tests/docker/openlineage.yml``.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime

from airflow import DAG
from airflow.providers.common.compat.assets import Asset
from airflow.providers.common.compat.lineage.hook import get_hook_lineage_collector
from airflow.providers.openlineage.api.emission_policy import extend_global_openlineage_emission_policy
from airflow.providers.standard.operators.python import PythonOperator

from system.openlineage.constants import DEFAULT_DAGRUN_TIMEOUT
from system.openlineage.expected_events import get_expected_event_file_path
from system.openlineage.operator import OpenLineageTestOperator


class QuietHook:
    """Hook that no rule is scoped to."""


class ExcludeDatasetsGlobalHook:
    """Hook with a hook rule and an operator + hook rule."""


class ExcludeDatasetsGlobalOtherHook:
    """Hook with only a hook rule, which is a regex."""


class ExcludeDatasetsGlobalDropHook:
    """Hook whose datasets a hook rule with ``hook_lineage: false`` drops."""


class ExcludeDatasetsGlobalOpDropHook:
    """Hook whose datasets an operator + hook rule with ``hook_lineage: false`` drops."""


class ExcludeDatasetsGlobalLockedHook:
    """Hook with a locked hook rule."""


class ExcludeDatasetsGlobalOperator(PythonOperator):
    """Operator with operator and operator + hook rules."""


class ExcludeDatasetsGlobalClearOperator(PythonOperator):
    """Operator whose operator rule clears the global list."""


class ExcludeDatasetsGlobalLockedOperator(PythonOperator):
    """Operator with a locked operator rule."""


def _register_stems(hook: object, stems: Sequence[str]):
    collector = get_hook_lineage_collector()
    for stem in stems:
        collector.add_input_asset(context=hook, uri=f"file://host1/{stem}_in.txt")
        collector.add_output_asset(context=hook, uri=f"file://host1/{stem}_out.txt")


def _inlets(stems: Sequence[str]) -> list[Asset]:
    return [Asset(uri=f"file://host1/{stem}_in.txt") for stem in stems]


def _outlets(stems: Sequence[str]) -> list[Asset]:
    return [Asset(uri=f"file://host1/{stem}_out.txt") for stem in stems]


def _global():
    _register_stems(QuietHook(), ["exclude_datasets_global", "keep"])


def _hook():
    _register_stems(ExcludeDatasetsGlobalHook(), ["hook_excluded", "exclude_datasets_global", "keep"])


def _operator():
    _register_stems(QuietHook(), ["operator_excluded", "exclude_datasets_global", "keep"])


def _operator_hook():
    _register_stems(
        ExcludeDatasetsGlobalHook(), ["operator_hook_excluded", "operator_excluded", "hook_excluded", "keep"]
    )


def _operator_vs_hook():
    _register_stems(ExcludeDatasetsGlobalOtherHook(), ["operator_excluded", "other_hook_excluded", "keep"])


def _hook_dropped():
    _register_stems(ExcludeDatasetsGlobalDropHook(), ["dropped"])
    _register_stems(QuietHook(), ["keep"])


def _operator_hook_dropped():
    _register_stems(ExcludeDatasetsGlobalOpDropHook(), ["dropped"])
    _register_stems(QuietHook(), ["operator_excluded", "keep"])


def _clear():
    _register_stems(QuietHook(), ["exclude_datasets_global", "keep"])


def _operator_locked():
    _register_stems(QuietHook(), ["operator_locked", "authored_a"])


def _hook_locked():
    _register_stems(ExcludeDatasetsGlobalLockedHook(), ["hook_locked", "authored_a"])


def _do_nothing():
    pass


DAG_ID = "openlineage_policy_exclude_datasets_global_dag"

with DAG(
    dagrun_timeout=DEFAULT_DAGRUN_TIMEOUT,
    dag_id=DAG_ID,
    start_date=datetime(2021, 1, 1),
    schedule=None,
    catchup=False,
    default_args={"retries": 0},
) as dag:
    # Only the global rule matches: COMPLETE drops exclude_datasets_global and keeps keep.
    t_global = PythonOperator(task_id="t_global", python_callable=_global)
    # The hook rule (hook_excluded) replaces the global list: COMPLETE drops hook_excluded and keeps
    # exclude_datasets_global and keep.
    t_hook = PythonOperator(task_id="t_hook", python_callable=_hook)
    # The operator rule (operator_excluded) replaces the global list: COMPLETE drops operator_excluded and
    # keeps exclude_datasets_global and keep.
    t_operator = ExcludeDatasetsGlobalOperator(task_id="t_operator", python_callable=_operator)
    # The operator + hook rule (operator_hook_excluded) beats the operator and the hook rules: COMPLETE drops
    # operator_hook_excluded and keeps operator_excluded, hook_excluded and keep.
    t_operator_hook = ExcludeDatasetsGlobalOperator(task_id="t_operator_hook", python_callable=_operator_hook)
    # The operator rule beats the hook rule of ExcludeDatasetsGlobalOtherHook: COMPLETE drops
    # operator_excluded and keeps other_hook_excluded and keep.
    t_operator_vs_hook = ExcludeDatasetsGlobalOperator(
        task_id="t_operator_vs_hook", python_callable=_operator_vs_hook
    )
    # The hook rule hook_lineage: false drops ExcludeDatasetsGlobalDropHook's dropped. QuietHook's keep is
    # kept.
    t_hook_dropped = PythonOperator(task_id="t_hook_dropped", python_callable=_hook_dropped)
    # The operator + hook rule hook_lineage: false drops ExcludeDatasetsGlobalOpDropHook's dropped. QuietHook
    # gets the operator list: COMPLETE drops operator_excluded and keeps keep.
    t_operator_hook_dropped = ExcludeDatasetsGlobalOperator(
        task_id="t_operator_hook_dropped", python_callable=_operator_hook_dropped
    )
    # The operator list filters the declared inlets and outlets, the operator + hook list does not: COMPLETE
    # drops operator_excluded and keeps operator_hook_excluded and exclude_datasets_global.
    t_operator_inlets = ExcludeDatasetsGlobalOperator(
        task_id="t_operator_inlets",
        python_callable=_do_nothing,
        inlets=_inlets(["operator_excluded", "operator_hook_excluded", "exclude_datasets_global"]),
        outlets=_outlets(["operator_excluded", "operator_hook_excluded", "exclude_datasets_global"]),
    )
    # The operator rule [] clears the global list: COMPLETE keeps exclude_datasets_global and keep.
    t_clear = ExcludeDatasetsGlobalClearOperator(task_id="t_clear", python_callable=_clear)
    # The locked operator rule (operator_locked) ignores the authored list (authored_): COMPLETE drops
    # operator_locked and keeps authored_a.
    t_operator_locked = extend_global_openlineage_emission_policy(
        ExcludeDatasetsGlobalLockedOperator(task_id="t_operator_locked", python_callable=_operator_locked),
        exclude_datasets=[".*/authored_.*"],
    )
    # The locked hook rule (hook_locked) ignores the authored list (authored_): COMPLETE drops hook_locked and
    # keeps authored_a.
    t_hook_locked = extend_global_openlineage_emission_policy(
        PythonOperator(task_id="t_hook_locked", python_callable=_hook_locked),
        exclude_datasets=[".*/authored_.*"],
    )
    check_events = OpenLineageTestOperator(
        task_id="check_events", file_path=get_expected_event_file_path(DAG_ID)
    )
    (
        t_global
        >> t_hook
        >> t_operator
        >> t_operator_hook
        >> t_operator_vs_hook
        >> t_hook_dropped
        >> t_operator_hook_dropped
        >> t_operator_inlets
        >> t_clear
        >> t_operator_locked
        >> t_hook_locked
        >> check_events
    )


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: contributing-docs/testing/system_tests.rst)
test_run = get_test_run(dag)
