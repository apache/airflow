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

Hook-reported datasets are attributed to the class of the ``context`` they were registered with, so the
DAG uses stand-in hooks. Rules use ``match_mode: regex`` because the stand-in classes live in the Dag module.
Patterns are matched against ``<namespace>/<name>`` of the OpenLineage dataset, and the most specific
matching rule replaces broader lists: operator + hook > hook > operator > global here.

Every pattern and class name is unique to this Dag, so the rules do not affect other Dags.

Required global conf:
```
[
    {"scope": {}, "controls": {"exclude_datasets": [".*/exclude_datasets_global_.*"]}},
    {
        "scope": {"hook": ".*\\.ExcludeDatasetsGlobalHook"},
        "match_mode": "regex",
        "controls": {"exclude_datasets": [".*/hook_excluded_.*"]},
    },
    {
        "scope": {"operator": ".*\\.ExcludeDatasetsGlobalOperator"},
        "match_mode": "regex",
        "controls": {"exclude_datasets": [".*/operator_excluded_.*"]},
    },
    {
        "scope": {"operator": ".*\\.ExcludeDatasetsGlobalOperator", "hook": ".*\\.ExcludeDatasetsGlobalHook"},
        "match_mode": "regex",
        "controls": {"exclude_datasets": [".*/operator_hook_excluded_.*"]},
    },
    {
        "scope": {"operator": ".*\\.ExcludeDatasetsGlobalClearOperator"},
        "match_mode": "regex",
        "controls": {"exclude_datasets": []},
    },
]
```
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime

from airflow import DAG
from airflow.providers.common.compat.assets import Asset
from airflow.providers.standard.operators.python import PythonOperator

from system.openlineage.constants import DEFAULT_DAGRUN_TIMEOUT
from system.openlineage.expected_events import get_expected_event_file_path
from system.openlineage.operator import OpenLineageTestOperator


class ExcludeDatasetsGlobalHook:
    """Stand-in for a hook with a hook-scoped rule."""


class QuietHook:
    """Stand-in for a hook with no hook-scoped rule."""


class ExcludeDatasetsGlobalOperator(PythonOperator):
    """Operator with operator-scoped rules."""


class ExcludeDatasetsGlobalClearOperator(PythonOperator):
    """Operator whose operator-scoped rule clears the global list."""


def _register(hook: object, inputs: Sequence[str] = (), outputs: Sequence[str] = ()):
    from airflow.providers.common.compat.lineage.hook import get_hook_lineage_collector

    collector = get_hook_lineage_collector()
    for name in inputs:
        collector.add_input_asset(context=hook, uri=f"file://host1/{name}")
    for name in outputs:
        collector.add_output_asset(context=hook, uri=f"file://host1/{name}")


def _global():
    _register(
        QuietHook(),
        inputs=["exclude_datasets_global_in.txt", "keep_in.txt"],
        outputs=["exclude_datasets_global_out.txt", "keep_out.txt"],
    )


def _hook():
    _register(
        ExcludeDatasetsGlobalHook(),
        inputs=["hook_excluded_in.txt", "exclude_datasets_global_in.txt", "keep_in.txt"],
    )


def _operator():
    _register(
        QuietHook(),
        inputs=["operator_excluded_in.txt", "exclude_datasets_global_in.txt", "keep_in.txt"],
    )


def _operator_hook():
    _register(
        ExcludeDatasetsGlobalHook(),
        inputs=[
            "operator_hook_excluded_in.txt",
            "operator_excluded_in.txt",
            "hook_excluded_in.txt",
            "keep_in.txt",
        ],
    )


def _clear():
    _register(QuietHook(), inputs=["exclude_datasets_global_in.txt", "keep_in.txt"])


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
    t_global = PythonOperator(task_id="t_global", python_callable=_global)
    t_hook = PythonOperator(task_id="t_hook", python_callable=_hook)
    t_operator = ExcludeDatasetsGlobalOperator(task_id="t_operator", python_callable=_operator)
    t_operator_hook = ExcludeDatasetsGlobalOperator(task_id="t_operator_hook", python_callable=_operator_hook)
    t_operator_inlets = ExcludeDatasetsGlobalOperator(
        task_id="t_operator_inlets",
        python_callable=_do_nothing,
        inlets=[
            Asset(uri="file://host1/operator_excluded_in.txt"),
            Asset(uri="file://host1/operator_hook_excluded_in.txt"),
            Asset(uri="file://host1/exclude_datasets_global_in.txt"),
        ],
    )
    t_clear = ExcludeDatasetsGlobalClearOperator(task_id="t_clear", python_callable=_clear)
    check_events = OpenLineageTestOperator(
        task_id="check_events", file_path=get_expected_event_file_path(DAG_ID)
    )
    t_global >> t_hook >> t_operator >> t_operator_hook >> t_operator_inlets >> t_clear >> check_events


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: contributing-docs/testing/system_tests.rst)
test_run = get_test_run(dag)
