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

Hook-reported datasets are attributed to the class of the ``context`` they were registered with, so the
DAG uses stand-in hooks. Rules use ``match_mode: regex`` because the stand-in classes live in the Dag module.
Patterns are matched against ``<namespace>/<name>`` of the OpenLineage dataset, and the most specific
matching rule replaces broader lists: dag + task + hook > dag + task > dag + hook > dag > global.

Hook-scoped rules apply only to hook-collected datasets, not to declared inlets. A hook-scoped
``hook_lineage`` drops (``false``) or keeps (``true``) every dataset the hook reports.

Required global conf (the first rule is the global rule of ``openlineage_policy_exclude_datasets_global_dag``,
which every dag-scoped rule here replaces)::

    [
        {"scope": {}, "controls": {"exclude_datasets": [".*/exclude_datasets_global_.*"]}},
        {
            "scope": {"dag_id": "openlineage_policy_exclude_datasets_conf_dag"},
            "controls": {"exclude_datasets": [".*/dag_excluded_.*"]},
        },
        {
            "scope": {"dag_id": "openlineage_policy_exclude_datasets_conf_dag", "hook": ".*\\.ConfHook"},
            "match_mode": "regex",
            "controls": {"exclude_datasets": [".*/dag_hook_excluded_.*"]},
        },
        {
            "scope": {"dag_id": "openlineage_policy_exclude_datasets_conf_dag", "task_id": "t_task"},
            "controls": {"exclude_datasets": [".*/task_excluded_.*"]},
        },
        {
            "scope": {
                "dag_id": "openlineage_policy_exclude_datasets_conf_dag",
                "task_id": "t_task_hook",
                "hook": ".*\\.ConfHook",
            },
            "match_mode": "regex",
            "controls": {"exclude_datasets": [".*/task_hook_excluded_.*"]},
        },
        {
            "scope": {"dag_id": "openlineage_policy_exclude_datasets_conf_dag", "task_id": "t_task_clear"},
            "controls": {"exclude_datasets": []},
        },
        {
            "scope": {"dag_id": "openlineage_policy_exclude_datasets_conf_dag", "hook": ".*\\.DroppedHook"},
            "match_mode": "regex",
            "controls": {"hook_lineage": false},
        },
        {
            "scope": {"dag_id": "openlineage_policy_exclude_datasets_conf_dag", "hook": ".*\\.KeptHook"},
            "match_mode": "regex",
            "controls": {"hook_lineage": true},
        },
    ]
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


class ConfHook:
    """Stand-in for a hook with hook-scoped exclusion rules."""


class QuietHook:
    """Stand-in for a hook with no hook-scoped rule."""


class DroppedHook:
    """Stand-in for a hook whose datasets are all dropped by a hook-scoped rule."""


class KeptHook:
    """Stand-in for a hook whose datasets are all kept by a hook-scoped rule."""


def _register(hook: object, inputs: Sequence[str] = (), outputs: Sequence[str] = ()):
    from airflow.providers.common.compat.lineage.hook import get_hook_lineage_collector

    collector = get_hook_lineage_collector()
    for name in inputs:
        collector.add_input_asset(context=hook, uri=f"file://host1/{name}")
    for name in outputs:
        collector.add_output_asset(context=hook, uri=f"file://host1/{name}")


def _dag():
    _register(
        QuietHook(),
        inputs=["dag_excluded_in.txt", "exclude_datasets_global_in.txt", "keep_in.txt"],
        outputs=["dag_excluded_out.txt", "keep_out.txt"],
    )


def _dag_hook():
    _register(ConfHook(), inputs=["dag_hook_excluded_in.txt", "dag_excluded_in.txt", "keep_in.txt"])


def _task():
    _register(QuietHook(), inputs=["task_excluded_in.txt", "dag_excluded_in.txt", "keep_in.txt"])


def _task_hook():
    _register(ConfHook(), inputs=["task_hook_excluded_in.txt", "dag_hook_excluded_in.txt"])
    _register(QuietHook(), inputs=["dag_excluded_in.txt", "keep_in.txt"])


def _task_clear():
    _register(QuietHook(), inputs=["dag_excluded_in.txt", "keep_in.txt"])


def _dropped_hook():
    _register(DroppedHook(), inputs=["dropped_in.txt"], outputs=["dropped_out.txt"])
    _register(QuietHook(), inputs=["keep_in.txt"], outputs=["keep_out.txt"])


def _kept_hook():
    _register(KeptHook(), inputs=["dag_excluded_in.txt", "keep_in.txt"])


def _all_excluded():
    _register(QuietHook(), inputs=["dag_excluded_in.txt"])


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
    t_dropped_hook = PythonOperator(task_id="t_dropped_hook", python_callable=_dropped_hook)
    t_kept_hook = PythonOperator(task_id="t_kept_hook", python_callable=_kept_hook)
    t_inlets = PythonOperator(
        task_id="t_inlets",
        python_callable=_do_nothing,
        inlets=[
            Asset(uri="file://host1/dag_excluded_in.txt"),
            Asset(uri="file://host1/dag_hook_excluded_in.txt"),
        ],
    )
    t_all_excluded = PythonOperator(
        task_id="t_all_excluded",
        python_callable=_all_excluded,
        inlets=[Asset(uri="file://host1/keep_in.txt")],
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
        >> t_dropped_hook
        >> t_kept_hook
        >> t_inlets
        >> t_all_excluded
        >> check_events
    )


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: contributing-docs/testing/system_tests.rst)
test_run = get_test_run(dag)
