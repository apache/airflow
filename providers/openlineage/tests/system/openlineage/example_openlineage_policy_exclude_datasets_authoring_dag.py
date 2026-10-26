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
DAG exercising ``exclude_datasets`` authoring overrides via ``extend_global_openlineage_emission_policy``.

Hook-reported datasets are attributed to the class of the ``context`` they were registered with, so the
DAG uses stand-in hooks. Rules use ``match_mode: regex`` because the stand-in classes live in the Dag module.

It checks that an authored list:
    - replaces the conf lists for every dataset of the task, including hook-scoped conf lists
    - can clear the conf list with ``[]``
    - applies to declared inlets
    - can be set for the whole Dag, but only reaches tasks that already exist at the time of the call
    - is ignored where a locked conf rule applies, even when a more specific conf rule wins the value
    - is ignored only for the datasets of the hook a locked hook-scoped rule applies to

Required global conf:
```
[
    {
        "scope": {"dag_id": "openlineage_policy_exclude_datasets_authoring_dag"},
        "controls": {"exclude_datasets": [".*/conf_excluded_.*"]},
    },
    {
        "scope": {"dag_id": "openlineage_policy_exclude_datasets_authoring_dag", "hook": ".*\\.AuthHook"},
        "match_mode": "regex",
        "controls": {"exclude_datasets": [".*/conf_hook_excluded_.*"]},
    },
    {
        "scope": {"dag_id": "openlineage_policy_exclude_datasets_authoring_dag", "task_id": "t_locked"},
        "locked": true,
        "controls": {"exclude_datasets": [".*/locked_excluded_.*"]},
    },
    {
        "scope": {
            "dag_id": "openlineage_policy_exclude_datasets_authoring_dag",
            "task_id": "t_locked",
            "hook": ".*\\.AuthHook",
        },
        "match_mode": "regex",
        "controls": {"exclude_datasets": [".*/specific_excluded_.*"]},
    },
    {
        "scope": {
            "dag_id": "openlineage_policy_exclude_datasets_authoring_dag",
            "task_id": "t_locked_hook",
            "hook": ".*\\.LockedHook",
        },
        "match_mode": "regex",
        "locked": true,
        "controls": {"exclude_datasets": [".*/locked_hook_excluded_.*"]},
    },
]
```
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime

from airflow import DAG
from airflow.providers.common.compat.assets import Asset
from airflow.providers.openlineage.api.emission_policy import extend_global_openlineage_emission_policy
from airflow.providers.standard.operators.python import PythonOperator

from system.openlineage.constants import DEFAULT_DAGRUN_TIMEOUT
from system.openlineage.expected_events import get_expected_event_file_path
from system.openlineage.operator import OpenLineageTestOperator


class AuthHook:
    """Stand-in for a hook with a hook-scoped conf list."""


class QuietHook:
    """Stand-in for a hook with no hook-scoped rule."""


class LockedHook:
    """Stand-in for a hook with a locked hook-scoped rule."""


def _register(hook: object, inputs: Sequence[str] = (), outputs: Sequence[str] = ()):
    from airflow.providers.common.compat.lineage.hook import get_hook_lineage_collector

    collector = get_hook_lineage_collector()
    for name in inputs:
        collector.add_input_asset(context=hook, uri=f"file://host1/{name}")
    for name in outputs:
        collector.add_output_asset(context=hook, uri=f"file://host1/{name}")


def _authored_dag():
    _register(QuietHook(), inputs=["dag_authored_in.txt", "conf_excluded_in.txt"])
    _register(AuthHook(), inputs=["conf_hook_excluded_in.txt"], outputs=["dag_authored_out.txt"])


def _after_dag_call():
    _register(QuietHook(), inputs=["conf_excluded_in.txt", "dag_authored_in.txt"])
    _register(AuthHook(), inputs=["conf_hook_excluded_in.txt", "keep_in.txt"])


def _authored_task():
    _register(AuthHook(), inputs=["conf_hook_excluded_in.txt", "authored_in.txt"])
    _register(QuietHook(), inputs=["conf_excluded_in.txt", "authored_other_in.txt"])


def _authored_clear():
    _register(QuietHook(), inputs=["conf_excluded_in.txt"])
    _register(AuthHook(), inputs=["conf_hook_excluded_in.txt"])


def _locked():
    _register(QuietHook(), inputs=["locked_excluded_in.txt", "authored_in.txt"])
    _register(
        AuthHook(), inputs=["specific_excluded_in.txt", "locked_excluded_in.txt", "authored_other_in.txt"]
    )


def _locked_hook():
    _register(LockedHook(), inputs=["locked_hook_excluded_in.txt", "authored_in.txt"])
    _register(QuietHook(), inputs=["conf_excluded_in.txt", "authored_other_in.txt"])


def _do_nothing():
    pass


DAG_ID = "openlineage_policy_exclude_datasets_authoring_dag"

with DAG(
    dagrun_timeout=DEFAULT_DAGRUN_TIMEOUT,
    dag_id=DAG_ID,
    start_date=datetime(2021, 1, 1),
    schedule=None,
    catchup=False,
    default_args={"retries": 0},
) as dag:
    t_authored_dag = PythonOperator(task_id="t_authored_dag", python_callable=_authored_dag)
    # Dag-level flags only reach tasks that already exist, so every task below is left out of this call.
    extend_global_openlineage_emission_policy(dag, exclude_datasets=[".*/dag_authored_.*"])

    t_after_dag_call = PythonOperator(task_id="t_after_dag_call", python_callable=_after_dag_call)
    t_authored_task = extend_global_openlineage_emission_policy(
        PythonOperator(task_id="t_authored_task", python_callable=_authored_task),
        exclude_datasets=[".*/authored_.*"],
    )
    t_authored_clear = extend_global_openlineage_emission_policy(
        PythonOperator(task_id="t_authored_clear", python_callable=_authored_clear),
        exclude_datasets=[],
    )
    t_authored_inlets = extend_global_openlineage_emission_policy(
        PythonOperator(
            task_id="t_authored_inlets",
            python_callable=_do_nothing,
            inlets=[
                Asset(uri="file://host1/authored_in.txt"),
                Asset(uri="file://host1/conf_excluded_in.txt"),
            ],
        ),
        exclude_datasets=[".*/authored_.*"],
    )
    t_locked = extend_global_openlineage_emission_policy(
        PythonOperator(task_id="t_locked", python_callable=_locked),
        exclude_datasets=[".*/authored_.*"],
    )
    t_locked_hook = extend_global_openlineage_emission_policy(
        PythonOperator(task_id="t_locked_hook", python_callable=_locked_hook),
        exclude_datasets=[".*/authored_.*"],
    )
    check_events = OpenLineageTestOperator(
        task_id="check_events", file_path=get_expected_event_file_path(DAG_ID)
    )
    (
        t_authored_dag
        >> t_after_dag_call
        >> t_authored_task
        >> t_authored_clear
        >> t_authored_inlets
        >> t_locked
        >> t_locked_hook
        >> check_events
    )


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: contributing-docs/testing/system_tests.rst)
test_run = get_test_run(dag)
