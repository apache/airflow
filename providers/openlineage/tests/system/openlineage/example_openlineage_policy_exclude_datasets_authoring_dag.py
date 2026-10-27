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

Hook rules are regexes on the names of the stand-in classes below. Every task reports an input and an output per
stem, and the expected events check both. It checks that an authored list:

- replaces the conf lists of the task, including a hook scoped one (``t_authored_task``)
- clears the conf lists when it is ``[]`` (``t_authored_clear``)
- applies to declared inlets and outlets (``t_authored_inlets``)
- set for the whole Dag reaches the tasks that exist at the call (``t_authored_dag``), not the ones defined later
  (``t_after_dag_call``), and a task level list set after it wins (``t_dag_then_task``)
- is matched against ``<namespace>/<name>`` with ``re.fullmatch``, each of several patterns on its own
  (``t_identity``, ``t_fullmatch``)
- does not turn hook lineage back on: with ``hook_lineage=False`` the hook assets are ignored and the declared
  ones are filtered by conf (``t_hook_lineage_off``)
- is ignored where a locked conf rule applies, even when a more specific conf rule wins the value
  (``t_locked``), and only for the datasets of the hook a locked hook rule applies to (``t_locked_hook``)
- replaces a hook scoped ``hook_lineage: false``, so the datasets of that hook are kept unless the list excludes
  them (``t_authored_over_dropped``), while a locked one keeps dropping them (``t_locked_dropped``)

Required global conf::

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
        {
            "scope": {
                "dag_id": "openlineage_policy_exclude_datasets_authoring_dag",
                "hook": ".*\\.AuthDroppedHook",
            },
            "match_mode": "regex",
            "controls": {"hook_lineage": false},
        },
        {
            "scope": {
                "dag_id": "openlineage_policy_exclude_datasets_authoring_dag",
                "task_id": "t_locked_dropped",
                "hook": ".*\\.AuthLockedDroppedHook",
            },
            "match_mode": "regex",
            "locked": true,
            "controls": {"hook_lineage": false},
        },
    ]
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


class AuthHook:
    """Hook with a Dag + hook rule that authoring replaces."""


class LockedHook:
    """Hook with a locked Dag + task + hook rule."""


class AuthDroppedHook:
    """Hook whose datasets a Dag + hook rule with ``hook_lineage: false`` drops."""


class AuthLockedDroppedHook:
    """Hook whose datasets a locked Dag + task + hook rule with ``hook_lineage: false`` drops."""


def _register_stems(hook: object, stems: Sequence[str]):
    collector = get_hook_lineage_collector()
    for stem in stems:
        collector.add_input_asset(context=hook, uri=f"file://host1/{stem}_in.txt")
        collector.add_output_asset(context=hook, uri=f"file://host1/{stem}_out.txt")


def _register_uris(hook: object, inputs: Sequence[str], outputs: Sequence[str]):
    collector = get_hook_lineage_collector()
    for uri in inputs:
        collector.add_input_asset(context=hook, uri=uri)
    for uri in outputs:
        collector.add_output_asset(context=hook, uri=uri)


def _inlets(stems: Sequence[str]) -> list[Asset]:
    return [Asset(uri=f"file://host1/{stem}_in.txt") for stem in stems]


def _outlets(stems: Sequence[str]) -> list[Asset]:
    return [Asset(uri=f"file://host1/{stem}_out.txt") for stem in stems]


def _authored_dag():
    _register_stems(QuietHook(), ["dag_authored", "conf_excluded"])
    _register_stems(AuthHook(), ["conf_hook_excluded"])


def _dag_then_task():
    _register_stems(QuietHook(), ["dag_authored", "task_authored", "conf_excluded"])


def _after_dag_call():
    _register_stems(QuietHook(), ["conf_excluded", "dag_authored"])
    _register_stems(AuthHook(), ["conf_hook_excluded", "keep"])


def _authored_task():
    _register_stems(AuthHook(), ["conf_hook_excluded", "authored_a"])
    _register_stems(QuietHook(), ["conf_excluded", "authored_b"])


def _authored_clear():
    _register_stems(QuietHook(), ["conf_excluded"])
    _register_stems(AuthHook(), ["conf_hook_excluded"])


def _hook_lineage_off():
    _register_stems(QuietHook(), ["hook_ignored"])


def _fullmatch():
    _register_stems(QuietHook(), ["authored_partial", "authored_full"])


def _identity():
    _register_uris(
        QuietHook(),
        inputs=[
            "s3://ol-exclude-bucket/staging/part_1.csv",
            "s3://ol-exclude-bucket/final/part_1.csv",
            "file://host1/slash_in.txt",
            "file://host1/keep_in.txt",
        ],
        outputs=[
            "s3://ol-exclude-bucket/staging/part_1_out.csv",
            "s3://ol-exclude-bucket/final/part_1_out.csv",
            "file://host1/slash_out.txt",
            "file://host1/keep_out.txt",
        ],
    )


def _locked():
    _register_stems(QuietHook(), ["locked_excluded_a", "authored_a"])
    _register_stems(AuthHook(), ["specific_excluded", "locked_excluded_b", "authored_b"])


def _locked_hook():
    _register_stems(LockedHook(), ["locked_hook_excluded", "authored_a"])
    _register_stems(QuietHook(), ["conf_excluded", "authored_b"])


def _authored_over_dropped():
    _register_stems(AuthDroppedHook(), ["authored_a", "dropped_kept"])


def _locked_dropped():
    _register_stems(AuthLockedDroppedHook(), ["dropped"])
    _register_stems(QuietHook(), ["conf_excluded", "authored_a", "keep"])


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
    t_dag_then_task = PythonOperator(task_id="t_dag_then_task", python_callable=_dag_then_task)
    # Dag-level flags only reach tasks that already exist, so every task defined below is left out of this call.
    extend_global_openlineage_emission_policy(dag, exclude_datasets=[".*/dag_authored_.*"])
    extend_global_openlineage_emission_policy(t_dag_then_task, exclude_datasets=[".*/task_authored_.*"])

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
            inlets=_inlets(["authored_a", "conf_excluded"]),
            outlets=_outlets(["authored_a", "conf_excluded"]),
        ),
        exclude_datasets=[".*/authored_.*"],
    )
    t_hook_lineage_off = extend_global_openlineage_emission_policy(
        PythonOperator(
            task_id="t_hook_lineage_off",
            python_callable=_hook_lineage_off,
            inlets=_inlets(["conf_excluded", "keep"]),
            outlets=_outlets(["conf_excluded", "keep"]),
        ),
        hook_lineage=False,
    )
    t_fullmatch = extend_global_openlineage_emission_policy(
        PythonOperator(task_id="t_fullmatch", python_callable=_fullmatch),
        exclude_datasets=[
            r"authored_partial_in\.txt",
            r"authored_partial_out\.txt",
            ".*/authored_full_.*",
        ],
    )
    t_identity = extend_global_openlineage_emission_policy(
        PythonOperator(task_id="t_identity", python_callable=_identity),
        exclude_datasets=["s3://ol-exclude-bucket/staging/part_.*", "file://host1//slash_.*"],
    )
    t_locked = extend_global_openlineage_emission_policy(
        PythonOperator(task_id="t_locked", python_callable=_locked),
        exclude_datasets=[".*/authored_.*"],
    )
    t_locked_hook = extend_global_openlineage_emission_policy(
        PythonOperator(task_id="t_locked_hook", python_callable=_locked_hook),
        exclude_datasets=[".*/authored_.*"],
    )
    t_authored_over_dropped = extend_global_openlineage_emission_policy(
        PythonOperator(task_id="t_authored_over_dropped", python_callable=_authored_over_dropped),
        exclude_datasets=[".*/authored_.*"],
    )
    t_locked_dropped = extend_global_openlineage_emission_policy(
        PythonOperator(task_id="t_locked_dropped", python_callable=_locked_dropped),
        exclude_datasets=[".*/authored_.*"],
    )
    check_events = OpenLineageTestOperator(
        task_id="check_events", file_path=get_expected_event_file_path(DAG_ID)
    )
    (
        t_authored_dag
        >> t_dag_then_task
        >> t_after_dag_call
        >> t_authored_task
        >> t_authored_clear
        >> t_authored_inlets
        >> t_hook_lineage_off
        >> t_fullmatch
        >> t_identity
        >> t_locked
        >> t_locked_hook
        >> t_authored_over_dropped
        >> t_locked_dropped
        >> check_events
    )


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: contributing-docs/testing/system_tests.rst)
test_run = get_test_run(dag)
