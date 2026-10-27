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
DAG exercising the OpenLineage ``exclude_datasets`` control on lineage that depends on the Airflow version.

- ``t_runtime_outlets``: outlets added at runtime through an ``AssetAlias``, next to declared ones. Airflow reports
  runtime outlets only since 3.1.
- ``t_collector_cap``: exclusion happens after the hook lineage collector cap, so it does not make room for the
  assets the cap dropped (requires ``AIRFLOW__LINEAGE__MAX_ASSETS_PER_COLLECTOR=67``). The cap exists only since
  Airflow 3.2.

Required global conf::

    [
        {
            "scope": {"dag_id": "openlineage_policy_exclude_datasets_versioned_dag"},
            "controls": {"exclude_datasets": [".*/dag_excluded_.*"]},
        }
    ]
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime

from airflow import DAG
from airflow.providers.common.compat.assets import Asset, AssetAlias
from airflow.providers.common.compat.lineage.hook import get_hook_lineage_collector
from airflow.providers.standard.operators.python import PythonOperator

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


DAG_ID = "openlineage_policy_exclude_datasets_versioned_dag"

with DAG(
    dagrun_timeout=DEFAULT_DAGRUN_TIMEOUT,
    dag_id=DAG_ID,
    start_date=datetime(2021, 1, 1),
    schedule=None,
    catchup=False,
    default_args={"retries": 0},
) as dag:
    t_runtime_outlets = PythonOperator(
        task_id="t_runtime_outlets",
        python_callable=_runtime_outlets,
        inlets=_inlets(["dag_excluded_declared", "keep_declared"]),
        outlets=[*_outlets(["dag_excluded_declared", "keep_declared"]), ALIAS],
    )
    t_collector_cap = PythonOperator(task_id="t_collector_cap", python_callable=_collector_cap)
    check_events = OpenLineageTestOperator(
        task_id="check_events", file_path=get_expected_event_file_path(DAG_ID)
    )
    t_runtime_outlets >> t_collector_cap >> check_events


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: contributing-docs/testing/system_tests.rst)
test_run = get_test_run(dag)
