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
Asset events produced by asset watchers in the triggerer, outside of any task.

The asset watchers of the assets scheduling the child Dag fire when ``create_trigger_files`` creates their files,
and the triggerer then emits the START and COMPLETE events of one asset watcher job per asset:

- the orders asset has two watchers, each firing on its own file: both report to the same job, as two runs;
- the watcher of the shared trigger asset has the same trigger as the first watcher of the orders asset, so the
  triggerer runs that trigger once and one file produces an asset event, and a run, for each of the two assets.

The excluded asset's watcher fires too, but its dataset matches the global ``exclude_datasets`` rule of the e2e
deployment (``.*/exclude_datasets_global_.*``, in ``airflow-e2e-tests/docker/openlineage.yml``), so it emits no events.

Asset events of the task asset are not produced by a watcher, so they emit no asset watcher events: one comes from
``emit_task_asset_event``, a task with the asset as outlet, and one from ``create_rest_asset_event``, through the REST
API with an extra claiming to come from a trigger.

``wait_for_asset_watcher_events`` waits for the asset watcher events, and for the excluded asset's watcher to
consume its file, before ``check_events`` reads them. The child
ends in ``__notrigger`` so the harness does not trigger it; it only exists so the triggerer runs the watchers, and
its runs started by the asset events are not checked.

It needs a triggerer, and asset watcher events are only emitted on Airflow 3.2+.
"""

from __future__ import annotations

import os
from datetime import datetime

from airflow import DAG
from airflow.providers.common.compat.sdk import Variable, conf
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.sensors.python import PythonSensor
from airflow.providers.standard.triggers.file import FileDeleteTrigger
from airflow.sdk import Asset, AssetWatcher

from system.openlineage.constants import DEFAULT_DAGRUN_TIMEOUT
from system.openlineage.expected_events import get_expected_event_file_path
from system.openlineage.manual_state import create_asset_event
from system.openlineage.operator import OpenLineageTestOperator

DAG_ID = "openlineage_asset_watcher_dag"
ORDERS_ASSET_NAME = "openlineage_asset_watcher_asset"
SHARED_TRIGGER_ASSET_NAME = "openlineage_asset_watcher_shared_trigger_asset"
TASK_ASSET_NAME = "openlineage_asset_watcher_task_asset"
EXCLUDED_ASSET_NAME = "openlineage_asset_watcher_excluded_asset"
# The worker creates the files and the triggerer watches them, so they must be in a folder both of them see.
FIRST_TRIGGER_FILE_PATH = os.path.join(conf.get("core", "dags_folder"), f"{DAG_ID}.first.trigger")
SECOND_TRIGGER_FILE_PATH = os.path.join(conf.get("core", "dags_folder"), f"{DAG_ID}.second.trigger")
EXCLUDED_TRIGGER_FILE_PATH = os.path.join(conf.get("core", "dags_folder"), f"{DAG_ID}.excluded.trigger")
EXPECTED_COMPLETE_EVENT_COUNTS = {
    f"asset_watcher.{ORDERS_ASSET_NAME}.event.complete": 2,
    f"asset_watcher.{SHARED_TRIGGER_ASSET_NAME}.event.complete": 1,
}

orders_asset = Asset(
    name=ORDERS_ASSET_NAME,
    uri="file://openlineage-asset-watcher/orders.csv",
    extra={"owner": "openlineage"},
    watchers=[
        AssetWatcher(
            name="openlineage_asset_watcher",
            trigger=FileDeleteTrigger(filepath=FIRST_TRIGGER_FILE_PATH, poke_interval=1),
        ),
        AssetWatcher(
            name="openlineage_asset_watcher_second",
            trigger=FileDeleteTrigger(filepath=SECOND_TRIGGER_FILE_PATH, poke_interval=1),
        ),
    ],
)
shared_trigger_asset = Asset(
    name=SHARED_TRIGGER_ASSET_NAME,
    uri="file://openlineage-asset-watcher/shared.csv",
    watchers=[
        AssetWatcher(
            name="openlineage_asset_watcher_shared_trigger",
            # Same class and kwargs as the first watcher of the orders asset, so both share one trigger
            trigger=FileDeleteTrigger(filepath=FIRST_TRIGGER_FILE_PATH, poke_interval=1),
        )
    ],
)
excluded_asset = Asset(
    name=EXCLUDED_ASSET_NAME,
    uri="file://openlineage-asset-watcher/exclude_datasets_global_orders.csv",
    watchers=[
        AssetWatcher(
            name="openlineage_asset_watcher_excluded",
            trigger=FileDeleteTrigger(filepath=EXCLUDED_TRIGGER_FILE_PATH, poke_interval=1),
        )
    ],
)
task_asset = Asset(name=TASK_ASSET_NAME, uri="file://openlineage-asset-watcher/task.csv")

with DAG(
    dagrun_timeout=DEFAULT_DAGRUN_TIMEOUT,
    dag_id=f"{DAG_ID}_child__notrigger",
    start_date=datetime(2021, 1, 1),
    schedule=orders_asset | shared_trigger_asset | excluded_asset,
    catchup=False,
    default_args={"retries": 0},
):
    EmptyOperator(task_id="consume_asset_event")


def _create_trigger_files():
    for path in (FIRST_TRIGGER_FILE_PATH, SECOND_TRIGGER_FILE_PATH, EXCLUDED_TRIGGER_FILE_PATH):
        with open(path, "w") as trigger_file:
            trigger_file.write("fire")


def _create_rest_asset_event():
    create_asset_event(TASK_ASSET_NAME, extra={"from_trigger": True})


def _are_asset_watcher_events_emitted():
    # The excluded asset's watcher deletes its file before its asset event is registered, which check_events
    # leaves time for, so it can't pass only because that watcher has not fired yet
    return not os.path.exists(EXCLUDED_TRIGGER_FILE_PATH) and all(
        len(Variable.get(key, default=[], deserialize_json=True)) >= count
        for key, count in EXPECTED_COMPLETE_EVENT_COUNTS.items()
    )


def _sort_by_watcher_name(event: dict) -> str:
    """Order the runs of the two watchers of the orders asset, which share a job."""
    asset_event = event["run"]["facets"].get("airflowAssetEvent", {}).get("assetEvent", {})
    return asset_event.get("watcherName", "")


with DAG(
    dagrun_timeout=DEFAULT_DAGRUN_TIMEOUT,
    dag_id=DAG_ID,
    start_date=datetime(2021, 1, 1),
    schedule=None,
    catchup=False,
    default_args={"retries": 0},
) as dag:
    create_trigger_files = PythonOperator(
        task_id="create_trigger_files", python_callable=_create_trigger_files
    )

    emit_task_asset_event = PythonOperator(
        task_id="emit_task_asset_event", python_callable=lambda: None, outlets=[task_asset]
    )

    create_rest_asset_event = PythonOperator(
        task_id="create_rest_asset_event", python_callable=_create_rest_asset_event
    )

    wait_for_asset_watcher_events = PythonSensor(
        task_id="wait_for_asset_watcher_events",
        python_callable=_are_asset_watcher_events_emitted,
        poke_interval=2,
        timeout=120,
        mode="poke",
    )

    check_events = OpenLineageTestOperator(
        task_id="check_events",
        file_path=get_expected_event_file_path(DAG_ID),
        event_sort_fn=_sort_by_watcher_name,
    )

    (
        [create_trigger_files, emit_task_asset_event, create_rest_asset_event]
        >> wait_for_asset_watcher_events
        >> check_events
    )


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: contributing-docs/testing/system_tests.rst)
test_run = get_test_run(dag)
