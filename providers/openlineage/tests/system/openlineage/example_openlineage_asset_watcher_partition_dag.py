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
Asset event for one partition of an asset, produced by an asset watcher in the triggerer.

Like ``example_openlineage_asset_watcher_dag``, but the trigger of the watcher reports a partition key, which the
asset watcher events carry in their ``airflowAssetEvent`` facet. The child is not partitioned, so the partitioned
asset event does not start it; it only exists so the triggerer runs the watcher. It ends in ``__notrigger`` so the
harness does not trigger it.

It needs a triggerer, and watchers can only report a partition on the Airflow versions with ``AssetTriggerEvent``.
"""

from __future__ import annotations

import os
from datetime import datetime

from airflow import DAG
from airflow.providers.common.compat.sdk import Variable, conf
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.sensors.python import PythonSensor
from airflow.sdk import Asset, AssetWatcher

from system.openlineage.asset_watcher_triggers import PartitionedFileDeleteTrigger
from system.openlineage.constants import DEFAULT_DAGRUN_TIMEOUT
from system.openlineage.expected_events import get_expected_event_file_path
from system.openlineage.operator import OpenLineageTestOperator

DAG_ID = "openlineage_asset_watcher_partition_dag"
ASSET_NAME = "openlineage_asset_watcher_partition_asset"
# The worker creates the file and the triggerer watches it, so it must be in a folder both of them see.
TRIGGER_FILE_PATH = os.path.join(conf.get("core", "dags_folder"), f"{DAG_ID}.trigger")
WATCHER_EVENT_VARIABLE_KEY = f"asset_watcher.{ASSET_NAME}.event.complete"

partitioned_asset = Asset(
    name=ASSET_NAME,
    uri="file://openlineage-asset-watcher/partitioned.csv",
    watchers=[
        AssetWatcher(
            name="openlineage_asset_watcher_partition",
            trigger=PartitionedFileDeleteTrigger(filepath=TRIGGER_FILE_PATH, partition_key="2026-11-04"),
        )
    ],
)

with DAG(
    dagrun_timeout=DEFAULT_DAGRUN_TIMEOUT,
    dag_id=f"{DAG_ID}_child__notrigger",
    start_date=datetime(2021, 1, 1),
    schedule=[partitioned_asset],
    catchup=False,
    default_args={"retries": 0},
):
    EmptyOperator(task_id="consume_asset_event")


def _create_trigger_file():
    with open(TRIGGER_FILE_PATH, "w") as trigger_file:
        trigger_file.write("fire")


def _is_asset_watcher_event_emitted():
    return Variable.get(WATCHER_EVENT_VARIABLE_KEY, default=None) is not None


with DAG(
    dagrun_timeout=DEFAULT_DAGRUN_TIMEOUT,
    dag_id=DAG_ID,
    start_date=datetime(2021, 1, 1),
    schedule=None,
    catchup=False,
    default_args={"retries": 0},
) as dag:
    create_trigger_file = PythonOperator(task_id="create_trigger_file", python_callable=_create_trigger_file)

    wait_for_asset_watcher_events = PythonSensor(
        task_id="wait_for_asset_watcher_events",
        python_callable=_is_asset_watcher_event_emitted,
        poke_interval=2,
        timeout=120,
        mode="poke",
    )

    check_events = OpenLineageTestOperator(
        task_id="check_events", file_path=get_expected_event_file_path(DAG_ID)
    )

    create_trigger_file >> wait_for_asset_watcher_events >> check_events


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: contributing-docs/testing/system_tests.rst)
test_run = get_test_run(dag)
