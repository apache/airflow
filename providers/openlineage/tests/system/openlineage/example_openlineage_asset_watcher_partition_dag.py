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
asset watcher events carry in their ``airflowAssetEvent`` facet. The child is scheduled on the partitions of the
asset, so the asset event starts a run of it for that partition, whose events carry the partition key too.
``wait_for_child_dag_run`` waits for the COMPLETE of that run, emitted once it finishes, so ``check_events`` sees
every event of it however long the scheduler takes to start it. The child ends in ``__notrigger`` so the harness
does not trigger it.

It needs a triggerer, and watchers can only report a partition on the Airflow versions with ``AssetTriggerEvent``.
"""

from __future__ import annotations

import os
from datetime import datetime

from airflow import DAG
from airflow.providers.common.compat.sdk import Variable, conf
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.sensors.python import PythonSensor
from airflow.sdk import Asset, AssetWatcher, PartitionedAssetTimetable

from system.openlineage.asset_watcher_triggers import PartitionedFileDeleteTrigger
from system.openlineage.constants import DEFAULT_DAGRUN_TIMEOUT
from system.openlineage.expected_events import get_expected_event_file_path
from system.openlineage.operator import OpenLineageTestOperator

DAG_ID = "openlineage_asset_watcher_partition_dag"
CHILD_DAG_ID = f"{DAG_ID}_child__notrigger"
ASSET_NAME = "openlineage_asset_watcher_partition_asset"
# The worker creates the file and the triggerer watches it, so it must be in a folder both of them see.
TRIGGER_FILE_PATH = os.path.join(conf.get("core", "dags_folder"), f"{DAG_ID}.trigger")

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


def _do_nothing():
    # Not an EmptyOperator, which the scheduler marks as successful without running it, so it emits no events
    pass


with DAG(
    dagrun_timeout=DEFAULT_DAGRUN_TIMEOUT,
    dag_id=CHILD_DAG_ID,
    start_date=datetime(2021, 1, 1),
    schedule=PartitionedAssetTimetable(assets=partitioned_asset),
    catchup=False,
    default_args={"retries": 0},
):
    PythonOperator(task_id="consume_asset_event", python_callable=_do_nothing)


def _create_trigger_file():
    with open(TRIGGER_FILE_PATH, "w") as trigger_file:
        trigger_file.write("fire")


def _is_child_dag_run_complete():
    return Variable.get(f"{CHILD_DAG_ID}.event.complete", default=None) is not None


with DAG(
    dagrun_timeout=DEFAULT_DAGRUN_TIMEOUT,
    dag_id=DAG_ID,
    start_date=datetime(2021, 1, 1),
    schedule=None,
    catchup=False,
    default_args={"retries": 0},
) as dag:
    create_trigger_file = PythonOperator(task_id="create_trigger_file", python_callable=_create_trigger_file)

    wait_for_child_dag_run = PythonSensor(
        task_id="wait_for_child_dag_run",
        python_callable=_is_child_dag_run_complete,
        poke_interval=2,
        timeout=180,
        mode="poke",
    )

    check_events = OpenLineageTestOperator(
        task_id="check_events", file_path=get_expected_event_file_path(DAG_ID)
    )

    create_trigger_file >> wait_for_child_dag_run >> check_events


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: contributing-docs/testing/system_tests.rst)
test_run = get_test_run(dag)
