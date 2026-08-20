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
from __future__ import annotations

# [START howto_trigger_iceberg_table_snapshot]
from airflow.providers.apache.iceberg.triggers.iceberg import IcebergTableSnapshotTrigger
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import DAG, Asset, AssetWatcher

trigger = IcebergTableSnapshotTrigger(table="default.orders", poll_interval=30)

orders = Asset("iceberg_orders", watchers=[AssetWatcher(name="orders_commits", trigger=trigger)])

with DAG(dag_id="example_iceberg_table_snapshot_watcher", schedule=[orders]) as dag:
    EmptyOperator(task_id="process_new_orders")
# [END howto_trigger_iceberg_table_snapshot]


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: contributing-docs/testing/system_tests.rst)
test_run = get_test_run(dag)
