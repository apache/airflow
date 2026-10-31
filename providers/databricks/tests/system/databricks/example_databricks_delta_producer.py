#
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
Example Dags that turn Delta table commits made outside Airflow into asset events.

The producer Dag polls a Unity Catalog table every 10 minutes and publishes an asset event when the
table has a newer Delta version. The consumer Dag is scheduled on that asset and reads the exact
version that the event reports.
"""

from __future__ import annotations

import os
from datetime import datetime

from airflow import DAG
from airflow.providers.databricks.assets.databricks import UnityTableIdentity
from airflow.providers.databricks.operators.databricks_delta_table import (
    DatabricksDeltaTableVersionProducerOperator,
)
from airflow.providers.databricks.operators.databricks_sql import DatabricksSqlOperator

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "example_databricks_delta_producer"
DATABRICKS_HOST = os.environ.get("DATABRICKS_HOST", "my-workspace.cloud.databricks.com")
SQL_WAREHOUSE_NAME = os.environ.get("DATABRICKS_SQL_WAREHOUSE_NAME", "My Warehouse")

# [START howto_operator_databricks_delta_table_version_producer]
orders = UnityTableIdentity(host=DATABRICKS_HOST, catalog="main", schema="sales", table="orders")

with DAG(
    dag_id=DAG_ID,
    schedule="*/10 * * * *",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["example"],
) as dag:
    DatabricksDeltaTableVersionProducerOperator(
        task_id="publish_orders_version",
        unity_table=orders,
        sql_warehouse_name=SQL_WAREHOUSE_NAME,
    )

with DAG(
    dag_id=f"{DAG_ID}_consumer",
    schedule=[orders.to_asset()],
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example"],
) as consumer_dag:
    DatabricksSqlOperator(
        task_id="read_published_version",
        sql_endpoint_name=SQL_WAREHOUSE_NAME,
        sql=(
            "SELECT count(*) FROM main.sales.orders VERSION AS OF "
            "{{ (triggering_asset_events.values() | first | last).extra['version'] }}"
        ),
    )
# [END howto_operator_databricks_delta_table_version_producer]


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: contributing-docs/testing/system_tests.rst)
test_run = get_test_run(dag)
