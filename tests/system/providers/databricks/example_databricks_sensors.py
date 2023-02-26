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
This is an example DAG which uses the DatabricksSqlSensor,
DatabricksPartitionSensor and DatabricksTableChangesSensor.
The first task checks for a generic SQL statement against a Delta table,
and if a result is returned, the task succeeds, else it times out.
The second task checks for the specified partitions' presence in a
Delta table. If it exists, the task succeeds, else it times out.
The third task checks for data related changes in a Delta
table via versions. If the version retrieved from hive metastore
is different from the version stored in Airflow metadata, the DAG
succeeds, else it times out.
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.databricks.sensors.databricks_partition import DatabricksPartitionSensor
from airflow.providers.databricks.sensors.databricks_sql import DatabricksSqlSensor
from airflow.providers.databricks.sensors.databricks_table_changes import DatabricksTableChangesSensor

# [docs]
ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
# [docs]
DAG_ID = "example_databricks_sensor"

with DAG(
    dag_id=DAG_ID,
    schedule="@daily",
    start_date=datetime(2021, 1, 1),
    tags=["example"],
    catchup=False,
) as dag:
    # [docs]
    connection_id = "databricks_default"
    sql_endpoint_name = "Starter Warehouse"

    # [START howto_sensor_databricks_sql]
    # Example of using the Databricks SQL Sensor to detect generic data presence for Delta tables.
    sql_sensor = DatabricksSqlSensor(
        databricks_conn_id=connection_id,
        sql_endpoint_name=sql_endpoint_name,
        catalog="hive_metastore",
        task_id="sql_sensor_task",
        sql="select * from hive_metastore.temp.sample_table_3 limit 1",
        timeout=60 * 2,
    )
    # [END howto_sensor_databricks_sql]

    # [START howto_sensor_databricks_partition]
    # Example of using the Databricks Partition Sensor to detect presence of
    # specified partitions in a Delta table.
    partition_sensor = DatabricksPartitionSensor(
        databricks_conn_id=connection_id,
        sql_endpoint_name=sql_endpoint_name,
        task_id="partition_sensor_task",
        table_name="sample_table_2",
        schema="temp",
        catalog="hive_metastore",
        partitions={"date": "2023-02-03", "name": ["abc", "def"]},
        partition_operator="=",
        timeout=60 * 2,
    )
    # [END howto_sensor_databricks_partition]

    # [START howto_sensor_databricks_table_changes]
    # Example of using the Databricks Table Changes Sensor to detect new versions of a Delta table
    # based on data changes.
    table_changes_sensor = DatabricksTableChangesSensor(
        databricks_conn_id=connection_id,
        sql_endpoint_name=sql_endpoint_name,
        task_id="check_changes_sample_table_3",
        table_name="sample_table_3",
        schema="temp",
        catalog="hive_metastore",
        timestamp=datetime.now() - timedelta(days=7),
        timeout=60 * 2,
    )
    # [END howto_sensor_databricks_table_changes]

    (sql_sensor >> partition_sensor >> table_changes_sensor)

    from tests.system.utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()


from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
