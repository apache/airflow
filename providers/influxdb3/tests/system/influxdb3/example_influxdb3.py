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
Example DAG demonstrating InfluxDB 3.x integration.

This example shows how to:
1. Write data points to InfluxDB 3.x
2. Query data using SQL
"""
from __future__ import annotations

import os
from datetime import datetime

try:
    from airflow.sdk import task
except ImportError:
    # Airflow 2 path
    from airflow.decorators import task  # type: ignore[attr-defined,no-redef]
from airflow.models.dag import DAG
from airflow.providers.influxdb3.hooks.influxdb3 import InfluxDB3Hook
from airflow.providers.influxdb3.operators.influxdb3 import InfluxDB3Operator


@task(task_id="write_data")
def write_to_influxdb3():
    """Write sample data to InfluxDB 3.x."""
    hook = InfluxDB3Hook()
    hook.write(
        measurement="temperature",
        tags={"location": "Prague", "sensor": "A1"},
        fields={"value": 25.3, "unit": "celsius"},
    )
    print("Data written successfully")


# [START howto_operator_influxdb3]
query_task = InfluxDB3Operator(
    task_id="query_data",
    sql='SELECT * FROM "temperature" WHERE time > now() - INTERVAL \'1 hour\'',
    influxdb3_conn_id="influxdb3_default",
)
# [END howto_operator_influxdb3]

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "influxdb3_example_dag"

with DAG(
    dag_id=DAG_ID,
    schedule=None,
    start_date=datetime(2021, 1, 1),
    max_active_runs=1,
    tags=["example", "influxdb3"],
) as dag:
    write_task = write_to_influxdb3()
    write_task >> query_task

    from tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
