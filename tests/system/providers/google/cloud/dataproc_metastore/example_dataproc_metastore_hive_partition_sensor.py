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
Example Airflow DAG that show how to check Hive partitions existence
using Dataproc Metastore Sensor.

Note that Metastore service must be configured to use gRPC endpoints,
"""
from __future__ import annotations

import datetime
import os

from airflow import models
from airflow.providers.google.cloud.sensors.dataproc_metastore import MetastoreHivePartitionSensor

DAG_ID = "dataproc_metastore_hive_partition_sensor"
PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT", "")
ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")

SERVICE_ID = f"{DAG_ID}-service-{ENV_ID}".replace("_", "-")
REGION = "europe-west1"
TABLE_NAME = "test_table"
PARTITION_1 = "column1=value1"
PARTITION_2 = "column2=value2/column3=value3"


with models.DAG(
    DAG_ID,
    start_date=datetime.datetime(2021, 1, 1),
    schedule="@once",
    catchup=False,
    tags=["example", "dataproc", "metastore"],
) as dag:

    # [START how_to_cloud_dataproc_metastore_hive_partition_sensor]
    sensor = MetastoreHivePartitionSensor(
        task_id="hive_partition_sensor",
        service_id=SERVICE_ID,
        region=REGION,
        table=TABLE_NAME,
        partitions=[PARTITION_1, PARTITION_2],
    )
    # [END how_to_cloud_dataproc_metastore_hive_partition_sensor]

    from tests.system.utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "teardown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()


from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
