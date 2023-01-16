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
Example Airflow DAG for Dataproc batch operators.
"""
from __future__ import annotations

import os
from datetime import datetime

from google.api_core.retry import Retry

from airflow import models
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCancelOperationOperator,
    DataprocCreateBatchOperator,
    DataprocDeleteBatchOperator,
    DataprocGetBatchOperator,
    DataprocListBatchesOperator,
)
from airflow.providers.google.cloud.sensors.dataproc import DataprocBatchSensor
from airflow.utils.trigger_rule import TriggerRule

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "dataproc_batch"
PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT", "")
REGION = "europe-west1"

BATCH_ID = f"test-batch-id-{ENV_ID}"
BATCH_ID_2 = f"test-batch-id-{ENV_ID}-2"
BATCH_ID_3 = f"test-batch-id-{ENV_ID}-3"
BATCH_ID_4 = f"test-batch-id-{ENV_ID}-4"

BATCH_CONFIG = {
    "spark_batch": {
        "jar_file_uris": ["file:///usr/lib/spark/examples/jars/spark-examples.jar"],
        "main_class": "org.apache.spark.examples.SparkPi",
    },
}


with models.DAG(
    DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example", "dataproc"],
) as dag:
    # [START how_to_cloud_dataproc_create_batch_operator]
    create_batch = DataprocCreateBatchOperator(
        task_id="create_batch",
        project_id=PROJECT_ID,
        region=REGION,
        batch=BATCH_CONFIG,
        batch_id=BATCH_ID,
    )

    create_batch_2 = DataprocCreateBatchOperator(
        task_id="create_batch_2",
        project_id=PROJECT_ID,
        region=REGION,
        batch=BATCH_CONFIG,
        batch_id=BATCH_ID_2,
        result_retry=Retry(maximum=10.0, initial=10.0, multiplier=1.0),
    )

    create_batch_3 = DataprocCreateBatchOperator(
        task_id="create_batch_3",
        project_id=PROJECT_ID,
        region=REGION,
        batch=BATCH_CONFIG,
        batch_id=BATCH_ID_3,
        asynchronous=True,
    )
    # [END how_to_cloud_dataproc_create_batch_operator]

    # [START how_to_cloud_dataproc_batch_async_sensor]
    batch_async_sensor = DataprocBatchSensor(
        task_id="batch_async_sensor",
        region=REGION,
        project_id=PROJECT_ID,
        batch_id=BATCH_ID_3,
        poke_interval=10,
    )
    # [END how_to_cloud_dataproc_batch_async_sensor]

    # [START how_to_cloud_dataproc_get_batch_operator]
    get_batch = DataprocGetBatchOperator(
        task_id="get_batch", project_id=PROJECT_ID, region=REGION, batch_id=BATCH_ID
    )

    get_batch_2 = DataprocGetBatchOperator(
        task_id="get_batch_2", project_id=PROJECT_ID, region=REGION, batch_id=BATCH_ID_2
    )
    # [END how_to_cloud_dataproc_get_batch_operator]

    # [START how_to_cloud_dataproc_list_batches_operator]
    list_batches = DataprocListBatchesOperator(
        task_id="list_batches",
        project_id=PROJECT_ID,
        region=REGION,
    )
    # [END how_to_cloud_dataproc_list_batches_operator]

    create_batch_4 = DataprocCreateBatchOperator(
        task_id="create_batch_4",
        project_id=PROJECT_ID,
        region=REGION,
        batch=BATCH_CONFIG,
        batch_id=BATCH_ID_4,
        asynchronous=True,
    )

    # [START how_to_cloud_dataproc_cancel_operation_operator]
    cancel_operation = DataprocCancelOperationOperator(
        task_id="cancel_operation",
        project_id=PROJECT_ID,
        region=REGION,
        operation_name="{{ task_instance.xcom_pull('create_batch') }}",
    )
    # [END how_to_cloud_dataproc_cancel_operation_operator]

    # [START how_to_cloud_dataproc_delete_batch_operator]
    delete_batch = DataprocDeleteBatchOperator(
        task_id="delete_batch", project_id=PROJECT_ID, region=REGION, batch_id=BATCH_ID
    )
    delete_batch_2 = DataprocDeleteBatchOperator(
        task_id="delete_batch_2", project_id=PROJECT_ID, region=REGION, batch_id=BATCH_ID_2
    )
    delete_batch_3 = DataprocDeleteBatchOperator(
        task_id="delete_batch_3", project_id=PROJECT_ID, region=REGION, batch_id=BATCH_ID_3
    )
    delete_batch_4 = DataprocDeleteBatchOperator(
        task_id="delete_batch_4", project_id=PROJECT_ID, region=REGION, batch_id=BATCH_ID_4
    )
    # [END how_to_cloud_dataproc_delete_batch_operator]
    delete_batch.trigger_rule = TriggerRule.ALL_DONE
    delete_batch_2.trigger_rule = TriggerRule.ALL_DONE
    delete_batch_3.trigger_rule = TriggerRule.ALL_DONE
    delete_batch_4.trigger_rule = TriggerRule.ALL_DONE

    (
        create_batch
        >> create_batch_2
        >> create_batch_3
        >> batch_async_sensor
        >> get_batch
        >> get_batch_2
        >> list_batches
        >> create_batch_4
        >> cancel_operation
        >> delete_batch
        >> delete_batch_2
        >> delete_batch_3
        >> delete_batch_4
    )

    from tests.system.utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "teardown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()


from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
