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
Example Airflow DAG for DataprocSubmitJobOperator with spark job
in deferrable mode.
"""

from __future__ import annotations

import os
from datetime import datetime

from google.api_core.retry import Retry

from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateBatchOperator,
    DataprocDeleteBatchOperator,
    DataprocGetBatchOperator,
)
from airflow.utils.trigger_rule import TriggerRule

from providers.tests.system.google import DEFAULT_GCP_SYSTEM_TEST_PROJECT_ID

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID", "default")
DAG_ID = "dataproc_batch_deferrable"
PROJECT_ID = (
    os.environ.get("SYSTEM_TESTS_GCP_PROJECT") or DEFAULT_GCP_SYSTEM_TEST_PROJECT_ID
)
REGION = "europe-north1"
BATCH_ID = f"batch-{ENV_ID}-{DAG_ID}".replace("_", "-")
BATCH_CONFIG = {
    "spark_batch": {
        "jar_file_uris": ["file:///usr/lib/spark/examples/jars/spark-examples.jar"],
        "main_class": "org.apache.spark.examples.SparkPi",
    },
}


with DAG(
    DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example", "dataproc", "batch", "deferrable"],
) as dag:
    # [START how_to_cloud_dataproc_create_batch_operator_async]
    create_batch = DataprocCreateBatchOperator(
        task_id="create_batch",
        project_id=PROJECT_ID,
        region=REGION,
        batch=BATCH_CONFIG,
        batch_id=BATCH_ID,
        deferrable=True,
        result_retry=Retry(maximum=100.0, initial=10.0, multiplier=1.0),
        num_retries_if_resource_is_not_ready=3,
    )
    # [END how_to_cloud_dataproc_create_batch_operator_async]

    get_batch = DataprocGetBatchOperator(
        task_id="get_batch", project_id=PROJECT_ID, region=REGION, batch_id=BATCH_ID
    )

    delete_batch = DataprocDeleteBatchOperator(
        task_id="delete_batch",
        project_id=PROJECT_ID,
        region=REGION,
        batch_id=BATCH_ID,
    )
    delete_batch.trigger_rule = TriggerRule.ALL_DONE

    (
        # TEST SETUP
        create_batch
        # TEST BODY
        >> get_batch
        # TEST TEARDOWN
        >> delete_batch
    )

    from tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "teardown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
