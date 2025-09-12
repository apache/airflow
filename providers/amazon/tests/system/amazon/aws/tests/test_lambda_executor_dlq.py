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

import os
import time
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse

import boto3

try:
    from airflow.sdk import TriggerRule
except ImportError:
    # Compatibility for Airflow < 3.1
    from airflow.utils.trigger_rule import TriggerRule  # type: ignore[no-redef,attr-defined]

from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS

if AIRFLOW_V_3_0_PLUS:
    from airflow.sdk import DAG, chain, task
else:
    from airflow.decorators import task  # type: ignore[attr-defined,no-redef]
    from airflow.models.baseoperator import chain  # type: ignore[attr-defined,no-redef]
    from airflow.models.dag import DAG  # type: ignore[attr-defined,no-redef,assignment]

from system.amazon.aws.utils import SystemTestContextBuilder

sys_test_context_task = SystemTestContextBuilder().build()

DAG_ID = "test_lambda_executor_dlq"


@task
def verify_test_setup():
    """Verify Lambda executor DLQ configuration is available"""
    dlq_url = os.environ.get("AIRFLOW__AWS_LAMBDA_EXECUTOR__DEAD_LETTER_QUEUE_URL")
    print(f"Lambda executor DLQ URL: {dlq_url}")

    parsed_url = urlparse(dlq_url)
    dlq_queue_name = parsed_url.path.split("/")[-1]
    return dlq_queue_name


# Configuration to search for this poison pill has been done within the Lambda app.py handler code
@task(executor_config={"poison_pill": True})
def cause_lambda_failure():
    return "This task is designed to fail at Lambda service level, injected within the function handler"


@task(trigger_rule=TriggerRule.ALL_DONE)
def verify_dlq_activity(dlq_queue_name: str):
    """Verify DLQ processing occurred by checking CloudWatch metrics"""

    cloudwatch = boto3.client("cloudwatch")
    # Try for up to 10 attempts (5 minutes total)
    for attempt in range(10):
        end_time = datetime.now(timezone.utc)
        start_time = end_time - timedelta(minutes=5)
        received_response = cloudwatch.get_metric_statistics(
            Namespace="AWS/SQS",
            MetricName="NumberOfMessagesDeleted",
            Dimensions=[{"Name": "QueueName", "Value": dlq_queue_name}],
            StartTime=start_time,
            EndTime=end_time,
            Period=300,
            Statistics=["Sum"],
        )

        total_deleted = sum(point["Sum"] for point in received_response["Datapoints"])
        if total_deleted > 0:
            print(f"Messages deleted from DLQ: {total_deleted}")
            return "DLQ Processing Confirmed"
        print("No messages detected in DLQ yet")
        if attempt < 9:
            time.sleep(30)
    raise AssertionError("FAIL: No DLQ activity detected after 5 minutes of polling")


with DAG(
    dag_id=DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    tags=["test"],
    catchup=False,
) as dag:
    test_context = sys_test_context_task()
    dlq_name = verify_test_setup()
    failed_task = cause_lambda_failure()
    verify_task = verify_dlq_activity(dlq_name)

    chain(
        test_context,
        dlq_name,
        failed_task,
        verify_task,
    )
    from tests_common.test_utils.watcher import watcher

    verify_task >> watcher()

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

test_run = get_test_run(dag)
