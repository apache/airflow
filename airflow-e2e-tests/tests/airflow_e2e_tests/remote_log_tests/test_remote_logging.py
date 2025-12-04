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

import time
from datetime import datetime, timezone

import boto3
import pytest

from airflow_e2e_tests.e2e_test_utils.clients import AirflowClient


class TestRemoteLogging:
    airflow_client = AirflowClient()
    dag_id = "example_xcom_test"
    task_count = 6
    retry_interval_in_seconds = 1
    max_retries = 5

    def test_dag_unpause(self):
        self.airflow_client.un_pause_dag(
            TestRemoteLogging.dag_id,
        )

    def test_remote_logging_s3(self):
        """Test that a DAG using remote logging to S3 completes successfully."""

        self.airflow_client.un_pause_dag(TestRemoteLogging.dag_id)

        resp = self.airflow_client.trigger_dag(
            TestRemoteLogging.dag_id, json={"logical_date": datetime.now(timezone.utc).isoformat()}
        )
        state = self.airflow_client.wait_for_dag_run(
            dag_id=TestRemoteLogging.dag_id,
            run_id=resp["dag_run_id"],
        )

        assert state == "success", (
            f"DAG {TestRemoteLogging.dag_id} did not complete successfully. Final state: {state}"
        )

        # This bucket will be created part of the docker-compose setup in
        bucket_name = "test-airflow-logs"
        s3_client = boto3.client(
            "s3",
            endpoint_url="http://localhost:4566",
            aws_access_key_id="test",
            aws_secret_access_key="test",
            region_name="us-east-1",
        )

        # Wait for logs to be available in S3 before we call `get_task_logs`
        for _ in range(self.max_retries):
            response = s3_client.list_objects_v2(Bucket=bucket_name)
            contents = response.get("Contents", [])
            if len(contents) >= self.task_count:
                break

            print(f"Expected at least {self.task_count} log files, found {len(contents)}. Retrying...")
            time.sleep(self.retry_interval_in_seconds)

        if len(contents) < self.task_count:
            pytest.fail(
                f"Expected at least {self.task_count} log files in S3 bucket {bucket_name}, "
                f"but found {len(contents)} objects: {[obj.get('Key') for obj in contents]}. \n"
                f"List Objects Response: {response}"
            )

        task_logs = self.airflow_client.get_task_logs(
            dag_id=TestRemoteLogging.dag_id,
            task_id="bash_pull",
            run_id=resp["dag_run_id"],
        )

        task_log_sources = [
            source for content in task_logs.get("content", [{}]) for source in content.get("sources", [])
        ]
        response = s3_client.list_objects_v2(Bucket=bucket_name)

        if "Contents" not in response:
            pytest.fail("No objects found in S3 bucket %s", bucket_name)

        # s3 key format: dag_id=example_xcom/run_id=manual__2025-09-29T23:32:09.457215+00:00/task_id=bash_pull/attempt=1.log

        log_files = [f"s3://{bucket_name}/{obj['Key']}" for obj in response["Contents"]]
        assert any(source in log_files for source in task_log_sources), (
            f"None of the log sources {task_log_sources} were found in S3 bucket logs {log_files}"
        )
