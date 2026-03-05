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

import pytest

from airflow_e2e_tests.e2e_test_utils.clients import AirflowClient, get_s3_client

XCOM_BUCKET = "test-xcom-objectstorage-backend"


class TestXComObjectStorageBackend:
    airflow_client = AirflowClient()
    dag_id = "example_xcom_test"
    retry_interval_in_seconds = 5
    max_retries = 12

    def test_dag_succeeds_and_xcom_values_stored_in_s3(self):
        """Test that a DAG using XComObjectStorageBackend completes successfully and persists XCom values to S3."""
        self.airflow_client.un_pause_dag(self.dag_id)

        resp = self.airflow_client.trigger_dag(
            self.dag_id, json={"logical_date": datetime.now(timezone.utc).isoformat()}
        )
        state = self.airflow_client.wait_for_dag_run(
            dag_id=self.dag_id,
            run_id=resp["dag_run_id"],
        )

        assert state == "success", f"DAG {self.dag_id} did not complete successfully. Final state: {state}"

        s3_client = get_s3_client()

        contents = []
        for _ in range(self.max_retries):
            response = s3_client.list_objects_v2(Bucket=XCOM_BUCKET)
            contents = response.get("Contents", [])
            if contents:
                break

            print(f"No XCom objects found in S3 bucket {XCOM_BUCKET!r} yet. Retrying...")
            time.sleep(self.retry_interval_in_seconds)

        if not contents:
            pytest.fail(
                f"Expected XCom objects in S3 bucket {XCOM_BUCKET!r}, but bucket is empty.\n"
                f"List Objects Response: {response}"
            )

        keys = [obj["Key"] for obj in contents]
        print(f"Found {len(keys)} XCom object(s) in S3: {keys}")
