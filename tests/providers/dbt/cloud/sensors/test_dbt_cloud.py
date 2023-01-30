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

from unittest.mock import patch

import pytest

from airflow.models.connection import Connection
from airflow.providers.dbt.cloud.hooks.dbt import DbtCloudHook, DbtCloudJobRunException, DbtCloudJobRunStatus
from airflow.providers.dbt.cloud.sensors.dbt import DbtCloudJobRunSensor
from airflow.utils import db

ACCOUNT_ID = 11111
RUN_ID = 5555
TOKEN = "token"


class TestDbtCloudJobRunSensor:
    def setup_class(self):
        self.sensor = DbtCloudJobRunSensor(
            task_id="job_run_sensor",
            dbt_cloud_conn_id="dbt",
            run_id=RUN_ID,
            account_id=ACCOUNT_ID,
            timeout=30,
            poke_interval=15,
        )

        # Connection
        conn = Connection(conn_id="dbt", conn_type=DbtCloudHook.conn_type, login=ACCOUNT_ID, password=TOKEN)

        db.merge_conn(conn)

    def test_init(self):
        assert self.sensor.dbt_cloud_conn_id == "dbt"
        assert self.sensor.run_id == RUN_ID
        assert self.sensor.timeout == 30
        assert self.sensor.poke_interval == 15

    @pytest.mark.parametrize(
        argnames=("job_run_status", "expected_poke_result"),
        argvalues=[
            (1, False),  # QUEUED
            (2, False),  # STARTING
            (3, False),  # RUNNING
            (10, True),  # SUCCESS
            (20, "exception"),  # ERROR
            (30, "exception"),  # CANCELLED
        ],
    )
    @patch.object(DbtCloudHook, "get_job_run_status")
    def test_poke(self, mock_job_run_status, job_run_status, expected_poke_result):
        mock_job_run_status.return_value = job_run_status

        if expected_poke_result != "exception":
            assert self.sensor.poke({}) == expected_poke_result
        else:
            # The sensor should fail if the job run status is 20 (aka Error) or 30 (aka Cancelled).
            if job_run_status == DbtCloudJobRunStatus.ERROR.value:
                error_message = f"Job run {RUN_ID} has failed."
            else:
                error_message = f"Job run {RUN_ID} has been cancelled."

            with pytest.raises(DbtCloudJobRunException, match=error_message):
                self.sensor.poke({})
