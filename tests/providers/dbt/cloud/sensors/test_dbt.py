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

from unittest import mock
from unittest.mock import patch

import pytest

from airflow.exceptions import (
    AirflowException,
    AirflowProviderDeprecationWarning,
    AirflowSkipException,
    TaskDeferred,
)
from airflow.models.connection import Connection
from airflow.providers.dbt.cloud.hooks.dbt import DbtCloudHook, DbtCloudJobRunException, DbtCloudJobRunStatus
from airflow.providers.dbt.cloud.sensors.dbt import DbtCloudJobRunAsyncSensor, DbtCloudJobRunSensor
from airflow.providers.dbt.cloud.triggers.dbt import DbtCloudRunJobTrigger
from airflow.utils import db

pytestmark = pytest.mark.db_test


ACCOUNT_ID = 11111
RUN_ID = 5555
TOKEN = "token"


class TestDbtCloudJobRunSensor:
    TASK_ID = "dbt_cloud_run_job"
    CONN_ID = "dbt_cloud_default"
    DBT_RUN_ID = 1234
    TIMEOUT = 300

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
        ],
    )
    @patch.object(DbtCloudHook, "get_job_run_status")
    def test_poke(self, mock_job_run_status, job_run_status, expected_poke_result):
        mock_job_run_status.return_value = job_run_status

        assert self.sensor.poke({}) == expected_poke_result

    @pytest.mark.parametrize(
        "soft_fail, expected_exception", ((False, DbtCloudJobRunException), (True, AirflowSkipException))
    )
    @pytest.mark.parametrize(
        argnames=("job_run_status", "expected_poke_result"),
        argvalues=[
            (20, "exception"),  # ERROR
            (30, "exception"),  # CANCELLED
        ],
    )
    @patch.object(DbtCloudHook, "get_job_run_status")
    def test_poke_with_exception(
        self, mock_job_run_status, job_run_status, expected_poke_result, soft_fail: bool, expected_exception
    ):
        self.sensor.soft_fail = soft_fail
        mock_job_run_status.return_value = job_run_status

        # The sensor should fail if the job run status is 20 (aka Error) or 30 (aka Cancelled).
        if job_run_status == DbtCloudJobRunStatus.ERROR.value:
            error_message = f"Job run {RUN_ID} has failed."
        else:
            error_message = f"Job run {RUN_ID} has been cancelled."

        with pytest.raises(expected_exception, match=error_message):
            self.sensor.poke({})

    @mock.patch("airflow.providers.dbt.cloud.sensors.dbt.DbtCloudHook")
    @mock.patch("airflow.providers.dbt.cloud.sensors.dbt.DbtCloudJobRunSensor.defer")
    def test_dbt_cloud_job_run_sensor_finish_before_deferred(self, mock_defer, mock_hook):
        task = DbtCloudJobRunSensor(
            dbt_cloud_conn_id=self.CONN_ID,
            task_id=self.TASK_ID,
            run_id=self.DBT_RUN_ID,
            timeout=self.TIMEOUT,
            deferrable=True,
        )
        mock_hook.return_value.get_job_run_status.return_value = DbtCloudJobRunStatus.SUCCESS.value
        task.execute(mock.MagicMock())
        assert not mock_defer.called

    @mock.patch("airflow.providers.dbt.cloud.sensors.dbt.DbtCloudHook")
    def test_execute_with_deferrable_mode(self, mock_hook):
        """Assert execute method defer for Dbt cloud job run status sensors"""
        task = DbtCloudJobRunSensor(
            dbt_cloud_conn_id=self.CONN_ID,
            task_id=self.TASK_ID,
            run_id=self.DBT_RUN_ID,
            timeout=self.TIMEOUT,
            deferrable=True,
        )
        mock_hook.return_value.get_job_run_status.return_value = DbtCloudJobRunStatus.STARTING.value
        with pytest.raises(TaskDeferred) as exc:
            task.execute({})
        assert isinstance(exc.value.trigger, DbtCloudRunJobTrigger), "Trigger is not a DbtCloudRunJobTrigger"

    def test_execute_complete_success(self):
        """Assert execute_complete log success message when trigger fire with target status"""
        task = DbtCloudJobRunSensor(
            dbt_cloud_conn_id=self.CONN_ID,
            task_id=self.TASK_ID,
            run_id=self.DBT_RUN_ID,
            timeout=self.TIMEOUT,
            deferrable=True,
        )

        msg = f"Job run {self.DBT_RUN_ID} has completed successfully."
        with mock.patch.object(task.log, "info") as mock_log_info:
            task.execute_complete(
                context={}, event={"status": "success", "message": msg, "run_id": self.DBT_RUN_ID}
            )
        mock_log_info.assert_called_with(msg)

    @pytest.mark.parametrize(
        "mock_status, mock_message",
        [
            ("cancelled", "Job run 1234 has been cancelled."),
            ("error", "Job run 1234 has failed."),
        ],
    )
    def test_execute_complete_failure(self, mock_status, mock_message):
        """Assert execute_complete method to raise exception on the cancelled and error status"""
        task = DbtCloudJobRunSensor(
            dbt_cloud_conn_id=self.CONN_ID,
            task_id=self.TASK_ID,
            run_id=self.DBT_RUN_ID,
            timeout=self.TIMEOUT,
            deferrable=True,
        )
        with pytest.raises(AirflowException):
            task.execute_complete(
                context={}, event={"status": mock_status, "message": mock_message, "run_id": self.DBT_RUN_ID}
            )


class TestDbtCloudJobRunAsyncSensor:
    TASK_ID = "dbt_cloud_run_job"
    CONN_ID = "dbt_cloud_default"
    DBT_RUN_ID = 1234
    TIMEOUT = 300

    depcrecation_message = (
        "Class `DbtCloudJobRunAsyncSensor` is deprecated and will be removed in a future release. "
        "Please use `DbtCloudJobRunSensor` and set `deferrable` attribute to `True` instead"
    )

    @mock.patch("airflow.providers.dbt.cloud.sensors.dbt.DbtCloudHook")
    def test_dbt_job_run_async_sensor(self, mock_hook):
        """Assert execute method defer for Dbt cloud job run status sensors"""

        with pytest.warns(AirflowProviderDeprecationWarning, match=self.depcrecation_message):
            task = DbtCloudJobRunAsyncSensor(
                dbt_cloud_conn_id=self.CONN_ID,
                task_id=self.TASK_ID,
                run_id=self.DBT_RUN_ID,
                timeout=self.TIMEOUT,
            )
        mock_hook.return_value.get_job_run_status.return_value = DbtCloudJobRunStatus.STARTING.value
        with pytest.raises(TaskDeferred) as exc:
            task.execute({})
        assert isinstance(exc.value.trigger, DbtCloudRunJobTrigger), "Trigger is not a DbtCloudRunJobTrigger"

    def test_dbt_job_run_async_sensor_execute_complete_success(self):
        """Assert execute_complete log success message when trigger fire with target status"""
        with pytest.warns(AirflowProviderDeprecationWarning, match=self.depcrecation_message):
            task = DbtCloudJobRunAsyncSensor(
                dbt_cloud_conn_id=self.CONN_ID,
                task_id=self.TASK_ID,
                run_id=self.DBT_RUN_ID,
                timeout=self.TIMEOUT,
            )

        msg = f"Job run {self.DBT_RUN_ID} has completed successfully."
        with mock.patch.object(task.log, "info") as mock_log_info:
            task.execute_complete(
                context={}, event={"status": "success", "message": msg, "run_id": self.DBT_RUN_ID}
            )
        mock_log_info.assert_called_with(msg)

    @pytest.mark.parametrize(
        "mock_status, mock_message",
        [
            ("cancelled", "Job run 1234 has been cancelled."),
            ("error", "Job run 1234 has failed."),
        ],
    )
    def test_dbt_job_run_async_sensor_execute_complete_failure(self, mock_status, mock_message):
        """Assert execute_complete method to raise exception on the cancelled and error status"""
        with pytest.warns(AirflowProviderDeprecationWarning, match=self.depcrecation_message):
            task = DbtCloudJobRunAsyncSensor(
                dbt_cloud_conn_id=self.CONN_ID,
                task_id=self.TASK_ID,
                run_id=self.DBT_RUN_ID,
                timeout=self.TIMEOUT,
            )
        with pytest.raises(AirflowException):
            task.execute_complete(
                context={}, event={"status": mock_status, "message": mock_message, "run_id": self.DBT_RUN_ID}
            )
