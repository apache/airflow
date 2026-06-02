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
from __future__ import annotations

from datetime import timedelta
from unittest import mock

import pytest
from airbyte_api.models import JobCreateRequest, JobResponse, JobStatusEnum, JobTypeEnum

from airflow.models import Connection
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow.providers.common.compat.sdk import AirflowException


class TestAirbyteTriggerSyncOp:
    """
    Test execute function from Airbyte Operator
    """

    airbyte_conn_id = "test_airbyte_conn_id"
    connection_id = "test_airbyte_connection"
    job_id = 1
    wait_seconds = 0
    timeout = 360

    @mock.patch("airflow.providers.airbyte.operators.airbyte.time")
    @mock.patch("airbyte_api.jobs.Jobs.create_job")
    @mock.patch("airflow.providers.airbyte.hooks.airbyte.AirbyteHook.wait_for_job", return_value=None)
    def test_execute(
        self, mock_wait_for_job, mock_submit_sync_connection, mock_time, create_connection_without_db
    ):
        conn = Connection(conn_id=self.airbyte_conn_id, conn_type="airbyte", host="airbyte.com")
        create_connection_without_db(conn)
        mock_response = mock.Mock()
        mock_response.job_response = JobResponse(
            connection_id="connection-mock",
            job_id=1,
            start_time="today",
            job_type=JobTypeEnum.SYNC,
            status=JobStatusEnum.RUNNING,
        )
        mock_submit_sync_connection.return_value = mock_response

        op = AirbyteTriggerSyncOperator(
            task_id="test_Airbyte_op",
            airbyte_conn_id=self.airbyte_conn_id,
            connection_id=self.connection_id,
            wait_seconds=self.wait_seconds,
            timeout=self.timeout,
        )
        op.execute({})

        mock_submit_sync_connection.assert_called_once_with(
            request=JobCreateRequest(connection_id=self.connection_id, job_type=JobTypeEnum.SYNC)
        )
        mock_wait_for_job.assert_called_once_with(
            job_id=self.job_id, wait_seconds=self.wait_seconds, timeout=self.timeout
        )

        # Ensure that wall-clock time is used during operator execution flow.
        mock_time.time.assert_called()
        mock_time.monotonic.assert_not_called()

    @mock.patch("airflow.providers.airbyte.operators.airbyte.time")
    @mock.patch("airflow.providers.airbyte.operators.airbyte.AirbyteTriggerSyncOperator.defer")
    @mock.patch("airflow.providers.airbyte.operators.airbyte.AirbyteSyncTrigger")
    @mock.patch("airbyte_api.jobs.Jobs.create_job")
    def test_execute_deferrable_without_execution_timeout(
        self,
        mock_create_job,
        mock_airbyte_trigger,
        mock_defer,
        mock_time,
        create_connection_without_db,
    ):
        conn = Connection(conn_id=self.airbyte_conn_id, conn_type="airbyte", host="airbyte.com")
        create_connection_without_db(conn)

        mock_time.time.return_value = 1000.0

        mock_response = mock.Mock()
        mock_response.job_response = JobResponse(
            connection_id="connection-mock",
            job_id=1,
            start_time="today",
            job_type=JobTypeEnum.SYNC,
            status=JobStatusEnum.RUNNING,
        )
        mock_create_job.return_value = mock_response

        op = AirbyteTriggerSyncOperator(
            task_id="test_airbyte_op",
            airbyte_conn_id=self.airbyte_conn_id,
            connection_id=self.connection_id,
            timeout=self.timeout,
            deferrable=True,
            execution_timeout=None,
        )

        op.execute({})

        mock_defer.assert_called_once_with(
            method_name="execute_complete",
            trigger=mock_airbyte_trigger.return_value,
            timeout=None,
        )

        mock_airbyte_trigger.assert_called_once_with(
            conn_id=self.airbyte_conn_id,
            job_id=self.job_id,
            end_time=1000.0 + self.timeout,
            execution_deadline=None,
            poll_interval=60,
        )

    @mock.patch("airflow.providers.airbyte.operators.airbyte.time")
    @mock.patch("airflow.providers.airbyte.operators.airbyte.AirbyteTriggerSyncOperator.defer")
    @mock.patch("airflow.providers.airbyte.operators.airbyte.AirbyteSyncTrigger")
    @mock.patch("airbyte_api.jobs.Jobs.create_job")
    def test_execute_deferrable_with_execution_timeout(
        self,
        mock_create_job,
        mock_airbyte_trigger,
        mock_defer,
        mock_time,
        create_connection_without_db,
    ):
        conn = Connection(conn_id=self.airbyte_conn_id, conn_type="airbyte", host="airbyte.com")
        create_connection_without_db(conn)

        mock_time.time.return_value = 1000.0

        mock_response = mock.Mock()
        mock_response.job_response = JobResponse(
            connection_id="connection-mock",
            job_id=1,
            start_time="today",
            job_type=JobTypeEnum.SYNC,
            status=JobStatusEnum.RUNNING,
        )
        mock_create_job.return_value = mock_response

        execution_timeout = timedelta(seconds=60)

        op = AirbyteTriggerSyncOperator(
            task_id="test_airbyte_op",
            airbyte_conn_id=self.airbyte_conn_id,
            connection_id=self.connection_id,
            timeout=self.timeout,
            deferrable=True,
            execution_timeout=execution_timeout,
        )

        op.execute({})

        mock_defer.assert_called_once_with(
            method_name="execute_complete",
            trigger=mock_airbyte_trigger.return_value,
            timeout=timedelta(seconds=180),  # 60s timeout + 120s buffer
        )

        mock_airbyte_trigger.assert_called_once_with(
            conn_id=self.airbyte_conn_id,
            job_id=self.job_id,
            end_time=1000.0 + self.timeout,
            execution_deadline=1060.0,
            poll_interval=60,
        )

    @pytest.mark.parametrize(
        ("status", "should_raise", "expected_message"),
        [
            (JobStatusEnum.SUCCEEDED, False, "Job Succeeded"),
            (JobStatusEnum.CANCELLED, True, "Job Cancelled"),
            ("error", True, "Job failed"),
        ],
    )
    def test_execute_complete(self, status, should_raise, expected_message, create_connection_without_db):
        conn = Connection(conn_id=self.airbyte_conn_id, conn_type="airbyte", host="airbyte.com")
        create_connection_without_db(conn)

        op = AirbyteTriggerSyncOperator(
            task_id="test_airbyte_op",
            airbyte_conn_id=self.airbyte_conn_id,
            connection_id=self.connection_id,
            wait_seconds=self.wait_seconds,
            timeout=self.timeout,
            deferrable=True,
        )

        event = {
            "status": status,
            "message": expected_message,
            "job_id": self.job_id,
        }

        if should_raise:
            with pytest.raises(RuntimeError, match=event["message"]):
                op.execute_complete(context={}, event=event)
        else:
            result = op.execute_complete(context={}, event=event)
            assert result is None

    @mock.patch("airflow.providers.airbyte.hooks.airbyte.AirbyteHook.get_job_status")
    @mock.patch("airflow.providers.airbyte.hooks.airbyte.AirbyteHook.cancel_job")
    def test_on_kill(self, mock_cancel_job, mock_get_job_status, create_connection_without_db):
        conn = Connection(conn_id=self.airbyte_conn_id, conn_type="airbyte", host="airbyte.com")
        create_connection_without_db(conn)

        op = AirbyteTriggerSyncOperator(
            task_id="test_Airbyte_op",
            airbyte_conn_id=self.airbyte_conn_id,
            connection_id=self.connection_id,
            wait_seconds=self.wait_seconds,
            timeout=self.timeout,
        )

        op.job_id = self.job_id
        op.on_kill()

        mock_cancel_job.assert_called_once_with(self.job_id)
        mock_get_job_status.assert_called_once_with(self.job_id)

    @mock.patch("airflow.providers.airbyte.hooks.airbyte.AirbyteHook.get_job_status")
    @mock.patch("airflow.providers.airbyte.hooks.airbyte.AirbyteHook.cancel_job")
    def test_on_kill_cancel_failure(self, mock_cancel_job, mock_get_job_status, create_connection_without_db):
        conn = Connection(conn_id=self.airbyte_conn_id, conn_type="airbyte", host="airbyte.com")
        create_connection_without_db(conn)

        mock_cancel_job.side_effect = Exception("cancel failed")

        op = AirbyteTriggerSyncOperator(
            task_id="test_Airbyte_op",
            airbyte_conn_id=self.airbyte_conn_id,
            connection_id=self.connection_id,
            wait_seconds=self.wait_seconds,
            timeout=self.timeout,
        )

        op.job_id = self.job_id
        op.on_kill()

        mock_get_job_status.assert_called_once_with(self.job_id)

    @mock.patch("airflow.providers.airbyte.operators.airbyte.AirbyteHook.cancel_job")
    def test_execute_complete_timeout_cancels_job(self, mock_cancel_job, create_connection_without_db):

        conn = Connection(conn_id=self.airbyte_conn_id, conn_type="airbyte", host="airbyte.com")
        create_connection_without_db(conn)

        op = AirbyteTriggerSyncOperator(
            task_id="test_Airbyte_op",
            airbyte_conn_id=self.airbyte_conn_id,
            connection_id=self.connection_id,
            wait_seconds=self.wait_seconds,
            timeout=self.timeout,
            deferrable=True,
        )

        timeout_event = {
            "status": "timeout",
            "message": "Job run 1 has reached execution timeout.",
            "job_id": self.job_id,
        }

        with pytest.raises(RuntimeError, match="has reached execution timeout"):
            op.execute_complete(
                context={},
                event=timeout_event,
            )

        mock_cancel_job.assert_called_once_with(
            job_id=self.job_id,
        )

    @mock.patch("airflow.providers.airbyte.operators.airbyte.AirbyteHook.cancel_job")
    def test_execute_complete_timeout_cancel_job_does_not_mask_original_error(
        self, mock_cancel_job, create_connection_without_db
    ):
        conn = Connection(conn_id=self.airbyte_conn_id, conn_type="airbyte", host="airbyte.com")
        create_connection_without_db(conn)

        op = AirbyteTriggerSyncOperator(
            task_id="test_airbyte_op",
            airbyte_conn_id=self.airbyte_conn_id,
            connection_id=self.connection_id,
            wait_seconds=self.wait_seconds,
            timeout=self.timeout,
            deferrable=True,
        )

        mock_cancel_job.side_effect = AirflowException("Cancellation failed")

        timeout_event = {
            "status": "timeout",
            "message": "Job run 1 has reached execution timeout.",
            "job_id": self.job_id,
        }

        # Task should still fail due to timeout.
        with pytest.raises(RuntimeError, match="has reached execution timeout"):
            op.execute_complete(context={}, event=timeout_event)

        mock_cancel_job.assert_called_once_with(job_id=self.job_id)
