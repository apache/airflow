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

import warnings
from datetime import timedelta
from types import SimpleNamespace
from unittest import mock

import pytest
from airbyte_api.models import JobCreateRequest, JobResponse, JobStatusEnum, JobTypeEnum

from airflow.models import Connection
from airflow.providers.airbyte.operators import airbyte as airbyte_module
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow.providers.common.compat.sdk import AirflowException

from tests_common.test_utils.version_compat import AIRFLOW_V_3_3_PLUS


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

        mock_cancel_job.assert_called_once_with(job_id=self.job_id)
        mock_get_job_status.assert_called_once_with(job_id=self.job_id)

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

        mock_get_job_status.assert_called_once_with(job_id=self.job_id)

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


class FakeTaskStateStore:
    def __init__(self, stored: dict[str, int] | None = None) -> None:
        self._store = dict(stored or {})

    def get(self, key: str) -> int | None:
        return self._store.get(key)

    def set(self, key: str, value: int) -> None:
        self._store[key] = value


@pytest.mark.skipif(
    not AIRFLOW_V_3_3_PLUS,
    reason="ResumableJobMixin requires task_state_store, available in Airflow 3.3+",
)
class TestAirbyteTriggerSyncOperatorResumable:
    connection_id = "test_airbyte_connection"
    submitted_job_id = 42
    stored_job_id = 7

    def _make_operator(self, **kwargs) -> AirbyteTriggerSyncOperator:
        return AirbyteTriggerSyncOperator(
            task_id="airbyte_resumable",
            connection_id=self.connection_id,
            wait_seconds=0,
            timeout=360,
            **kwargs,
        )

    @pytest.fixture
    def hook(self):
        with mock.patch.object(airbyte_module, "AirbyteHook", autospec=True) as hook_class:
            hook = hook_class.return_value
            hook.submit_sync_connection.return_value = SimpleNamespace(
                job_id=self.submitted_job_id,
                status=JobStatusEnum.RUNNING,
            )
            yield hook

    def test_first_run_persists_job_id_before_polling(self, hook):
        operator = self._make_operator()
        task_store = FakeTaskStateStore()

        def assert_job_id_persisted(*, job_id: int, wait_seconds: float, timeout: float) -> None:
            assert job_id == self.submitted_job_id
            assert wait_seconds == operator.wait_seconds
            assert timeout == operator.timeout
            assert task_store.get("airbyte_job_id") == self.submitted_job_id

        hook.wait_for_job.side_effect = assert_job_id_persisted

        result = operator.execute(context={"task_state_store": task_store})

        assert result == self.submitted_job_id
        assert operator.job_id == self.submitted_job_id

    @pytest.mark.parametrize(
        "status", [JobStatusEnum.RUNNING, JobStatusEnum.PENDING, JobStatusEnum.INCOMPLETE]
    )
    def test_retry_reconnects_to_active_job(self, hook, status):
        operator = self._make_operator()
        task_store = FakeTaskStateStore({"airbyte_job_id": self.stored_job_id})
        hook.get_job_status.return_value = status

        result = operator.execute(context={"task_state_store": task_store})

        hook.submit_sync_connection.assert_not_called()
        hook.wait_for_job.assert_called_once_with(
            job_id=self.stored_job_id,
            wait_seconds=operator.wait_seconds,
            timeout=operator.timeout,
        )
        assert result == self.stored_job_id
        assert operator.job_id == self.stored_job_id

    def test_retry_recovers_succeeded_job(self, hook):
        operator = self._make_operator()
        task_store = FakeTaskStateStore({"airbyte_job_id": self.stored_job_id})
        hook.get_job_status.return_value = JobStatusEnum.SUCCEEDED

        result = operator.execute(context={"task_state_store": task_store})

        hook.submit_sync_connection.assert_not_called()
        hook.wait_for_job.assert_not_called()
        assert result == self.stored_job_id
        assert operator.job_id == self.stored_job_id

    @pytest.mark.parametrize("status", [JobStatusEnum.FAILED, JobStatusEnum.CANCELLED])
    def test_retry_submits_fresh_after_terminal_job(self, hook, status):
        operator = self._make_operator()
        task_store = FakeTaskStateStore({"airbyte_job_id": self.stored_job_id})
        hook.get_job_status.return_value = status

        result = operator.execute(context={"task_state_store": task_store})

        hook.submit_sync_connection.assert_called_once_with(connection_id=self.connection_id)
        hook.wait_for_job.assert_called_once_with(
            job_id=self.submitted_job_id,
            wait_seconds=operator.wait_seconds,
            timeout=operator.timeout,
        )
        assert task_store.get("airbyte_job_id") == self.submitted_job_id
        assert result == self.submitted_job_id

    def test_durable_false_submits_fresh(self, hook):
        operator = self._make_operator(durable=False)
        task_store = FakeTaskStateStore({"airbyte_job_id": self.stored_job_id})

        result = operator.execute(context={"task_state_store": task_store})

        hook.get_job_status.assert_not_called()
        hook.submit_sync_connection.assert_called_once_with(connection_id=self.connection_id)
        assert task_store.get("airbyte_job_id") == self.stored_job_id
        assert result == self.submitted_job_id

    def test_asynchronous_mode_does_not_use_recovery(self, hook):
        operator = self._make_operator(asynchronous=True)
        task_store = mock.Mock(spec=["get", "set"])
        task_store.get.return_value = self.stored_job_id

        result = operator.execute(context={"task_state_store": task_store})

        task_store.get.assert_not_called()
        task_store.set.assert_not_called()
        hook.get_job_status.assert_not_called()
        hook.wait_for_job.assert_not_called()
        assert operator.durable is True
        assert result == self.submitted_job_id

    @mock.patch.object(AirbyteTriggerSyncOperator, "defer", autospec=True)
    def test_deferrable_mode_does_not_use_recovery(self, mock_defer, hook):
        operator = self._make_operator(deferrable=True)
        task_store = mock.Mock(spec=["get", "set"])
        task_store.get.return_value = self.stored_job_id

        operator.execute(context={"task_state_store": task_store})

        task_store.get.assert_not_called()
        task_store.set.assert_not_called()
        hook.get_job_status.assert_not_called()
        hook.submit_sync_connection.assert_called_once_with(connection_id=self.connection_id)
        mock_defer.assert_called_once()
        assert operator.durable is True

    def test_default_args_durable_reaches_operator(self):
        operator = AirbyteTriggerSyncOperator(
            task_id="airbyte_default_args",
            connection_id=self.connection_id,
            default_args={"durable": False},
        )

        assert operator.durable is False

    def test_on_kill_cancels_reconnected_job(self, hook):
        operator = self._make_operator()
        task_store = FakeTaskStateStore({"airbyte_job_id": self.stored_job_id})
        hook.get_job_status.return_value = JobStatusEnum.RUNNING

        operator.execute(context={"task_state_store": task_store})
        operator.on_kill()

        assert operator.job_id == self.stored_job_id
        hook.cancel_job.assert_called_once_with(job_id=self.stored_job_id)


class TestWarnAndDisableDurableAirflowPre3_3:
    def test_no_warning_when_unset(self):
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            result = airbyte_module._warn_and_disable_durable_pre_3_3(airbyte_module._DURABLE_UNSET)

        assert result is False
        assert caught == []

    @pytest.mark.parametrize("value", [True, False])
    def test_warns_and_disables_when_explicitly_set(self, value):
        with pytest.warns(UserWarning, match="durable.*no effect"):
            result = airbyte_module._warn_and_disable_durable_pre_3_3(value)

        assert result is False
