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
from datetime import timedelta
from unittest.mock import MagicMock, patch

import pytest

from airflow.models import DAG, Connection
from airflow.providers.common.compat.sdk import TaskDeferred, timezone
from airflow.providers.dbt.cloud.hooks.dbt import DbtCloudHook, DbtCloudJobRunException, DbtCloudJobRunStatus
from airflow.providers.dbt.cloud.operators.dbt import (
    DbtCloudGetJobRunArtifactOperator,
    DbtCloudListJobsOperator,
    DbtCloudRunJobOperator,
)
from airflow.providers.dbt.cloud.triggers.dbt import DbtCloudRunJobTrigger

from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS

if AIRFLOW_V_3_0_PLUS:
    from airflow.sdk.execution_time.comms import XComResult

DEFAULT_DATE = timezone.datetime(2021, 1, 1)
TASK_ID = "run_job_op"
ACCOUNT_ID_CONN = "account_id_conn"
NO_ACCOUNT_ID_CONN = "no_account_id_conn"
DEFAULT_ACCOUNT_ID = 11111
ACCOUNT_ID = 22222
TOKEN = "token"
PROJECT_ID = 33333
PROJECT_NAME = "project_name"
ENVIRONMENT_ID = 44444
ENVIRONMENT_NAME = "environment_name"
JOB_ID = 4444
JOB_NAME = "job_name"
RUN_ID = 5555
EXPECTED_JOB_RUN_OP_EXTRA_LINK = (
    "https://cloud.getdbt.com/#/accounts/{account_id}/projects/{project_id}/runs/{run_id}/"
)
DEFAULT_ACCOUNT_JOB_RUN_RESPONSE = {
    "data": {
        "id": RUN_ID,
        "href": EXPECTED_JOB_RUN_OP_EXTRA_LINK.format(
            account_id=DEFAULT_ACCOUNT_ID, project_id=PROJECT_ID, run_id=RUN_ID
        ),
    }
}
EXPLICIT_ACCOUNT_JOB_RUN_RESPONSE = {
    "data": {
        "id": RUN_ID,
        "href": EXPECTED_JOB_RUN_OP_EXTRA_LINK.format(
            account_id=ACCOUNT_ID, project_id=PROJECT_ID, run_id=RUN_ID
        ),
    }
}
JOB_RUN_ERROR_RESPONSE = {
    "data": [
        {
            "id": RUN_ID,
            "href": EXPECTED_JOB_RUN_OP_EXTRA_LINK.format(
                account_id=ACCOUNT_ID, project_id=PROJECT_ID, run_id=RUN_ID
            ),
            "status": DbtCloudJobRunStatus.ERROR.value,
        }
    ]
}
DEFAULT_ACCOUNT_JOB_RESPONSE = {
    "data": {
        "id": JOB_ID,
        "account_id": DEFAULT_ACCOUNT_ID,
    }
}


def mock_response_json(response: dict):
    run_response = MagicMock(**response)
    run_response.json.return_value = response
    return run_response


# TODO: Potential performance issue, converted setup_module to a setup_connections function level fixture
@pytest.fixture(autouse=True)
def setup_connections(create_connection_without_db):
    # Connection with ``account_id`` specified
    conn_account_id = Connection(
        conn_id=ACCOUNT_ID_CONN,
        conn_type=DbtCloudHook.conn_type,
        login=str(DEFAULT_ACCOUNT_ID),
        password=TOKEN,
    )

    # Connection with no ``account_id`` specified
    conn_no_account_id = Connection(
        conn_id=NO_ACCOUNT_ID_CONN,
        conn_type=DbtCloudHook.conn_type,
        password=TOKEN,
    )

    create_connection_without_db(conn_account_id)
    create_connection_without_db(conn_no_account_id)


class TestDbtCloudRunJobOperator:
    def setup_method(self):
        self.dag = DAG("test_dbt_cloud_job_run_op", schedule=None, start_date=DEFAULT_DATE)
        self.mock_ti = MagicMock()
        self.mock_context = {"ti": self.mock_ti}
        self.config = {
            "job_id": JOB_ID,
            "check_interval": 1,
            "timeout": 3,
            "steps_override": ["dbt run --select my_first_dbt_model"],
            "schema_override": "another_schema",
            "additional_run_config": {"threads_override": 8},
        }

    @patch(
        "airflow.providers.dbt.cloud.hooks.dbt.DbtCloudHook.get_job_run_status",
        return_value=DbtCloudJobRunStatus.SUCCESS.value,
    )
    @patch("airflow.providers.dbt.cloud.operators.dbt.DbtCloudRunJobOperator.defer")
    @patch("airflow.providers.dbt.cloud.hooks.dbt.DbtCloudHook.get_connection")
    @patch("airflow.providers.dbt.cloud.hooks.dbt.DbtCloudHook.trigger_job_run")
    def test_execute_succeeded_before_getting_deferred(
        self, mock_trigger_job_run, mock_dbt_hook, mock_defer, mock_job_run_status
    ):
        dbt_op = DbtCloudRunJobOperator(
            dbt_cloud_conn_id=ACCOUNT_ID_CONN,
            task_id=TASK_ID,
            job_id=JOB_ID,
            check_interval=1,
            timeout=3,
            dag=self.dag,
            deferrable=True,
        )
        dbt_op.execute(MagicMock())
        assert not mock_defer.called

    @patch(
        "airflow.providers.dbt.cloud.hooks.dbt.DbtCloudHook.get_job_run_status",
        return_value=DbtCloudJobRunStatus.ERROR.value,
    )
    @patch("airflow.providers.dbt.cloud.operators.dbt.DbtCloudRunJobOperator.defer")
    @patch("airflow.providers.dbt.cloud.hooks.dbt.DbtCloudHook.get_connection")
    @patch(
        "airflow.providers.dbt.cloud.hooks.dbt.DbtCloudHook.trigger_job_run",
        return_value=mock_response_json(DEFAULT_ACCOUNT_JOB_RUN_RESPONSE),
    )
    def test_execute_failed_before_getting_deferred(
        self, mock_trigger_job_run, mock_dbt_hook, mock_defer, mock_job_run_status
    ):
        dbt_op = DbtCloudRunJobOperator(
            dbt_cloud_conn_id=ACCOUNT_ID_CONN,
            task_id=TASK_ID,
            job_id=JOB_ID,
            check_interval=1,
            timeout=3,
            dag=self.dag,
            deferrable=True,
        )
        with pytest.raises(DbtCloudJobRunException):
            dbt_op.execute(MagicMock())
        assert not mock_defer.called

    @pytest.mark.parametrize(
        "status",
        (
            DbtCloudJobRunStatus.QUEUED.value,
            DbtCloudJobRunStatus.STARTING.value,
            DbtCloudJobRunStatus.RUNNING.value,
        ),
    )
    @patch(
        "airflow.providers.dbt.cloud.hooks.dbt.DbtCloudHook.get_job_run_status",
    )
    @patch("airflow.providers.dbt.cloud.hooks.dbt.DbtCloudHook.get_connection")
    @patch(
        "airflow.providers.dbt.cloud.hooks.dbt.DbtCloudHook.trigger_job_run",
        return_value=mock_response_json(DEFAULT_ACCOUNT_JOB_RUN_RESPONSE),
    )
    def test_dbt_run_job_op_async(self, mock_trigger_job_run, mock_dbt_hook, mock_job_run_status, status):
        """
        Asserts that a task is deferred and an DbtCloudRunJobTrigger will be fired
        when the DbtCloudRunJobOperator has deferrable param set to True
        """
        mock_job_run_status.return_value = status
        dbt_op = DbtCloudRunJobOperator(
            dbt_cloud_conn_id=ACCOUNT_ID_CONN,
            task_id=TASK_ID,
            job_id=JOB_ID,
            check_interval=1,
            timeout=3,
            dag=self.dag,
            deferrable=True,
        )
        with pytest.raises(TaskDeferred) as exc:
            dbt_op.execute(MagicMock())
        assert isinstance(exc.value.trigger, DbtCloudRunJobTrigger), "Trigger is not a DbtCloudRunJobTrigger"

    @patch(
        "airflow.providers.dbt.cloud.hooks.dbt.DbtCloudHook.get_job_by_name",
        return_value=mock_response_json(DEFAULT_ACCOUNT_JOB_RESPONSE),
    )
    @patch(
        "airflow.providers.dbt.cloud.hooks.dbt.DbtCloudHook.get_job_run_status",
        return_value=DbtCloudJobRunStatus.SUCCESS.value,
    )
    @patch("airflow.providers.dbt.cloud.hooks.dbt.DbtCloudHook.get_connection")
    @patch(
        "airflow.providers.dbt.cloud.hooks.dbt.DbtCloudHook.trigger_job_run",
        return_value=mock_response_json(DEFAULT_ACCOUNT_JOB_RUN_RESPONSE),
    )
    def test_dbt_run_job_by_name(
        self, mock_trigger_job_run, mock_dbt_hook, mock_job_run_status, mock_job_by_name
    ):
        """
        Test alternative way to run a job by project,
        environment and job name instead of job id.
        """
        dbt_op = DbtCloudRunJobOperator(
            dbt_cloud_conn_id=ACCOUNT_ID_CONN,
            task_id=TASK_ID,
            project_name=PROJECT_NAME,
            environment_name=ENVIRONMENT_NAME,
            job_name=JOB_NAME,
            check_interval=1,
            timeout=3,
            dag=self.dag,
        )
        dbt_op.execute(MagicMock())
        mock_trigger_job_run.assert_called_once()

    @pytest.mark.parametrize(
        argnames=("project_name", "environment_name", "job_name"),
        argvalues=[
            (None, ENVIRONMENT_NAME, JOB_NAME),
            (PROJECT_NAME, "", JOB_NAME),
            (PROJECT_NAME, ENVIRONMENT_NAME, None),
            ("", "", ""),
        ],
    )
    @patch(
        "airflow.providers.dbt.cloud.hooks.dbt.DbtCloudHook.get_job_by_name",
        return_value=mock_response_json(DEFAULT_ACCOUNT_JOB_RESPONSE),
    )
    @patch(
        "airflow.providers.dbt.cloud.hooks.dbt.DbtCloudHook.get_job_run_status",
        return_value=DbtCloudJobRunStatus.SUCCESS.value,
    )
    @patch("airflow.providers.dbt.cloud.hooks.dbt.DbtCloudHook.get_connection")
    @patch(
        "airflow.providers.dbt.cloud.hooks.dbt.DbtCloudHook.trigger_job_run",
        return_value=mock_response_json(DEFAULT_ACCOUNT_JOB_RUN_RESPONSE),
    )
    def test_dbt_run_job_by_incorrect_name_raises_exception(
        self,
        mock_trigger_job_run,
        mock_dbt_hook,
        mock_job_run_status,
        mock_job_by_name,
        project_name,
        environment_name,
        job_name,
    ):
        """
        Test alternative way to run a job by project,
        environment and job name instead of job id.

        This test is to check if the operator raises an exception
        when the project, environment or job name is missing.
        """
        dbt_op = DbtCloudRunJobOperator(
            dbt_cloud_conn_id=ACCOUNT_ID_CONN,
            task_id=TASK_ID,
            project_name=project_name,
            environment_name=environment_name,
            job_name=job_name,
            check_interval=1,
            timeout=3,
            dag=self.dag,
        )
        with pytest.raises(
            ValueError,
            match="Either job_id or project_name, environment_name, and job_name must be provided.",
        ):
            dbt_op.execute(MagicMock())
        mock_trigger_job_run.assert_not_called()

    @patch.object(
        DbtCloudHook, "trigger_job_run", return_value=mock_response_json(DEFAULT_ACCOUNT_JOB_RUN_RESPONSE)
    )
    @pytest.mark.parametrize(
        ("job_run_status", "expected_output"),
        [
            (DbtCloudJobRunStatus.SUCCESS.value, "success"),
            (DbtCloudJobRunStatus.ERROR.value, "exception"),
            (DbtCloudJobRunStatus.CANCELLED.value, "exception"),
            (DbtCloudJobRunStatus.RUNNING.value, "timeout"),
            (DbtCloudJobRunStatus.QUEUED.value, "timeout"),
            (DbtCloudJobRunStatus.STARTING.value, "timeout"),
        ],
    )
    @pytest.mark.parametrize(
        ("conn_id", "account_id"),
        [(ACCOUNT_ID_CONN, None), (NO_ACCOUNT_ID_CONN, ACCOUNT_ID)],
        ids=["default_account", "explicit_account"],
    )
    def test_execute_wait_for_termination(
        self, mock_run_job, conn_id, account_id, job_run_status, expected_output, time_machine
    ):
        operator = DbtCloudRunJobOperator(
            task_id=TASK_ID, dbt_cloud_conn_id=conn_id, account_id=account_id, dag=self.dag, **self.config
        )

        assert operator.dbt_cloud_conn_id == conn_id
        assert operator.job_id == self.config["job_id"]
        assert operator.account_id == account_id
        assert operator.check_interval == self.config["check_interval"]
        assert operator.timeout == self.config["timeout"]
        assert operator.wait_for_termination
        assert operator.steps_override == self.config["steps_override"]
        assert operator.schema_override == self.config["schema_override"]
        assert operator.additional_run_config == self.config["additional_run_config"]

        # Freeze time for avoid real clock side effects
        time_machine.move_to(timezone.datetime(1970, 1, 1), tick=False)

        def fake_sleep(seconds):
            # Shift frozen time every time we call a ``time.sleep`` during this test case.
            # Because we freeze a time, we also need to add a small shift
            # which is emulating time which we spent in a loop
            overall_delta = timedelta(seconds=seconds) + timedelta(microseconds=42)
            time_machine.shift(overall_delta)

        with (
            patch.object(DbtCloudHook, "get_job_run") as mock_get_job_run,
            patch("airflow.providers.dbt.cloud.hooks.dbt.time.sleep", side_effect=fake_sleep),
        ):
            mock_get_job_run.return_value.json.return_value = {
                "data": {"status": job_run_status, "id": RUN_ID}
            }

            if expected_output == "success":
                operator.execute(context=self.mock_context)

                assert mock_run_job.return_value.data["id"] == RUN_ID
            elif expected_output == "exception":
                # The operator should fail if the job run fails or is cancelled.
                error_message = r"has failed or has been cancelled\.$"
                with pytest.raises(DbtCloudJobRunException, match=error_message):
                    operator.execute(context=self.mock_context)
            else:
                # Demonstrating the operator timing out after surpassing the configured timeout value.
                timeout = self.config["timeout"]
                error_message = rf"has not reached a terminal status after {timeout} seconds\.$"
                with pytest.raises(DbtCloudJobRunException, match=error_message):
                    operator.execute(context=self.mock_context)

            mock_run_job.assert_called_once_with(
                account_id=account_id,
                job_id=JOB_ID,
                cause=f"Triggered via Apache Airflow by task {TASK_ID!r} in the {self.dag.dag_id} DAG.",
                steps_override=self.config["steps_override"],
                schema_override=self.config["schema_override"],
                retry_from_failure=False,
                additional_run_config=self.config["additional_run_config"],
            )

            if job_run_status in DbtCloudJobRunStatus.TERMINAL_STATUSES.value:
                assert mock_get_job_run.call_count == 1
            else:
                # When the job run status is not in a terminal status or "Success", the operator will
                # continue to call ``get_job_run()`` until a ``timeout`` number of seconds has passed
                assert mock_get_job_run.call_count >= 1

                # To make it more dynamic, try and calculate number of calls
                max_number_of_calls = timeout // self.config["check_interval"] + 1
                assert mock_get_job_run.call_count <= max_number_of_calls

    @patch.object(DbtCloudHook, "trigger_job_run")
    @pytest.mark.parametrize(
        ("conn_id", "account_id"),
        [(ACCOUNT_ID_CONN, None), (NO_ACCOUNT_ID_CONN, ACCOUNT_ID)],
        ids=["default_account", "explicit_account"],
    )
    def test_execute_no_wait_for_termination(self, mock_run_job, conn_id, account_id):
        operator = DbtCloudRunJobOperator(
            task_id=TASK_ID,
            dbt_cloud_conn_id=conn_id,
            account_id=account_id,
            trigger_reason=None,
            dag=self.dag,
            wait_for_termination=False,
            **self.config,
        )

        assert operator.dbt_cloud_conn_id == conn_id
        assert operator.job_id == self.config["job_id"]
        assert operator.account_id == account_id
        assert operator.check_interval == self.config["check_interval"]
        assert operator.timeout == self.config["timeout"]
        assert not operator.wait_for_termination
        assert operator.steps_override == self.config["steps_override"]
        assert operator.schema_override == self.config["schema_override"]
        assert operator.additional_run_config == self.config["additional_run_config"]

        with patch.object(DbtCloudHook, "get_job_run") as mock_get_job_run:
            operator.execute(context=self.mock_context)

            mock_run_job.assert_called_once_with(
                account_id=account_id,
                job_id=JOB_ID,
                cause=f"Triggered via Apache Airflow by task {TASK_ID!r} in the {self.dag.dag_id} DAG.",
                steps_override=self.config["steps_override"],
                schema_override=self.config["schema_override"],
                retry_from_failure=False,
                additional_run_config=self.config["additional_run_config"],
            )

            mock_get_job_run.assert_not_called()

    @patch.object(DbtCloudHook, "get_job_runs")
    @patch.object(DbtCloudHook, "trigger_job_run")
    @pytest.mark.parametrize(
        ("conn_id", "account_id"),
        [(ACCOUNT_ID_CONN, None), (NO_ACCOUNT_ID_CONN, ACCOUNT_ID)],
        ids=["default_account", "explicit_account"],
    )
    def test_execute_no_wait_for_termination_and_reuse_existing_run(
        self, mock_run_job, mock_get_job_runs, conn_id, account_id
    ):
        mock_get_job_runs.return_value.json.return_value = {
            "data": [
                {
                    "id": 10000,
                    "status": 1,
                    "href": EXPECTED_JOB_RUN_OP_EXTRA_LINK.format(
                        account_id=DEFAULT_ACCOUNT_ID, project_id=PROJECT_ID, run_id=RUN_ID
                    ),
                },
                {
                    "id": 10001,
                    "status": 2,
                    "href": EXPECTED_JOB_RUN_OP_EXTRA_LINK.format(
                        account_id=DEFAULT_ACCOUNT_ID, project_id=PROJECT_ID, run_id=RUN_ID
                    ),
                },
            ]
        }

        operator = DbtCloudRunJobOperator(
            task_id=TASK_ID,
            dbt_cloud_conn_id=conn_id,
            account_id=account_id,
            trigger_reason=None,
            dag=self.dag,
            wait_for_termination=False,
            reuse_existing_run=True,
            **self.config,
        )

        assert operator.dbt_cloud_conn_id == conn_id
        assert operator.job_id == self.config["job_id"]
        assert operator.account_id == account_id
        assert operator.check_interval == self.config["check_interval"]
        assert operator.timeout == self.config["timeout"]
        assert not operator.wait_for_termination
        assert operator.steps_override == self.config["steps_override"]
        assert operator.schema_override == self.config["schema_override"]
        assert operator.additional_run_config == self.config["additional_run_config"]

        operator.execute(context=self.mock_context)

        mock_run_job.assert_not_called()
        mock_get_job_runs.assert_called_with(
            account_id=account_id,
            payload={
                "job_definition_id": self.config["job_id"],
                "status__in": str(list(DbtCloudJobRunStatus.NON_TERMINAL_STATUSES.value)),
                "order_by": "-created_at",
            },
        )

    @patch.object(DbtCloudHook, "trigger_job_run")
    @pytest.mark.parametrize(
        ("conn_id", "account_id"),
        [(ACCOUNT_ID_CONN, None), (NO_ACCOUNT_ID_CONN, ACCOUNT_ID)],
        ids=["default_account", "explicit_account"],
    )
    def test_execute_retry_from_failure(self, mock_run_job, conn_id, account_id):
        operator = DbtCloudRunJobOperator(
            task_id=TASK_ID,
            dbt_cloud_conn_id=conn_id,
            account_id=account_id,
            trigger_reason=None,
            dag=self.dag,
            retry_from_failure=True,
            **self.config,
        )

        assert operator.dbt_cloud_conn_id == conn_id
        assert operator.job_id == self.config["job_id"]
        assert operator.account_id == account_id
        assert operator.check_interval == self.config["check_interval"]
        assert operator.timeout == self.config["timeout"]
        assert operator.retry_from_failure
        assert operator.steps_override == self.config["steps_override"]
        assert operator.schema_override == self.config["schema_override"]
        assert operator.additional_run_config == self.config["additional_run_config"]

        operator.execute(context=self.mock_context)

        mock_run_job.assert_called_once_with(
            account_id=account_id,
            job_id=JOB_ID,
            cause=f"Triggered via Apache Airflow by task {TASK_ID!r} in the {self.dag.dag_id} DAG.",
            steps_override=self.config["steps_override"],
            schema_override=self.config["schema_override"],
            retry_from_failure=True,
            additional_run_config=self.config["additional_run_config"],
        )

    @patch.object(DbtCloudHook, "_run_and_get_response")
    @pytest.mark.parametrize(
        ("conn_id", "account_id"),
        [(ACCOUNT_ID_CONN, None), (NO_ACCOUNT_ID_CONN, ACCOUNT_ID)],
        ids=["default_account", "explicit_account"],
    )
    def test_execute_retry_from_failure_run(self, mock_run_req, conn_id, account_id):
        operator = DbtCloudRunJobOperator(
            task_id=TASK_ID,
            dbt_cloud_conn_id=conn_id,
            account_id=account_id,
            trigger_reason=None,
            dag=self.dag,
            retry_from_failure=True,
            **self.config,
        )
        self.mock_context["ti"].try_number = 1

        assert operator.dbt_cloud_conn_id == conn_id
        assert operator.job_id == self.config["job_id"]
        assert operator.account_id == account_id
        assert operator.check_interval == self.config["check_interval"]
        assert operator.timeout == self.config["timeout"]
        assert operator.retry_from_failure
        assert operator.steps_override == self.config["steps_override"]
        assert operator.schema_override == self.config["schema_override"]
        assert operator.additional_run_config == self.config["additional_run_config"]

        operator.execute(context=self.mock_context)

        mock_run_req.assert_called()

    @patch.object(
        DbtCloudHook, "_run_and_get_response", return_value=mock_response_json(JOB_RUN_ERROR_RESPONSE)
    )
    @patch.object(DbtCloudHook, "retry_failed_job_run")
    @pytest.mark.parametrize(
        ("conn_id", "account_id"),
        [(ACCOUNT_ID_CONN, None), (NO_ACCOUNT_ID_CONN, ACCOUNT_ID)],
        ids=["default_account", "explicit_account"],
    )
    def test_execute_retry_from_failure_rerun(self, mock_run_req, mock_rerun_req, conn_id, account_id):
        operator = DbtCloudRunJobOperator(
            task_id=TASK_ID,
            dbt_cloud_conn_id=conn_id,
            account_id=account_id,
            trigger_reason=None,
            dag=self.dag,
            retry_from_failure=True,
            **self.config,
        )
        self.mock_context["ti"].try_number = 2

        assert operator.dbt_cloud_conn_id == conn_id
        assert operator.job_id == self.config["job_id"]
        assert operator.account_id == account_id
        assert operator.check_interval == self.config["check_interval"]
        assert operator.timeout == self.config["timeout"]
        assert operator.retry_from_failure
        assert operator.steps_override == self.config["steps_override"]
        assert operator.schema_override == self.config["schema_override"]
        assert operator.additional_run_config == self.config["additional_run_config"]

        operator.execute(context=self.mock_context)

        mock_rerun_req.assert_called_once()

    @patch.object(DbtCloudHook, "trigger_job_run")
    @pytest.mark.parametrize(
        ("conn_id", "account_id"),
        [(ACCOUNT_ID_CONN, None), (NO_ACCOUNT_ID_CONN, ACCOUNT_ID)],
        ids=["default_account", "explicit_account"],
    )
    def test_custom_trigger_reason(self, mock_run_job, conn_id, account_id):
        custom_trigger_reason = "Some other trigger reason."
        operator = DbtCloudRunJobOperator(
            task_id=TASK_ID,
            dbt_cloud_conn_id=conn_id,
            account_id=account_id,
            trigger_reason=custom_trigger_reason,
            dag=self.dag,
            **self.config,
        )

        assert operator.trigger_reason == custom_trigger_reason

        with patch.object(DbtCloudHook, "get_job_run") as mock_get_job_run:
            mock_get_job_run.return_value.json.return_value = {
                "data": {"status": DbtCloudJobRunStatus.SUCCESS.value, "id": RUN_ID}
            }

            operator.execute(context=self.mock_context)

            mock_run_job.assert_called_once_with(
                account_id=account_id,
                job_id=JOB_ID,
                cause=custom_trigger_reason,
                steps_override=self.config["steps_override"],
                schema_override=self.config["schema_override"],
                retry_from_failure=False,
                additional_run_config=self.config["additional_run_config"],
            )

    @pytest.mark.parametrize(
        ("conn_id", "account_id"),
        [(ACCOUNT_ID_CONN, None), (NO_ACCOUNT_ID_CONN, ACCOUNT_ID)],
        ids=["default_account", "explicit_account"],
    )
    @pytest.mark.db_test
    def test_run_job_operator_link(
        self, conn_id, account_id, dag_maker, create_task_instance_of_operator, request, mock_supervisor_comms
    ):
        ti = create_task_instance_of_operator(
            DbtCloudRunJobOperator,
            dag_id="test_dbt_cloud_run_job_op_link",
            task_id="trigger_dbt_cloud_job",
            dbt_cloud_conn_id=conn_id,
            job_id=JOB_ID,
            account_id=account_id,
        )

        if request.node.callspec.id == "default_account":
            _run_response = DEFAULT_ACCOUNT_JOB_RUN_RESPONSE
        else:
            _run_response = EXPLICIT_ACCOUNT_JOB_RUN_RESPONSE

        ti.xcom_push(key="job_run_url", value=_run_response["data"]["href"])

        if AIRFLOW_V_3_0_PLUS and mock_supervisor_comms:
            mock_supervisor_comms.send.return_value = XComResult(
                key="job_run_url",
                value=EXPECTED_JOB_RUN_OP_EXTRA_LINK.format(
                    account_id=account_id or DEFAULT_ACCOUNT_ID,
                    project_id=PROJECT_ID,
                    run_id=_run_response["data"]["id"],
                ),
            )

        task = dag_maker.dag.get_task(ti.task_id)
        url = task.operator_extra_links[0].get_link(operator=ti.task, ti_key=ti.key)

        assert url == (
            EXPECTED_JOB_RUN_OP_EXTRA_LINK.format(
                account_id=account_id or DEFAULT_ACCOUNT_ID,
                project_id=PROJECT_ID,
                run_id=_run_response["data"]["id"],
            )
        )


class TestDbtCloudGetJobRunArtifactOperator:
    def setup_method(self):
        self.dag = DAG("test_dbt_cloud_get_artifact_op", schedule=None, start_date=DEFAULT_DATE)

    @patch("airflow.providers.dbt.cloud.hooks.dbt.DbtCloudHook.get_job_run_artifact")
    @pytest.mark.parametrize(
        ("conn_id", "account_id"),
        [(ACCOUNT_ID_CONN, None), (NO_ACCOUNT_ID_CONN, ACCOUNT_ID)],
        ids=["default_account", "explicit_account"],
    )
    def test_get_json_artifact(self, mock_get_artifact, conn_id, account_id, tmp_path, monkeypatch):
        operator = DbtCloudGetJobRunArtifactOperator(
            task_id=TASK_ID,
            dbt_cloud_conn_id=conn_id,
            run_id=RUN_ID,
            account_id=account_id,
            path="path/to/my/manifest.json",
            dag=self.dag,
        )

        mock_get_artifact.return_value.json.return_value = {"data": "file contents"}
        with monkeypatch.context() as ctx:
            # Let's change current working directory to temp,
            # otherwise the output file will be created in the current working directory
            ctx.chdir(tmp_path)
            return_value = operator.execute(context={})

        mock_get_artifact.assert_called_once_with(
            run_id=RUN_ID,
            path="path/to/my/manifest.json",
            account_id=account_id,
            step=None,
        )

        assert operator.output_file_name == f"{RUN_ID}_path-to-my-manifest.json"
        assert os.path.exists(tmp_path / operator.output_file_name)
        assert return_value == operator.output_file_name

    @patch("airflow.providers.dbt.cloud.hooks.dbt.DbtCloudHook.get_job_run_artifact")
    @pytest.mark.parametrize(
        ("conn_id", "account_id"),
        [(ACCOUNT_ID_CONN, None), (NO_ACCOUNT_ID_CONN, ACCOUNT_ID)],
        ids=["default_account", "explicit_account"],
    )
    def test_get_json_artifact_with_step(self, mock_get_artifact, conn_id, account_id, tmp_path, monkeypatch):
        operator = DbtCloudGetJobRunArtifactOperator(
            task_id=TASK_ID,
            dbt_cloud_conn_id=conn_id,
            run_id=RUN_ID,
            account_id=account_id,
            path="path/to/my/manifest.json",
            step=2,
            dag=self.dag,
        )

        mock_get_artifact.return_value.json.return_value = {"data": "file contents"}
        with monkeypatch.context() as ctx:
            # Let's change current working directory to temp,
            # otherwise the output file will be created in the current working directory
            ctx.chdir(tmp_path)
            return_value = operator.execute(context={})

        mock_get_artifact.assert_called_once_with(
            run_id=RUN_ID,
            path="path/to/my/manifest.json",
            account_id=account_id,
            step=2,
        )

        assert operator.output_file_name == f"{RUN_ID}_path-to-my-manifest.json"
        assert os.path.exists(tmp_path / operator.output_file_name)
        assert return_value == operator.output_file_name

    @patch("airflow.providers.dbt.cloud.hooks.dbt.DbtCloudHook.get_job_run_artifact")
    @pytest.mark.parametrize(
        ("conn_id", "account_id"),
        [(ACCOUNT_ID_CONN, None), (NO_ACCOUNT_ID_CONN, ACCOUNT_ID)],
        ids=["default_account", "explicit_account"],
    )
    def test_get_text_artifact(self, mock_get_artifact, conn_id, account_id, tmp_path, monkeypatch):
        operator = DbtCloudGetJobRunArtifactOperator(
            task_id=TASK_ID,
            dbt_cloud_conn_id=conn_id,
            run_id=RUN_ID,
            account_id=account_id,
            path="path/to/my/model.sql",
            dag=self.dag,
        )

        mock_get_artifact.return_value.text = "file contents"
        with monkeypatch.context() as ctx:
            # Let's change current working directory to temp,
            # otherwise the output file will be created in the current working directory
            ctx.chdir(tmp_path)
            return_value = operator.execute(context={})

        mock_get_artifact.assert_called_once_with(
            run_id=RUN_ID,
            path="path/to/my/model.sql",
            account_id=account_id,
            step=None,
        )

        assert operator.output_file_name == f"{RUN_ID}_path-to-my-model.sql"
        assert os.path.exists(tmp_path / operator.output_file_name)
        assert return_value == operator.output_file_name

    @patch("airflow.providers.dbt.cloud.hooks.dbt.DbtCloudHook.get_job_run_artifact")
    @pytest.mark.parametrize(
        ("conn_id", "account_id"),
        [(ACCOUNT_ID_CONN, None), (NO_ACCOUNT_ID_CONN, ACCOUNT_ID)],
        ids=["default_account", "explicit_account"],
    )
    def test_get_text_artifact_with_step(self, mock_get_artifact, conn_id, account_id, tmp_path, monkeypatch):
        operator = DbtCloudGetJobRunArtifactOperator(
            task_id=TASK_ID,
            dbt_cloud_conn_id=conn_id,
            run_id=RUN_ID,
            account_id=account_id,
            path="path/to/my/model.sql",
            step=2,
            dag=self.dag,
        )

        mock_get_artifact.return_value.text = "file contents"
        with monkeypatch.context() as ctx:
            # Let's change current working directory to temp,
            # otherwise the output file will be created in the current working directory
            ctx.chdir(tmp_path)
            return_value = operator.execute(context={})

        mock_get_artifact.assert_called_once_with(
            run_id=RUN_ID,
            path="path/to/my/model.sql",
            account_id=account_id,
            step=2,
        )

        assert operator.output_file_name == f"{RUN_ID}_path-to-my-model.sql"
        assert os.path.exists(tmp_path / operator.output_file_name)
        assert return_value == operator.output_file_name

    @patch("airflow.providers.dbt.cloud.hooks.dbt.DbtCloudHook.get_job_run_artifact")
    @pytest.mark.parametrize(
        ("conn_id", "account_id"),
        [(ACCOUNT_ID_CONN, None), (NO_ACCOUNT_ID_CONN, ACCOUNT_ID)],
        ids=["default_account", "explicit_account"],
    )
    def test_get_artifact_with_specified_output_file(self, mock_get_artifact, conn_id, account_id, tmp_path):
        specified_output_file = (tmp_path / "run_results.json").as_posix()
        operator = DbtCloudGetJobRunArtifactOperator(
            task_id=TASK_ID,
            dbt_cloud_conn_id=conn_id,
            run_id=RUN_ID,
            account_id=account_id,
            path="run_results.json",
            dag=self.dag,
            output_file_name=specified_output_file,
        )

        mock_get_artifact.return_value.json.return_value = {"data": "file contents"}
        return_value = operator.execute(context={})

        mock_get_artifact.assert_called_once_with(
            run_id=RUN_ID,
            path="run_results.json",
            account_id=account_id,
            step=None,
        )

        assert operator.output_file_name == specified_output_file
        assert os.path.exists(operator.output_file_name)
        assert return_value == operator.output_file_name


class TestDbtCloudListJobsOperator:
    def setup_method(self):
        self.dag = DAG("test_dbt_cloud_list_jobs_op", schedule=None, start_date=DEFAULT_DATE)
        self.mock_ti = MagicMock()
        self.mock_context = {"ti": self.mock_ti}

    @patch("airflow.providers.dbt.cloud.hooks.dbt.DbtCloudHook.list_jobs")
    @pytest.mark.parametrize(
        ("conn_id", "account_id"),
        [(ACCOUNT_ID_CONN, None), (NO_ACCOUNT_ID_CONN, ACCOUNT_ID)],
    )
    def test_execute_list_jobs(self, mock_list_jobs, conn_id, account_id):
        operator = DbtCloudListJobsOperator(
            task_id=TASK_ID,
            dbt_cloud_conn_id=conn_id,
            account_id=account_id,
            project_id=PROJECT_ID,
        )

        mock_list_jobs.return_value.json.return_value = {}
        operator.execute(context=self.mock_context)
        mock_list_jobs.assert_called_once_with(account_id=account_id, order_by=None, project_id=PROJECT_ID)
