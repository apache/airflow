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

from unittest.mock import MagicMock, patch

import pytest

from airflow.models import DAG, Connection
from airflow.providers.dbt.cloud.hooks.dbt import DbtCloudHook, DbtCloudJobRunException, DbtCloudJobRunStatus
from airflow.providers.dbt.cloud.operators.dbt import (
    DbtCloudGetJobRunArtifactOperator,
    DbtCloudListJobsOperator,
    DbtCloudRunJobOperator,
)
from airflow.utils import db, timezone

DEFAULT_DATE = timezone.datetime(2021, 1, 1)
TASK_ID = "run_job_op"
ACCOUNT_ID_CONN = "account_id_conn"
NO_ACCOUNT_ID_CONN = "no_account_id_conn"
DEFAULT_ACCOUNT_ID = 11111
ACCOUNT_ID = 22222
TOKEN = "token"
PROJECT_ID = 33333
JOB_ID = 4444
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


def setup_module():
    # Connection with ``account_id`` specified
    conn_account_id = Connection(
        conn_id=ACCOUNT_ID_CONN,
        conn_type=DbtCloudHook.conn_type,
        login=DEFAULT_ACCOUNT_ID,
        password=TOKEN,
    )

    # Connection with no ``account_id`` specified
    conn_no_account_id = Connection(
        conn_id=NO_ACCOUNT_ID_CONN,
        conn_type=DbtCloudHook.conn_type,
        password=TOKEN,
    )

    db.merge_conn(conn_account_id)
    db.merge_conn(conn_no_account_id)


class TestDbtCloudRunJobOperator:
    def setup_method(self):
        self.dag = DAG("test_dbt_cloud_job_run_op", start_date=DEFAULT_DATE)
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

    @patch.object(DbtCloudHook, "trigger_job_run", return_value=MagicMock(**DEFAULT_ACCOUNT_JOB_RUN_RESPONSE))
    @pytest.mark.parametrize(
        "job_run_status, expected_output",
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
        "conn_id, account_id",
        [(ACCOUNT_ID_CONN, None), (NO_ACCOUNT_ID_CONN, ACCOUNT_ID)],
        ids=["default_account", "explicit_account"],
    )
    def test_execute_wait_for_termination(
        self, mock_run_job, conn_id, account_id, job_run_status, expected_output
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

        with patch.object(DbtCloudHook, "get_job_run") as mock_get_job_run:
            mock_get_job_run.return_value.json.return_value = {
                "data": {"status": job_run_status, "id": RUN_ID}
            }

            if expected_output == "success":
                operator.execute(context=self.mock_context)

                assert mock_run_job.return_value.data["id"] == RUN_ID
            elif expected_output == "exception":
                # The operator should fail if the job run fails or is cancelled.
                with pytest.raises(DbtCloudJobRunException) as err:
                    operator.execute(context=self.mock_context)

                    assert err.value.endswith("has failed or has been cancelled.")
            else:
                # Demonstrating the operator timing out after surpassing the configured timeout value.
                with pytest.raises(DbtCloudJobRunException) as err:
                    operator.execute(context=self.mock_context)

                    assert err.value.endswith(
                        f"has not reached a terminal status after {self.config['timeout']} seconds."
                    )

            mock_run_job.assert_called_once_with(
                account_id=account_id,
                job_id=JOB_ID,
                cause=f"Triggered via Apache Airflow by task {TASK_ID!r} in the {self.dag.dag_id} DAG.",
                steps_override=self.config["steps_override"],
                schema_override=self.config["schema_override"],
                additional_run_config=self.config["additional_run_config"],
            )

            if job_run_status in DbtCloudJobRunStatus.TERMINAL_STATUSES.value:
                assert mock_get_job_run.call_count == 1
            else:
                # When the job run status is not in a terminal status or "Success", the operator will
                # continue to call ``get_job_run()`` until a ``timeout`` number of seconds has passed
                # (3 seconds for this test).  Therefore, there should be 4 calls of this function: one
                # initially and 3 for each check done at a 1 second interval.
                assert mock_get_job_run.call_count == 4

    @patch.object(DbtCloudHook, "trigger_job_run")
    @pytest.mark.parametrize(
        "conn_id, account_id",
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
                additional_run_config=self.config["additional_run_config"],
            )

            mock_get_job_run.assert_not_called()

    @patch.object(DbtCloudHook, "trigger_job_run")
    @pytest.mark.parametrize(
        "conn_id, account_id",
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
                additional_run_config=self.config["additional_run_config"],
            )

    @pytest.mark.parametrize(
        "conn_id, account_id",
        [(ACCOUNT_ID_CONN, None), (NO_ACCOUNT_ID_CONN, ACCOUNT_ID)],
        ids=["default_account", "explicit_account"],
    )
    def test_run_job_operator_link(self, conn_id, account_id, create_task_instance_of_operator, request):
        ti = create_task_instance_of_operator(
            DbtCloudRunJobOperator,
            dag_id="test_dbt_cloud_run_job_op_link",
            execution_date=DEFAULT_DATE,
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

        url = ti.task.get_extra_links(ti, "Monitor Job Run")

        assert url == (
            EXPECTED_JOB_RUN_OP_EXTRA_LINK.format(
                account_id=account_id if account_id else DEFAULT_ACCOUNT_ID,
                project_id=PROJECT_ID,
                run_id=_run_response["data"]["id"],
            )
        )


class TestDbtCloudGetJobRunArtifactOperator:
    def setup_method(self):
        self.dag = DAG("test_dbt_cloud_get_artifact_op", start_date=DEFAULT_DATE)

    @patch("airflow.providers.dbt.cloud.hooks.dbt.DbtCloudHook.get_job_run_artifact")
    @pytest.mark.parametrize(
        "conn_id, account_id",
        [(ACCOUNT_ID_CONN, None), (NO_ACCOUNT_ID_CONN, ACCOUNT_ID)],
        ids=["default_account", "explicit_account"],
    )
    def test_get_json_artifact(self, mock_get_artifact, conn_id, account_id):
        operator = DbtCloudGetJobRunArtifactOperator(
            task_id=TASK_ID,
            dbt_cloud_conn_id=conn_id,
            run_id=RUN_ID,
            account_id=account_id,
            path="path/to/my/manifest.json",
            dag=self.dag,
        )

        mock_get_artifact.return_value.json.return_value = {"data": "file contents"}
        operator.execute(context={})

        mock_get_artifact.assert_called_once_with(
            run_id=RUN_ID,
            path="path/to/my/manifest.json",
            account_id=account_id,
            step=None,
        )

    @patch("airflow.providers.dbt.cloud.hooks.dbt.DbtCloudHook.get_job_run_artifact")
    @pytest.mark.parametrize(
        "conn_id, account_id",
        [(ACCOUNT_ID_CONN, None), (NO_ACCOUNT_ID_CONN, ACCOUNT_ID)],
        ids=["default_account", "explicit_account"],
    )
    def test_get_json_artifact_with_step(self, mock_get_artifact, conn_id, account_id):
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
        operator.execute(context={})

        mock_get_artifact.assert_called_once_with(
            run_id=RUN_ID,
            path="path/to/my/manifest.json",
            account_id=account_id,
            step=2,
        )

    @patch("airflow.providers.dbt.cloud.hooks.dbt.DbtCloudHook.get_job_run_artifact")
    @pytest.mark.parametrize(
        "conn_id, account_id",
        [(ACCOUNT_ID_CONN, None), (NO_ACCOUNT_ID_CONN, ACCOUNT_ID)],
        ids=["default_account", "explicit_account"],
    )
    def test_get_text_artifact(self, mock_get_artifact, conn_id, account_id):
        operator = DbtCloudGetJobRunArtifactOperator(
            task_id=TASK_ID,
            dbt_cloud_conn_id=conn_id,
            run_id=RUN_ID,
            account_id=account_id,
            path="path/to/my/model.sql",
            dag=self.dag,
        )

        mock_get_artifact.return_value.text = "file contents"
        operator.execute(context={})

        mock_get_artifact.assert_called_once_with(
            run_id=RUN_ID,
            path="path/to/my/model.sql",
            account_id=account_id,
            step=None,
        )

    @patch("airflow.providers.dbt.cloud.hooks.dbt.DbtCloudHook.get_job_run_artifact")
    @pytest.mark.parametrize(
        "conn_id, account_id",
        [(ACCOUNT_ID_CONN, None), (NO_ACCOUNT_ID_CONN, ACCOUNT_ID)],
        ids=["default_account", "explicit_account"],
    )
    def test_get_text_artifact_with_step(self, mock_get_artifact, conn_id, account_id):
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
        operator.execute(context={})

        mock_get_artifact.assert_called_once_with(
            run_id=RUN_ID,
            path="path/to/my/model.sql",
            account_id=account_id,
            step=2,
        )


class TestDbtCloudListJobsOperator:
    def setup_method(self):
        self.dag = DAG("test_dbt_cloud_list_jobs_op", start_date=DEFAULT_DATE)
        self.mock_ti = MagicMock()
        self.mock_context = {"ti": self.mock_ti}

    @patch("airflow.providers.dbt.cloud.hooks.dbt.DbtCloudHook.list_jobs")
    @pytest.mark.parametrize(
        "conn_id, account_id",
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
