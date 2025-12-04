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

import json
import re
from copy import deepcopy
from unittest import mock
from unittest.mock import MagicMock, PropertyMock

import httplib2
import pytest
from googleapiclient.errors import HttpError

from airflow.exceptions import AirflowException
from airflow.providers.google.cloud.hooks.cloud_storage_transfer_service import (
    COUNTERS,
    DESCRIPTION,
    FILTER_JOB_NAMES,
    FILTER_PROJECT_ID,
    METADATA,
    OPERATIONS,
    PROJECT_ID,
    STATUS,
    TIME_TO_SLEEP_IN_SECONDS,
    TRANSFER_JOB,
    TRANSFER_JOB_FIELD_MASK,
    TRANSFER_JOBS,
    CloudDataTransferServiceHook,
    GcpTransferJobsStatus,
    GcpTransferOperationStatus,
    gen_job_name,
)

from unit.google.cloud.utils.base_gcp_mock import (
    GCP_PROJECT_ID_HOOK_UNIT_TEST,
    mock_base_gcp_hook_default_project_id,
    mock_base_gcp_hook_no_default_project_id,
)

NAME = "name"

TEST_PROJECT_ID = "project-id"
TEST_TRANSFER_JOB_NAME = "transfer-job"
TEST_CLEAR_JOB_NAME = "jobNames/transfer-job-clear"

TEST_BODY = {DESCRIPTION: "AAA", PROJECT_ID: TEST_PROJECT_ID}

TEST_TRANSFER_OPERATION_NAME = "transfer-operation"

TEST_TRANSFER_JOB = {NAME: TEST_TRANSFER_JOB_NAME}
TEST_TRANSFER_OPERATION = {NAME: TEST_TRANSFER_OPERATION_NAME}

TEST_TRANSFER_JOB_FILTER = {FILTER_PROJECT_ID: TEST_PROJECT_ID, FILTER_JOB_NAMES: [TEST_TRANSFER_JOB_NAME]}
TEST_TRANSFER_OPERATION_FILTER = {
    FILTER_PROJECT_ID: TEST_PROJECT_ID,
    FILTER_JOB_NAMES: [TEST_TRANSFER_JOB_NAME],
}
TEST_UPDATE_TRANSFER_JOB_BODY = {
    TRANSFER_JOB: {DESCRIPTION: "description-1"},
    PROJECT_ID: TEST_PROJECT_ID,
    TRANSFER_JOB_FIELD_MASK: "description",
}

TEST_HTTP_ERR_CODE = 409
TEST_HTTP_ERR_CONTENT = b"Conflict"

TEST_RESULT_STATUS_ENABLED = {STATUS: GcpTransferJobsStatus.ENABLED}
TEST_RESULT_STATUS_DISABLED = {STATUS: GcpTransferJobsStatus.DISABLED}
TEST_RESULT_STATUS_DELETED = {STATUS: GcpTransferJobsStatus.DELETED}

TEST_NAME = "transferOperations/transferJobs-123-456"
TEST_COUNTERS = {
    "bytesFoundFromSource": 512,
    "bytesCopiedToSink": 1024,
}


def _without_key(body, key):
    obj = deepcopy(body)
    del obj[key]
    return obj


def _with_name(body, job_name):
    obj = deepcopy(body)
    obj[NAME] = job_name
    return obj


class TestGCPTransferServiceHookWithPassedName:
    def setup_method(self):
        with mock.patch(
            "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.__init__",
            new=mock_base_gcp_hook_no_default_project_id,
        ):
            self.gct_hook = CloudDataTransferServiceHook(gcp_conn_id="test")

    @mock.patch(
        "airflow.providers.google.cloud.hooks.cloud_storage_transfer_service"
        ".CloudDataTransferServiceHook.enable_transfer_job"
    )
    @mock.patch(
        "airflow.providers.google.cloud.hooks.cloud_storage_transfer_service"
        ".CloudDataTransferServiceHook.get_transfer_job"
    )
    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.project_id",
        new_callable=PropertyMock,
        return_value=None,
    )
    @mock.patch(
        "airflow.providers.google.cloud.hooks.cloud_storage_transfer_service"
        ".CloudDataTransferServiceHook.get_conn"
    )
    def test_pass_name_on_create_job(
        self,
        get_conn: MagicMock,
        project_id: PropertyMock,
        get_transfer_job: MagicMock,
        enable_transfer_job: MagicMock,
    ):
        body = _with_name(TEST_BODY, TEST_CLEAR_JOB_NAME)
        get_conn.side_effect = HttpError(
            httplib2.Response({"status": TEST_HTTP_ERR_CODE}), TEST_HTTP_ERR_CONTENT
        )

        get_transfer_job.return_value = TEST_RESULT_STATUS_DELETED
        with pytest.raises(HttpError):
            # check status DELETED generates new job name
            self.gct_hook.create_transfer_job(body=body)

        # check status DISABLED changes to status ENABLED
        get_transfer_job.return_value = TEST_RESULT_STATUS_DISABLED
        enable_transfer_job.return_value = TEST_RESULT_STATUS_ENABLED

        res = self.gct_hook.create_transfer_job(body=body)
        assert res == TEST_RESULT_STATUS_ENABLED


class TestJobNames:
    re_suffix = re.compile("^[0-9]{10}$")

    @pytest.mark.parametrize("job_name", ["jobNames/new_job", "jobNames/new_job_h", "jobNames/newJob"])
    def test_new_suffix(self, job_name):
        assert self.re_suffix.match(gen_job_name(job_name).split("_")[-1]) is not None


class TestGCPTransferServiceHookWithPassedProjectId:
    def setup_method(self):
        with mock.patch(
            "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.__init__",
            new=mock_base_gcp_hook_no_default_project_id,
        ):
            self.gct_hook = CloudDataTransferServiceHook(gcp_conn_id="test")

    @mock.patch(
        "airflow.providers.google.cloud.hooks.cloud_storage_transfer_service"
        ".CloudDataTransferServiceHook._authorize"
    )
    @mock.patch("airflow.providers.google.cloud.hooks.cloud_storage_transfer_service.build")
    def test_gct_client_creation(self, mock_build, mock_authorize):
        result = self.gct_hook.get_conn()
        mock_build.assert_called_once_with(
            "storagetransfer", "v1", http=mock_authorize.return_value, cache_discovery=False
        )
        assert mock_build.return_value == result
        assert self.gct_hook._conn == result

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.project_id",
        new_callable=PropertyMock,
        return_value=None,
    )
    @mock.patch(
        "airflow.providers.google.cloud.hooks.cloud_storage_transfer_service"
        ".CloudDataTransferServiceHook.get_conn"
    )
    def test_create_transfer_job(self, get_conn, mock_project_id):
        create_method = get_conn.return_value.transferJobs.return_value.create
        execute_method = create_method.return_value.execute
        execute_method.return_value = TEST_TRANSFER_JOB
        res = self.gct_hook.create_transfer_job(body=TEST_BODY)
        assert res == TEST_TRANSFER_JOB
        create_method.assert_called_once_with(body=TEST_BODY)
        execute_method.assert_called_once_with(num_retries=5)

    @mock.patch(
        "airflow.providers.google.cloud.hooks.cloud_storage_transfer_service"
        ".CloudDataTransferServiceHook.get_conn"
    )
    def test_get_transfer_job(self, get_conn):
        get_method = get_conn.return_value.transferJobs.return_value.get
        execute_method = get_method.return_value.execute
        execute_method.return_value = TEST_TRANSFER_JOB
        res = self.gct_hook.get_transfer_job(job_name=TEST_TRANSFER_JOB_NAME, project_id=TEST_PROJECT_ID)
        assert res is not None
        assert res[NAME] == TEST_TRANSFER_JOB_NAME
        get_method.assert_called_once_with(jobName=TEST_TRANSFER_JOB_NAME, projectId=TEST_PROJECT_ID)
        execute_method.assert_called_once_with(num_retries=5)

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.project_id",
        new_callable=PropertyMock,
        return_value=None,
    )
    @mock.patch(
        "airflow.providers.google.cloud.hooks.cloud_storage_transfer_service"
        ".CloudDataTransferServiceHook.get_conn"
    )
    def test_list_transfer_job(self, get_conn, mock_project_id):
        list_method = get_conn.return_value.transferJobs.return_value.list
        list_execute_method = list_method.return_value.execute
        list_execute_method.return_value = {TRANSFER_JOBS: [TEST_TRANSFER_JOB]}

        list_next = get_conn.return_value.transferJobs.return_value.list_next
        list_next.return_value = None

        res = self.gct_hook.list_transfer_job(request_filter=TEST_TRANSFER_JOB_FILTER)
        assert res is not None
        assert res == [TEST_TRANSFER_JOB]
        list_method.assert_called_once_with(filter=mock.ANY)
        args, kwargs = list_method.call_args_list[0]
        assert json.loads(kwargs["filter"]) == {
            FILTER_PROJECT_ID: TEST_PROJECT_ID,
            FILTER_JOB_NAMES: [TEST_TRANSFER_JOB_NAME],
        }
        list_execute_method.assert_called_once_with(num_retries=5)

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.project_id",
        new_callable=PropertyMock,
        return_value=None,
    )
    @mock.patch(
        "airflow.providers.google.cloud.hooks.cloud_storage_transfer_service"
        ".CloudDataTransferServiceHook.get_conn"
    )
    def test_update_transfer_job(self, get_conn, mock_project_id):
        update_method = get_conn.return_value.transferJobs.return_value.patch
        execute_method = update_method.return_value.execute
        execute_method.return_value = TEST_TRANSFER_JOB
        res = self.gct_hook.update_transfer_job(
            job_name=TEST_TRANSFER_JOB_NAME, body=TEST_UPDATE_TRANSFER_JOB_BODY
        )
        assert res is not None
        update_method.assert_called_once_with(
            jobName=TEST_TRANSFER_JOB_NAME, body=TEST_UPDATE_TRANSFER_JOB_BODY
        )
        execute_method.assert_called_once_with(num_retries=5)

    @mock.patch(
        "airflow.providers.google.cloud.hooks.cloud_storage_transfer_service."
        "CloudDataTransferServiceHook.get_conn"
    )
    def test_delete_transfer_job(self, get_conn):
        update_method = get_conn.return_value.transferJobs.return_value.patch
        execute_method = update_method.return_value.execute

        self.gct_hook.delete_transfer_job(job_name=TEST_TRANSFER_JOB_NAME, project_id=TEST_PROJECT_ID)

        update_method.assert_called_once_with(
            jobName=TEST_TRANSFER_JOB_NAME,
            body={
                PROJECT_ID: TEST_PROJECT_ID,
                TRANSFER_JOB: {STATUS: GcpTransferJobsStatus.DELETED},
                TRANSFER_JOB_FIELD_MASK: STATUS,
            },
        )
        execute_method.assert_called_once_with(num_retries=5)

    @mock.patch(
        "airflow.providers.google.cloud.hooks.cloud_storage_transfer_service."
        "CloudDataTransferServiceHook.get_conn"
    )
    def test_run_transfer_job(self, get_conn):
        run_method = get_conn.return_value.transferJobs.return_value.run
        execute_method = run_method.return_value.execute
        execute_method.return_value = TEST_TRANSFER_OPERATION

        res = self.gct_hook.run_transfer_job(job_name=TEST_TRANSFER_JOB_NAME, project_id=TEST_PROJECT_ID)
        assert res == TEST_TRANSFER_OPERATION
        run_method.assert_called_once_with(
            jobName=TEST_TRANSFER_JOB_NAME,
            body={
                PROJECT_ID: TEST_PROJECT_ID,
            },
        )
        execute_method.assert_called_once_with(num_retries=5)

    @mock.patch(
        "airflow.providers.google.cloud.hooks.cloud_storage_transfer_service"
        ".CloudDataTransferServiceHook.get_conn"
    )
    def test_cancel_transfer_operation(self, get_conn):
        cancel_method = get_conn.return_value.transferOperations.return_value.cancel
        execute_method = cancel_method.return_value.execute

        self.gct_hook.cancel_transfer_operation(operation_name=TEST_TRANSFER_OPERATION_NAME)
        cancel_method.assert_called_once_with(name=TEST_TRANSFER_OPERATION_NAME)
        execute_method.assert_called_once_with(num_retries=5)

    @mock.patch(
        "airflow.providers.google.cloud.hooks.cloud_storage_transfer_service"
        ".CloudDataTransferServiceHook.get_conn"
    )
    def test_get_transfer_operation(self, get_conn):
        get_method = get_conn.return_value.transferOperations.return_value.get
        execute_method = get_method.return_value.execute
        execute_method.return_value = TEST_TRANSFER_OPERATION
        res = self.gct_hook.get_transfer_operation(operation_name=TEST_TRANSFER_OPERATION_NAME)
        assert res == TEST_TRANSFER_OPERATION
        get_method.assert_called_once_with(name=TEST_TRANSFER_OPERATION_NAME)
        execute_method.assert_called_once_with(num_retries=5)

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.project_id",
        new_callable=PropertyMock,
        return_value=None,
    )
    @mock.patch(
        "airflow.providers.google.cloud.hooks.cloud_storage_transfer_service"
        ".CloudDataTransferServiceHook.get_conn"
    )
    def test_list_transfer_operation(self, get_conn, mock_project_id):
        list_method = get_conn.return_value.transferOperations.return_value.list
        list_execute_method = list_method.return_value.execute
        list_execute_method.return_value = {OPERATIONS: [TEST_TRANSFER_OPERATION]}

        list_next = get_conn.return_value.transferOperations.return_value.list_next
        list_next.return_value = None

        res = self.gct_hook.list_transfer_operations(request_filter=TEST_TRANSFER_OPERATION_FILTER)
        assert res is not None
        assert res == [TEST_TRANSFER_OPERATION]
        list_method.assert_called_once_with(filter=mock.ANY, name="transferOperations")
        args, kwargs = list_method.call_args_list[0]
        assert json.loads(kwargs["filter"]) == {
            FILTER_PROJECT_ID: TEST_PROJECT_ID,
            FILTER_JOB_NAMES: [TEST_TRANSFER_JOB_NAME],
        }
        list_execute_method.assert_called_once_with(num_retries=5)

    @mock.patch(
        "airflow.providers.google.cloud.hooks.cloud_storage_transfer_service"
        ".CloudDataTransferServiceHook.get_conn"
    )
    def test_pause_transfer_operation(self, get_conn):
        pause_method = get_conn.return_value.transferOperations.return_value.pause
        execute_method = pause_method.return_value.execute

        self.gct_hook.pause_transfer_operation(operation_name=TEST_TRANSFER_OPERATION_NAME)
        pause_method.assert_called_once_with(name=TEST_TRANSFER_OPERATION_NAME)
        execute_method.assert_called_once_with(num_retries=5)

    @mock.patch(
        "airflow.providers.google.cloud.hooks.cloud_storage_transfer_service"
        ".CloudDataTransferServiceHook.get_conn"
    )
    def test_resume_transfer_operation(self, get_conn):
        resume_method = get_conn.return_value.transferOperations.return_value.resume
        execute_method = resume_method.return_value.execute

        self.gct_hook.resume_transfer_operation(operation_name=TEST_TRANSFER_OPERATION_NAME)
        resume_method.assert_called_once_with(name=TEST_TRANSFER_OPERATION_NAME)
        execute_method.assert_called_once_with(num_retries=5)

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.project_id",
        new_callable=PropertyMock,
        return_value=None,
    )
    @mock.patch("airflow.providers.google.cloud.hooks.cloud_storage_transfer_service.time.sleep")
    @mock.patch(
        "airflow.providers.google.cloud.hooks.cloud_storage_transfer_service."
        "CloudDataTransferServiceHook.list_transfer_operations"
    )
    def test_wait_for_transfer_job(self, mock_list, mock_sleep, mock_project_id):
        mock_list.side_effect = [
            [
                {
                    NAME: TEST_NAME,
                    METADATA: {
                        STATUS: GcpTransferOperationStatus.IN_PROGRESS,
                        COUNTERS: TEST_COUNTERS,
                    },
                },
            ],
            [
                {
                    NAME: TEST_NAME,
                    METADATA: {
                        STATUS: GcpTransferOperationStatus.SUCCESS,
                        COUNTERS: TEST_COUNTERS,
                    },
                },
            ],
        ]

        job_name = "transferJobs/test-job"
        self.gct_hook.wait_for_transfer_job({PROJECT_ID: TEST_PROJECT_ID, "name": job_name})

        calls = [
            mock.call(request_filter={FILTER_PROJECT_ID: TEST_PROJECT_ID, FILTER_JOB_NAMES: [job_name]}),
            mock.call(request_filter={FILTER_PROJECT_ID: TEST_PROJECT_ID, FILTER_JOB_NAMES: [job_name]}),
        ]
        mock_list.assert_has_calls(calls, any_order=True)

        mock_sleep.assert_called_once_with(TIME_TO_SLEEP_IN_SECONDS)

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.project_id",
        new_callable=PropertyMock,
        return_value=None,
    )
    @mock.patch("airflow.providers.google.cloud.hooks.cloud_storage_transfer_service.time.sleep")
    @mock.patch(
        "airflow.providers.google.cloud.hooks.cloud_storage_transfer_service"
        ".CloudDataTransferServiceHook.get_conn"
    )
    def test_wait_for_transfer_job_failed(self, mock_get_conn, mock_sleep, mock_project_id):
        list_method = mock_get_conn.return_value.transferOperations.return_value.list
        list_execute_method = list_method.return_value.execute
        list_execute_method.return_value = {
            OPERATIONS: [
                {
                    NAME: TEST_TRANSFER_OPERATION_NAME,
                    METADATA: {
                        STATUS: GcpTransferOperationStatus.FAILED,
                        COUNTERS: TEST_COUNTERS,
                    },
                }
            ]
        }

        mock_get_conn.return_value.transferOperations.return_value.list_next.return_value = None

        with pytest.raises(AirflowException):
            self.gct_hook.wait_for_transfer_job({PROJECT_ID: TEST_PROJECT_ID, NAME: "transferJobs/test-job"})
        assert list_method.called

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.project_id",
        new_callable=PropertyMock,
        return_value=None,
    )
    @mock.patch("airflow.providers.google.cloud.hooks.cloud_storage_transfer_service.time.sleep")
    @mock.patch(
        "airflow.providers.google.cloud.hooks.cloud_storage_transfer_service"
        ".CloudDataTransferServiceHook.get_conn"
    )
    def test_wait_for_transfer_job_expect_failed(self, get_conn, mock_sleep, mock_project_id):
        list_method = get_conn.return_value.transferOperations.return_value.list
        list_execute_method = list_method.return_value.execute
        list_execute_method.return_value = {
            OPERATIONS: [
                {
                    NAME: TEST_TRANSFER_OPERATION_NAME,
                    METADATA: {
                        STATUS: GcpTransferOperationStatus.FAILED,
                        COUNTERS: TEST_COUNTERS,
                    },
                }
            ]
        }

        get_conn.return_value.transferOperations.return_value.list_next.return_value = None
        with pytest.raises(
            AirflowException, match="An unexpected operation status was encountered. Expected: SUCCESS"
        ):
            self.gct_hook.wait_for_transfer_job(
                job={PROJECT_ID: "test-project", NAME: "transferJobs/test-job"},
                expected_statuses=GcpTransferOperationStatus.SUCCESS,
            )

    @pytest.mark.parametrize(
        ("statuses", "expected_statuses"),
        [
            ([GcpTransferOperationStatus.ABORTED], (GcpTransferOperationStatus.IN_PROGRESS,)),
            (
                [GcpTransferOperationStatus.SUCCESS, GcpTransferOperationStatus.ABORTED],
                (GcpTransferOperationStatus.IN_PROGRESS,),
            ),
            (
                [GcpTransferOperationStatus.PAUSED, GcpTransferOperationStatus.ABORTED],
                (GcpTransferOperationStatus.IN_PROGRESS,),
            ),
        ],
    )
    def test_operations_contain_expected_statuses_red_path(self, statuses, expected_statuses):
        operations = [{NAME: TEST_TRANSFER_OPERATION_NAME, METADATA: {STATUS: status}} for status in statuses]

        with pytest.raises(
            AirflowException,
            match=f"An unexpected operation status was encountered. Expected: {', '.join(expected_statuses)}",
        ):
            CloudDataTransferServiceHook.operations_contain_expected_statuses(
                operations, GcpTransferOperationStatus.IN_PROGRESS
            )

    @pytest.mark.parametrize(
        ("statuses", "expected_statuses"),
        [
            ([GcpTransferOperationStatus.ABORTED], GcpTransferOperationStatus.ABORTED),
            (
                [GcpTransferOperationStatus.SUCCESS, GcpTransferOperationStatus.ABORTED],
                GcpTransferOperationStatus.ABORTED,
            ),
            (
                [GcpTransferOperationStatus.PAUSED, GcpTransferOperationStatus.ABORTED],
                GcpTransferOperationStatus.ABORTED,
            ),
            ([GcpTransferOperationStatus.ABORTED], (GcpTransferOperationStatus.ABORTED,)),
            (
                [GcpTransferOperationStatus.SUCCESS, GcpTransferOperationStatus.ABORTED],
                (GcpTransferOperationStatus.ABORTED,),
            ),
            (
                [GcpTransferOperationStatus.PAUSED, GcpTransferOperationStatus.ABORTED],
                (GcpTransferOperationStatus.ABORTED,),
            ),
        ],
    )
    def test_operations_contain_expected_statuses_green_path(self, statuses, expected_statuses):
        operations = [
            {NAME: TEST_TRANSFER_OPERATION_NAME, METADATA: {STATUS: status, COUNTERS: TEST_COUNTERS}}
            for status in statuses
        ]

        result = CloudDataTransferServiceHook.operations_contain_expected_statuses(
            operations, expected_statuses
        )

        assert result


class TestGCPTransferServiceHookWithProjectIdFromConnection:
    def setup_method(self):
        with mock.patch(
            "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.__init__",
            new=mock_base_gcp_hook_default_project_id,
        ):
            self.gct_hook = CloudDataTransferServiceHook(gcp_conn_id="test")

    @mock.patch(
        "airflow.providers.google.cloud.hooks.cloud_storage_transfer_service"
        ".CloudDataTransferServiceHook._authorize"
    )
    @mock.patch("airflow.providers.google.cloud.hooks.cloud_storage_transfer_service.build")
    def test_gct_client_creation(self, mock_build, mock_authorize):
        result = self.gct_hook.get_conn()
        mock_build.assert_called_once_with(
            "storagetransfer", "v1", http=mock_authorize.return_value, cache_discovery=False
        )
        assert mock_build.return_value == result
        assert self.gct_hook._conn == result

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.project_id",
        new_callable=PropertyMock,
        return_value=GCP_PROJECT_ID_HOOK_UNIT_TEST,
    )
    @mock.patch(
        "airflow.providers.google.cloud.hooks.cloud_storage_transfer_service"
        ".CloudDataTransferServiceHook.get_conn"
    )
    def test_create_transfer_job(self, get_conn, mock_project_id):
        create_method = get_conn.return_value.transferJobs.return_value.create
        execute_method = create_method.return_value.execute
        execute_method.return_value = deepcopy(TEST_TRANSFER_JOB)
        res = self.gct_hook.create_transfer_job(body=self._without_project_id(TEST_BODY))
        assert res == TEST_TRANSFER_JOB
        create_method.assert_called_once_with(body=self._with_project_id(TEST_BODY, "example-project"))
        execute_method.assert_called_once_with(num_retries=5)

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.project_id",
        new_callable=PropertyMock,
        return_value=GCP_PROJECT_ID_HOOK_UNIT_TEST,
    )
    @mock.patch(
        "airflow.providers.google.cloud.hooks.cloud_storage_transfer_service"
        ".CloudDataTransferServiceHook.get_conn"
    )
    def test_get_transfer_job(self, get_conn, mock_project_id):
        get_method = get_conn.return_value.transferJobs.return_value.get
        execute_method = get_method.return_value.execute
        execute_method.return_value = TEST_TRANSFER_JOB
        res = self.gct_hook.get_transfer_job(job_name=TEST_TRANSFER_JOB_NAME)
        assert res is not None
        assert res[NAME] == TEST_TRANSFER_JOB_NAME
        get_method.assert_called_once_with(jobName=TEST_TRANSFER_JOB_NAME, projectId="example-project")
        execute_method.assert_called_once_with(num_retries=5)

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.project_id",
        new_callable=PropertyMock,
        return_value=GCP_PROJECT_ID_HOOK_UNIT_TEST,
    )
    @mock.patch(
        "airflow.providers.google.cloud.hooks.cloud_storage_transfer_service"
        ".CloudDataTransferServiceHook.get_conn"
    )
    def test_list_transfer_job(self, get_conn, mock_project_id):
        list_method = get_conn.return_value.transferJobs.return_value.list
        list_execute_method = list_method.return_value.execute
        list_execute_method.return_value = {TRANSFER_JOBS: [TEST_TRANSFER_JOB]}

        list_next = get_conn.return_value.transferJobs.return_value.list_next
        list_next.return_value = None

        res = self.gct_hook.list_transfer_job(
            request_filter=_without_key(TEST_TRANSFER_JOB_FILTER, FILTER_PROJECT_ID)
        )
        assert res is not None
        assert res == [TEST_TRANSFER_JOB]

        list_method.assert_called_once_with(filter=mock.ANY)
        args, kwargs = list_method.call_args_list[0]
        assert json.loads(kwargs["filter"]) == {
            FILTER_PROJECT_ID: "example-project",
            FILTER_JOB_NAMES: [TEST_TRANSFER_JOB_NAME],
        }
        list_execute_method.assert_called_once_with(num_retries=5)

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.project_id",
        new_callable=PropertyMock,
        return_value=GCP_PROJECT_ID_HOOK_UNIT_TEST,
    )
    @mock.patch(
        "airflow.providers.google.cloud.hooks.cloud_storage_transfer_service"
        ".CloudDataTransferServiceHook.get_conn"
    )
    def test_update_transfer_job(self, get_conn, mock_project_id):
        update_method = get_conn.return_value.transferJobs.return_value.patch
        execute_method = update_method.return_value.execute
        execute_method.return_value = TEST_TRANSFER_JOB
        res = self.gct_hook.update_transfer_job(
            job_name=TEST_TRANSFER_JOB_NAME, body=self._without_project_id(TEST_UPDATE_TRANSFER_JOB_BODY)
        )
        assert res is not None
        update_method.assert_called_once_with(
            jobName=TEST_TRANSFER_JOB_NAME,
            body=self._with_project_id(TEST_UPDATE_TRANSFER_JOB_BODY, "example-project"),
        )
        execute_method.assert_called_once_with(num_retries=5)

    @mock.patch(
        "airflow.providers.google.cloud.hooks.cloud_storage_transfer_service"
        ".CloudDataTransferServiceHook.get_conn"
    )
    def test_delete_transfer_job(self, get_conn):
        update_method = get_conn.return_value.transferJobs.return_value.patch
        execute_method = update_method.return_value.execute

        self.gct_hook.delete_transfer_job(job_name=TEST_TRANSFER_JOB_NAME, project_id=TEST_PROJECT_ID)

        update_method.assert_called_once_with(
            jobName=TEST_TRANSFER_JOB_NAME,
            body={
                PROJECT_ID: TEST_PROJECT_ID,
                TRANSFER_JOB: {STATUS: "DELETED"},
                TRANSFER_JOB_FIELD_MASK: STATUS,
            },
        )
        execute_method.assert_called_once_with(num_retries=5)

    @mock.patch(
        "airflow.providers.google.cloud.hooks.cloud_storage_transfer_service"
        ".CloudDataTransferServiceHook.get_conn"
    )
    def test_cancel_transfer_operation(self, get_conn):
        cancel_method = get_conn.return_value.transferOperations.return_value.cancel
        execute_method = cancel_method.return_value.execute

        self.gct_hook.cancel_transfer_operation(operation_name=TEST_TRANSFER_OPERATION_NAME)
        cancel_method.assert_called_once_with(name=TEST_TRANSFER_OPERATION_NAME)
        execute_method.assert_called_once_with(num_retries=5)

    @mock.patch(
        "airflow.providers.google.cloud.hooks.cloud_storage_transfer_service"
        ".CloudDataTransferServiceHook.get_conn"
    )
    def test_get_transfer_operation(self, get_conn):
        get_method = get_conn.return_value.transferOperations.return_value.get
        execute_method = get_method.return_value.execute
        execute_method.return_value = TEST_TRANSFER_OPERATION
        res = self.gct_hook.get_transfer_operation(operation_name=TEST_TRANSFER_OPERATION_NAME)
        assert res == TEST_TRANSFER_OPERATION
        get_method.assert_called_once_with(name=TEST_TRANSFER_OPERATION_NAME)
        execute_method.assert_called_once_with(num_retries=5)

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.project_id",
        new_callable=PropertyMock,
        return_value=GCP_PROJECT_ID_HOOK_UNIT_TEST,
    )
    @mock.patch(
        "airflow.providers.google.cloud.hooks.cloud_storage_transfer_service"
        ".CloudDataTransferServiceHook.get_conn"
    )
    def test_list_transfer_operation(self, get_conn, mock_project_id):
        list_method = get_conn.return_value.transferOperations.return_value.list
        list_execute_method = list_method.return_value.execute
        list_execute_method.return_value = {OPERATIONS: [TEST_TRANSFER_OPERATION]}

        list_next = get_conn.return_value.transferOperations.return_value.list_next
        list_next.return_value = None

        res = self.gct_hook.list_transfer_operations(
            request_filter=_without_key(TEST_TRANSFER_OPERATION_FILTER, FILTER_PROJECT_ID)
        )
        assert res is not None
        assert res == [TEST_TRANSFER_OPERATION]
        list_method.assert_called_once_with(filter=mock.ANY, name="transferOperations")
        args, kwargs = list_method.call_args_list[0]
        assert json.loads(kwargs["filter"]) == {
            FILTER_PROJECT_ID: "example-project",
            FILTER_JOB_NAMES: [TEST_TRANSFER_JOB_NAME],
        }
        list_execute_method.assert_called_once_with(num_retries=5)

    @staticmethod
    def _without_project_id(body):
        body = deepcopy(body)
        del body[PROJECT_ID]
        return body

    @staticmethod
    def _with_project_id(body, project_id):
        body = deepcopy(body)
        del body[PROJECT_ID]
        body[PROJECT_ID] = project_id
        return body


class TestGCPTransferServiceHookWithoutProjectId:
    def setup_method(self):
        with mock.patch(
            "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.__init__",
            new=mock_base_gcp_hook_no_default_project_id,
        ):
            self.gct_hook = CloudDataTransferServiceHook(gcp_conn_id="test")

    @mock.patch(
        "airflow.providers.google.cloud.hooks.cloud_storage_transfer_service"
        ".CloudDataTransferServiceHook._authorize"
    )
    @mock.patch("airflow.providers.google.cloud.hooks.cloud_storage_transfer_service.build")
    def test_gct_client_creation(self, mock_build, mock_authorize):
        result = self.gct_hook.get_conn()
        mock_build.assert_called_once_with(
            "storagetransfer", "v1", http=mock_authorize.return_value, cache_discovery=False
        )
        assert mock_build.return_value == result
        assert self.gct_hook._conn == result

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.project_id",
        new_callable=PropertyMock,
        return_value=None,
    )
    @mock.patch(
        "airflow.providers.google.cloud.hooks.cloud_storage_transfer_service"
        ".CloudDataTransferServiceHook.get_conn"
    )
    def test_create_transfer_job(self, get_conn, mock_project_id):
        create_method = get_conn.return_value.transferJobs.return_value.create
        execute_method = create_method.return_value.execute
        execute_method.return_value = deepcopy(TEST_TRANSFER_JOB)
        with pytest.raises(AirflowException) as ctx:
            self.gct_hook.create_transfer_job(body=_without_key(TEST_BODY, PROJECT_ID))

        assert (
            str(ctx.value) == "The project id must be passed either as `projectId` key in `body` "
            "parameter or as project_id "
            "extra in Google Cloud connection definition. Both are not set!"
        )

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.project_id",
        new_callable=PropertyMock,
        return_value=None,
    )
    @mock.patch(
        "airflow.providers.google.cloud.hooks.cloud_storage_transfer_service"
        ".CloudDataTransferServiceHook.get_conn"
    )
    def test_get_transfer_job(self, get_conn, mock_project_id):
        get_method = get_conn.return_value.transferJobs.return_value.get
        execute_method = get_method.return_value.execute
        execute_method.return_value = TEST_TRANSFER_JOB
        with pytest.raises(AirflowException) as ctx:
            self.gct_hook.get_transfer_job(job_name=TEST_TRANSFER_JOB_NAME)
        assert (
            str(ctx.value) == "The project id must be passed either as keyword project_id "
            "parameter or as project_id extra in Google Cloud connection definition. "
            "Both are not set!"
        )

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.project_id",
        new_callable=PropertyMock,
        return_value=None,
    )
    @mock.patch(
        "airflow.providers.google.cloud.hooks.cloud_storage_transfer_service"
        ".CloudDataTransferServiceHook.get_conn"
    )
    def test_list_transfer_job(self, get_conn, mock_project_id):
        list_method = get_conn.return_value.transferJobs.return_value.list
        list_execute_method = list_method.return_value.execute
        list_execute_method.return_value = {"transferJobs": [TEST_TRANSFER_JOB]}

        list_next = get_conn.return_value.transferJobs.return_value.list_next
        list_next.return_value = None

        with pytest.raises(AirflowException) as ctx:
            self.gct_hook.list_transfer_job(
                request_filter=_without_key(TEST_TRANSFER_JOB_FILTER, FILTER_PROJECT_ID)
            )

        assert (
            str(ctx.value)
            == "The project id must be passed either as `project_id` key in `filter` parameter or as "
            "project_id extra in Google Cloud connection definition. Both are not set!"
        )

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.project_id",
        new_callable=PropertyMock,
        return_value=None,
    )
    @mock.patch(
        "airflow.providers.google.cloud.hooks.cloud_storage_transfer_service"
        ".CloudDataTransferServiceHook.get_conn"
    )
    def test_list_transfer_operation_multiple_page(self, get_conn, mock_project_id):
        pages_requests = [
            mock.Mock(**{"execute.return_value": {"operations": [TEST_TRANSFER_OPERATION]}}) for _ in range(4)
        ]
        transfer_operation_mock = mock.Mock(
            **{"list.return_value": pages_requests[1], "list_next.side_effect": pages_requests[1:] + [None]}
        )

        get_conn.return_value.transferOperations.return_value = transfer_operation_mock

        res = self.gct_hook.list_transfer_operations(request_filter=TEST_TRANSFER_OPERATION_FILTER)
        assert res == [TEST_TRANSFER_OPERATION] * 4

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.project_id",
        new_callable=PropertyMock,
        return_value=None,
    )
    @mock.patch(
        "airflow.providers.google.cloud.hooks.cloud_storage_transfer_service"
        ".CloudDataTransferServiceHook.get_conn"
    )
    def test_update_transfer_job(self, get_conn, mock_project_id):
        update_method = get_conn.return_value.transferJobs.return_value.patch
        execute_method = update_method.return_value.execute
        execute_method.return_value = TEST_TRANSFER_JOB
        with pytest.raises(AirflowException) as ctx:
            self.gct_hook.update_transfer_job(
                job_name=TEST_TRANSFER_JOB_NAME, body=_without_key(TEST_UPDATE_TRANSFER_JOB_BODY, PROJECT_ID)
            )

        assert (
            str(ctx.value)
            == "The project id must be passed either as `projectId` key in `body` parameter or as project_id "
            "extra in Google Cloud connection definition. Both are not set!"
        )

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.project_id",
        new_callable=PropertyMock,
        return_value=None,
    )
    @mock.patch(
        "airflow.providers.google.cloud.hooks.cloud_storage_transfer_service"
        ".CloudDataTransferServiceHook.get_conn"
    )
    def test_delete_transfer_job(self, get_conn, mock_project_id):
        with pytest.raises(AirflowException) as ctx:
            self.gct_hook.delete_transfer_job(job_name=TEST_TRANSFER_JOB_NAME)

        assert (
            str(ctx.value)
            == "The project id must be passed either as keyword project_id parameter or as project_id extra in "
            "Google Cloud connection definition. Both are not set!"
        )

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.project_id",
        new_callable=PropertyMock,
        return_value=None,
    )
    @mock.patch(
        "airflow.providers.google.cloud.hooks.cloud_storage_transfer_service"
        ".CloudDataTransferServiceHook.get_conn"
    )
    def test_list_transfer_operation(self, get_conn, mock_project_id):
        list_method = get_conn.return_value.transferOperations.return_value.list
        list_execute_method = list_method.return_value.execute
        list_execute_method.return_value = {"operations": [TEST_TRANSFER_OPERATION]}

        list_next = get_conn.return_value.transferOperations.return_value.list_next
        list_next.return_value = None

        with pytest.raises(AirflowException) as ctx:
            self.gct_hook.list_transfer_operations(
                request_filter=_without_key(TEST_TRANSFER_OPERATION_FILTER, FILTER_PROJECT_ID)
            )

        assert (
            str(ctx.value)
            == "The project id must be passed either as `project_id` key in `filter` parameter or as project_id "
            "extra in Google Cloud connection definition. Both are not set!"
        )
