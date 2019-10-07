# -*- coding: utf-8 -*-
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
#
import json
import unittest
from copy import deepcopy

from parameterized import parameterized

from airflow import AirflowException
from airflow.gcp.hooks.cloud_storage_transfer_service import (
    DESCRIPTION, FILTER_JOB_NAMES, FILTER_PROJECT_ID, METADATA, OPERATIONS, PROJECT_ID, STATUS,
    TIME_TO_SLEEP_IN_SECONDS, TRANSFER_JOB, TRANSFER_JOB_FIELD_MASK, TRANSFER_JOBS, GcpTransferJobsStatus,
    GcpTransferOperationStatus, GCPTransferServiceHook,
)
from tests.compat import PropertyMock, mock
from tests.gcp.utils.base_gcp_mock import (
    GCP_PROJECT_ID_HOOK_UNIT_TEST, mock_base_gcp_hook_default_project_id,
    mock_base_gcp_hook_no_default_project_id,
)

NAME = "name"

TEST_PROJECT_ID = 'project-id'
TEST_BODY = {DESCRIPTION: 'AAA', PROJECT_ID: TEST_PROJECT_ID}

TEST_TRANSFER_JOB_NAME = "transfer-job"
TEST_TRANSFER_OPERATION_NAME = "transfer-operation"

TEST_TRANSFER_JOB = {NAME: TEST_TRANSFER_JOB_NAME}
TEST_TRANSFER_OPERATION = {NAME: TEST_TRANSFER_OPERATION_NAME}

TEST_TRANSFER_JOB_FILTER = {FILTER_PROJECT_ID: TEST_PROJECT_ID, FILTER_JOB_NAMES: [TEST_TRANSFER_JOB_NAME]}
TEST_TRANSFER_OPERATION_FILTER = {
    FILTER_PROJECT_ID: TEST_PROJECT_ID,
    FILTER_JOB_NAMES: [TEST_TRANSFER_JOB_NAME],
}
TEST_UPDATE_TRANSFER_JOB_BODY = {
    TRANSFER_JOB: {DESCRIPTION: 'description-1'},
    PROJECT_ID: TEST_PROJECT_ID,
    TRANSFER_JOB_FIELD_MASK: 'description',
}


def _without_key(body, key):
    obj = deepcopy(body)
    del obj[key]
    return obj


class TestGCPTransferServiceHookWithPassedProjectId(unittest.TestCase):
    def setUp(self):
        with mock.patch(
            'airflow.gcp.hooks.base.GoogleCloudBaseHook.__init__',
            new=mock_base_gcp_hook_no_default_project_id,
        ):
            self.gct_hook = GCPTransferServiceHook(gcp_conn_id='test')

    @mock.patch("airflow.gcp.hooks.cloud_storage_transfer_service.GCPTransferServiceHook._authorize")
    @mock.patch("airflow.gcp.hooks.cloud_storage_transfer_service.build")
    def test_gct_client_creation(self, mock_build, mock_authorize):
        result = self.gct_hook.get_conn()
        mock_build.assert_called_once_with(
            'storagetransfer', 'v1', http=mock_authorize.return_value, cache_discovery=False
        )
        self.assertEqual(mock_build.return_value, result)
        self.assertEqual(self.gct_hook._conn, result)

    @mock.patch(
        'airflow.gcp.hooks.base.GoogleCloudBaseHook.project_id',
        new_callable=PropertyMock,
        return_value=None
    )
    @mock.patch('airflow.gcp.hooks.cloud_storage_transfer_service.GCPTransferServiceHook.get_conn')
    def test_create_transfer_job(self, get_conn, mock_project_id):
        create_method = get_conn.return_value.transferJobs.return_value.create
        execute_method = create_method.return_value.execute
        execute_method.return_value = TEST_TRANSFER_JOB
        res = self.gct_hook.create_transfer_job(body=TEST_BODY)
        self.assertEqual(res, TEST_TRANSFER_JOB)
        create_method.assert_called_once_with(body=TEST_BODY)
        execute_method.assert_called_once_with(num_retries=5)

    @mock.patch('airflow.gcp.hooks.cloud_storage_transfer_service.GCPTransferServiceHook.get_conn')
    def test_get_transfer_job(self, get_conn):
        get_method = get_conn.return_value.transferJobs.return_value.get
        execute_method = get_method.return_value.execute
        execute_method.return_value = TEST_TRANSFER_JOB
        res = self.gct_hook.get_transfer_job(job_name=TEST_TRANSFER_JOB_NAME, project_id=TEST_PROJECT_ID)
        self.assertIsNotNone(res)
        self.assertEqual(TEST_TRANSFER_JOB_NAME, res[NAME])
        get_method.assert_called_once_with(jobName=TEST_TRANSFER_JOB_NAME, projectId=TEST_PROJECT_ID)
        execute_method.assert_called_once_with(num_retries=5)

    @mock.patch(
        'airflow.gcp.hooks.base.GoogleCloudBaseHook.project_id',
        new_callable=PropertyMock,
        return_value=None
    )
    @mock.patch('airflow.gcp.hooks.cloud_storage_transfer_service.GCPTransferServiceHook.get_conn')
    def test_list_transfer_job(self, get_conn, mock_project_id):
        list_method = get_conn.return_value.transferJobs.return_value.list
        list_execute_method = list_method.return_value.execute
        list_execute_method.return_value = {TRANSFER_JOBS: [TEST_TRANSFER_JOB]}

        list_next = get_conn.return_value.transferJobs.return_value.list_next
        list_next.return_value = None

        res = self.gct_hook.list_transfer_job(request_filter=TEST_TRANSFER_JOB_FILTER)
        self.assertIsNotNone(res)
        self.assertEqual(res, [TEST_TRANSFER_JOB])
        list_method.assert_called_once_with(filter=mock.ANY)
        args, kwargs = list_method.call_args_list[0]
        self.assertEqual(
            json.loads(kwargs['filter']),
            {FILTER_PROJECT_ID: TEST_PROJECT_ID, FILTER_JOB_NAMES: [TEST_TRANSFER_JOB_NAME]},
        )
        list_execute_method.assert_called_once_with(num_retries=5)

    @mock.patch(
        'airflow.gcp.hooks.base.GoogleCloudBaseHook.project_id',
        new_callable=PropertyMock,
        return_value=None
    )
    @mock.patch('airflow.gcp.hooks.cloud_storage_transfer_service.GCPTransferServiceHook.get_conn')
    def test_update_transfer_job(self, get_conn, mock_project_id):
        update_method = get_conn.return_value.transferJobs.return_value.patch
        execute_method = update_method.return_value.execute
        execute_method.return_value = TEST_TRANSFER_JOB
        res = self.gct_hook.update_transfer_job(
            job_name=TEST_TRANSFER_JOB_NAME, body=TEST_UPDATE_TRANSFER_JOB_BODY
        )
        self.assertIsNotNone(res)
        update_method.assert_called_once_with(
            jobName=TEST_TRANSFER_JOB_NAME, body=TEST_UPDATE_TRANSFER_JOB_BODY
        )
        execute_method.assert_called_once_with(num_retries=5)

    @mock.patch('airflow.gcp.hooks.cloud_storage_transfer_service.GCPTransferServiceHook.get_conn')
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

    @mock.patch('airflow.gcp.hooks.cloud_storage_transfer_service.GCPTransferServiceHook.get_conn')
    def test_cancel_transfer_operation(self, get_conn):
        cancel_method = get_conn.return_value.transferOperations.return_value.cancel
        execute_method = cancel_method.return_value.execute

        self.gct_hook.cancel_transfer_operation(operation_name=TEST_TRANSFER_OPERATION_NAME)
        cancel_method.assert_called_once_with(name=TEST_TRANSFER_OPERATION_NAME)
        execute_method.assert_called_once_with(num_retries=5)

    @mock.patch('airflow.gcp.hooks.cloud_storage_transfer_service.GCPTransferServiceHook.get_conn')
    def test_get_transfer_operation(self, get_conn):
        get_method = get_conn.return_value.transferOperations.return_value.get
        execute_method = get_method.return_value.execute
        execute_method.return_value = TEST_TRANSFER_OPERATION
        res = self.gct_hook.get_transfer_operation(operation_name=TEST_TRANSFER_OPERATION_NAME)
        self.assertEqual(res, TEST_TRANSFER_OPERATION)
        get_method.assert_called_once_with(name=TEST_TRANSFER_OPERATION_NAME)
        execute_method.assert_called_once_with(num_retries=5)

    @mock.patch(
        'airflow.gcp.hooks.base.GoogleCloudBaseHook.project_id',
        new_callable=PropertyMock,
        return_value=None
    )
    @mock.patch('airflow.gcp.hooks.cloud_storage_transfer_service.GCPTransferServiceHook.get_conn')
    def test_list_transfer_operation(self, get_conn, mock_project_id):
        list_method = get_conn.return_value.transferOperations.return_value.list
        list_execute_method = list_method.return_value.execute
        list_execute_method.return_value = {OPERATIONS: [TEST_TRANSFER_OPERATION]}

        list_next = get_conn.return_value.transferOperations.return_value.list_next
        list_next.return_value = None

        res = self.gct_hook.list_transfer_operations(request_filter=TEST_TRANSFER_OPERATION_FILTER)
        self.assertIsNotNone(res)
        self.assertEqual(res, [TEST_TRANSFER_OPERATION])
        list_method.assert_called_once_with(filter=mock.ANY, name='transferOperations')
        args, kwargs = list_method.call_args_list[0]
        self.assertEqual(
            json.loads(kwargs['filter']),
            {FILTER_PROJECT_ID: TEST_PROJECT_ID, FILTER_JOB_NAMES: [TEST_TRANSFER_JOB_NAME]},
        )
        list_execute_method.assert_called_once_with(num_retries=5)

    @mock.patch('airflow.gcp.hooks.cloud_storage_transfer_service.GCPTransferServiceHook.get_conn')
    def test_pause_transfer_operation(self, get_conn):
        pause_method = get_conn.return_value.transferOperations.return_value.pause
        execute_method = pause_method.return_value.execute

        self.gct_hook.pause_transfer_operation(operation_name=TEST_TRANSFER_OPERATION_NAME)
        pause_method.assert_called_once_with(name=TEST_TRANSFER_OPERATION_NAME)
        execute_method.assert_called_once_with(num_retries=5)

    @mock.patch('airflow.gcp.hooks.cloud_storage_transfer_service.GCPTransferServiceHook.get_conn')
    def test_resume_transfer_operation(self, get_conn):
        resume_method = get_conn.return_value.transferOperations.return_value.resume
        execute_method = resume_method.return_value.execute

        self.gct_hook.resume_transfer_operation(operation_name=TEST_TRANSFER_OPERATION_NAME)
        resume_method.assert_called_once_with(name=TEST_TRANSFER_OPERATION_NAME)
        execute_method.assert_called_once_with(num_retries=5)

    @mock.patch(
        'airflow.gcp.hooks.base.GoogleCloudBaseHook.project_id',
        new_callable=PropertyMock,
        return_value=None
    )
    @mock.patch('airflow.gcp.hooks.cloud_storage_transfer_service.time.sleep')
    @mock.patch('airflow.gcp.hooks.cloud_storage_transfer_service.'
                'GCPTransferServiceHook.list_transfer_operations')
    def test_wait_for_transfer_job(self, mock_list, mock_sleep, mock_project_id):
        mock_list.side_effect = [
            [{METADATA: {STATUS: GcpTransferOperationStatus.IN_PROGRESS}}],
            [{METADATA: {STATUS: GcpTransferOperationStatus.SUCCESS}}],
        ]

        job_name = 'transferJobs/test-job'
        self.gct_hook.wait_for_transfer_job({PROJECT_ID: TEST_PROJECT_ID, 'name': job_name})

        calls = [
            mock.call(request_filter={FILTER_PROJECT_ID: TEST_PROJECT_ID, FILTER_JOB_NAMES: [job_name]}),
            mock.call(request_filter={FILTER_PROJECT_ID: TEST_PROJECT_ID, FILTER_JOB_NAMES: [job_name]})
        ]
        mock_list.assert_has_calls(calls, any_order=True)

        mock_sleep.assert_called_once_with(TIME_TO_SLEEP_IN_SECONDS)

    @mock.patch(
        'airflow.gcp.hooks.base.GoogleCloudBaseHook.project_id',
        new_callable=PropertyMock,
        return_value=None
    )
    @mock.patch('airflow.gcp.hooks.cloud_storage_transfer_service.time.sleep')
    @mock.patch('airflow.gcp.hooks.cloud_storage_transfer_service.GCPTransferServiceHook.get_conn')
    def test_wait_for_transfer_job_failed(
        self, mock_get_conn, mock_sleep, mock_project_id
    ):
        list_method = mock_get_conn.return_value.transferOperations.return_value.list
        list_execute_method = list_method.return_value.execute
        list_execute_method.return_value = {
            OPERATIONS: [
                {NAME: TEST_TRANSFER_OPERATION_NAME, METADATA: {STATUS: GcpTransferOperationStatus.FAILED}}
            ]
        }

        mock_get_conn.return_value.transferOperations.return_value.list_next.return_value = None

        with self.assertRaises(AirflowException):
            self.gct_hook.wait_for_transfer_job({PROJECT_ID: TEST_PROJECT_ID, NAME: 'transferJobs/test-job'})
            self.assertTrue(list_method.called)

    @mock.patch(
        'airflow.gcp.hooks.base.GoogleCloudBaseHook.project_id',
        new_callable=PropertyMock,
        return_value=None
    )
    @mock.patch('airflow.gcp.hooks.cloud_storage_transfer_service.time.sleep')
    @mock.patch('airflow.gcp.hooks.cloud_storage_transfer_service.GCPTransferServiceHook.get_conn')
    def test_wait_for_transfer_job_expect_failed(
        self, get_conn, mock_sleep, mock_project_id
    ):  # pylint: disable=unused-argument
        list_method = get_conn.return_value.transferOperations.return_value.list
        list_execute_method = list_method.return_value.execute
        list_execute_method.return_value = {
            OPERATIONS: [
                {NAME: TEST_TRANSFER_OPERATION_NAME, METADATA: {STATUS: GcpTransferOperationStatus.FAILED}}
            ]
        }

        get_conn.return_value.transferOperations.return_value.list_next.return_value = None
        with self.assertRaisesRegex(
            AirflowException, "An unexpected operation status was encountered. Expected: SUCCESS"
        ):
            self.gct_hook.wait_for_transfer_job(
                job={PROJECT_ID: 'test-project', NAME: 'transferJobs/test-job'},
                expected_statuses=GcpTransferOperationStatus.SUCCESS,
            )

    @parameterized.expand(
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
            ([GcpTransferOperationStatus.ABORTED], (GcpTransferOperationStatus.IN_PROGRESS,)),
            (
                [GcpTransferOperationStatus.SUCCESS, GcpTransferOperationStatus.ABORTED],
                (GcpTransferOperationStatus.IN_PROGRESS,),
            ),
            (
                [GcpTransferOperationStatus.PAUSED, GcpTransferOperationStatus.ABORTED],
                (GcpTransferOperationStatus.IN_PROGRESS,),
            ),
        ]
    )
    def test_operations_contain_expected_statuses_red_path(self, statuses, expected_statuses):
        operations = [{NAME: TEST_TRANSFER_OPERATION_NAME, METADATA: {STATUS: status}} for status in statuses]

        with self.assertRaisesRegex(
            AirflowException,
            "An unexpected operation status was encountered. Expected: {}".format(
                ", ".join(expected_statuses)
            ),
        ):
            GCPTransferServiceHook.operations_contain_expected_statuses(
                operations, GcpTransferOperationStatus.IN_PROGRESS
            )

    @parameterized.expand(
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
        ]
    )
    def test_operations_contain_expected_statuses_green_path(self, statuses, expected_statuses):
        operations = [{NAME: TEST_TRANSFER_OPERATION_NAME, METADATA: {STATUS: status}} for status in statuses]

        result = GCPTransferServiceHook.operations_contain_expected_statuses(operations, expected_statuses)

        self.assertTrue(result)


class TestGCPTransferServiceHookWithProjectIdFromConnection(unittest.TestCase):
    def setUp(self):
        with mock.patch(
            'airflow.gcp.hooks.base.GoogleCloudBaseHook.__init__',
            new=mock_base_gcp_hook_default_project_id,
        ):
            self.gct_hook = GCPTransferServiceHook(gcp_conn_id='test')

    @mock.patch("airflow.gcp.hooks.cloud_storage_transfer_service.GCPTransferServiceHook._authorize")
    @mock.patch("airflow.gcp.hooks.cloud_storage_transfer_service.build")
    def test_gct_client_creation(self, mock_build, mock_authorize):
        result = self.gct_hook.get_conn()
        mock_build.assert_called_once_with(
            'storagetransfer', 'v1', http=mock_authorize.return_value, cache_discovery=False
        )
        self.assertEqual(mock_build.return_value, result)
        self.assertEqual(self.gct_hook._conn, result)

    @mock.patch(
        'airflow.gcp.hooks.base.GoogleCloudBaseHook.project_id',
        new_callable=PropertyMock,
        return_value=GCP_PROJECT_ID_HOOK_UNIT_TEST
    )
    @mock.patch('airflow.gcp.hooks.cloud_storage_transfer_service.GCPTransferServiceHook.get_conn')
    def test_create_transfer_job(self, get_conn, mock_project_id):
        create_method = get_conn.return_value.transferJobs.return_value.create
        execute_method = create_method.return_value.execute
        execute_method.return_value = deepcopy(TEST_TRANSFER_JOB)
        res = self.gct_hook.create_transfer_job(body=self._without_project_id(TEST_BODY))
        self.assertEqual(res, TEST_TRANSFER_JOB)
        create_method.assert_called_once_with(body=self._with_project_id(TEST_BODY, 'example-project'))
        execute_method.assert_called_once_with(num_retries=5)

    @mock.patch(
        'airflow.gcp.hooks.base.GoogleCloudBaseHook.project_id',
        new_callable=PropertyMock,
        return_value=GCP_PROJECT_ID_HOOK_UNIT_TEST
    )
    @mock.patch('airflow.gcp.hooks.cloud_storage_transfer_service.GCPTransferServiceHook.get_conn')
    def test_get_transfer_job(self, get_conn, mock_project_id):
        get_method = get_conn.return_value.transferJobs.return_value.get
        execute_method = get_method.return_value.execute
        execute_method.return_value = TEST_TRANSFER_JOB
        res = self.gct_hook.get_transfer_job(job_name=TEST_TRANSFER_JOB_NAME)
        self.assertIsNotNone(res)
        self.assertEqual(TEST_TRANSFER_JOB_NAME, res[NAME])
        get_method.assert_called_once_with(jobName=TEST_TRANSFER_JOB_NAME, projectId='example-project')
        execute_method.assert_called_once_with(num_retries=5)

    @mock.patch(
        'airflow.gcp.hooks.base.GoogleCloudBaseHook.project_id',
        new_callable=PropertyMock,
        return_value=GCP_PROJECT_ID_HOOK_UNIT_TEST
    )
    @mock.patch('airflow.gcp.hooks.cloud_storage_transfer_service.GCPTransferServiceHook.get_conn')
    def test_list_transfer_job(self, get_conn, mock_project_id):
        list_method = get_conn.return_value.transferJobs.return_value.list
        list_execute_method = list_method.return_value.execute
        list_execute_method.return_value = {TRANSFER_JOBS: [TEST_TRANSFER_JOB]}

        list_next = get_conn.return_value.transferJobs.return_value.list_next
        list_next.return_value = None

        res = self.gct_hook.list_transfer_job(
            request_filter=_without_key(TEST_TRANSFER_JOB_FILTER, FILTER_PROJECT_ID)
        )
        self.assertIsNotNone(res)
        self.assertEqual(res, [TEST_TRANSFER_JOB])

        list_method.assert_called_once_with(filter=mock.ANY)
        args, kwargs = list_method.call_args_list[0]
        self.assertEqual(
            json.loads(kwargs['filter']),
            {FILTER_PROJECT_ID: 'example-project', FILTER_JOB_NAMES: [TEST_TRANSFER_JOB_NAME]},
        )
        list_execute_method.assert_called_once_with(num_retries=5)

    @mock.patch(
        'airflow.gcp.hooks.base.GoogleCloudBaseHook.project_id',
        new_callable=PropertyMock,
        return_value=GCP_PROJECT_ID_HOOK_UNIT_TEST
    )
    @mock.patch('airflow.gcp.hooks.cloud_storage_transfer_service.GCPTransferServiceHook.get_conn')
    def test_update_transfer_job(self, get_conn, mock_project_id):
        update_method = get_conn.return_value.transferJobs.return_value.patch
        execute_method = update_method.return_value.execute
        execute_method.return_value = TEST_TRANSFER_JOB
        res = self.gct_hook.update_transfer_job(
            job_name=TEST_TRANSFER_JOB_NAME, body=self._without_project_id(TEST_UPDATE_TRANSFER_JOB_BODY)
        )
        self.assertIsNotNone(res)
        update_method.assert_called_once_with(
            jobName=TEST_TRANSFER_JOB_NAME,
            body=self._with_project_id(TEST_UPDATE_TRANSFER_JOB_BODY, 'example-project'),
        )
        execute_method.assert_called_once_with(num_retries=5)

    @mock.patch('airflow.gcp.hooks.cloud_storage_transfer_service.GCPTransferServiceHook.get_conn')
    def test_delete_transfer_job(self, get_conn):
        update_method = get_conn.return_value.transferJobs.return_value.patch
        execute_method = update_method.return_value.execute

        self.gct_hook.delete_transfer_job(job_name=TEST_TRANSFER_JOB_NAME, project_id=TEST_PROJECT_ID)

        update_method.assert_called_once_with(
            jobName=TEST_TRANSFER_JOB_NAME,
            body={
                PROJECT_ID: TEST_PROJECT_ID,
                TRANSFER_JOB: {STATUS: 'DELETED'},
                TRANSFER_JOB_FIELD_MASK: STATUS,
            },
        )
        execute_method.assert_called_once_with(num_retries=5)

    @mock.patch('airflow.gcp.hooks.cloud_storage_transfer_service.GCPTransferServiceHook.get_conn')
    def test_cancel_transfer_operation(self, get_conn):
        cancel_method = get_conn.return_value.transferOperations.return_value.cancel
        execute_method = cancel_method.return_value.execute

        self.gct_hook.cancel_transfer_operation(operation_name=TEST_TRANSFER_OPERATION_NAME)
        cancel_method.assert_called_once_with(name=TEST_TRANSFER_OPERATION_NAME)
        execute_method.assert_called_once_with(num_retries=5)

    @mock.patch('airflow.gcp.hooks.cloud_storage_transfer_service.GCPTransferServiceHook.get_conn')
    def test_get_transfer_operation(self, get_conn):
        get_method = get_conn.return_value.transferOperations.return_value.get
        execute_method = get_method.return_value.execute
        execute_method.return_value = TEST_TRANSFER_OPERATION
        res = self.gct_hook.get_transfer_operation(operation_name=TEST_TRANSFER_OPERATION_NAME)
        self.assertEqual(res, TEST_TRANSFER_OPERATION)
        get_method.assert_called_once_with(name=TEST_TRANSFER_OPERATION_NAME)
        execute_method.assert_called_once_with(num_retries=5)

    @mock.patch(
        'airflow.gcp.hooks.base.GoogleCloudBaseHook.project_id',
        new_callable=PropertyMock,
        return_value=GCP_PROJECT_ID_HOOK_UNIT_TEST
    )
    @mock.patch('airflow.gcp.hooks.cloud_storage_transfer_service.GCPTransferServiceHook.get_conn')
    def test_list_transfer_operation(self, get_conn, mock_project_id):
        list_method = get_conn.return_value.transferOperations.return_value.list
        list_execute_method = list_method.return_value.execute
        list_execute_method.return_value = {OPERATIONS: [TEST_TRANSFER_OPERATION]}

        list_next = get_conn.return_value.transferOperations.return_value.list_next
        list_next.return_value = None

        res = self.gct_hook.list_transfer_operations(
            request_filter=_without_key(TEST_TRANSFER_OPERATION_FILTER, FILTER_PROJECT_ID)
        )
        self.assertIsNotNone(res)
        self.assertEqual(res, [TEST_TRANSFER_OPERATION])
        list_method.assert_called_once_with(filter=mock.ANY, name='transferOperations')
        args, kwargs = list_method.call_args_list[0]
        self.assertEqual(
            json.loads(kwargs['filter']),
            {FILTER_PROJECT_ID: 'example-project', FILTER_JOB_NAMES: [TEST_TRANSFER_JOB_NAME]},
        )
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


class TestGCPTransferServiceHookWithoutProjectId(unittest.TestCase):
    def setUp(self):
        with mock.patch(
            'airflow.gcp.hooks.base.GoogleCloudBaseHook.__init__',
            new=mock_base_gcp_hook_no_default_project_id,
        ):
            self.gct_hook = GCPTransferServiceHook(gcp_conn_id='test')

    @mock.patch("airflow.gcp.hooks.cloud_storage_transfer_service.GCPTransferServiceHook._authorize")
    @mock.patch("airflow.gcp.hooks.cloud_storage_transfer_service.build")
    def test_gct_client_creation(self, mock_build, mock_authorize):
        result = self.gct_hook.get_conn()
        mock_build.assert_called_once_with(
            'storagetransfer', 'v1', http=mock_authorize.return_value, cache_discovery=False
        )
        self.assertEqual(mock_build.return_value, result)
        self.assertEqual(self.gct_hook._conn, result)

    @mock.patch(
        'airflow.gcp.hooks.base.GoogleCloudBaseHook.project_id',
        new_callable=PropertyMock,
        return_value=None
    )
    @mock.patch('airflow.gcp.hooks.cloud_storage_transfer_service.GCPTransferServiceHook.get_conn')
    def test_create_transfer_job(self, get_conn, mock_project_id):
        create_method = get_conn.return_value.transferJobs.return_value.create
        execute_method = create_method.return_value.execute
        execute_method.return_value = deepcopy(TEST_TRANSFER_JOB)
        with self.assertRaises(AirflowException) as e:
            self.gct_hook.create_transfer_job(body=_without_key(TEST_BODY, PROJECT_ID))

        self.assertEqual(
            'The project id must be passed either as `projectId` key in `body` parameter or as project_id '
            'extra in GCP connection definition. Both are not set!',
            str(e.exception),
        )

    @mock.patch(
        'airflow.gcp.hooks.base.GoogleCloudBaseHook.project_id',
        new_callable=PropertyMock,
        return_value=None
    )
    @mock.patch('airflow.gcp.hooks.cloud_storage_transfer_service.GCPTransferServiceHook.get_conn')
    def test_get_transfer_job(self, get_conn, mock_project_id):
        get_method = get_conn.return_value.transferJobs.return_value.get
        execute_method = get_method.return_value.execute
        execute_method.return_value = TEST_TRANSFER_JOB
        with self.assertRaises(AirflowException) as e:
            self.gct_hook.get_transfer_job(job_name=TEST_TRANSFER_JOB_NAME)
        self.assertEqual(
            'The project id must be passed either as keyword project_id '
            'parameter or as project_id extra in GCP connection definition. '
            'Both are not set!',
            str(e.exception),
        )

    @mock.patch(
        'airflow.gcp.hooks.base.GoogleCloudBaseHook.project_id',
        new_callable=PropertyMock,
        return_value=None
    )
    @mock.patch('airflow.gcp.hooks.cloud_storage_transfer_service.GCPTransferServiceHook.get_conn')
    def test_list_transfer_job(self, get_conn, mock_project_id):
        list_method = get_conn.return_value.transferJobs.return_value.list
        list_execute_method = list_method.return_value.execute
        list_execute_method.return_value = {"transferJobs": [TEST_TRANSFER_JOB]}

        list_next = get_conn.return_value.transferJobs.return_value.list_next
        list_next.return_value = None

        with self.assertRaises(AirflowException) as e:
            self.gct_hook.list_transfer_job(
                request_filter=_without_key(TEST_TRANSFER_JOB_FILTER, FILTER_PROJECT_ID))

        self.assertEqual(
            'The project id must be passed either as `project_id` key in `filter` parameter or as '
            'project_id extra in GCP connection definition. Both are not set!',
            str(e.exception),
        )

    @mock.patch(
        'airflow.gcp.hooks.base.GoogleCloudBaseHook.project_id',
        new_callable=PropertyMock,
        return_value=None
    )
    @mock.patch('airflow.gcp.hooks.cloud_storage_transfer_service.GCPTransferServiceHook.get_conn')
    def test_list_transfer_operation_multiple_page(self, get_conn, mock_project_id):
        pages_requests = [
            mock.Mock(**{'execute.return_value': {"operations": [TEST_TRANSFER_OPERATION]}}) for _ in range(4)
        ]
        transfer_operation_mock = mock.Mock(
            **{'list.return_value': pages_requests[1], 'list_next.side_effect': pages_requests[1:] + [None]}
        )

        get_conn.return_value.transferOperations.return_value = transfer_operation_mock

        res = self.gct_hook.list_transfer_operations(request_filter=TEST_TRANSFER_OPERATION_FILTER)
        self.assertEqual(res, [TEST_TRANSFER_OPERATION] * 4)

    @mock.patch(
        'airflow.gcp.hooks.base.GoogleCloudBaseHook.project_id',
        new_callable=PropertyMock,
        return_value=None
    )
    @mock.patch('airflow.gcp.hooks.cloud_storage_transfer_service.GCPTransferServiceHook.get_conn')
    def test_update_transfer_job(self, get_conn, mock_project_id):
        update_method = get_conn.return_value.transferJobs.return_value.patch
        execute_method = update_method.return_value.execute
        execute_method.return_value = TEST_TRANSFER_JOB
        with self.assertRaises(AirflowException) as e:
            self.gct_hook.update_transfer_job(
                job_name=TEST_TRANSFER_JOB_NAME, body=_without_key(TEST_UPDATE_TRANSFER_JOB_BODY, PROJECT_ID)
            )

        self.assertEqual(
            'The project id must be passed either as `projectId` key in `body` parameter or as project_id '
            'extra in GCP connection definition. Both are not set!',
            str(e.exception),
        )

    @mock.patch(
        'airflow.gcp.hooks.base.GoogleCloudBaseHook.project_id',
        new_callable=PropertyMock,
        return_value=None
    )
    @mock.patch('airflow.gcp.hooks.cloud_storage_transfer_service.GCPTransferServiceHook.get_conn')
    def test_delete_transfer_job(self, get_conn, mock_project_id):  # pylint: disable=unused-argument
        with self.assertRaises(AirflowException) as e:
            self.gct_hook.delete_transfer_job(  # pylint: disable=no-value-for-parameter
                job_name=TEST_TRANSFER_JOB_NAME)

        self.assertEqual(
            'The project id must be passed either as keyword project_id parameter or as project_id extra in '
            'GCP connection definition. Both are not set!',
            str(e.exception),
        )

    @mock.patch(
        'airflow.gcp.hooks.base.GoogleCloudBaseHook.project_id',
        new_callable=PropertyMock,
        return_value=None
    )
    @mock.patch('airflow.gcp.hooks.cloud_storage_transfer_service.GCPTransferServiceHook.get_conn')
    def test_list_transfer_operation(self, get_conn, mock_project_id):
        list_method = get_conn.return_value.transferOperations.return_value.list
        list_execute_method = list_method.return_value.execute
        list_execute_method.return_value = {"operations": [TEST_TRANSFER_OPERATION]}

        list_next = get_conn.return_value.transferOperations.return_value.list_next
        list_next.return_value = None

        with self.assertRaises(AirflowException) as e:
            self.gct_hook.list_transfer_operations(
                request_filter=_without_key(TEST_TRANSFER_OPERATION_FILTER, FILTER_PROJECT_ID)
            )

        self.assertEqual(
            'The project id must be passed either as `project_id` key in `filter` parameter or as project_id '
            'extra in GCP connection definition. Both are not set!',
            str(e.exception),
        )
