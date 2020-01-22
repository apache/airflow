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
import unittest

import mock
from parameterized import parameterized

from airflow.gcp.hooks.cloud_storage_transfer_service import GcpTransferOperationStatus
from airflow.gcp.sensors.cloud_storage_transfer_service import CloudDataTransferServiceJobStatusSensor


class TestGcpStorageTransferOperationWaitForJobStatusSensor(unittest.TestCase):
    @mock.patch('airflow.gcp.sensors.cloud_storage_transfer_service.CloudDataTransferServiceHook')
    def test_wait_for_status_success(self, mock_tool):
        operations = [{'metadata': {'status': GcpTransferOperationStatus.SUCCESS}}]
        mock_tool.return_value.list_transfer_operations.return_value = operations
        mock_tool.operations_contain_expected_statuses.return_value = True

        op = CloudDataTransferServiceJobStatusSensor(
            task_id='task-id',
            job_name='job-name',
            project_id='project-id',
            expected_statuses=GcpTransferOperationStatus.SUCCESS,
        )

        context = {'ti': (mock.Mock(**{'xcom_push.return_value': None}))}
        result = op.poke(context)

        mock_tool.return_value.list_transfer_operations.assert_called_once_with(
            request_filter={'project_id': 'project-id', 'job_names': ['job-name']}
        )
        mock_tool.operations_contain_expected_statuses.assert_called_once_with(
            operations=operations, expected_statuses={GcpTransferOperationStatus.SUCCESS}
        )
        self.assertTrue(result)

    @mock.patch('airflow.gcp.sensors.cloud_storage_transfer_service.CloudDataTransferServiceHook')
    def test_wait_for_status_success_default_expected_status(self, mock_tool):

        op = CloudDataTransferServiceJobStatusSensor(
            task_id='task-id',
            job_name='job-name',
            project_id='project-id',
            expected_statuses=GcpTransferOperationStatus.SUCCESS,
        )

        context = {'ti': (mock.Mock(**{'xcom_push.return_value': None}))}

        result = op.poke(context)

        mock_tool.operations_contain_expected_statuses.assert_called_once_with(
            operations=mock.ANY, expected_statuses={GcpTransferOperationStatus.SUCCESS}
        )
        self.assertTrue(result)

    @mock.patch('airflow.gcp.sensors.cloud_storage_transfer_service.CloudDataTransferServiceHook')
    def test_wait_for_status_after_retry(self, mock_tool):
        operations_set = [
            [{'metadata': {'status': GcpTransferOperationStatus.SUCCESS}}],
            [{'metadata': {'status': GcpTransferOperationStatus.SUCCESS}}],
        ]

        mock_tool.return_value.list_transfer_operations.side_effect = operations_set
        mock_tool.operations_contain_expected_statuses.side_effect = [False, True]

        op = CloudDataTransferServiceJobStatusSensor(
            task_id='task-id',
            job_name='job-name',
            project_id='project-id',
            expected_statuses=GcpTransferOperationStatus.SUCCESS,
        )

        context = {'ti': (mock.Mock(**{'xcom_push.return_value': None}))}

        result = op.poke(context)
        self.assertFalse(result)

        mock_tool.operations_contain_expected_statuses.assert_called_once_with(
            operations=operations_set[0], expected_statuses={GcpTransferOperationStatus.SUCCESS}
        )
        mock_tool.operations_contain_expected_statuses.reset_mock()

        result = op.poke(context)
        self.assertTrue(result)

        mock_tool.operations_contain_expected_statuses.assert_called_once_with(
            operations=operations_set[1], expected_statuses={GcpTransferOperationStatus.SUCCESS}
        )

    @parameterized.expand(
        [
            (GcpTransferOperationStatus.SUCCESS, {GcpTransferOperationStatus.SUCCESS}),
            ({GcpTransferOperationStatus.SUCCESS}, {GcpTransferOperationStatus.SUCCESS}),
            (
                {GcpTransferOperationStatus.SUCCESS, GcpTransferOperationStatus.SUCCESS},
                {GcpTransferOperationStatus.SUCCESS, GcpTransferOperationStatus.SUCCESS},
            ),
        ]
    )
    @mock.patch('airflow.gcp.sensors.cloud_storage_transfer_service.CloudDataTransferServiceHook')
    def test_wait_for_status_normalize_status(self, expected_status, received_status, mock_tool):
        operations = [{'metadata': {'status': GcpTransferOperationStatus.SUCCESS}}]

        mock_tool.return_value.list_transfer_operations.return_value = operations
        mock_tool.operations_contain_expected_statuses.side_effect = [False, True]

        op = CloudDataTransferServiceJobStatusSensor(
            task_id='task-id',
            job_name='job-name',
            project_id='project-id',
            expected_statuses=expected_status,
        )

        context = {'ti': (mock.Mock(**{'xcom_push.return_value': None}))}

        result = op.poke(context)
        self.assertFalse(result)

        mock_tool.operations_contain_expected_statuses.assert_called_once_with(
            operations=operations, expected_statuses=received_status
        )
