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
from unittest import mock
from unittest.mock import MagicMock, patch

import pytest

from airflow import configuration
from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.emr import EmrContainerHook
from airflow.providers.amazon.aws.operators.emr import EmrContainerOperator

SUBMIT_JOB_SUCCESS_RETURN = {
    'ResponseMetadata': {'HTTPStatusCode': 200},
    'id': 'job123456',
    'virtualClusterId': 'vc1234',
}

GENERATED_UUID = '800647a9-adda-4237-94e6-f542c85fa55b'


class TestEmrContainerOperator(unittest.TestCase):
    @mock.patch('airflow.providers.amazon.aws.hooks.emr.EmrContainerHook')
    def setUp(self, emr_hook_mock):
        configuration.load_test_config()

        self.emr_hook_mock = emr_hook_mock
        self.emr_container = EmrContainerOperator(
            task_id='start_job',
            name='test_emr_job',
            virtual_cluster_id='vzw123456',
            execution_role_arn='arn:aws:somerole',
            release_label='6.3.0-latest',
            job_driver={},
            configuration_overrides={},
            poll_interval=0,
            client_request_token=GENERATED_UUID,
            tags={},
        )

    @mock.patch.object(EmrContainerHook, 'submit_job')
    @mock.patch.object(EmrContainerHook, 'check_query_status')
    def test_execute_without_failure(
        self,
        mock_check_query_status,
        mock_submit_job,
    ):
        mock_submit_job.return_value = "jobid_123456"
        mock_check_query_status.return_value = 'COMPLETED'

        self.emr_container.execute(None)

        mock_submit_job.assert_called_once_with(
            'test_emr_job', 'arn:aws:somerole', '6.3.0-latest', {}, {}, GENERATED_UUID, {}
        )
        mock_check_query_status.assert_called_once_with('jobid_123456')
        assert self.emr_container.release_label == '6.3.0-latest'

    @mock.patch.object(
        EmrContainerHook,
        'check_query_status',
        side_effect=['PENDING', 'PENDING', 'SUBMITTED', 'RUNNING', 'COMPLETED'],
    )
    def test_execute_with_polling(self, mock_check_query_status):
        # Mock out the emr_client creator
        emr_client_mock = MagicMock()
        emr_client_mock.start_job_run.return_value = SUBMIT_JOB_SUCCESS_RETURN
        emr_session_mock = MagicMock()
        emr_session_mock.client.return_value = emr_client_mock
        boto3_session_mock = MagicMock(return_value=emr_session_mock)

        with patch('boto3.session.Session', boto3_session_mock):
            assert self.emr_container.execute(None) == 'job123456'
            assert mock_check_query_status.call_count == 5

    @mock.patch.object(EmrContainerHook, 'submit_job')
    @mock.patch.object(EmrContainerHook, 'check_query_status')
    @mock.patch.object(EmrContainerHook, 'get_job_failure_reason')
    def test_execute_with_failure(
        self, mock_get_job_failure_reason, mock_check_query_status, mock_submit_job
    ):
        mock_submit_job.return_value = "jobid_123456"
        mock_check_query_status.return_value = 'FAILED'
        mock_get_job_failure_reason.return_value = (
            "CLUSTER_UNAVAILABLE - Cluster EKS eks123456 does not exist."
        )
        with pytest.raises(AirflowException) as ctx:
            self.emr_container.execute(None)
        assert 'EMR Containers job failed' in str(ctx.value)
        assert 'Error: CLUSTER_UNAVAILABLE - Cluster EKS eks123456 does not exist.' in str(ctx.value)

    @mock.patch.object(
        EmrContainerHook,
        'check_query_status',
        side_effect=['PENDING', 'PENDING', 'SUBMITTED', 'RUNNING', 'COMPLETED'],
    )
    def test_execute_with_polling_timeout(self, mock_check_query_status):
        # Mock out the emr_client creator
        emr_client_mock = MagicMock()
        emr_client_mock.start_job_run.return_value = SUBMIT_JOB_SUCCESS_RETURN
        emr_session_mock = MagicMock()
        emr_session_mock.client.return_value = emr_client_mock
        boto3_session_mock = MagicMock(return_value=emr_session_mock)

        timeout_container = EmrContainerOperator(
            task_id='start_job',
            name='test_emr_job',
            virtual_cluster_id='vzw123456',
            execution_role_arn='arn:aws:somerole',
            release_label='6.3.0-latest',
            job_driver={},
            configuration_overrides={},
            poll_interval=0,
            max_tries=3,
        )

        with patch('boto3.session.Session', boto3_session_mock):
            with pytest.raises(AirflowException) as ctx:
                timeout_container.execute(None)

            assert mock_check_query_status.call_count == 3
            assert 'Final state of EMR Containers job is SUBMITTED' in str(ctx.value)
            assert 'Max tries of poll status exceeded' in str(ctx.value)
