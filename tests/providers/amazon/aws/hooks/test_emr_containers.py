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

import unittest
from unittest import mock

from airflow.providers.amazon.aws.hooks.emr import EmrContainerHook

SUBMIT_JOB_SUCCESS_RETURN = {
    'ResponseMetadata': {'HTTPStatusCode': 200},
    'id': 'job123456',
    'virtualClusterId': 'vc1234',
}

CREATE_EMR_ON_EKS_CLUSTER_RETURN = {'ResponseMetadata': {'HTTPStatusCode': 200}, 'id': 'vc1234'}

JOB1_RUN_DESCRIPTION = {
    'jobRun': {
        'id': 'job123456',
        'virtualClusterId': 'vc1234',
        'state': 'COMPLETED',
    }
}

JOB2_RUN_DESCRIPTION = {
    'jobRun': {
        'id': 'job123456',
        'virtualClusterId': 'vc1234',
        'state': 'RUNNING',
    }
}


class TestEmrContainerHook(unittest.TestCase):
    def setUp(self):
        self.emr_containers = EmrContainerHook(virtual_cluster_id='vc1234')

    def test_init(self):
        assert self.emr_containers.aws_conn_id == 'aws_default'
        assert self.emr_containers.virtual_cluster_id == 'vc1234'

    @mock.patch("boto3.session.Session")
    def test_create_emr_on_eks_cluster(self, mock_session):
        emr_client_mock = mock.MagicMock()
        emr_client_mock.create_virtual_cluster.return_value = CREATE_EMR_ON_EKS_CLUSTER_RETURN
        emr_session_mock = mock.MagicMock()
        emr_session_mock.client.return_value = emr_client_mock
        mock_session.return_value = emr_session_mock

        emr_on_eks_create_cluster_response = self.emr_containers.create_emr_on_eks_cluster(
            virtual_cluster_name="test_virtual_cluster",
            eks_cluster_name="test_eks_cluster",
            eks_namespace="test_eks_namespace",
        )
        assert emr_on_eks_create_cluster_response == "vc1234"

    @mock.patch("boto3.session.Session")
    def test_submit_job(self, mock_session):
        # Mock out the emr_client creator
        emr_client_mock = mock.MagicMock()
        emr_client_mock.start_job_run.return_value = SUBMIT_JOB_SUCCESS_RETURN
        emr_session_mock = mock.MagicMock()
        emr_session_mock.client.return_value = emr_client_mock
        mock_session.return_value = emr_session_mock

        emr_containers_job = self.emr_containers.submit_job(
            name="test-job-run",
            execution_role_arn="arn:aws:somerole",
            release_label="emr-6.3.0-latest",
            job_driver={},
            configuration_overrides={},
            client_request_token="uuidtoken",
        )
        assert emr_containers_job == 'job123456'

    @mock.patch("boto3.session.Session")
    def test_query_status_polling_when_terminal(self, mock_session):
        emr_client_mock = mock.MagicMock()
        emr_session_mock = mock.MagicMock()
        emr_session_mock.client.return_value = emr_client_mock
        mock_session.return_value = emr_session_mock
        emr_client_mock.describe_job_run.return_value = JOB1_RUN_DESCRIPTION

        query_status = self.emr_containers.poll_query_status(job_id='job123456')
        # should only poll once since query is already in terminal state
        emr_client_mock.describe_job_run.assert_called_once()
        assert query_status == 'COMPLETED'

    @mock.patch("boto3.session.Session")
    def test_query_status_polling_with_timeout(self, mock_session):
        emr_client_mock = mock.MagicMock()
        emr_session_mock = mock.MagicMock()
        emr_session_mock.client.return_value = emr_client_mock
        mock_session.return_value = emr_session_mock
        emr_client_mock.describe_job_run.return_value = JOB2_RUN_DESCRIPTION

        query_status = self.emr_containers.poll_query_status(job_id='job123456', max_tries=2)
        # should poll until max_tries is reached since query is in non-terminal state
        assert emr_client_mock.describe_job_run.call_count == 2
        assert query_status == 'RUNNING'
