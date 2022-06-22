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

from airflow.providers.amazon.aws.hooks.emr_containers import EMRContainerHook

SUBMIT_JOB_SUCCESS_RETURN = {
    'ResponseMetadata': {'HTTPStatusCode': 200},
    'id': 'job123456',
    'virtualClusterId': 'vc1234',
}


class TestEMRContainerHook(unittest.TestCase):
    def setUp(self):
        self.emr_containers = EMRContainerHook(virtual_cluster_id='vc1234')

    def test_init(self):
        assert self.emr_containers.aws_conn_id == 'aws_default'
        assert self.emr_containers.virtual_cluster_id == 'vc1234'

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
