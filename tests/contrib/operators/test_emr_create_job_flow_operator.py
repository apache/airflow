# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import unittest
from mock import MagicMock, patch

from airflow import configuration
from airflow.contrib.operators.emr_create_job_flow_operator import EmrCreateJobFlowOperator

RUN_JOB_FLOW_SUCCESS_RETURN = {
    'ResponseMetadata': {
        'HTTPStatusCode': 200
    },
    'JobFlowId': 'j-8989898989'
}

class TestEmrCreateJobFlowOperator(unittest.TestCase):
    def setUp(self):
        configuration.load_test_config()

        # Mock out the emr_client (moto has incorrect response)
        mock_emr_client = MagicMock()
        mock_emr_client.run_job_flow.return_value = RUN_JOB_FLOW_SUCCESS_RETURN

        mock_emr_session = MagicMock()
        mock_emr_session.client.return_value = mock_emr_client

        # Mock out the emr_client creator
        self.boto3_session_mock = MagicMock(return_value=mock_emr_session)


    def test_execute_uses_the_emr_config_to_create_a_cluster_and_returns_job_id(self):
        with patch('boto3.session.Session', self.boto3_session_mock):

            operator = EmrCreateJobFlowOperator(
                task_id='test_task',
                aws_conn_id='aws_default',
                emr_conn_id='emr_default'
            )

            self.assertEqual(operator.execute(None), 'j-8989898989')

if __name__ == '__main__':
    unittest.main()
