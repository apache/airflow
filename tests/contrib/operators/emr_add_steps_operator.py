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

import unittest
from mock import MagicMock, patch

from airflow import configuration
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator

ADD_STEPS_SUCCESS_RETURN = {
    'ResponseMetadata': {
        'HTTPStatusCode': 200
    },
    'StepIds': ['s-2LH3R5GW3A53T']
}


class TestEmrAddStepsOperator(unittest.TestCase):
    def setUp(self):
        configuration.test_mode()

        # Mock out the emr_client (moto has incorrect response)
        mock_emr_client = MagicMock()
        mock_emr_client.add_job_flow_steps.return_value = ADD_STEPS_SUCCESS_RETURN

        # Mock out the emr_client creator
        self.boto3_client_mock = MagicMock(return_value=mock_emr_client)


    def test_execute_adds_steps_to_the_job_flow_and_returns_step_ids(self):
        with patch('boto3.client', self.boto3_client_mock):

            operator = EmrAddStepsOperator(
                task_id='test_task',
                job_flow_id='j-8989898989',
                aws_conn_id='aws_default'
            )

            self.assertEqual(operator.execute(None), ['s-2LH3R5GW3A53T'])

if __name__ == '__main__':
    unittest.main()
