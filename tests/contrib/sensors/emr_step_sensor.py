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
import datetime
from dateutil.tz import tzlocal
from mock import MagicMock, patch
import boto3

from airflow import configuration
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor

DESCRIBE_JOB_STEP_RUNNING_RETURN = {
    'ResponseMetadata': {
        'HTTPStatusCode': 200,
        'RequestId': '8dee8db2-3719-11e6-9e20-35b2f861a2a6'
    },
    'Step': {
        'ActionOnFailure': 'CONTINUE',
        'Config': {
            'Args': [
                '/usr/lib/spark/bin/run-example',
                'SparkPi',
                '10'
            ],
            'Jar': 'command-runner.jar',
            'Properties': {}
        },
        'Id': 's-VK57YR1Z9Z5N',
        'Name': 'calculate_pi',
        'Status': {
            'State': 'RUNNING',
            'StateChangeReason': {},
            'Timeline': {
                'CreationDateTime': datetime.datetime(2016, 6, 20, 19, 0, 18, 787000, tzinfo=tzlocal()),
                'StartDateTime': datetime.datetime(2016, 6, 20, 19, 2, 34, 889000, tzinfo=tzlocal())
            }
        }
    }
}

DESCRIBE_JOB_STEP_COMPLETED_RETURN = {
    'ResponseMetadata': {
        'HTTPStatusCode': 200,
        'RequestId': '8dee8db2-3719-11e6-9e20-35b2f861a2a6'
    },
    'Step': {
        'ActionOnFailure': 'CONTINUE',
        'Config': {
            'Args': [
                '/usr/lib/spark/bin/run-example',
                'SparkPi',
                '10'
            ],
            'Jar': 'command-runner.jar',
            'Properties': {}
        },
        'Id': 's-VK57YR1Z9Z5N',
        'Name': 'calculate_pi',
        'Status': {
            'State': 'COMPLETED',
            'StateChangeReason': {},
            'Timeline': {
                'CreationDateTime': datetime.datetime(2016, 6, 20, 19, 0, 18, 787000, tzinfo=tzlocal()),
                'StartDateTime': datetime.datetime(2016, 6, 20, 19, 2, 34, 889000, tzinfo=tzlocal())
            }
        }
    }
}


class TestEmrStepSensor(unittest.TestCase):
    def setUp(self):
        configuration.load_test_config()

        # Mock out the emr_client (moto has incorrect response)
        self.mock_emr_client = MagicMock()
        self.mock_emr_client.describe_step.side_effect = [
            DESCRIBE_JOB_STEP_RUNNING_RETURN,
            DESCRIBE_JOB_STEP_COMPLETED_RETURN
        ]

        # Mock out the emr_client creator
        self.boto3_client_mock = MagicMock(return_value=self.mock_emr_client)


    def test_execute_calls_with_the_job_flow_id_and_step_id_until_it_reaches_a_terminal_state(self):
        with patch('boto3.client', self.boto3_client_mock):

            operator = EmrStepSensor(
                task_id='test_task',
                poke_interval=1,
                job_flow_id='j-8989898989',
                step_id='s-VK57YR1Z9Z5N',
                aws_conn_id='aws_default',
            )

            operator.execute(None)

            # make sure we called twice
            self.assertEqual(self.mock_emr_client.describe_step.call_count, 2)

            # make sure it was called with the job_flow_id and step_id
            self.mock_emr_client.describe_step.assert_called_with(ClusterId='j-8989898989', StepId='s-VK57YR1Z9Z5N')


if __name__ == '__main__':
    unittest.main()
