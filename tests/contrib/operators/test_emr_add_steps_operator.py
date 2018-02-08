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
from datetime import timedelta

from mock import MagicMock, patch

from airflow import configuration
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.models import TaskInstance
from airflow.utils import timezone

DEFAULT_DATE = timezone.datetime(2017, 1, 1)

ADD_STEPS_SUCCESS_RETURN = {
    'ResponseMetadata': {
        'HTTPStatusCode': 200
    },
    'StepIds': ['s-2LH3R5GW3A53T']
}


class TestEmrAddStepsOperator(unittest.TestCase):
    # When
    _config = [{
        'Name': 'test_step',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                '/usr/lib/spark/bin/run-example',
                '{{ macros.ds_add(ds, -1) }}',
                '{{ ds }}'
            ]
        }
    }]

    def setUp(self):
        configuration.load_test_config()

        # Mock out the emr_client (moto has incorrect response)
        self.emr_client_mock = MagicMock()
        self.operator = EmrAddStepsOperator(
            task_id='test_task',
            job_flow_id='j-8989898989',
            aws_conn_id='aws_default',
            steps=self._config
        )

    def test_init(self):
        self.assertEqual(self.operator.aws_conn_id, 'aws_default')
        self.assertEqual(self.operator.emr_conn_id, 'emr_default')

    def test_render_template(self):
        ti = TaskInstance(self.operator, DEFAULT_DATE)
        ti.render_templates()

        expected_args = [{
            'Name': 'test_step',
            'ActionOnFailure': 'CONTINUE',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': [
                    '/usr/lib/spark/bin/run-example',
                    (DEFAULT_DATE - timedelta(days=1)).strftime("%Y-%m-%d"),
                    DEFAULT_DATE.strftime("%Y-%m-%d"),
                ]
            }
        }]

        self.assertListEqual(self.operator.steps, expected_args)

    def test_execute_returns_step_id(self):
        # Mock out the emr_client creator
        self.emr_client_mock.add_job_flow_steps.return_value = ADD_STEPS_SUCCESS_RETURN
        self.boto3_client_mock = MagicMock(return_value=self.emr_client_mock)

        with patch('boto3.client', self.boto3_client_mock):
            self.assertEqual(self.operator.execute(None), ['s-2LH3R5GW3A53T'])


if __name__ == '__main__':
    unittest.main()
