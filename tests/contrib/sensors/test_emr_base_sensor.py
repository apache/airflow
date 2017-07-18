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

from airflow import configuration
from airflow.exceptions import AirflowException
from airflow.contrib.sensors.emr_base_sensor import EmrBaseSensor


class TestEmrBaseSensor(unittest.TestCase):
    def setUp(self):
        configuration.load_test_config()

    def test_subclasses_that_implment_required_methods_and_constants_succeed_when_response_is_good(self):
        class EmrBaseSensorSubclass(EmrBaseSensor):
            NON_TERMINAL_STATES = ['PENDING', 'RUNNING', 'CONTINUE']
            FAILED_STATE = 'FAILED'

            def get_emr_response(self):
                return {
                    'SomeKey': {'State': 'COMPLETED'},
                    'ResponseMetadata': {'HTTPStatusCode': 200}
                }

            def state_from_response(self, response):
                return response['SomeKey']['State']

        operator = EmrBaseSensorSubclass(
            task_id='test_task',
            poke_interval=2,
            job_flow_id='j-8989898989',
            aws_conn_id='aws_test'
        )

        operator.execute(None)

    def test_poke_returns_false_when_state_is_a_non_terminal_state(self):
        class EmrBaseSensorSubclass(EmrBaseSensor):
            NON_TERMINAL_STATES = ['PENDING', 'RUNNING', 'CONTINUE']
            FAILED_STATE = 'FAILED'

            def get_emr_response(self):
                return {
                    'SomeKey': {'State': 'PENDING'},
                    'ResponseMetadata': {'HTTPStatusCode': 200}
                }

            def state_from_response(self, response):
                return response['SomeKey']['State']

        operator = EmrBaseSensorSubclass(
            task_id='test_task',
            poke_interval=2,
            job_flow_id='j-8989898989',
            aws_conn_id='aws_test'
        )

        self.assertEqual(operator.poke(None), False)

    def test_poke_returns_false_when_http_response_is_bad(self):
        class EmrBaseSensorSubclass(EmrBaseSensor):
            NON_TERMINAL_STATES = ['PENDING', 'RUNNING', 'CONTINUE']
            FAILED_STATE = 'FAILED'

            def get_emr_response(self):
                return {
                    'SomeKey': {'State': 'COMPLETED'},
                    'ResponseMetadata': {'HTTPStatusCode': 400}
                }

            def state_from_response(self, response):
                return response['SomeKey']['State']

        operator = EmrBaseSensorSubclass(
            task_id='test_task',
            poke_interval=2,
            job_flow_id='j-8989898989',
            aws_conn_id='aws_test'
        )

        self.assertEqual(operator.poke(None), False)


    def test_poke_raises_error_when_job_has_failed(self):
        class EmrBaseSensorSubclass(EmrBaseSensor):
            NON_TERMINAL_STATES = ['PENDING', 'RUNNING', 'CONTINUE']
            FAILED_STATE = 'FAILED'

            def get_emr_response(self):
                return {
                    'SomeKey': {'State': 'FAILED'},
                    'ResponseMetadata': {'HTTPStatusCode': 200}
                }

            def state_from_response(self, response):
                return response['SomeKey']['State']

        operator = EmrBaseSensorSubclass(
            task_id='test_task',
            poke_interval=2,
            job_flow_id='j-8989898989',
            aws_conn_id='aws_test'
        )

        with self.assertRaises(AirflowException) as context:

            operator.poke(None)


        self.assertIn('EMR job failed', str(context.exception))


if __name__ == '__main__':
    unittest.main()
