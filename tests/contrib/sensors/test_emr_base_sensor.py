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

from airflow.contrib.sensors.emr_base_sensor import EmrBaseSensor
from airflow.exceptions import AirflowException


class TestEmrBaseSensor(unittest.TestCase):
    def test_subclasses_that_implement_required_methods_and_constants_succeed_when_response_is_good(self):
        class EmrBaseSensorSubclass(EmrBaseSensor):
            NON_TERMINAL_STATES = ['PENDING', 'RUNNING', 'CONTINUE']
            FAILED_STATE = ['FAILED']

            @staticmethod
            def get_emr_response():
                return {
                    'SomeKey': {'State': 'COMPLETED'},
                    'ResponseMetadata': {'HTTPStatusCode': 200}
                }

            @staticmethod
            def state_from_response(response):
                return response['SomeKey']['State']

            @staticmethod
            def failure_message_from_response(response):
                change_reason = response['Cluster']['Status'].get('StateChangeReason')
                if change_reason:
                    return 'for code: {} with message {}'.format(change_reason.get('Code', 'No code'),
                                                                 change_reason.get('Message', 'Unknown'))
                return None

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
            FAILED_STATE = ['FAILED']

            @staticmethod
            def get_emr_response():
                return {
                    'SomeKey': {'State': 'PENDING'},
                    'ResponseMetadata': {'HTTPStatusCode': 200}
                }

            @staticmethod
            def state_from_response(response):
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
            FAILED_STATE = ['FAILED']

            @staticmethod
            def get_emr_response():
                return {
                    'SomeKey': {'State': 'COMPLETED'},
                    'ResponseMetadata': {'HTTPStatusCode': 400}
                }

            @staticmethod
            def state_from_response(response):
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
            FAILED_STATE = ['FAILED']
            EXPECTED_CODE = 'EXPECTED_TEST_FAILURE'
            EMPTY_CODE = 'No code'

            @staticmethod
            def get_emr_response():
                return {
                    'SomeKey': {'State': 'FAILED',
                                'StateChangeReason': {'Code': EmrBaseSensorSubclass.EXPECTED_CODE}},
                    'ResponseMetadata': {'HTTPStatusCode': 200}
                }

            @staticmethod
            def state_from_response(response):
                return response['SomeKey']['State']

            @staticmethod
            def failure_message_from_response(response):
                state_change_reason = response['SomeKey']['StateChangeReason']
                if state_change_reason:
                    return 'with code: {}'.format(state_change_reason.get('Code',
                                                                          EmrBaseSensorSubclass.EMPTY_CODE))
                return None

        operator = EmrBaseSensorSubclass(
            task_id='test_task',
            poke_interval=2,
            job_flow_id='j-8989898989',
            aws_conn_id='aws_test'
        )

        with self.assertRaises(AirflowException) as context:
            operator.poke(None)

        self.assertIn('EMR job failed', str(context.exception))
        self.assertIn(EmrBaseSensorSubclass.EXPECTED_CODE, str(context.exception))
        self.assertNotIn(EmrBaseSensorSubclass.EMPTY_CODE, str(context.exception))


if __name__ == '__main__':
    unittest.main()
