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

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.sensors.emr_base import EmrBaseSensor

TARGET_STATE = 'TARGET_STATE'
FAILED_STATE = 'FAILED_STATE'
NON_TARGET_STATE = 'NON_TARGET_STATE'

GOOD_HTTP_STATUS = 200
BAD_HTTP_STATUS = 400

EXPECTED_CODE = 'EXPECTED_TEST_FAILURE'
EMPTY_CODE = 'No code'


class EmrBaseSensorSubclass(EmrBaseSensor):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.target_states = [TARGET_STATE]
        self.failed_states = [FAILED_STATE]
        self.response = {}  # will be set in tests

    def get_emr_response(self):
        return self.response

    @staticmethod
    def state_from_response(response):
        return response['SomeKey']['State']

    @staticmethod
    def failure_message_from_response(response):
        change_reason = response['SomeKey'].get('StateChangeReason')
        if change_reason:
            return 'for code: {} with message {}'.format(
                change_reason.get('Code', EMPTY_CODE), change_reason.get('Message', 'Unknown')
            )
        return None


class TestEmrBaseSensor(unittest.TestCase):
    def test_poke_returns_true_when_state_is_in_target_states(self):
        operator = EmrBaseSensorSubclass(
            task_id='test_task',
            poke_interval=2,
        )
        operator.response = {
            'SomeKey': {'State': TARGET_STATE},
            'ResponseMetadata': {'HTTPStatusCode': GOOD_HTTP_STATUS},
        }

        operator.execute(None)

    def test_poke_returns_false_when_state_is_not_in_target_states(self):
        operator = EmrBaseSensorSubclass(
            task_id='test_task',
            poke_interval=2,
        )
        operator.response = {
            'SomeKey': {'State': NON_TARGET_STATE},
            'ResponseMetadata': {'HTTPStatusCode': GOOD_HTTP_STATUS},
        }

        self.assertEqual(operator.poke(None), False)

    def test_poke_returns_false_when_http_response_is_bad(self):
        operator = EmrBaseSensorSubclass(
            task_id='test_task',
            poke_interval=2,
        )
        operator.response = {
            'SomeKey': {'State': TARGET_STATE},
            'ResponseMetadata': {'HTTPStatusCode': BAD_HTTP_STATUS},
        }

        self.assertEqual(operator.poke(None), False)

    def test_poke_raises_error_when_state_is_in_failed_states(self):
        operator = EmrBaseSensorSubclass(
            task_id='test_task',
            poke_interval=2,
        )
        operator.response = {
            'SomeKey': {'State': FAILED_STATE, 'StateChangeReason': {'Code': EXPECTED_CODE}},
            'ResponseMetadata': {'HTTPStatusCode': GOOD_HTTP_STATUS},
        }

        with self.assertRaises(AirflowException) as context:
            operator.poke(None)

        self.assertIn('EMR job failed', str(context.exception))
        self.assertIn(EXPECTED_CODE, str(context.exception))
        self.assertNotIn(EMPTY_CODE, str(context.exception))
