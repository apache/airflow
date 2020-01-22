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

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.sensors.sagemaker_base import SageMakerBaseSensor


class TestSagemakerBaseSensor(unittest.TestCase):
    def test_execute(self):
        class SageMakerBaseSensorSubclass(SageMakerBaseSensor):
            def non_terminal_states(self):
                return ['PENDING', 'RUNNING', 'CONTINUE']

            def failed_states(self):
                return ['FAILED']

            def get_sagemaker_response(self):
                return {
                    'SomeKey': {'State': 'COMPLETED'},
                    'ResponseMetadata': {'HTTPStatusCode': 200}
                }

            def state_from_response(self, response):
                return response['SomeKey']['State']

        sensor = SageMakerBaseSensorSubclass(
            task_id='test_task',
            poke_interval=2,
            aws_conn_id='aws_test'
        )

        sensor.execute(None)

    def test_poke_with_unfinished_job(self):
        class SageMakerBaseSensorSubclass(SageMakerBaseSensor):
            def non_terminal_states(self):
                return ['PENDING', 'RUNNING', 'CONTINUE']

            def failed_states(self):
                return ['FAILED']

            def get_sagemaker_response(self):
                return {
                    'SomeKey': {'State': 'PENDING'},
                    'ResponseMetadata': {'HTTPStatusCode': 200}
                }

            def state_from_response(self, response):
                return response['SomeKey']['State']

        sensor = SageMakerBaseSensorSubclass(
            task_id='test_task',
            poke_interval=2,
            aws_conn_id='aws_test'
        )

        self.assertEqual(sensor.poke(None), False)

    def test_poke_with_not_implemented_method(self):
        class SageMakerBaseSensorSubclass(SageMakerBaseSensor):
            def non_terminal_states(self):
                return ['PENDING', 'RUNNING', 'CONTINUE']

            def failed_states(self):
                return ['FAILED']

        sensor = SageMakerBaseSensorSubclass(
            task_id='test_task',
            poke_interval=2,
            aws_conn_id='aws_test'
        )

        self.assertRaises(NotImplementedError, sensor.poke, None)

    def test_poke_with_bad_response(self):
        class SageMakerBaseSensorSubclass(SageMakerBaseSensor):
            def non_terminal_states(self):
                return ['PENDING', 'RUNNING', 'CONTINUE']

            def failed_states(self):
                return ['FAILED']

            def get_sagemaker_response(self):
                return {
                    'SomeKey': {'State': 'COMPLETED'},
                    'ResponseMetadata': {'HTTPStatusCode': 400}
                }

            def state_from_response(self, response):
                return response['SomeKey']['State']

        sensor = SageMakerBaseSensorSubclass(
            task_id='test_task',
            poke_interval=2,
            aws_conn_id='aws_test'
        )

        self.assertEqual(sensor.poke(None), False)

    def test_poke_with_job_failure(self):
        class SageMakerBaseSensorSubclass(SageMakerBaseSensor):
            def non_terminal_states(self):
                return ['PENDING', 'RUNNING', 'CONTINUE']

            def failed_states(self):
                return ['FAILED']

            def get_sagemaker_response(self):
                return {
                    'SomeKey': {'State': 'FAILED'},
                    'ResponseMetadata': {'HTTPStatusCode': 200}
                }

            def state_from_response(self, response):
                return response['SomeKey']['State']

        sensor = SageMakerBaseSensorSubclass(
            task_id='test_task',
            poke_interval=2,
            aws_conn_id='aws_test'
        )

        self.assertRaises(AirflowException, sensor.poke, None)


if __name__ == '__main__':
    unittest.main()
