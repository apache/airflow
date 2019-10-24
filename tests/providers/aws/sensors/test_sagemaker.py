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
from datetime import datetime

from airflow.exceptions import AirflowException
from airflow.providers.aws.hooks.cloudwatch import AwsLogsHook
from airflow.providers.aws.hooks.sagemaker import LogState, SageMakerHook
from airflow.providers.aws.sensors.sagemaker import (
    SageMakerBaseSensor, SageMakerEndpointSensor, SageMakerTrainingSensor, SageMakerTransformSensor,
    SageMakerTuningSensor,
)
from tests.compat import mock


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


class TestSageMakerEndpointSensor(unittest.TestCase):
    DESCRIBE_ENDPOINT_CREATING_RESPONSE = {
        "EndpointStatus": "Creating",
        "ResponseMetadata": {"HTTPStatusCode": 200},
    }
    DESCRIBE_ENDPOINT_INSERVICE_RESPONSE = {
        "EndpointStatus": "InService",
        "ResponseMetadata": {"HTTPStatusCode": 200},
    }
    DESCRIBE_ENDPOINT_FAILED_RESPONSE = {
        "EndpointStatus": "Failed",
        "ResponseMetadata": {"HTTPStatusCode": 200},
        "FailureReason": "Unknown",
    }
    DESCRIBE_ENDPOINT_UPDATING_RESPONSE = {
        "EndpointStatus": "Updating",
        "ResponseMetadata": {"HTTPStatusCode": 200},
    }

    @mock.patch.object(SageMakerHook, 'get_conn')
    @mock.patch.object(SageMakerHook, 'describe_endpoint')
    def test_sensor_with_failure(self, mock_describe, mock_get_conn):
        mock_describe.side_effect = [self.DESCRIBE_ENDPOINT_FAILED_RESPONSE]
        sensor = SageMakerEndpointSensor(
            task_id='test_task',
            poke_interval=1,
            aws_conn_id='aws_test',
            endpoint_name='test_job_name'
        )
        self.assertRaises(AirflowException, sensor.execute, None)
        mock_describe.assert_called_once_with('test_job_name')

    @mock.patch.object(SageMakerHook, 'get_conn')
    @mock.patch.object(SageMakerHook, '__init__')
    @mock.patch.object(SageMakerHook, 'describe_endpoint')
    def test_sensor(self, mock_describe, hook_init, mock_get_conn):
        hook_init.return_value = None

        mock_describe.side_effect = [
            self.DESCRIBE_ENDPOINT_CREATING_RESPONSE,
            self.DESCRIBE_ENDPOINT_UPDATING_RESPONSE,
            self.DESCRIBE_ENDPOINT_INSERVICE_RESPONSE
        ]
        sensor = SageMakerEndpointSensor(
            task_id='test_task',
            poke_interval=1,
            aws_conn_id='aws_test',
            endpoint_name='test_job_name'
        )

        sensor.execute(None)

        # make sure we called 3 times(terminated when its completed)
        self.assertEqual(mock_describe.call_count, 3)

        # make sure the hook was initialized with the specific params
        calls = [
            mock.call(aws_conn_id='aws_test'),
            mock.call(aws_conn_id='aws_test'),
            mock.call(aws_conn_id='aws_test')
        ]
        hook_init.assert_has_calls(calls)


class TestSageMakerTrainingSensor(unittest.TestCase):
    DESCRIBE_TRAINING_COMPELETED_RESPONSE = {
        "TrainingJobStatus": "Completed",
        "ResourceConfig": {"InstanceCount": 1, "InstanceType": "ml.c4.xlarge", "VolumeSizeInGB": 10},
        "TrainingStartTime": datetime(2018, 2, 17, 7, 15, 0, 103000),
        "TrainingEndTime": datetime(2018, 2, 17, 7, 19, 34, 953000),
        "ResponseMetadata": {"HTTPStatusCode": 200},
    }

    DESCRIBE_TRAINING_INPROGRESS_RESPONSE = dict(DESCRIBE_TRAINING_COMPELETED_RESPONSE)
    DESCRIBE_TRAINING_INPROGRESS_RESPONSE.update({'TrainingJobStatus': 'InProgress'})

    DESCRIBE_TRAINING_FAILED_RESPONSE = dict(DESCRIBE_TRAINING_COMPELETED_RESPONSE)
    DESCRIBE_TRAINING_FAILED_RESPONSE.update({'TrainingJobStatus': 'Failed', 'FailureReason': 'Unknown'})

    DESCRIBE_TRAINING_STOPPING_RESPONSE = dict(DESCRIBE_TRAINING_COMPELETED_RESPONSE)
    DESCRIBE_TRAINING_STOPPING_RESPONSE.update({'TrainingJobStatus': 'Stopping'})

    @mock.patch.object(SageMakerHook, 'get_conn')
    @mock.patch.object(SageMakerHook, '__init__')
    @mock.patch.object(SageMakerHook, 'describe_training_job')
    def test_sensor_with_failure(self, mock_describe_job, hook_init, mock_client):
        hook_init.return_value = None

        mock_describe_job.side_effect = [self.DESCRIBE_TRAINING_FAILED_RESPONSE]
        sensor = SageMakerTrainingSensor(
            task_id='test_task',
            poke_interval=2,
            aws_conn_id='aws_test',
            job_name='test_job_name',
            print_log=False
        )
        self.assertRaises(AirflowException, sensor.execute, None)
        mock_describe_job.assert_called_once_with('test_job_name')

    @mock.patch.object(SageMakerHook, 'get_conn')
    @mock.patch.object(SageMakerHook, '__init__')
    @mock.patch.object(SageMakerHook, 'describe_training_job')
    def test_sensor(self, mock_describe_job, hook_init, mock_client):
        hook_init.return_value = None

        mock_describe_job.side_effect = [
            self.DESCRIBE_TRAINING_INPROGRESS_RESPONSE,
            self.DESCRIBE_TRAINING_STOPPING_RESPONSE,
            self.DESCRIBE_TRAINING_COMPELETED_RESPONSE
        ]
        sensor = SageMakerTrainingSensor(
            task_id='test_task',
            poke_interval=2,
            aws_conn_id='aws_test',
            job_name='test_job_name',
            print_log=False
        )

        sensor.execute(None)

        # make sure we called 3 times(terminated when its compeleted)
        self.assertEqual(mock_describe_job.call_count, 3)

        # make sure the hook was initialized with the specific params
        calls = [
            mock.call(aws_conn_id='aws_test'),
            mock.call(aws_conn_id='aws_test'),
            mock.call(aws_conn_id='aws_test')
        ]
        hook_init.assert_has_calls(calls)

    @mock.patch.object(SageMakerHook, 'get_conn')
    @mock.patch.object(AwsLogsHook, 'get_conn')
    @mock.patch.object(SageMakerHook, '__init__')
    @mock.patch.object(SageMakerHook, 'describe_training_job_with_log')
    @mock.patch.object(SageMakerHook, 'describe_training_job')
    def test_sensor_with_log(self, mock_describe_job, mock_describe_job_with_log,
                             hook_init, mock_log_client, mock_client):
        hook_init.return_value = None

        mock_describe_job.return_value = self.DESCRIBE_TRAINING_COMPELETED_RESPONSE
        mock_describe_job_with_log.side_effect = [
            (LogState.WAIT_IN_PROGRESS, self.DESCRIBE_TRAINING_INPROGRESS_RESPONSE, 0),
            (LogState.JOB_COMPLETE, self.DESCRIBE_TRAINING_STOPPING_RESPONSE, 0),
            (LogState.COMPLETE, self.DESCRIBE_TRAINING_COMPELETED_RESPONSE, 0)
        ]
        sensor = SageMakerTrainingSensor(
            task_id='test_task',
            poke_interval=2,
            aws_conn_id='aws_test',
            job_name='test_job_name',
            print_log=True
        )

        sensor.execute(None)

        self.assertEqual(mock_describe_job_with_log.call_count, 3)
        self.assertEqual(mock_describe_job.call_count, 1)

        calls = [
            mock.call(aws_conn_id='aws_test'),
            mock.call(aws_conn_id='aws_test'),
            mock.call(aws_conn_id='aws_test')
        ]
        hook_init.assert_has_calls(calls)


class TestSageMakerTransformSensor(unittest.TestCase):
    DESCRIBE_TRANSFORM_INPROGRESS_RESPONSE = {
        "TransformJobStatus": "InProgress",
        "ResponseMetadata": {"HTTPStatusCode": 200},
    }
    DESCRIBE_TRANSFORM_COMPELETED_RESPONSE = {
        "TransformJobStatus": "Compeleted",
        "ResponseMetadata": {"HTTPStatusCode": 200},
    }
    DESCRIBE_TRANSFORM_FAILED_RESPONSE = {
        "TransformJobStatus": "Failed",
        "ResponseMetadata": {"HTTPStatusCode": 200},
        "FailureReason": "Unknown",
    }
    DESCRIBE_TRANSFORM_STOPPING_RESPONSE = {
        "TransformJobStatus": "Stopping",
        "ResponseMetadata": {"HTTPStatusCode": 200},
    }

    @mock.patch.object(SageMakerHook, 'get_conn')
    @mock.patch.object(SageMakerHook, 'describe_transform_job')
    def test_sensor_with_failure(self, mock_describe_job, mock_client):
        mock_describe_job.side_effect = [self.DESCRIBE_TRANSFORM_FAILED_RESPONSE]
        sensor = SageMakerTransformSensor(
            task_id='test_task',
            poke_interval=2,
            aws_conn_id='aws_test',
            job_name='test_job_name'
        )
        self.assertRaises(AirflowException, sensor.execute, None)
        mock_describe_job.assert_called_once_with('test_job_name')

    @mock.patch.object(SageMakerHook, 'get_conn')
    @mock.patch.object(SageMakerHook, '__init__')
    @mock.patch.object(SageMakerHook, 'describe_transform_job')
    def test_sensor(self, mock_describe_job, hook_init, mock_client):
        hook_init.return_value = None

        mock_describe_job.side_effect = [
            self.DESCRIBE_TRANSFORM_INPROGRESS_RESPONSE,
            self.DESCRIBE_TRANSFORM_STOPPING_RESPONSE,
            self.DESCRIBE_TRANSFORM_COMPELETED_RESPONSE
        ]
        sensor = SageMakerTransformSensor(
            task_id='test_task',
            poke_interval=2,
            aws_conn_id='aws_test',
            job_name='test_job_name'
        )

        sensor.execute(None)

        # make sure we called 3 times(terminated when its compeleted)
        self.assertEqual(mock_describe_job.call_count, 3)

        # make sure the hook was initialized with the specific params
        calls = [
            mock.call(aws_conn_id='aws_test'),
            mock.call(aws_conn_id='aws_test'),
            mock.call(aws_conn_id='aws_test')
        ]
        hook_init.assert_has_calls(calls)


class TestSageMakerTuningSensor(unittest.TestCase):
    DESCRIBE_TUNING_INPROGRESS_RESPONSE = {
        "HyperParameterTuningJobStatus": "InProgress",
        "ResponseMetadata": {"HTTPStatusCode": 200},
    }

    DESCRIBE_TUNING_COMPELETED_RESPONSE = {
        "HyperParameterTuningJobStatus": "Compeleted",
        "ResponseMetadata": {"HTTPStatusCode": 200},
    }

    DESCRIBE_TUNING_FAILED_RESPONSE = {
        "HyperParameterTuningJobStatus": "Failed",
        "ResponseMetadata": {"HTTPStatusCode": 200},
        "FailureReason": "Unknown",
    }

    DESCRIBE_TUNING_STOPPING_RESPONSE = {
        "HyperParameterTuningJobStatus": "Stopping",
        "ResponseMetadata": {"HTTPStatusCode": 200},
    }

    @mock.patch.object(SageMakerHook, 'get_conn')
    @mock.patch.object(SageMakerHook, 'describe_tuning_job')
    def test_sensor_with_failure(self, mock_describe_job, mock_client):
        mock_describe_job.side_effect = [self.DESCRIBE_TUNING_FAILED_RESPONSE]
        sensor = SageMakerTuningSensor(
            task_id='test_task',
            poke_interval=2,
            aws_conn_id='aws_test',
            job_name='test_job_name'
        )
        self.assertRaises(AirflowException, sensor.execute, None)
        mock_describe_job.assert_called_once_with('test_job_name')

    @mock.patch.object(SageMakerHook, 'get_conn')
    @mock.patch.object(SageMakerHook, '__init__')
    @mock.patch.object(SageMakerHook, 'describe_tuning_job')
    def test_sensor(self, mock_describe_job, hook_init, mock_client):
        hook_init.return_value = None

        mock_describe_job.side_effect = [
            self.DESCRIBE_TUNING_INPROGRESS_RESPONSE,
            self.DESCRIBE_TUNING_STOPPING_RESPONSE,
            self.DESCRIBE_TUNING_COMPELETED_RESPONSE
        ]
        sensor = SageMakerTuningSensor(
            task_id='test_task',
            poke_interval=2,
            aws_conn_id='aws_test',
            job_name='test_job_name'
        )

        sensor.execute(None)

        # make sure we called 3 times(terminated when its compeleted)
        self.assertEqual(mock_describe_job.call_count, 3)

        # make sure the hook was initialized with the specific params
        calls = [
            mock.call(aws_conn_id='aws_test'),
            mock.call(aws_conn_id='aws_test'),
            mock.call(aws_conn_id='aws_test'),
        ]
        hook_init.assert_has_calls(calls)


if __name__ == '__main__':
    unittest.main()
