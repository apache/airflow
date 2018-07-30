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

try:
    from unittest import mock
except ImportError:
    try:
        import mock
    except ImportError:
        mock = None

from airflow import configuration
from airflow.contrib.sensors.sagemaker_training_sensor \
    import SageMakerTrainingSensor
from airflow.contrib.hooks.sagemaker_hook import SageMakerHook
from airflow.exceptions import AirflowException

DESCRIBE_TRAINING_INPROGRESS_RETURN = {
    'TrainingJobStatus': 'InProgress',
    'ResponseMetadata': {
        'HTTPStatusCode': 200,
    }
}
DESCRIBE_TRAINING_COMPELETED_RETURN = {
    'TrainingJobStatus': 'Compeleted',
    'ResponseMetadata': {
        'HTTPStatusCode': 200,
    }
}
DESCRIBE_TRAINING_FAILED_RETURN = {
    'TrainingJobStatus': 'Failed',
    'ResponseMetadata': {
        'HTTPStatusCode': 200,
    },
    'FailureReason': 'Unknown'
}
DESCRIBE_TRAINING_STOPPING_RETURN = {
    'TrainingJobStatus': 'Stopping',
    'ResponseMetadata': {
        'HTTPStatusCode': 200,
    }
}
DESCRIBE_TRAINING_STOPPED_RETURN = {
    'TrainingJobStatus': 'Stopped',
    'ResponseMetadata': {
        'HTTPStatusCode': 200,
    }
}


class TestSageMakerTrainingSensor(unittest.TestCase):
    def setUp(self):
        configuration.load_test_config()

    @mock.patch.object(SageMakerHook, 'get_conn')
    @mock.patch.object(SageMakerHook, 'describe_training_job')
    def test_raises_errors_failed_state(self, mock_describe_job, mock_client):
        mock_describe_job.side_effect = [DESCRIBE_TRAINING_FAILED_RETURN]
        sensor = SageMakerTrainingSensor(
            task_id='test_task',
            poke_interval=2,
            aws_conn_id='aws_test',
            job_name='test_job_name'
        )
        self.assertRaises(AirflowException, sensor.execute, None)
        mock_describe_job.assert_called_once_with('test_job_name')

    @mock.patch.object(SageMakerHook, 'get_conn')
    @mock.patch.object(SageMakerHook, '__init__')
    @mock.patch.object(SageMakerHook, 'describe_training_job')
    def test_calls_until_a_terminal_state(self,
                                          mock_describe_job, hook_init, mock_client):
        hook_init.return_value = None

        mock_describe_job.side_effect = [
            DESCRIBE_TRAINING_INPROGRESS_RETURN,
            DESCRIBE_TRAINING_STOPPING_RETURN,
            DESCRIBE_TRAINING_STOPPED_RETURN,
            DESCRIBE_TRAINING_COMPELETED_RETURN
        ]
        sensor = SageMakerTrainingSensor(
            task_id='test_task',
            poke_interval=2,
            aws_conn_id='aws_test',
            job_name='test_job_name',
            region_name='us-east-1'
        )

        sensor.execute(None)

        # make sure we called 4 times(terminated when its compeleted)
        self.assertEqual(mock_describe_job.call_count, 4)

        # make sure the hook was initialized with the specific params
        hook_init.assert_called_with(aws_conn_id='aws_test',
                                     region_name='us-east-1')


if __name__ == '__main__':
    unittest.main()
