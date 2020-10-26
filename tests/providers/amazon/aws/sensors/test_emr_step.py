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
from unittest.mock import MagicMock, patch

from dateutil.tz import tzlocal

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.sensors.emr_step import EmrStepSensor

from airflow.models import TaskInstance, DAG
from airflow.utils import timezone


DESCRIBE_JOB_STEP_RUNNING_RETURN = {
    'ResponseMetadata': {'HTTPStatusCode': 200, 'RequestId': '8dee8db2-3719-11e6-9e20-35b2f861a2a6'},
    'Step': {
        'ActionOnFailure': 'CONTINUE',
        'Config': {
            'Args': ['/usr/lib/spark/bin/run-example', 'SparkPi', '10'],
            'Jar': 'command-runner.jar',
            'Properties': {},
        },
        'Id': 's-VK57YR1Z9Z5N',
        'Name': 'calculate_pi',
        'Status': {
            'State': 'RUNNING',
            'StateChangeReason': {},
            'Timeline': {
                'CreationDateTime': datetime(2016, 6, 20, 19, 0, 18, tzinfo=tzlocal()),
                'StartDateTime': datetime(2016, 6, 20, 19, 2, 34, tzinfo=tzlocal()),
            },
        },
    },
}

DESCRIBE_JOB_STEP_CANCELLED_RETURN = {
    'ResponseMetadata': {'HTTPStatusCode': 200, 'RequestId': '8dee8db2-3719-11e6-9e20-35b2f861a2a6'},
    'Step': {
        'ActionOnFailure': 'CONTINUE',
        'Config': {
            'Args': ['/usr/lib/spark/bin/run-example', 'SparkPi', '10'],
            'Jar': 'command-runner.jar',
            'Properties': {},
        },
        'Id': 's-VK57YR1Z9Z5N',
        'Name': 'calculate_pi',
        'Status': {
            'State': 'CANCELLED',
            'StateChangeReason': {},
            'Timeline': {
                'CreationDateTime': datetime(2016, 6, 20, 19, 0, 18, tzinfo=tzlocal()),
                'StartDateTime': datetime(2016, 6, 20, 19, 2, 34, tzinfo=tzlocal()),
            },
        },
    },
}

DESCRIBE_JOB_STEP_FAILED_RETURN = {
    'ResponseMetadata': {'HTTPStatusCode': 200, 'RequestId': '8dee8db2-3719-11e6-9e20-35b2f861a2a6'},
    'Step': {
        'ActionOnFailure': 'CONTINUE',
        'Config': {
            'Args': ['/usr/lib/spark/bin/run-example', 'SparkPi', '10'],
            'Jar': 'command-runner.jar',
            'Properties': {},
        },
        'Id': 's-VK57YR1Z9Z5N',
        'Name': 'calculate_pi',
        'Status': {
            'State': 'FAILED',
            'StateChangeReason': {},
            'FailureDetails': {
                'LogFile': 's3://fake-log-files/emr-logs/j-8989898989/steps/s-VK57YR1Z9Z5N',
                'Reason': 'Unknown Error.',
            },
            'Timeline': {
                'CreationDateTime': datetime(2016, 6, 20, 19, 0, 18, tzinfo=tzlocal()),
                'StartDateTime': datetime(2016, 6, 20, 19, 2, 34, tzinfo=tzlocal()),
            },
        },
    },
}

DESCRIBE_JOB_STEP_INTERRUPTED_RETURN = {
    'ResponseMetadata': {'HTTPStatusCode': 200, 'RequestId': '8dee8db2-3719-11e6-9e20-35b2f861a2a6'},
    'Step': {
        'ActionOnFailure': 'CONTINUE',
        'Config': {
            'Args': ['/usr/lib/spark/bin/run-example', 'SparkPi', '10'],
            'Jar': 'command-runner.jar',
            'Properties': {},
        },
        'Id': 's-VK57YR1Z9Z5N',
        'Name': 'calculate_pi',
        'Status': {
            'State': 'INTERRUPTED',
            'StateChangeReason': {},
            'Timeline': {
                'CreationDateTime': datetime(2016, 6, 20, 19, 0, 18, tzinfo=tzlocal()),
                'StartDateTime': datetime(2016, 6, 20, 19, 2, 34, tzinfo=tzlocal()),
            },
        },
    },
}

DESCRIBE_JOB_STEP_COMPLETED_RETURN = {
    'ResponseMetadata': {'HTTPStatusCode': 200, 'RequestId': '8dee8db2-3719-11e6-9e20-35b2f861a2a6'},
    'Step': {
        'ActionOnFailure': 'CONTINUE',
        'Config': {
            'Args': ['/usr/lib/spark/bin/run-example', 'SparkPi', '10'],
            'Jar': 'command-runner.jar',
            'Properties': {},
        },
        'Id': 's-VK57YR1Z9Z5N',
        'Name': 'calculate_pi',
        'Status': {
            'State': 'COMPLETED',
            'StateChangeReason': {},
            'Timeline': {
                'CreationDateTime': datetime(2016, 6, 20, 19, 0, 18, tzinfo=tzlocal()),
                'StartDateTime': datetime(2016, 6, 20, 19, 2, 34, tzinfo=tzlocal()),
            },
        },
    },
}


class TestEmrStepSensor(unittest.TestCase):
    def setUp(self):
        self.emr_client_mock = MagicMock()
        self.sensor = EmrStepSensor(
            task_id='test_task',
            poke_interval=0,
            job_flow_id='j-8989898989',
            step_id='s-VK57YR1Z9Z5N',
            aws_conn_id='aws_default',
        )
        # raise Exception(self.step_id)
        mock_emr_session = MagicMock()
        mock_emr_session.client.return_value = self.emr_client_mock

        # Mock out the emr_client creator
        self.boto3_session_mock = MagicMock(return_value=mock_emr_session)

    def test_step_completed(self):
        self.emr_client_mock.describe_step.side_effect = [
            DESCRIBE_JOB_STEP_RUNNING_RETURN,
            DESCRIBE_JOB_STEP_COMPLETED_RETURN,
        ]

        with patch('boto3.session.Session', self.boto3_session_mock):
            self.sensor.execute(None)

            self.assertEqual(self.emr_client_mock.describe_step.call_count, 2)
            calls = [
                unittest.mock.call(ClusterId='j-8989898989', StepId='s-VK57YR1Z9Z5N'),
                unittest.mock.call(ClusterId='j-8989898989', StepId='s-VK57YR1Z9Z5N'),
            ]
            self.emr_client_mock.describe_step.assert_has_calls(calls)

    def test_step_cancelled(self):
        self.emr_client_mock.describe_step.side_effect = [
            DESCRIBE_JOB_STEP_RUNNING_RETURN,
            DESCRIBE_JOB_STEP_CANCELLED_RETURN,
        ]

        with patch('boto3.session.Session', self.boto3_session_mock):
            self.assertRaises(AirflowException, self.sensor.execute, None)

    def test_step_failed(self):
        self.emr_client_mock.describe_step.side_effect = [
            DESCRIBE_JOB_STEP_RUNNING_RETURN,
            DESCRIBE_JOB_STEP_FAILED_RETURN,
        ]

        with patch('boto3.session.Session', self.boto3_session_mock):
            self.assertRaises(AirflowException, self.sensor.execute, None)

    def test_step_interrupted(self):
        self.emr_client_mock.describe_step.side_effect = [
            DESCRIBE_JOB_STEP_RUNNING_RETURN,
            DESCRIBE_JOB_STEP_INTERRUPTED_RETURN,
        ]

        with patch('boto3.session.Session', self.boto3_session_mock):
            self.assertRaises(AirflowException, self.sensor.execute, None)


LIST_STEPS_RUNNING_RESPONSES = {
    'Steps': [
        {
            'Id': 's-VK57YR1Z9Z5N',
            'Name': 'calculate_pi',
            'Config': {
                'Args': ['/usr/lib/spark/bin/run-example', 'SparkPi', '10'],
                'Jar': 'command-runner.jar',
                'Properties': {},
            },
            'ActionOnFailure': 'CONTINUE',
            'Status': {
                'State': 'RUNNING',
                'StateChangeReason': {},
                'Timeline': {
                    'CreationDateTime': datetime(2016, 6, 20, 19, 0, 18),
                    'StartDateTime': datetime(2016, 6, 20, 19, 2, 34),
                },
            },
        },
        {
            'Id': 's-VK57YR1Z9Z6N',
            'Name': 'calculate_pi',
            'Config': {
                'Args': ['/usr/lib/spark/bin/run-example', 'SparkPi', '10'],
                'Jar': 'command-runner.jar',
                'Properties': {},
            },
            'ActionOnFailure': 'CONTINUE',
            'Status': {
                'State': 'RUNNING',
                'StateChangeReason': {},
                'Timeline': {
                    'CreationDateTime': datetime(2016, 6, 20, 19, 0, 18),
                    'StartDateTime': datetime(2016, 6, 20, 19, 2, 34),
                },
            },
        },
        {
            'Id': 's-VK57YR1Z9Z7N',
            'Name': 'calculate_pi',
            'Config': {
                'Args': ['/usr/lib/spark/bin/run-example', 'SparkPi', '10'],
                'Jar': 'command-runner.jar',
                'Properties': {},
            },
            'ActionOnFailure': 'CONTINUE',
            'Status': {
                'State': 'RUNNING',
                'StateChangeReason': {},
                'Timeline': {
                    'CreationDateTime': datetime(2016, 6, 20, 19, 0, 18),
                    'StartDateTime': datetime(2016, 6, 20, 19, 2, 34),
                },
            },
        },
        {
            'Id': 's-VK57YR1Z9Z8N',
            'Name': 'calculate_pi',
            'Config': {
                'Args': ['/usr/lib/spark/bin/run-example', 'SparkPi', '10'],
                'Jar': 'command-runner.jar',
                'Properties': {},
            },
            'ActionOnFailure': 'CONTINUE',
            'Status': {
                'State': 'RUNNING',
                'StateChangeReason': {},
                'Timeline': {
                    'CreationDateTime': datetime(2016, 6, 20, 19, 0, 18),
                    'StartDateTime': datetime(2016, 6, 20, 19, 2, 34),
                },
            },
        },
    ],
    'ResponseMetadata': {'HTTPStatusCode': 200, 'RequestId': 'omitted'},
}

LIST_STEPS_COMPLETED_RESPONSES = {
    'Steps': [
        {
            'Id': 's-VK57YR1Z9Z5N',
            'Name': 'calculate_pi',
            'Config': {
                'Args': ['/usr/lib/spark/bin/run-example', 'SparkPi', '10'],
                'Jar': 'command-runner.jar',
                'Properties': {},
            },
            'ActionOnFailure': 'CONTINUE',
            'Status': {
                'State': 'COMPLETED',
                'StateChangeReason': {},
                'Timeline': {
                    'CreationDateTime': datetime(2016, 6, 20, 19, 0, 18),
                    'StartDateTime': datetime(2016, 6, 20, 19, 2, 34),
                },
            },
        },
        {
            'Id': 's-VK57YR1Z9Z6N',
            'Name': 'calculate_pi',
            'Config': {
                'Args': ['/usr/lib/spark/bin/run-example', 'SparkPi', '10'],
                'Jar': 'command-runner.jar',
                'Properties': {},
            },
            'ActionOnFailure': 'CONTINUE',
            'Status': {
                'State': 'COMPLETED',
                'StateChangeReason': {},
                'Timeline': {
                    'CreationDateTime': datetime(2016, 6, 20, 19, 0, 18),
                    'StartDateTime': datetime(2016, 6, 20, 19, 2, 34),
                },
            },
        },
        {
            'Id': 's-VK57YR1Z9Z7N',
            'Name': 'calculate_pi',
            'Config': {
                'Args': ['/usr/lib/spark/bin/run-example', 'SparkPi', '10'],
                'Jar': 'command-runner.jar',
                'Properties': {},
            },
            'ActionOnFailure': 'CONTINUE',
            'Status': {
                'State': 'COMPLETED',
                'StateChangeReason': {},
                'Timeline': {
                    'CreationDateTime': datetime(2016, 6, 20, 19, 0, 18),
                    'StartDateTime': datetime(2016, 6, 20, 19, 2, 34),
                },
            },
        },
        {
            'Id': 's-VK57YR1Z9Z8N',
            'Name': 'calculate_pi',
            'Config': {
                'Args': ['/usr/lib/spark/bin/run-example', 'SparkPi', '10'],
                'Jar': 'command-runner.jar',
                'Properties': {},
            },
            'ActionOnFailure': 'CONTINUE',
            'Status': {
                'State': 'COMPLETED',
                'StateChangeReason': {},
                'Timeline': {
                    'CreationDateTime': datetime(2016, 6, 20, 19, 0, 18),
                    'StartDateTime': datetime(2016, 6, 20, 19, 2, 34),
                },
            },
        },
    ],
    'ResponseMetadata': {'HTTPStatusCode': 200, 'RequestId': 'omitted'},
}

LIST_STEPS_FAILED_RESPONSES = {
    'Steps': [
        {
            'Id': 's-VK57YR1Z9Z5N',
            'Name': 'calculate_pi',
            'Config': {
                'Args': ['/usr/lib/spark/bin/run-example', 'SparkPi', '10'],
                'Jar': 'command-runner.jar',
                'Properties': {},
            },
            'ActionOnFailure': 'CONTINUE',
            'Status': {
                'State': 'FAILED',
                'StateChangeReason': {},
                'FailureDetails': {
                    'LogFile': 's3://fake-log-files/emr-logs/j-8989898989/steps/s-VK57YR1Z9Z5N',
                    'Reason': 'Unknown Error.',
                },
                'Timeline': {
                    'CreationDateTime': datetime(2016, 6, 20, 19, 0, 18),
                    'StartDateTime': datetime(2016, 6, 20, 19, 2, 34),
                },
            },
        },
        {
            'Id': 's-VK57YR1Z9Z6N',
            'Name': 'calculate_pi',
            'Config': {
                'Args': ['/usr/lib/spark/bin/run-example', 'SparkPi', '10'],
                'Jar': 'command-runner.jar',
                'Properties': {},
            },
            'ActionOnFailure': 'CONTINUE',
            'Status': {
                'State': 'FAILED',
                'StateChangeReason': {},
                'FailureDetails': {
                    'LogFile': 's3://fake-log-files/emr-logs/j-8989898989/steps/s-VK57YR1Z9Z5N',
                    'Reason': 'Unknown Error.',
                },
                'Timeline': {
                    'CreationDateTime': datetime(2016, 6, 20, 19, 0, 18),
                    'StartDateTime': datetime(2016, 6, 20, 19, 2, 34),
                },
            },
        },
        {
            'Id': 's-VK57YR1Z9Z7N',
            'Name': 'calculate_pi',
            'Config': {
                'Args': ['/usr/lib/spark/bin/run-example', 'SparkPi', '10'],
                'Jar': 'command-runner.jar',
                'Properties': {},
            },
            'ActionOnFailure': 'CONTINUE',
            'Status': {
                'State': 'FAILED',
                'StateChangeReason': {},
                'FailureDetails': {
                    'LogFile': 's3://fake-log-files/emr-logs/j-8989898989/steps/s-VK57YR1Z9Z5N',
                    'Reason': 'Unknown Error.',
                },
                'Timeline': {
                    'CreationDateTime': datetime(2016, 6, 20, 19, 0, 18),
                    'StartDateTime': datetime(2016, 6, 20, 19, 2, 34),
                },
            },
        },
        {
            'Id': 's-VK57YR1Z9Z8N',
            'Name': 'calculate_pi',
            'Config': {
                'Args': ['/usr/lib/spark/bin/run-example', 'SparkPi', '10'],
                'Jar': 'command-runner.jar',
                'Properties': {},
            },
            'ActionOnFailure': 'CONTINUE',
            'Status': {
                'State': 'FAILED',
                'StateChangeReason': {},
                'FailureDetails': {
                    'LogFile': 's3://fake-log-files/emr-logs/j-8989898989/steps/s-VK57YR1Z9Z5N',
                    'Reason': 'Unknown Error.',
                },
                'Timeline': {
                    'CreationDateTime': datetime(2016, 6, 20, 19, 0, 18),
                    'StartDateTime': datetime(2016, 6, 20, 19, 2, 34),
                },
            },
        },
    ],
    'ResponseMetadata': {'HTTPStatusCode': 200, 'RequestId': 'omitted'},
}

LIST_STEPS_INTERRUPTED_RESPONSES = {
    'Steps': [
        {
            'Id': 's-VK57YR1Z9Z5N',
            'Name': 'calculate_pi',
            'Config': {
                'Args': ['/usr/lib/spark/bin/run-example', 'SparkPi', '10'],
                'Jar': 'command-runner.jar',
                'Properties': {},
            },
            'ActionOnFailure': 'CONTINUE',
            'Status': {
                'State': 'INTERRUPTED',
                'StateChangeReason': {},
                'FailureDetails': {
                    'LogFile': 's3://fake-log-files/emr-logs/j-8989898989/steps/s-VK57YR1Z9Z5N',
                    'Reason': 'Unknown Error.',
                },
                'Timeline': {
                    'CreationDateTime': datetime(2016, 6, 20, 19, 0, 18),
                    'StartDateTime': datetime(2016, 6, 20, 19, 2, 34),
                },
            },
        },
        {
            'Id': 's-VK57YR1Z9Z6N',
            'Name': 'calculate_pi',
            'Config': {
                'Args': ['/usr/lib/spark/bin/run-example', 'SparkPi', '10'],
                'Jar': 'command-runner.jar',
                'Properties': {},
            },
            'ActionOnFailure': 'CONTINUE',
            'Status': {
                'State': 'INTERRUPTED',
                'StateChangeReason': {},
                'FailureDetails': {
                    'LogFile': 's3://fake-log-files/emr-logs/j-8989898989/steps/s-VK57YR1Z9Z5N',
                    'Reason': 'Unknown Error.',
                },
                'Timeline': {
                    'CreationDateTime': datetime(2016, 6, 20, 19, 0, 18),
                    'StartDateTime': datetime(2016, 6, 20, 19, 2, 34),
                },
            },
        },
        {
            'Id': 's-VK57YR1Z9Z7N',
            'Name': 'calculate_pi',
            'Config': {
                'Args': ['/usr/lib/spark/bin/run-example', 'SparkPi', '10'],
                'Jar': 'command-runner.jar',
                'Properties': {},
            },
            'ActionOnFailure': 'CONTINUE',
            'Status': {
                'State': 'INTERRUPTED',
                'StateChangeReason': {},
                'FailureDetails': {
                    'LogFile': 's3://fake-log-files/emr-logs/j-8989898989/steps/s-VK57YR1Z9Z5N',
                    'Reason': 'Unknown Error.',
                },
                'Timeline': {
                    'CreationDateTime': datetime(2016, 6, 20, 19, 0, 18),
                    'StartDateTime': datetime(2016, 6, 20, 19, 2, 34),
                },
            },
        },
        {
            'Id': 's-VK57YR1Z9Z8N',
            'Name': 'calculate_pi',
            'Config': {
                'Args': ['/usr/lib/spark/bin/run-example', 'SparkPi', '10'],
                'Jar': 'command-runner.jar',
                'Properties': {},
            },
            'ActionOnFailure': 'CONTINUE',
            'Status': {
                'State': 'INTERRUPTED',
                'StateChangeReason': {},
                'FailureDetails': {
                    'LogFile': 's3://fake-log-files/emr-logs/j-8989898989/steps/s-VK57YR1Z9Z5N',
                    'Reason': 'Unknown Error.',
                },
                'Timeline': {
                    'CreationDateTime': datetime(2016, 6, 20, 19, 0, 18),
                    'StartDateTime': datetime(2016, 6, 20, 19, 2, 34),
                },
            },
        },
    ],
    'ResponseMetadata': {'HTTPStatusCode': 200, 'RequestId': 'omitted'},
}

LIST_STEPS_MIXED_RESPONSES = {
    'Steps': [
        {
            'Id': 's-VK57YR1Z9Z5N',
            'Name': 'calculate_pi',
            'Config': {
                'Args': ['/usr/lib/spark/bin/run-example', 'SparkPi', '10'],
                'Jar': 'command-runner.jar',
                'Properties': {},
            },
            'ActionOnFailure': 'CONTINUE',
            'Status': {
                'State': 'INTERRUPTED',
                'StateChangeReason': {},
                'Timeline': {
                    'CreationDateTime': datetime(2016, 6, 20, 19, 0, 18),
                    'StartDateTime': datetime(2016, 6, 20, 19, 2, 34),
                },
            },
        },
        {
            'Id': 's-VK57YR1Z9Z6N',
            'Name': 'calculate_pi',
            'Config': {
                'Args': ['/usr/lib/spark/bin/run-example', 'SparkPi', '10'],
                'Jar': 'command-runner.jar',
                'Properties': {},
            },
            'ActionOnFailure': 'CONTINUE',
            'Status': {
                'State': 'FAILED',
                'StateChangeReason': {},
                'FailureDetails': {
                    'LogFile': 's3://fake-log-files/emr-logs/j-8989898989/steps/s-VK57YR1Z9Z5N',
                    'Reason': 'Unknown Error.',
                },
                'Timeline': {
                    'CreationDateTime': datetime(2016, 6, 20, 19, 0, 18),
                    'StartDateTime': datetime(2016, 6, 20, 19, 2, 34),
                },
            },
        },
        {
            'Id': 's-VK57YR1Z9Z7N',
            'Name': 'calculate_pi',
            'Config': {
                'Args': ['/usr/lib/spark/bin/run-example', 'SparkPi', '10'],
                'Jar': 'command-runner.jar',
                'Properties': {},
            },
            'ActionOnFailure': 'CONTINUE',
            'Status': {
                'State': 'CANCELLED',
                'StateChangeReason': {},
                'Timeline': {
                    'CreationDateTime': datetime(2016, 6, 20, 19, 0, 18),
                    'StartDateTime': datetime(2016, 6, 20, 19, 2, 34),
                },
            },
        },
        {
            'Id': 's-VK57YR1Z9Z8N',
            'Name': 'calculate_pi',
            'Config': {
                'Args': ['/usr/lib/spark/bin/run-example', 'SparkPi', '10'],
                'Jar': 'command-runner.jar',
                'Properties': {},
            },
            'ActionOnFailure': 'CONTINUE',
            'Status': {
                'State': 'COMPLETED',
                'StateChangeReason': {},
                'Timeline': {
                    'CreationDateTime': datetime(2016, 6, 20, 19, 0, 18),
                    'StartDateTime': datetime(2016, 6, 20, 19, 2, 34),
                },
            },
        },
    ],
    'ResponseMetadata': {'HTTPStatusCode': 200, 'RequestId': 'omitted'},
}


class TestGetListSteps(unittest.TestCase):
    def setUp(self):
        args = {
            "owner": "airflow",
            "start_date": timezone.datetime(2018, 1, 1),
        }

        dag = DAG(
            'test_dag',
            default_args=args,
            schedule_interval="@once",
        )
        self.emr_client_mock = MagicMock()
        self.sensor = EmrStepSensor(
            task_id='test_task',
            poke_interval=0,
            job_flow_id='j-8989898989',
            step_id=None,
            aws_conn_id='aws_default',
            dag=dag,
        )

        mock_emr_session = MagicMock()
        mock_emr_session.client.return_value = self.emr_client_mock

        # Mock out the emr_client creator
        self.boto3_session_mock = MagicMock(return_value=mock_emr_session)

    def test_get_steps_if_no_steps_specified_completed(self):
        self.emr_client_mock.list_steps.side_effect = [
            LIST_STEPS_COMPLETED_RESPONSES,
        ]

        with patch('boto3.session.Session', self.boto3_session_mock):
            ti = TaskInstance(task=self.sensor, execution_date=timezone.utcnow())
            ti.run()
            xcom_result = ti.xcom_pull(task_ids=self.sensor.task_id, key="return_value")
            calls = [
                unittest.mock.call(ClusterId='j-8989898989'),
            ]
            self.assertEqual(self.emr_client_mock.list_steps.call_count, 1)
            self.emr_client_mock.list_steps.assert_has_calls(calls)
            self.assertIsNotNone(xcom_result)

    def test_get_steps_if_no_steps_specified_failed(self):
        self.emr_client_mock.list_steps.side_effect = [
            LIST_STEPS_FAILED_RESPONSES,
        ]

        with patch('boto3.session.Session', self.boto3_session_mock):
            ti = TaskInstance(task=self.sensor, execution_date=timezone.utcnow())
            self.assertRaises(AirflowException, ti.run, None)
            xcom_result = ti.xcom_pull(task_ids=self.sensor.task_id, key="return_value")
            calls = [
                unittest.mock.call(ClusterId='j-8989898989'),
            ]
            self.assertEqual(self.emr_client_mock.list_steps.call_count, 1)
            self.emr_client_mock.list_steps.assert_has_calls(calls)
            self.assertIsNotNone(xcom_result)

    def test_get_steps_if_no_steps_specified_interrupted(self):
        self.emr_client_mock.list_steps.side_effect = [
            LIST_STEPS_INTERRUPTED_RESPONSES,
        ]

        with patch('boto3.session.Session', self.boto3_session_mock):
            ti = TaskInstance(task=self.sensor, execution_date=timezone.utcnow())
            self.assertRaises(AirflowException, ti.run, None)
            xcom_result = ti.xcom_pull(task_ids=self.sensor.task_id, key="return_value")
            calls = [
                unittest.mock.call(ClusterId='j-8989898989'),
            ]
            self.assertEqual(self.emr_client_mock.list_steps.call_count, 1)
            self.emr_client_mock.list_steps.assert_has_calls(calls)
            self.assertIsNotNone(xcom_result)

    def test_get_steps_if_no_steps_specified_running_completed(self):
        self.emr_client_mock.list_steps.side_effect = [
            LIST_STEPS_RUNNING_RESPONSES,
            LIST_STEPS_COMPLETED_RESPONSES,
        ]

        with patch('boto3.session.Session', self.boto3_session_mock):
            ti = TaskInstance(task=self.sensor, execution_date=timezone.utcnow())
            ti.run()
            xcom_result = ti.xcom_pull(task_ids=self.sensor.task_id, key="return_value")
            self.assertEqual(self.emr_client_mock.list_steps.call_count, 2)
            calls = [
                unittest.mock.call(ClusterId='j-8989898989'),
                unittest.mock.call(ClusterId='j-8989898989'),
            ]
            self.emr_client_mock.list_steps.assert_has_calls(calls)
            self.assertIsNotNone(xcom_result)

    def test_get_steps_if_no_steps_specified_running_failed(self):
        self.emr_client_mock.list_steps.side_effect = [
            LIST_STEPS_RUNNING_RESPONSES,
            LIST_STEPS_RUNNING_RESPONSES,
            LIST_STEPS_FAILED_RESPONSES,
        ]

        with patch('boto3.session.Session', self.boto3_session_mock):
            ti = TaskInstance(task=self.sensor, execution_date=timezone.utcnow())
            self.assertRaises(AirflowException, ti.run, None)
            xcom_result = ti.xcom_pull(task_ids=self.sensor.task_id, key="return_value")
            self.assertEqual(self.emr_client_mock.list_steps.call_count, 3)
            calls = [
                unittest.mock.call(ClusterId='j-8989898989'),
                unittest.mock.call(ClusterId='j-8989898989'),
                unittest.mock.call(ClusterId='j-8989898989'),
            ]
            self.emr_client_mock.list_steps.assert_has_calls(calls)
            self.assertIsNotNone(xcom_result)
