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
from unittest.mock import MagicMock, patch

from dateutil.tz import tzlocal

from airflow.exceptions import AirflowException
from airflow.providers.aws.sensors.emr import EmrBaseSensor, EmrJobFlowSensor, EmrStepSensor


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
        )

        with self.assertRaises(AirflowException) as context:
            operator.poke(None)

        self.assertIn('EMR job failed', str(context.exception))
        self.assertIn(EmrBaseSensorSubclass.EXPECTED_CODE, str(context.exception))
        self.assertNotIn(EmrBaseSensorSubclass.EMPTY_CODE, str(context.exception))


class TestEmrJobFlowSensor(unittest.TestCase):
    DESCRIBE_CLUSTER_RUNNING_RETURN = {
        "Cluster": {
            "Applications": [{"Name": "Spark", "Version": "1.6.1"}],
            "AutoTerminate": True,
            "Configurations": [],
            "Ec2InstanceAttributes": {"IamInstanceProfile": "EMR_EC2_DefaultRole"},
            "Id": "j-27ZY9GBEEU2GU",
            "LogUri": "s3n://some-location/",
            "Name": "PiCalc",
            "NormalizedInstanceHours": 0,
            "ReleaseLabel": "emr-4.6.0",
            "ServiceRole": "EMR_DefaultRole",
            "Status": {
                "State": "STARTING",
                "StateChangeReason": {},
                "Timeline": {
                    "CreationDateTime": datetime(2016, 6, 27, 21, 5, 2, 348000, tzinfo=tzlocal())
                },
            },
            "Tags": [{"Key": "app", "Value": "analytics"}, {"Key": "environment", "Value": "development"}],
            "TerminationProtected": False,
            "VisibleToAllUsers": True,
        },
        "ResponseMetadata": {"HTTPStatusCode": 200, "RequestId": "d5456308-3caa-11e6-9d46-951401f04e0e"},
    }

    DESCRIBE_CLUSTER_TERMINATED_RETURN = {
        "Cluster": {
            "Applications": [{"Name": "Spark", "Version": "1.6.1"}],
            "AutoTerminate": True,
            "Configurations": [],
            "Ec2InstanceAttributes": {"IamInstanceProfile": "EMR_EC2_DefaultRole"},
            "Id": "j-27ZY9GBEEU2GU",
            "LogUri": "s3n://some-location/",
            "Name": "PiCalc",
            "NormalizedInstanceHours": 0,
            "ReleaseLabel": "emr-4.6.0",
            "ServiceRole": "EMR_DefaultRole",
            "Status": {
                "State": "TERMINATED",
                "StateChangeReason": {},
                "Timeline": {
                    "CreationDateTime": datetime(2016, 6, 27, 21, 5, 2, 348000, tzinfo=tzlocal())
                },
            },
            "Tags": [{"Key": "app", "Value": "analytics"}, {"Key": "environment", "Value": "development"}],
            "TerminationProtected": False,
            "VisibleToAllUsers": True,
        },
        "ResponseMetadata": {"HTTPStatusCode": 200, "RequestId": "d5456308-3caa-11e6-9d46-951401f04e0e"},
    }

    DESCRIBE_CLUSTER_TERMINATED_WITH_ERRORS_RETURN = {
        "Cluster": {
            "Applications": [{"Name": "Spark", "Version": "1.6.1"}],
            "AutoTerminate": True,
            "Configurations": [],
            "Ec2InstanceAttributes": {"IamInstanceProfile": "EMR_EC2_DefaultRole"},
            "Id": "j-27ZY9GBEEU2GU",
            "LogUri": "s3n://some-location/",
            "Name": "PiCalc",
            "NormalizedInstanceHours": 0,
            "ReleaseLabel": "emr-4.6.0",
            "ServiceRole": "EMR_DefaultRole",
            "Status": {
                "State": "TERMINATED_WITH_ERRORS",
                "StateChangeReason": {
                    "Code": "BOOTSTRAP_FAILURE",
                    "Message": "Master instance (i-0663047709b12345c) failed attempting to "
                               "download bootstrap action 1 file from S3",
                },
                "Timeline": {
                    "CreationDateTime": datetime(2016, 6, 27, 21, 5, 2, 348000, tzinfo=tzlocal())
                },
            },
            "Tags": [{"Key": "app", "Value": "analytics"}, {"Key": "environment", "Value": "development"}],
            "TerminationProtected": False,
            "VisibleToAllUsers": True,
        },
        "ResponseMetadata": {"HTTPStatusCode": 200, "RequestId": "d5456308-3caa-11e6-9d46-951401f04e0e"},
    }

    def setUp(self):
        # Mock out the emr_client (moto has incorrect response)
        self.mock_emr_client = MagicMock()
        self.mock_emr_client.describe_cluster.side_effect = [
            self.DESCRIBE_CLUSTER_RUNNING_RETURN,
            self.DESCRIBE_CLUSTER_TERMINATED_RETURN
        ]

        mock_emr_session = MagicMock()
        mock_emr_session.client.return_value = self.mock_emr_client

        # Mock out the emr_client creator
        self.boto3_session_mock = MagicMock(return_value=mock_emr_session)

    def test_execute_calls_with_the_job_flow_id_until_it_reaches_a_terminal_state(self):
        self.mock_emr_client.describe_cluster.side_effect = [
            self.DESCRIBE_CLUSTER_RUNNING_RETURN,
            self.DESCRIBE_CLUSTER_TERMINATED_RETURN
        ]
        with patch('boto3.session.Session', self.boto3_session_mock):
            operator = EmrJobFlowSensor(
                task_id='test_task',
                poke_interval=2,
                job_flow_id='j-8989898989',
                aws_conn_id='aws_default'
            )

            operator.execute(None)

            # make sure we called twice
            self.assertEqual(self.mock_emr_client.describe_cluster.call_count, 2)

            # make sure it was called with the job_flow_id
            calls = [
                unittest.mock.call(ClusterId='j-8989898989'),
                unittest.mock.call(ClusterId='j-8989898989')
            ]
            self.mock_emr_client.describe_cluster.assert_has_calls(calls)

    def test_execute_calls_with_the_job_flow_id_until_it_reaches_failed_state_with_exception(self):
        self.mock_emr_client.describe_cluster.side_effect = [
            self.DESCRIBE_CLUSTER_RUNNING_RETURN,
            self.DESCRIBE_CLUSTER_TERMINATED_WITH_ERRORS_RETURN
        ]
        with patch('boto3.session.Session', self.boto3_session_mock):
            operator = EmrJobFlowSensor(
                task_id='test_task',
                poke_interval=2,
                job_flow_id='j-8989898989',
                aws_conn_id='aws_default'
            )

            with self.assertRaises(AirflowException):
                operator.execute(None)

                # make sure we called twice
                self.assertEqual(self.mock_emr_client.describe_cluster.call_count, 2)

                # make sure it was called with the job_flow_id
                self.mock_emr_client.describe_cluster.assert_called_once_with(ClusterId='j-8989898989')


class TestEmrStepSensor(unittest.TestCase):
    DESCRIBE_JOB_STEP_RUNNING_RETURN = {
        "ResponseMetadata": {"HTTPStatusCode": 200, "RequestId": "8dee8db2-3719-11e6-9e20-35b2f861a2a6"},
        "Step": {
            "ActionOnFailure": "CONTINUE",
            "Config": {
                "Args": ["/usr/lib/spark/bin/run-example", "SparkPi", "10"],
                "Jar": "command-runner.jar",
                "Properties": {},
            },
            "Id": "s-VK57YR1Z9Z5N",
            "Name": "calculate_pi",
            "Status": {
                "State": "RUNNING",
                "StateChangeReason": {},
                "Timeline": {
                    "CreationDateTime": datetime(2016, 6, 20, 19, 0, 18, tzinfo=tzlocal()),
                    "StartDateTime": datetime(2016, 6, 20, 19, 2, 34, tzinfo=tzlocal()),
                },
            },
        },
    }

    DESCRIBE_JOB_STEP_CANCELLED_RETURN = {
        "ResponseMetadata": {"HTTPStatusCode": 200, "RequestId": "8dee8db2-3719-11e6-9e20-35b2f861a2a6"},
        "Step": {
            "ActionOnFailure": "CONTINUE",
            "Config": {
                "Args": ["/usr/lib/spark/bin/run-example", "SparkPi", "10"],
                "Jar": "command-runner.jar",
                "Properties": {},
            },
            "Id": "s-VK57YR1Z9Z5N",
            "Name": "calculate_pi",
            "Status": {
                "State": "CANCELLED",
                "StateChangeReason": {},
                "Timeline": {
                    "CreationDateTime": datetime(2016, 6, 20, 19, 0, 18, tzinfo=tzlocal()),
                    "StartDateTime": datetime(2016, 6, 20, 19, 2, 34, tzinfo=tzlocal()),
                },
            },
        },
    }

    DESCRIBE_JOB_STEP_FAILED_RETURN = {
        "ResponseMetadata": {"HTTPStatusCode": 200, "RequestId": "8dee8db2-3719-11e6-9e20-35b2f861a2a6"},
        "Step": {
            "ActionOnFailure": "CONTINUE",
            "Config": {
                "Args": ["/usr/lib/spark/bin/run-example", "SparkPi", "10"],
                "Jar": "command-runner.jar",
                "Properties": {},
            },
            "Id": "s-VK57YR1Z9Z5N",
            "Name": "calculate_pi",
            "Status": {
                "State": "FAILED",
                "StateChangeReason": {},
                "FailureDetails": {
                    "LogFile": "s3://fake-log-files/emr-logs/j-8989898989/steps/s-VK57YR1Z9Z5N",
                    "Reason": "Unknown Error.",
                },
                "Timeline": {
                    "CreationDateTime": datetime(2016, 6, 20, 19, 0, 18, tzinfo=tzlocal()),
                    "StartDateTime": datetime(2016, 6, 20, 19, 2, 34, tzinfo=tzlocal()),
                },
            },
        },
    }

    DESCRIBE_JOB_STEP_INTERRUPTED_RETURN = {
        "ResponseMetadata": {"HTTPStatusCode": 200, "RequestId": "8dee8db2-3719-11e6-9e20-35b2f861a2a6"},
        "Step": {
            "ActionOnFailure": "CONTINUE",
            "Config": {
                "Args": ["/usr/lib/spark/bin/run-example", "SparkPi", "10"],
                "Jar": "command-runner.jar",
                "Properties": {},
            },
            "Id": "s-VK57YR1Z9Z5N",
            "Name": "calculate_pi",
            "Status": {
                "State": "INTERRUPTED",
                "StateChangeReason": {},
                "Timeline": {
                    "CreationDateTime": datetime(2016, 6, 20, 19, 0, 18, tzinfo=tzlocal()),
                    "StartDateTime": datetime(2016, 6, 20, 19, 2, 34, tzinfo=tzlocal()),
                },
            },
        },
    }

    DESCRIBE_JOB_STEP_COMPLETED_RETURN = {
        "ResponseMetadata": {"HTTPStatusCode": 200, "RequestId": "8dee8db2-3719-11e6-9e20-35b2f861a2a6"},
        "Step": {
            "ActionOnFailure": "CONTINUE",
            "Config": {
                "Args": ["/usr/lib/spark/bin/run-example", "SparkPi", "10"],
                "Jar": "command-runner.jar",
                "Properties": {},
            },
            "Id": "s-VK57YR1Z9Z5N",
            "Name": "calculate_pi",
            "Status": {
                "State": "COMPLETED",
                "StateChangeReason": {},
                "Timeline": {
                    "CreationDateTime": datetime(2016, 6, 20, 19, 0, 18, tzinfo=tzlocal()),
                    "StartDateTime": datetime(2016, 6, 20, 19, 2, 34, tzinfo=tzlocal()),
                },
            },
        },
    }

    def setUp(self):
        self.emr_client_mock = MagicMock()
        self.sensor = EmrStepSensor(
            task_id='test_task',
            poke_interval=1,
            job_flow_id='j-8989898989',
            step_id='s-VK57YR1Z9Z5N',
            aws_conn_id='aws_default',
        )

        mock_emr_session = MagicMock()
        mock_emr_session.client.return_value = self.emr_client_mock

        # Mock out the emr_client creator
        self.boto3_session_mock = MagicMock(return_value=mock_emr_session)
        self.boto3_client_mock = None

    def test_step_completed(self):
        self.emr_client_mock.describe_step.side_effect = [
            self.DESCRIBE_JOB_STEP_RUNNING_RETURN,
            self.DESCRIBE_JOB_STEP_COMPLETED_RETURN
        ]

        with patch('boto3.session.Session', self.boto3_session_mock):
            self.sensor.execute(None)

            self.assertEqual(self.emr_client_mock.describe_step.call_count, 2)
            calls = [
                unittest.mock.call(ClusterId='j-8989898989', StepId='s-VK57YR1Z9Z5N'),
                unittest.mock.call(ClusterId='j-8989898989', StepId='s-VK57YR1Z9Z5N')
            ]
            self.emr_client_mock.describe_step.assert_has_calls(calls)

    def test_step_cancelled(self):
        self.emr_client_mock.describe_step.side_effect = [
            self.DESCRIBE_JOB_STEP_RUNNING_RETURN,
            self.DESCRIBE_JOB_STEP_CANCELLED_RETURN
        ]

        self.boto3_client_mock = MagicMock(return_value=self.emr_client_mock)

        with patch('boto3.session.Session', self.boto3_session_mock):
            self.assertRaises(AirflowException, self.sensor.execute, None)

    def test_step_failed(self):
        self.emr_client_mock.describe_step.side_effect = [
            self.DESCRIBE_JOB_STEP_RUNNING_RETURN,
            self.DESCRIBE_JOB_STEP_FAILED_RETURN
        ]

        self.boto3_client_mock = MagicMock(return_value=self.emr_client_mock)

        with patch('boto3.session.Session', self.boto3_session_mock):
            self.assertRaises(AirflowException, self.sensor.execute, None)

    def test_step_interrupted(self):
        self.emr_client_mock.describe_step.side_effect = [
            self.DESCRIBE_JOB_STEP_RUNNING_RETURN,
            self.DESCRIBE_JOB_STEP_INTERRUPTED_RETURN
        ]

        self.boto3_client_mock = MagicMock(return_value=self.emr_client_mock)

        with patch('boto3.session.Session', self.boto3_session_mock):
            self.assertRaises(AirflowException, self.sensor.execute, None)


if __name__ == '__main__':
    unittest.main()
