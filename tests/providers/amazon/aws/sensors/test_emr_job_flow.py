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

import datetime
import unittest
from unittest.mock import MagicMock, patch

from dateutil.tz import tzlocal

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.sensors.emr_job_flow import EmrJobFlowSensor

DESCRIBE_CLUSTER_STARTING_RETURN = {
    'Cluster': {
        'Applications': [{'Name': 'Spark', 'Version': '1.6.1'}],
        'AutoTerminate': True,
        'Configurations': [],
        'Ec2InstanceAttributes': {'IamInstanceProfile': 'EMR_EC2_DefaultRole'},
        'Id': 'j-27ZY9GBEEU2GU',
        'LogUri': 's3n://some-location/',
        'Name': 'PiCalc',
        'NormalizedInstanceHours': 0,
        'ReleaseLabel': 'emr-4.6.0',
        'ServiceRole': 'EMR_DefaultRole',
        'Status': {
            'State': 'STARTING',
            'StateChangeReason': {},
            'Timeline': {
                'CreationDateTime': datetime.datetime(2016, 6, 27, 21, 5, 2, 348000, tzinfo=tzlocal())
            },
        },
        'Tags': [{'Key': 'app', 'Value': 'analytics'}, {'Key': 'environment', 'Value': 'development'}],
        'TerminationProtected': False,
        'VisibleToAllUsers': True,
    },
    'ResponseMetadata': {'HTTPStatusCode': 200, 'RequestId': 'd5456308-3caa-11e6-9d46-951401f04e0e'},
}

DESCRIBE_CLUSTER_BOOTSTRAPPING_RETURN = {
    'Cluster': {
        'Applications': [{'Name': 'Spark', 'Version': '1.6.1'}],
        'AutoTerminate': True,
        'Configurations': [],
        'Ec2InstanceAttributes': {'IamInstanceProfile': 'EMR_EC2_DefaultRole'},
        'Id': 'j-27ZY9GBEEU2GU',
        'LogUri': 's3n://some-location/',
        'Name': 'PiCalc',
        'NormalizedInstanceHours': 0,
        'ReleaseLabel': 'emr-4.6.0',
        'ServiceRole': 'EMR_DefaultRole',
        'Status': {
            'State': 'BOOTSTRAPPING',
            'StateChangeReason': {},
            'Timeline': {
                'CreationDateTime': datetime.datetime(2016, 6, 27, 21, 5, 2, 348000, tzinfo=tzlocal())
            },
        },
        'Tags': [{'Key': 'app', 'Value': 'analytics'}, {'Key': 'environment', 'Value': 'development'}],
        'TerminationProtected': False,
        'VisibleToAllUsers': True,
    },
    'ResponseMetadata': {'HTTPStatusCode': 200, 'RequestId': 'd5456308-3caa-11e6-9d46-951401f04e0e'},
}

DESCRIBE_CLUSTER_RUNNING_RETURN = {
    'Cluster': {
        'Applications': [{'Name': 'Spark', 'Version': '1.6.1'}],
        'AutoTerminate': True,
        'Configurations': [],
        'Ec2InstanceAttributes': {'IamInstanceProfile': 'EMR_EC2_DefaultRole'},
        'Id': 'j-27ZY9GBEEU2GU',
        'LogUri': 's3n://some-location/',
        'Name': 'PiCalc',
        'NormalizedInstanceHours': 0,
        'ReleaseLabel': 'emr-4.6.0',
        'ServiceRole': 'EMR_DefaultRole',
        'Status': {
            'State': 'RUNNING',
            'StateChangeReason': {},
            'Timeline': {
                'CreationDateTime': datetime.datetime(2016, 6, 27, 21, 5, 2, 348000, tzinfo=tzlocal())
            },
        },
        'Tags': [{'Key': 'app', 'Value': 'analytics'}, {'Key': 'environment', 'Value': 'development'}],
        'TerminationProtected': False,
        'VisibleToAllUsers': True,
    },
    'ResponseMetadata': {'HTTPStatusCode': 200, 'RequestId': 'd5456308-3caa-11e6-9d46-951401f04e0e'},
}

DESCRIBE_CLUSTER_WAITING_RETURN = {
    'Cluster': {
        'Applications': [{'Name': 'Spark', 'Version': '1.6.1'}],
        'AutoTerminate': True,
        'Configurations': [],
        'Ec2InstanceAttributes': {'IamInstanceProfile': 'EMR_EC2_DefaultRole'},
        'Id': 'j-27ZY9GBEEU2GU',
        'LogUri': 's3n://some-location/',
        'Name': 'PiCalc',
        'NormalizedInstanceHours': 0,
        'ReleaseLabel': 'emr-4.6.0',
        'ServiceRole': 'EMR_DefaultRole',
        'Status': {
            'State': 'WAITING',
            'StateChangeReason': {},
            'Timeline': {
                'CreationDateTime': datetime.datetime(2016, 6, 27, 21, 5, 2, 348000, tzinfo=tzlocal())
            },
        },
        'Tags': [{'Key': 'app', 'Value': 'analytics'}, {'Key': 'environment', 'Value': 'development'}],
        'TerminationProtected': False,
        'VisibleToAllUsers': True,
    },
    'ResponseMetadata': {'HTTPStatusCode': 200, 'RequestId': 'd5456308-3caa-11e6-9d46-951401f04e0e'},
}

DESCRIBE_CLUSTER_TERMINATED_RETURN = {
    'Cluster': {
        'Applications': [{'Name': 'Spark', 'Version': '1.6.1'}],
        'AutoTerminate': True,
        'Configurations': [],
        'Ec2InstanceAttributes': {'IamInstanceProfile': 'EMR_EC2_DefaultRole'},
        'Id': 'j-27ZY9GBEEU2GU',
        'LogUri': 's3n://some-location/',
        'Name': 'PiCalc',
        'NormalizedInstanceHours': 0,
        'ReleaseLabel': 'emr-4.6.0',
        'ServiceRole': 'EMR_DefaultRole',
        'Status': {
            'State': 'TERMINATED',
            'StateChangeReason': {},
            'Timeline': {
                'CreationDateTime': datetime.datetime(2016, 6, 27, 21, 5, 2, 348000, tzinfo=tzlocal())
            },
        },
        'Tags': [{'Key': 'app', 'Value': 'analytics'}, {'Key': 'environment', 'Value': 'development'}],
        'TerminationProtected': False,
        'VisibleToAllUsers': True,
    },
    'ResponseMetadata': {'HTTPStatusCode': 200, 'RequestId': 'd5456308-3caa-11e6-9d46-951401f04e0e'},
}

DESCRIBE_CLUSTER_TERMINATED_WITH_ERRORS_RETURN = {
    'Cluster': {
        'Applications': [{'Name': 'Spark', 'Version': '1.6.1'}],
        'AutoTerminate': True,
        'Configurations': [],
        'Ec2InstanceAttributes': {'IamInstanceProfile': 'EMR_EC2_DefaultRole'},
        'Id': 'j-27ZY9GBEEU2GU',
        'LogUri': 's3n://some-location/',
        'Name': 'PiCalc',
        'NormalizedInstanceHours': 0,
        'ReleaseLabel': 'emr-4.6.0',
        'ServiceRole': 'EMR_DefaultRole',
        'Status': {
            'State': 'TERMINATED_WITH_ERRORS',
            'StateChangeReason': {
                'Code': 'BOOTSTRAP_FAILURE',
                'Message': 'Master instance (i-0663047709b12345c) failed attempting to '
                'download bootstrap action 1 file from S3',
            },
            'Timeline': {
                'CreationDateTime': datetime.datetime(2016, 6, 27, 21, 5, 2, 348000, tzinfo=tzlocal())
            },
        },
        'Tags': [{'Key': 'app', 'Value': 'analytics'}, {'Key': 'environment', 'Value': 'development'}],
        'TerminationProtected': False,
        'VisibleToAllUsers': True,
    },
    'ResponseMetadata': {'HTTPStatusCode': 200, 'RequestId': 'd5456308-3caa-11e6-9d46-951401f04e0e'},
}


class TestEmrJobFlowSensor(unittest.TestCase):
    def setUp(self):
        # Mock out the emr_client (moto has incorrect response)
        self.mock_emr_client = MagicMock()

        mock_emr_session = MagicMock()
        mock_emr_session.client.return_value = self.mock_emr_client

        # Mock out the emr_client creator
        self.boto3_session_mock = MagicMock(return_value=mock_emr_session)

    def test_execute_calls_with_the_job_flow_id_until_it_reaches_a_target_state(self):
        self.mock_emr_client.describe_cluster.side_effect = [
            DESCRIBE_CLUSTER_STARTING_RETURN,
            DESCRIBE_CLUSTER_RUNNING_RETURN,
            DESCRIBE_CLUSTER_TERMINATED_RETURN,
        ]
        with patch('boto3.session.Session', self.boto3_session_mock):
            operator = EmrJobFlowSensor(
                task_id='test_task', poke_interval=0, job_flow_id='j-8989898989', aws_conn_id='aws_default'
            )

            operator.execute(None)

            # make sure we called twice
            self.assertEqual(self.mock_emr_client.describe_cluster.call_count, 3)

            # make sure it was called with the job_flow_id
            calls = [unittest.mock.call(ClusterId='j-8989898989')]
            self.mock_emr_client.describe_cluster.assert_has_calls(calls)

    def test_execute_calls_with_the_job_flow_id_until_it_reaches_failed_state_with_exception(self):
        self.mock_emr_client.describe_cluster.side_effect = [
            DESCRIBE_CLUSTER_RUNNING_RETURN,
            DESCRIBE_CLUSTER_TERMINATED_WITH_ERRORS_RETURN,
        ]
        with patch('boto3.session.Session', self.boto3_session_mock):
            operator = EmrJobFlowSensor(
                task_id='test_task', poke_interval=0, job_flow_id='j-8989898989', aws_conn_id='aws_default'
            )

            with self.assertRaises(AirflowException):
                operator.execute(None)

                # make sure we called twice
                self.assertEqual(self.mock_emr_client.describe_cluster.call_count, 2)

                # make sure it was called with the job_flow_id
                self.mock_emr_client.describe_cluster.assert_called_once_with(ClusterId='j-8989898989')

    def test_different_target_states(self):
        self.mock_emr_client.describe_cluster.side_effect = [
            DESCRIBE_CLUSTER_STARTING_RETURN,  # return False
            DESCRIBE_CLUSTER_BOOTSTRAPPING_RETURN,  # return False
            DESCRIBE_CLUSTER_RUNNING_RETURN,  # return True
            DESCRIBE_CLUSTER_WAITING_RETURN,  # will not be used
            DESCRIBE_CLUSTER_TERMINATED_RETURN,  # will not be used
            DESCRIBE_CLUSTER_TERMINATED_WITH_ERRORS_RETURN,  # will not be used
        ]
        with patch('boto3.session.Session', self.boto3_session_mock):
            operator = EmrJobFlowSensor(
                task_id='test_task',
                poke_interval=0,
                job_flow_id='j-8989898989',
                aws_conn_id='aws_default',
                target_states=['RUNNING', 'WAITING'],
            )

            operator.execute(None)

            # make sure we called twice
            self.assertEqual(self.mock_emr_client.describe_cluster.call_count, 3)

            # make sure it was called with the job_flow_id
            calls = [unittest.mock.call(ClusterId='j-8989898989')]
            self.mock_emr_client.describe_cluster.assert_has_calls(calls)
