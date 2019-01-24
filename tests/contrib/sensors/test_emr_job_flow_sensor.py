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
import datetime
from dateutil.tz import tzlocal
from mock import MagicMock, patch

from airflow import configuration
from airflow.contrib.sensors.emr_job_flow_sensor import EmrJobFlowSensor

DESCRIBE_CLUSTER_RUNNING_RETURN = {
    'Cluster': {
        'Applications': [
            {'Name': 'Spark', 'Version': '1.6.1'}
        ],
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
                'CreationDateTime': datetime.datetime(2016, 6, 27, 21, 5, 2, 348000, tzinfo=tzlocal())}
        },
        'Tags': [
            {'Key': 'app', 'Value': 'analytics'},
            {'Key': 'environment', 'Value': 'development'}
        ],
        'TerminationProtected': False,
        'VisibleToAllUsers': True
    },
    'ResponseMetadata': {
        'HTTPStatusCode': 200,
        'RequestId': 'd5456308-3caa-11e6-9d46-951401f04e0e'
    }
}

DESCRIBE_CLUSTER_TERMINATED_RETURN = {
    'Cluster': {
        'Applications': [
            {'Name': 'Spark', 'Version': '1.6.1'}
        ],
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
                'CreationDateTime': datetime.datetime(2016, 6, 27, 21, 5, 2, 348000, tzinfo=tzlocal())}
        },
        'Tags': [
            {'Key': 'app', 'Value': 'analytics'},
            {'Key': 'environment', 'Value': 'development'}
        ],
        'TerminationProtected': False,
        'VisibleToAllUsers': True
    },
    'ResponseMetadata': {
        'HTTPStatusCode': 200,
        'RequestId': 'd5456308-3caa-11e6-9d46-951401f04e0e'
    }
}


class TestEmrJobFlowSensor(unittest.TestCase):
    def setUp(self):
        configuration.load_test_config()

        # Mock out the emr_client (moto has incorrect response)
        self.mock_emr_client = MagicMock()
        self.mock_emr_client.describe_cluster.side_effect = [
            DESCRIBE_CLUSTER_RUNNING_RETURN,
            DESCRIBE_CLUSTER_TERMINATED_RETURN
        ]

        mock_emr_session = MagicMock()
        mock_emr_session.client.return_value = self.mock_emr_client

        # Mock out the emr_client creator
        self.boto3_session_mock = MagicMock(return_value=mock_emr_session)

    def test_execute_calls_with_the_job_flow_id_until_it_reaches_a_terminal_state(self):
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
            self.mock_emr_client.describe_cluster.assert_called_with(ClusterId='j-8989898989')


if __name__ == '__main__':
    unittest.main()
