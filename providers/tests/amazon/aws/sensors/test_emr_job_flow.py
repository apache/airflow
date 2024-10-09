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
from __future__ import annotations

import datetime
from unittest import mock
from unittest.mock import MagicMock, patch

import pytest
from dateutil.tz import tzlocal

from airflow.exceptions import AirflowException, TaskDeferred
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.sensors.emr import EmrJobFlowSensor
from airflow.providers.amazon.aws.triggers.emr import EmrTerminateJobFlowTrigger

DESCRIBE_CLUSTER_STARTING_RETURN = {
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
                "CreationDateTime": datetime.datetime(2016, 6, 27, 21, 5, 2, 348000, tzinfo=tzlocal())
            },
        },
        "Tags": [{"Key": "app", "Value": "analytics"}, {"Key": "environment", "Value": "development"}],
        "TerminationProtected": False,
        "VisibleToAllUsers": True,
    },
    "ResponseMetadata": {"HTTPStatusCode": 200, "RequestId": "d5456308-3caa-11e6-9d46-951401f04e0e"},
}

DESCRIBE_CLUSTER_BOOTSTRAPPING_RETURN = {
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
            "State": "BOOTSTRAPPING",
            "StateChangeReason": {},
            "Timeline": {
                "CreationDateTime": datetime.datetime(2016, 6, 27, 21, 5, 2, 348000, tzinfo=tzlocal())
            },
        },
        "Tags": [{"Key": "app", "Value": "analytics"}, {"Key": "environment", "Value": "development"}],
        "TerminationProtected": False,
        "VisibleToAllUsers": True,
    },
    "ResponseMetadata": {"HTTPStatusCode": 200, "RequestId": "d5456308-3caa-11e6-9d46-951401f04e0e"},
}

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
            "State": "RUNNING",
            "StateChangeReason": {},
            "Timeline": {
                "CreationDateTime": datetime.datetime(2016, 6, 27, 21, 5, 2, 348000, tzinfo=tzlocal())
            },
        },
        "Tags": [{"Key": "app", "Value": "analytics"}, {"Key": "environment", "Value": "development"}],
        "TerminationProtected": False,
        "VisibleToAllUsers": True,
    },
    "ResponseMetadata": {"HTTPStatusCode": 200, "RequestId": "d5456308-3caa-11e6-9d46-951401f04e0e"},
}

DESCRIBE_CLUSTER_WAITING_RETURN = {
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
            "State": "WAITING",
            "StateChangeReason": {},
            "Timeline": {
                "CreationDateTime": datetime.datetime(2016, 6, 27, 21, 5, 2, 348000, tzinfo=tzlocal())
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
                "CreationDateTime": datetime.datetime(2016, 6, 27, 21, 5, 2, 348000, tzinfo=tzlocal())
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
                "CreationDateTime": datetime.datetime(2016, 6, 27, 21, 5, 2, 348000, tzinfo=tzlocal())
            },
        },
        "Tags": [{"Key": "app", "Value": "analytics"}, {"Key": "environment", "Value": "development"}],
        "TerminationProtected": False,
        "VisibleToAllUsers": True,
    },
    "ResponseMetadata": {"HTTPStatusCode": 200, "RequestId": "d5456308-3caa-11e6-9d46-951401f04e0e"},
}


@pytest.fixture
def mocked_hook_client():
    with mock.patch("airflow.providers.amazon.aws.hooks.emr.EmrHook.conn") as m:
        yield m


class TestEmrJobFlowSensor:
    def test_execute_calls_with_the_job_flow_id_until_it_reaches_a_target_state(self, mocked_hook_client):
        mocked_hook_client.describe_cluster.side_effect = [
            DESCRIBE_CLUSTER_STARTING_RETURN,
            DESCRIBE_CLUSTER_RUNNING_RETURN,
            DESCRIBE_CLUSTER_TERMINATED_RETURN,
        ]

        operator = EmrJobFlowSensor(
            task_id="test_task", poke_interval=0, job_flow_id="j-8989898989", aws_conn_id="aws_default"
        )
        with patch.object(S3Hook, "parse_s3_url", return_value="valid_uri"):
            operator.execute(MagicMock())

        assert mocked_hook_client.describe_cluster.call_count == 3
        # make sure it was called with the job_flow_id
        calls = [mock.call(ClusterId="j-8989898989")] * 3
        mocked_hook_client.describe_cluster.assert_has_calls(calls)

    def test_execute_calls_with_the_job_flow_id_until_it_reaches_failed_state_with_exception(
        self, mocked_hook_client
    ):
        mocked_hook_client.describe_cluster.side_effect = [
            DESCRIBE_CLUSTER_RUNNING_RETURN,
            DESCRIBE_CLUSTER_TERMINATED_WITH_ERRORS_RETURN,
        ]

        operator = EmrJobFlowSensor(
            task_id="test_task", poke_interval=0, job_flow_id="j-8989898989", aws_conn_id="aws_default"
        )
        with pytest.raises(AirflowException):
            operator.execute(MagicMock())

        # make sure we called twice
        assert mocked_hook_client.describe_cluster.call_count == 2
        # make sure it was called with the job_flow_id
        calls = [mock.call(ClusterId="j-8989898989")] * 2
        mocked_hook_client.describe_cluster.assert_has_calls(calls=calls)

    def test_different_target_states(self, mocked_hook_client):
        mocked_hook_client.describe_cluster.side_effect = [
            DESCRIBE_CLUSTER_STARTING_RETURN,  # return False
            DESCRIBE_CLUSTER_BOOTSTRAPPING_RETURN,  # return False
            DESCRIBE_CLUSTER_RUNNING_RETURN,  # return True
            DESCRIBE_CLUSTER_WAITING_RETURN,  # will not be used
            DESCRIBE_CLUSTER_TERMINATED_RETURN,  # will not be used
            DESCRIBE_CLUSTER_TERMINATED_WITH_ERRORS_RETURN,  # will not be used
        ]

        operator = EmrJobFlowSensor(
            task_id="test_task",
            poke_interval=0,
            job_flow_id="j-8989898989",
            aws_conn_id="aws_default",
            target_states=["RUNNING", "WAITING"],
        )

        operator.execute(MagicMock())

        assert mocked_hook_client.describe_cluster.call_count == 3
        # make sure it was called with the job_flow_id
        calls = [mock.call(ClusterId="j-8989898989")] * 3
        mocked_hook_client.describe_cluster.assert_has_calls(calls)

    def test_sensor_defer(self):
        """Test the execute method raise TaskDeferred if running sensor in deferrable mode"""
        sensor = EmrJobFlowSensor(
            task_id="test_task",
            poke_interval=0,
            job_flow_id="j-8989898989",
            aws_conn_id="aws_default",
            target_states=["RUNNING", "WAITING"],
            deferrable=True,
        )

        with patch.object(EmrJobFlowSensor, "poke", return_value=False):
            with pytest.raises(TaskDeferred) as exc:
                sensor.execute(context=None)

        assert isinstance(
            exc.value.trigger, EmrTerminateJobFlowTrigger
        ), f"{exc.value.trigger} is not a EmrTerminateJobFlowTrigger"
