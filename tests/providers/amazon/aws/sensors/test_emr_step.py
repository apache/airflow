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

import unittest
from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest
from dateutil.tz import tzlocal

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor

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


class TestEmrStepSensor(unittest.TestCase):
    def setUp(self):
        self.emr_client_mock = MagicMock()
        self.sensor = EmrStepSensor(
            task_id="test_task",
            poke_interval=0,
            job_flow_id="j-8989898989",
            step_id="s-VK57YR1Z9Z5N",
            aws_conn_id="aws_default",
        )

        mock_emr_session = MagicMock()
        mock_emr_session.client.return_value = self.emr_client_mock

        # Mock out the emr_client creator
        self.boto3_session_mock = MagicMock(return_value=mock_emr_session)

    def test_step_completed(self):
        self.emr_client_mock.describe_step.side_effect = [
            DESCRIBE_JOB_STEP_RUNNING_RETURN,
            DESCRIBE_JOB_STEP_COMPLETED_RETURN,
        ]

        with patch("boto3.session.Session", self.boto3_session_mock):
            self.sensor.execute(None)

            assert self.emr_client_mock.describe_step.call_count == 2
            calls = [
                unittest.mock.call(ClusterId="j-8989898989", StepId="s-VK57YR1Z9Z5N"),
                unittest.mock.call(ClusterId="j-8989898989", StepId="s-VK57YR1Z9Z5N"),
            ]
            self.emr_client_mock.describe_step.assert_has_calls(calls)

    def test_step_cancelled(self):
        self.emr_client_mock.describe_step.side_effect = [
            DESCRIBE_JOB_STEP_RUNNING_RETURN,
            DESCRIBE_JOB_STEP_CANCELLED_RETURN,
        ]

        with patch("boto3.session.Session", self.boto3_session_mock):
            with pytest.raises(AirflowException):
                self.sensor.execute(None)

    def test_step_failed(self):
        self.emr_client_mock.describe_step.side_effect = [
            DESCRIBE_JOB_STEP_RUNNING_RETURN,
            DESCRIBE_JOB_STEP_FAILED_RETURN,
        ]

        with patch("boto3.session.Session", self.boto3_session_mock):
            with pytest.raises(AirflowException):
                self.sensor.execute(None)

    def test_step_interrupted(self):
        self.emr_client_mock.describe_step.side_effect = [
            DESCRIBE_JOB_STEP_RUNNING_RETURN,
            DESCRIBE_JOB_STEP_INTERRUPTED_RETURN,
        ]

        with patch("boto3.session.Session", self.boto3_session_mock):
            with pytest.raises(AirflowException):
                self.sensor.execute(None)
