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

from unittest import mock

import pytest

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.comprehend import ComprehendHook
from airflow.providers.amazon.aws.sensors.comprehend import (
    ComprehendCreateDocumentClassifierCompletedSensor,
    ComprehendStartPiiEntitiesDetectionJobCompletedSensor,
)


class TestComprehendStartPiiEntitiesDetectionJobCompletedSensor:
    SENSOR = ComprehendStartPiiEntitiesDetectionJobCompletedSensor

    def setup_method(self):
        self.default_op_kwargs = dict(
            task_id="test_pii_entities_detection_job_sensor",
            job_id="job_id",
            poke_interval=5,
            max_retries=1,
        )
        self.sensor = self.SENSOR(**self.default_op_kwargs, aws_conn_id=None)

    def test_base_aws_op_attributes(self):
        op = self.SENSOR(**self.default_op_kwargs)
        assert op.hook.aws_conn_id == "aws_default"
        assert op.hook._region_name is None
        assert op.hook._verify is None
        assert op.hook._config is None

        op = self.SENSOR(
            **self.default_op_kwargs,
            aws_conn_id="aws-test-custom-conn",
            region_name="eu-west-1",
            verify=False,
            botocore_config={"read_timeout": 42},
        )
        assert op.hook.aws_conn_id == "aws-test-custom-conn"
        assert op.hook._region_name == "eu-west-1"
        assert op.hook._verify is False
        assert op.hook._config is not None
        assert op.hook._config.read_timeout == 42

    @pytest.mark.parametrize("state", SENSOR.SUCCESS_STATES)
    @mock.patch.object(ComprehendHook, "conn")
    def test_poke_success_state(self, mock_conn, state):
        mock_conn.describe_pii_entities_detection_job.return_value = {
            "PiiEntitiesDetectionJobProperties": {"JobStatus": state}
        }
        assert self.sensor.poke({}) is True

    @pytest.mark.parametrize("state", SENSOR.INTERMEDIATE_STATES)
    @mock.patch.object(ComprehendHook, "conn")
    def test_intermediate_state(self, mock_conn, state):
        mock_conn.describe_pii_entities_detection_job.return_value = {
            "PiiEntitiesDetectionJobProperties": {"JobStatus": state}
        }
        assert self.sensor.poke({}) is False

    @pytest.mark.parametrize("state", SENSOR.FAILURE_STATES)
    @mock.patch.object(ComprehendHook, "conn")
    def test_poke_failure_states(self, mock_conn, state):
        mock_conn.describe_pii_entities_detection_job.return_value = {
            "PiiEntitiesDetectionJobProperties": {"JobStatus": state}
        }
        sensor = self.SENSOR(**self.default_op_kwargs, aws_conn_id=None)

        with pytest.raises(AirflowException, match=sensor.FAILURE_MESSAGE):
            sensor.poke({})


class TestComprehendCreateDocumentClassifierCompletedSensor:
    SENSOR = ComprehendCreateDocumentClassifierCompletedSensor
    DOCUMENT_CLASSIFIER_ARN = (
        "arn:aws:comprehend:us-east-1:123456789012:document-classifier/insurance-classifier/version/v1"
    )
    EVALUATION_METRICS = {
        "EvaluationMetrics": {
            "Accuracy": 1,
            "Precision": 1,
            "Recall": 1,
            "F1Score": 1,
            "MicroPrecision": 1,
            "MicroRecall": 1,
            "MicroF1Score": 1,
            "HammingLoss": 0,
        }
    }

    def setup_method(self):
        self.default_op_kwargs = dict(
            task_id="test_create_document_classifier_sensor",
            document_classifier_arn=self.DOCUMENT_CLASSIFIER_ARN,
            fail_on_warnings=False,
            poke_interval=5,
            max_retries=1,
        )
        self.sensor = self.SENSOR(**self.default_op_kwargs, aws_conn_id=None)

    def test_base_aws_op_attributes(self):
        op = self.SENSOR(**self.default_op_kwargs)
        assert op.hook.aws_conn_id == "aws_default"
        assert op.hook._region_name is None
        assert op.hook._verify is None
        assert op.hook._config is None

        op = self.SENSOR(
            **self.default_op_kwargs,
            aws_conn_id="aws-test-custom-conn",
            region_name="eu-west-1",
            verify=False,
            botocore_config={"read_timeout": 42},
        )
        assert op.hook.aws_conn_id == "aws-test-custom-conn"
        assert op.hook._region_name == "eu-west-1"
        assert op.hook._verify is False
        assert op.hook._config is not None
        assert op.hook._config.read_timeout == 42

    @pytest.mark.parametrize(
        "state, message, output",
        [
            pytest.param("TRAINED", "", "s3://test-output", id="training succeeded"),
            pytest.param(
                "TRAINED_WITH_WARNING",
                "Unable to parse some documents. See details in the output S3 location",
                "s3://test-output",
                id="trained with warning",
            ),
        ],
    )
    @mock.patch.object(ComprehendHook, "conn")
    def test_poke_success_state(self, mock_conn, state, message, output):
        mock_conn.describe_document_classifier.return_value = {
            "DocumentClassifierProperties": {
                "Status": state,
                "Message": message,
                "OutputDataConfig": {"S3Uri": output},
                "ClassifierMetadata": self.EVALUATION_METRICS,
            }
        }

        assert self.sensor.poke({}) is True

    @pytest.mark.parametrize("state", SENSOR.INTERMEDIATE_STATES)
    @mock.patch.object(ComprehendHook, "conn")
    def test_intermediate_state(self, mock_conn, state):
        mock_conn.describe_document_classifier.return_value = {
            "DocumentClassifierProperties": {"Status": state}
        }
        assert self.sensor.poke({}) is False

    @pytest.mark.parametrize("state", SENSOR.FAILURE_STATES)
    @mock.patch.object(ComprehendHook, "conn")
    def test_poke_failure_states(self, mock_conn, state):
        mock_conn.describe_document_classifier.return_value = {
            "DocumentClassifierProperties": {"Status": state}
        }
        sensor = self.SENSOR(**self.default_op_kwargs, aws_conn_id=None)

        with pytest.raises(AirflowException, match=sensor.FAILURE_MESSAGE):
            sensor.poke({})
