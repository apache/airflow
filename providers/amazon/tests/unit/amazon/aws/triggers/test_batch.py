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

from unittest.mock import patch

from airflow.providers.amazon.aws.triggers.batch import (
    BatchCreateComputeEnvironmentTrigger,
    BatchJobTrigger,
)


class TestBatchJobTrigger:
    def test_serialization(self):
        trigger = BatchJobTrigger(
            job_id="test_job_id",
            aws_conn_id="aws_default",
            region_name="us-west-2",
        )

        classpath, kwargs = trigger.serialize()
        assert classpath == "airflow.providers.amazon.aws.triggers.batch.BatchJobTrigger"
        assert kwargs == {
            "job_id": "test_job_id",
            "waiter_delay": 5,
            "waiter_max_attempts": 720,
            "aws_conn_id": "aws_default",
            "region_name": "us-west-2",
        }

    def test_serialization_with_verify_and_botocore_config(self):
        trigger = BatchJobTrigger(
            job_id="test_job_id",
            aws_conn_id="aws_default",
            region_name="us-west-2",
            verify=False,
            botocore_config={"connect_timeout": 30},
        )

        classpath, kwargs = trigger.serialize()
        assert classpath == "airflow.providers.amazon.aws.triggers.batch.BatchJobTrigger"
        assert kwargs["verify"] is False
        assert kwargs["botocore_config"] == {"connect_timeout": 30}

    @patch("airflow.providers.amazon.aws.triggers.batch.BatchClientHook")
    def test_hook_propagates_verify_and_botocore_config(self, mock_hook_cls):
        trigger = BatchJobTrigger(
            job_id="test_job_id",
            aws_conn_id="test_conn",
            region_name="eu-west-1",
            verify="/path/to/ca-bundle.crt",
            botocore_config={"read_timeout": 60},
        )

        trigger.hook()

        mock_hook_cls.assert_called_once_with(
            aws_conn_id="test_conn",
            region_name="eu-west-1",
            verify="/path/to/ca-bundle.crt",
            config={"read_timeout": 60},
        )


class TestBatchCreateComputeEnvironmentTrigger:
    def test_serialization(self):
        trigger = BatchCreateComputeEnvironmentTrigger(
            compute_env_arn="arn:aws:batch:us-east-1:123456789012:compute-environment/test",
            aws_conn_id="aws_default",
            region_name="us-east-1",
        )

        classpath, kwargs = trigger.serialize()
        assert classpath == "airflow.providers.amazon.aws.triggers.batch.BatchCreateComputeEnvironmentTrigger"
        assert kwargs == {
            "compute_env_arn": "arn:aws:batch:us-east-1:123456789012:compute-environment/test",
            "waiter_delay": 30,
            "waiter_max_attempts": 10,
            "aws_conn_id": "aws_default",
            "region_name": "us-east-1",
        }

    def test_serialization_with_verify_and_botocore_config(self):
        trigger = BatchCreateComputeEnvironmentTrigger(
            compute_env_arn="arn:aws:batch:us-east-1:123456789012:compute-environment/test",
            aws_conn_id="aws_default",
            verify=False,
            botocore_config={"connect_timeout": 30},
        )

        classpath, kwargs = trigger.serialize()
        assert kwargs["verify"] is False
        assert kwargs["botocore_config"] == {"connect_timeout": 30}

    @patch("airflow.providers.amazon.aws.triggers.batch.BatchClientHook")
    def test_hook_propagates_verify_and_botocore_config(self, mock_hook_cls):
        trigger = BatchCreateComputeEnvironmentTrigger(
            compute_env_arn="arn:aws:batch:us-east-1:123456789012:compute-environment/test",
            aws_conn_id="test_conn",
            region_name="eu-west-1",
            verify="/path/to/ca-bundle.crt",
            botocore_config={"read_timeout": 60},
        )

        trigger.hook()

        mock_hook_cls.assert_called_once_with(
            aws_conn_id="test_conn",
            region_name="eu-west-1",
            verify="/path/to/ca-bundle.crt",
            config={"read_timeout": 60},
        )
