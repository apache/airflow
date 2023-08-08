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

import pytest
from moto import mock_sagemaker

from airflow.providers.amazon.aws.hooks.sagemaker_notebook import (
    SageMakerNotebookHook,
)

INSTANCE_NAME = "my-instance"
INSTANCE_TYPE = "ml.t3.medium"
ROLE_ARN = "arn:aws:iam:role/test-role"


@pytest.fixture(scope="session")
def hook():
    return SageMakerNotebookHook()


class TestSageMakerNotebookHook:
    def test_conn(self):
        hook = SageMakerNotebookHook(aws_conn_id="sagemaker_test_conn_id")
        assert hook.aws_conn_id == "sagemaker_test_conn_id"

    @mock_sagemaker
    def test_create_instance(self, hook, capsys):
        # create a notebook
        resp = hook.create_instance(
            instance_name=INSTANCE_NAME,
            instance_type=INSTANCE_TYPE,
            role_arn=ROLE_ARN,
        )
        assert resp["NotebookInstanceArn"]

    @mock_sagemaker
    def test_start_instance(self, hook, capsys):
        hook.create_instance(instance_name=INSTANCE_NAME, instance_type=INSTANCE_TYPE, role_arn=ROLE_ARN)

        resp = hook.start_instance(INSTANCE_NAME)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    @mock_sagemaker
    def test_stop_instance(self, hook, capsys):
        hook.create_instance(instance_name=INSTANCE_NAME, instance_type=INSTANCE_TYPE, role_arn=ROLE_ARN)
        resp = hook.stop_instance(instance_name=INSTANCE_NAME)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    @mock_sagemaker
    def test_delete_instance(self, hook, capsys):
        hook.create_instance(
            instance_name=INSTANCE_NAME,
            instance_type=INSTANCE_TYPE,
            role_arn=ROLE_ARN,
        )
        hook.stop_instance(instance_name=INSTANCE_NAME)
        resp = hook.delete_instance(instance_name=INSTANCE_NAME)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
