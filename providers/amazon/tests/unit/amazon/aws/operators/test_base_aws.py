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

from typing import Any

import pytest

from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.providers.amazon.aws.operators.base_aws import AwsBaseOperator
from airflow.providers.common.compat.sdk import BaseHook

TEST_CONN = "aws_test_conn"


class FakeS3Hook(AwsBaseHook):
    """Hook for tests, implements thin-wrapper around s3 client."""

    def __init__(self, **kwargs):
        kwargs.update({"client_type": "s3", "resource_type": None})
        super().__init__(**kwargs)


class FakeS3Operator(AwsBaseOperator):
    aws_hook_class = FakeS3Hook

    def __init__(self, *, value: Any = None, **kwargs):
        super().__init__(**kwargs)
        self.value = value

    def execute(self, context):
        """For test purpose"""
        from botocore.config import Config

        hook = self.hook

        assert self.aws_conn_id == hook.aws_conn_id
        assert self.region_name == hook._region_name
        assert self.verify == hook._verify

        botocore_config = hook._config
        if botocore_config:
            assert isinstance(botocore_config, Config)
        else:
            assert botocore_config is None


@pytest.fixture(autouse=True)
def fake_conn(monkeypatch):
    monkeypatch.setenv(f"AWS_CONN_{TEST_CONN.upper()}", '{"conn_type": "aws"}')


class TestAwsBaseOperator:
    def test_default_parameters(self):
        op = FakeS3Operator(task_id="fake_task_id")
        msg = "Attention! Changes in default parameters might produce breaking changes in multiple operators"
        assert op.aws_conn_id == "aws_default", msg
        assert op.region_name is None, msg
        assert op.verify is None, msg
        assert op.botocore_config is None, msg

    def test_parameters(self):
        op = FakeS3Operator(
            task_id="fake-task-id",
            aws_conn_id=TEST_CONN,
            region_name="eu-central-1",
            verify=False,
            botocore_config={"read_timeout": 777, "connect_timeout": 42},
        )

        assert op.aws_conn_id == TEST_CONN
        assert op.region_name == "eu-central-1"
        assert op.verify is False
        assert op.botocore_config == {"read_timeout": 777, "connect_timeout": 42}

        hook = op.hook
        assert isinstance(hook, FakeS3Hook)
        assert hook.aws_conn_id == op.aws_conn_id
        assert hook._region_name == op.region_name
        assert hook._verify == op.verify
        assert hook._config.read_timeout == 777
        assert hook._config.connect_timeout == 42

    @pytest.mark.db_test
    @pytest.mark.parametrize(
        "op_kwargs",
        [
            pytest.param(
                {
                    "aws_conn_id": TEST_CONN,
                    "region_name": "eu-central-1",
                    "verify": False,
                    "botocore_config": {"read_timeout": 777, "connect_timeout": 42},
                },
                id="all-params-provided",
            ),
            pytest.param({}, id="default-only"),
        ],
    )
    def test_execute(self, op_kwargs, dag_maker):
        with dag_maker("test_aws_base_operator", serialized=True):
            FakeS3Operator(task_id="fake-task-id", **op_kwargs)

        dag_maker.run_ti("fake-task-id")

    def test_no_aws_hook_class_attr(self):
        class NoAwsHookClassOperator(AwsBaseOperator): ...

        error_match = r"Class attribute 'NoAwsHookClassOperator\.aws_hook_class' should be set"
        with pytest.raises(AttributeError, match=error_match):
            NoAwsHookClassOperator(task_id="fake-task-id")

    def test_aws_hook_class_wrong_hook_type(self):
        class WrongHookOperator(AwsBaseOperator):
            aws_hook_class = BaseHook

        error_match = (
            r"Class attribute 'WrongHookOperator.aws_hook_class' is not a subclass of AwsGenericHook"
        )
        with pytest.raises(AttributeError, match=error_match):
            WrongHookOperator(task_id="fake-task-id")

    def test_aws_hook_class_class_instance(self):
        class SoWrongOperator(AwsBaseOperator):
            aws_hook_class = FakeS3Hook()

        error_match = r"Class attribute 'SoWrongOperator.aws_hook_class' is not a subclass of AwsGenericHook"
        with pytest.raises(AttributeError, match=error_match):
            SoWrongOperator(task_id="fake-task-id")
