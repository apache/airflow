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

from airflow.exceptions import AirflowProviderDeprecationWarning
from airflow.hooks.base import BaseHook
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.providers.amazon.aws.sensors.base_aws import AwsBaseSensor
from airflow.utils import timezone

TEST_CONN = "aws_test_conn"


class FakeDynamoDbHook(AwsBaseHook):
    """Hook for tests, implements thin-wrapper around dynamodb resource-client."""

    def __init__(self, **kwargs):
        kwargs.update({"client_type": None, "resource_type": "dynamodb"})
        super().__init__(**kwargs)


class FakeDynamoDBSensor(AwsBaseSensor):
    aws_hook_class = FakeDynamoDbHook

    def __init__(self, *, value: Any = None, **kwargs):
        super().__init__(**kwargs)
        self.value = value

    def poke(self, context):
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

        return True


@pytest.fixture(autouse=True)
def fake_conn(monkeypatch):
    monkeypatch.setenv(f"AWS_CONN_{TEST_CONN.upper()}", '{"conn_type": "aws"}')


class TestAwsBaseSensor:
    def test_default_parameters(self):
        op = FakeDynamoDBSensor(task_id="fake_task_id")
        msg = "Attention! Changes in default parameters might produce breaking changes in multiple sensors"
        assert op.aws_conn_id == "aws_default", msg
        assert op.region_name is None, msg
        assert op.verify is None, msg
        assert op.botocore_config is None, msg

    def test_parameters(self):
        op = FakeDynamoDBSensor(
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
        assert isinstance(hook, FakeDynamoDbHook)
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
    def test_execute(self, dag_maker, op_kwargs):
        with dag_maker("test_aws_base_sensor", serialized=True):
            FakeDynamoDBSensor(task_id="fake-task-id", **op_kwargs, poke_interval=1)

        dagrun = dag_maker.create_dagrun(execution_date=timezone.utcnow())
        tis = {ti.task_id: ti for ti in dagrun.task_instances}
        tis["fake-task-id"].run()

    @pytest.mark.skip_if_database_isolation_mode
    @pytest.mark.parametrize(
        "region, region_name",
        [
            pytest.param("eu-west-1", None, id="region-only"),
            pytest.param("us-east-1", "us-east-1", id="non-ambiguous-params"),
        ],
    )
    def test_deprecated_region_name(self, region, region_name):
        warning_match = r"`region` is deprecated and will be removed"
        with pytest.warns(AirflowProviderDeprecationWarning, match=warning_match):
            op = FakeDynamoDBSensor(
                task_id="fake-task-id",
                aws_conn_id=TEST_CONN,
                region=region,
                region_name=region_name,
            )
        assert op.region_name == region

        with pytest.warns(AirflowProviderDeprecationWarning, match=warning_match):
            assert op.region == region

    def test_conflicting_region_name(self):
        error_match = r"Conflicting `region_name` provided, region_name='us-west-1', region='eu-west-1'"
        with pytest.raises(ValueError, match=error_match), pytest.warns(
            AirflowProviderDeprecationWarning,
            match="`region` is deprecated and will be removed in the future. Please use `region_name` instead.",
        ):
            FakeDynamoDBSensor(
                task_id="fake-task-id",
                aws_conn_id=TEST_CONN,
                region="eu-west-1",
                region_name="us-west-1",
            )

    def test_no_aws_hook_class_attr(self):
        class NoAwsHookClassSensor(AwsBaseSensor): ...

        error_match = r"Class attribute 'NoAwsHookClassSensor\.aws_hook_class' should be set"
        with pytest.raises(AttributeError, match=error_match):
            NoAwsHookClassSensor(task_id="fake-task-id")

    def test_aws_hook_class_wrong_hook_type(self):
        class WrongHookSensor(AwsBaseSensor):
            aws_hook_class = BaseHook

        error_match = r"Class attribute 'WrongHookSensor.aws_hook_class' is not a subclass of AwsGenericHook"
        with pytest.raises(AttributeError, match=error_match):
            WrongHookSensor(task_id="fake-task-id")

    def test_aws_hook_class_class_instance(self):
        class SoWrongSensor(AwsBaseSensor):
            aws_hook_class = FakeDynamoDbHook()

        error_match = r"Class attribute 'SoWrongSensor.aws_hook_class' is not a subclass of AwsGenericHook"
        with pytest.raises(AttributeError, match=error_match):
            SoWrongSensor(task_id="fake-task-id")

    @pytest.mark.skip_if_database_isolation_mode
    @pytest.mark.parametrize(
        "region, region_name, expected_region_name",
        [
            pytest.param("ca-west-1", None, "ca-west-1", id="region-only"),
            pytest.param("us-west-1", "us-west-1", "us-west-1", id="non-ambiguous-params"),
        ],
    )
    @pytest.mark.db_test
    def test_region_in_partial_sensor(self, region, region_name, expected_region_name, dag_maker):
        with dag_maker("test_region_in_partial_sensor", serialized=True):
            FakeDynamoDBSensor.partial(
                task_id="fake-task-id",
                region=region,
                region_name=region_name,
            ).expand(value=[1, 2, 3])

        dr = dag_maker.create_dagrun(execution_date=timezone.utcnow())
        warning_match = r"`region` is deprecated and will be removed"
        for ti in dr.task_instances:
            with pytest.warns(AirflowProviderDeprecationWarning, match=warning_match):
                ti.run()
            assert ti.task.region_name == expected_region_name

    @pytest.mark.skip_if_database_isolation_mode
    @pytest.mark.db_test
    def test_ambiguous_region_in_partial_sensor(self, dag_maker):
        with dag_maker("test_ambiguous_region_in_partial_sensor", serialized=True):
            FakeDynamoDBSensor.partial(
                task_id="fake-task-id",
                region="eu-west-1",
                region_name="us-east-1",
            ).expand(value=[1, 2, 3])

        dr = dag_maker.create_dagrun(execution_date=timezone.utcnow())
        warning_match = r"`region` is deprecated and will be removed"
        for ti in dr.task_instances:
            with pytest.warns(AirflowProviderDeprecationWarning, match=warning_match), pytest.raises(
                ValueError, match="Conflicting `region_name` provided"
            ):
                ti.run()
