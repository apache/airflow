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
from botocore.config import Config

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.base_aws import AwsBaseOperator
from airflow.providers.amazon.aws.utils.mixins import (
    AwsHookParams,
    aws_template_fields,
    normalize_botocore_config,
)

from tests_common.test_utils.version_compat import AIRFLOW_V_3_2_PLUS


def test_aws_template_fields():
    result = aws_template_fields()
    assert result == ("aws_conn_id", "region_name", "verify")
    assert aws_template_fields() is result, "aws_template_fields expect to return cached result"

    assert aws_template_fields("foo", "aws_conn_id", "bar") == (
        "aws_conn_id",
        "bar",
        "foo",
        "region_name",
        "verify",
    )


def test_aws_template_fields_incorrect_types():
    with pytest.raises(TypeError, match="Expected that all provided arguments are strings"):
        aws_template_fields(1, None, "test")


class FakeBotocoreConfigOperator(AwsBaseOperator[S3Hook]):
    aws_hook_class = S3Hook


class TestNormalizeBotocoreConfig:
    @pytest.mark.parametrize("value", [None, {}, {"read_timeout": 120, "retries": {"max_attempts": 5}}])
    def test_keeps_mapping_and_none_as_given(self, value):
        assert normalize_botocore_config(value) is value

    def test_reduces_config_object_to_the_options_it_was_given(self):
        config = Config(read_timeout=120, retries={"max_attempts": 5}, region_name="eu-west-1")

        assert normalize_botocore_config(config) == {
            "read_timeout": 120,
            "retries": {"max_attempts": 5},
            "region_name": "eu-west-1",
        }

    def test_reduces_config_object_resolved_by_from_constructor(self):
        params = AwsHookParams.from_constructor(
            "aws_default", None, None, Config(read_timeout=120), additional_params={}
        )

        assert params.botocore_config == {"read_timeout": 120}

    def test_keeps_config_object_when_botocore_withholds_the_given_options(self, monkeypatch):
        config = Config(read_timeout=120)
        monkeypatch.delattr(type(config), "_user_provided_options", raising=False)
        monkeypatch.delattr(config, "_user_provided_options", raising=False)

        assert normalize_botocore_config(config) is config

    def test_operator_resolves_config_object_to_a_mapping(self):
        op = FakeBotocoreConfigOperator(task_id="fake", botocore_config=Config(read_timeout=120))

        assert op.botocore_config == {"read_timeout": 120}

    @pytest.mark.skipif(
        not AIRFLOW_V_3_2_PLUS, reason="Trigger arguments go through airflow.sdk.serde from Airflow 3.2.0"
    )
    def test_operator_settings_can_cross_the_trigger_boundary(self):
        """A ``Config`` cannot cross serde, so the deferred half of the task never receives one."""
        from airflow.sdk.serde import serialize

        op = FakeBotocoreConfigOperator(task_id="fake", botocore_config=Config(read_timeout=120))

        assert serialize({"botocore_config": op.botocore_config}) == {
            "botocore_config": {"read_timeout": 120}
        }
