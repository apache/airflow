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

import json
from unittest import mock

import pytest

from airflow.providers.amazon.aws.executors.ecs.ecs_executor_config import (
    _fetch_config_values,
    _fetch_templated_kwargs,
    build_task_kwargs,
)
from airflow.providers.amazon.aws.executors.ecs.utils import (
    CONFIG_GROUP_NAME,
    ECS_LAUNCH_TYPE_EC2,
    ECS_LAUNCH_TYPE_FARGATE,
    AllEcsConfigKeys,
    RunTaskKwargsConfigKeys,
)


class TestFetchTemplatedKwargs:
    def test_valid_json_string(self):
        conf = mock.MagicMock()
        payload = {"paramA": "valA", "paramB": 123}
        conf.get.return_value = json.dumps(payload)

        result = _fetch_templated_kwargs(conf)

        conf.get.assert_called_once_with(CONFIG_GROUP_NAME, AllEcsConfigKeys.RUN_TASK_KWARGS, fallback={})
        assert result == payload

    def test_fallback_empty_dict(self):
        conf = mock.MagicMock()
        conf.get.side_effect = lambda group, key, fallback: fallback

        result = _fetch_templated_kwargs(conf)
        assert result == {}


class TestFetchConfigValues:
    def test_prunes_none_values_and_uses_defaults(self):
        conf = mock.MagicMock()
        test_values = {
            RunTaskKwargsConfigKeys.CLUSTER: "test-cluster",
            RunTaskKwargsConfigKeys.CONTAINER_NAME: "test-container",
            RunTaskKwargsConfigKeys.PLATFORM_VERSION: None,
        }
        conf.get.side_effect = lambda group, key, fallback=None: test_values.get(key, fallback)

        result = _fetch_config_values(conf)

        assert result["cluster"] == "test-cluster"
        assert result["container_name"] == "test-container"
        assert "platform_version" not in result


class TestBuildTaskKwargs:
    def _mock_conf(self, config_values: dict[str, str | None], templated: dict | None = None):
        conf = mock.MagicMock()
        templated_str = json.dumps(templated or {})
        conf.get.side_effect = lambda group, key, fallback=None: (
            templated_str if key == AllEcsConfigKeys.RUN_TASK_KWARGS else config_values.get(key, fallback)
        )
        return conf

    def test_mutually_exclusive_launch_type_and_capacity_provider(self):
        conf = self._mock_conf(
            {
                AllEcsConfigKeys.CONTAINER_NAME: "my-container",
                AllEcsConfigKeys.LAUNCH_TYPE: ECS_LAUNCH_TYPE_FARGATE,
                AllEcsConfigKeys.CAPACITY_PROVIDER_STRATEGY: "some-strategy",
            }
        )
        with pytest.raises(
            ValueError,
            match="capacity_provider_strategy and launch_type are mutually exclusive",
        ):
            build_task_kwargs(conf)

    def test_basic_fargate_task_with_networking(self):
        conf = self._mock_conf(
            {
                AllEcsConfigKeys.CONTAINER_NAME: "my-container",
                AllEcsConfigKeys.LAUNCH_TYPE: ECS_LAUNCH_TYPE_FARGATE,
                AllEcsConfigKeys.PLATFORM_VERSION: "1.4.0",
                AllEcsConfigKeys.SUBNETS: "subnet-1,subnet-2",
                AllEcsConfigKeys.SECURITY_GROUPS: "sg-1,sg-2",
                AllEcsConfigKeys.ASSIGN_PUBLIC_IP: "True",
            }
        )
        result = build_task_kwargs(conf)

        assert result["count"] == 1
        assert result["launchType"] == ECS_LAUNCH_TYPE_FARGATE
        assert result["platformVersion"] == "1.4.0"
        assert result["overrides"]["containerOverrides"][0]["name"] == "my-container"
        assert result["overrides"]["containerOverrides"][0]["command"] == []
        assert result["networkConfiguration"]["awsvpcConfiguration"]["subnets"] == [
            "subnet-1",
            "subnet-2",
        ]
        assert result["networkConfiguration"]["awsvpcConfiguration"]["securityGroups"] == ["sg-1", "sg-2"]
        assert result["networkConfiguration"]["awsvpcConfiguration"]["assignPublicIp"] == "ENABLED"

    def test_ec2_launch_type_drops_platform_version(self):
        conf = self._mock_conf(
            {
                AllEcsConfigKeys.CONTAINER_NAME: "my-ec2-container",
                AllEcsConfigKeys.LAUNCH_TYPE: ECS_LAUNCH_TYPE_EC2,
                AllEcsConfigKeys.PLATFORM_VERSION: "LATEST",
                AllEcsConfigKeys.ASSIGN_PUBLIC_IP: "False",
            }
        )
        result = build_task_kwargs(conf)

        assert result["launchType"] == ECS_LAUNCH_TYPE_EC2
        assert "platformVersion" not in result
        assert "networkConfiguration" not in result

    @mock.patch("airflow.providers.amazon.aws.executors.ecs.ecs_executor_config.EcsHook")
    def test_cluster_without_launch_type_falls_back_to_fargate_when_no_default_capacity_provider(
        self, mock_ecs_hook
    ):
        mock_conn = mock.MagicMock()
        mock_conn.describe_clusters.return_value = {"clusters": [{"clusterName": "c1"}]}
        mock_ecs_hook.return_value.conn = mock_conn

        conf = self._mock_conf(
            {
                AllEcsConfigKeys.CONTAINER_NAME: "my-container",
                AllEcsConfigKeys.CLUSTER: "c1",
                AllEcsConfigKeys.ASSIGN_PUBLIC_IP: "False",
            }
        )
        result = build_task_kwargs(conf)

        mock_conn.describe_clusters.assert_called_once_with(clusters=["c1"])
        assert result["launchType"] == ECS_LAUNCH_TYPE_FARGATE

    @mock.patch("airflow.providers.amazon.aws.executors.ecs.ecs_executor_config.EcsHook")
    def test_cluster_retains_default_capacity_provider_when_present(self, mock_ecs_hook):
        mock_conn = mock.MagicMock()
        mock_conn.describe_clusters.return_value = {
            "clusters": [
                {
                    "clusterName": "c1",
                    "defaultCapacityProviderStrategy": [{"capacityProvider": "FARGATE"}],
                }
            ]
        }
        mock_ecs_hook.return_value.conn = mock_conn

        conf = self._mock_conf(
            {
                AllEcsConfigKeys.CONTAINER_NAME: "my-container",
                AllEcsConfigKeys.CLUSTER: "c1",
                AllEcsConfigKeys.ASSIGN_PUBLIC_IP: "False",
            }
        )
        result = build_task_kwargs(conf)

        assert "launchType" not in result

    def test_missing_subnets_in_network_config_raises_value_error(self):
        conf = self._mock_conf(
            {
                AllEcsConfigKeys.CONTAINER_NAME: "my-container",
                AllEcsConfigKeys.SECURITY_GROUPS: "sg-1",
            }
        )
        with pytest.raises(ValueError, match="At least one subnet is required to run a task."):
            build_task_kwargs(conf)

    def test_json_decode_error_raises_value_error(self):
        conf = self._mock_conf(
            {
                AllEcsConfigKeys.CONTAINER_NAME: "my-container",
                AllEcsConfigKeys.ASSIGN_PUBLIC_IP: "False",
            }
        )
        with mock.patch(
            "airflow.providers.amazon.aws.executors.ecs.ecs_executor_config.json.dumps",
            return_value="not valid json",
        ):
            with pytest.raises(
                ValueError,
                match="AWS ECS Executor config values must be JSON serializable.",
            ):
                build_task_kwargs(conf)
