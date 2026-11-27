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
import re
from unittest import mock

import pytest
from inflection import camelize

from airflow.providers.amazon.aws.executors.ecs import ecs_executor_config
from airflow.providers.amazon.aws.executors.ecs.utils import (
    CONFIG_DEFAULTS,
    CONFIG_GROUP_NAME,
    ECS_LAUNCH_TYPE_EC2,
    AllEcsConfigKeys,
    _recursive_flatten_dict,
    parse_assign_public_ip,
)
from airflow.providers.amazon.aws.hooks.ecs import EcsHook
from airflow.providers.common.compat.sdk import conf
from airflow.utils.helpers import convert_camel_to_snake

from tests_common.test_utils.config import conf_vars


@pytest.fixture
def set_env_vars():
    overrides: dict[tuple[str, str], str] = {
        (CONFIG_GROUP_NAME, AllEcsConfigKeys.REGION_NAME): "us-west-1",
        (CONFIG_GROUP_NAME, AllEcsConfigKeys.CLUSTER): "some-cluster",
        (CONFIG_GROUP_NAME, AllEcsConfigKeys.CONTAINER_NAME): "container-name",
        (CONFIG_GROUP_NAME, AllEcsConfigKeys.TASK_DEFINITION): "some-task-def",
        (CONFIG_GROUP_NAME, AllEcsConfigKeys.LAUNCH_TYPE): "FARGATE",
        (CONFIG_GROUP_NAME, AllEcsConfigKeys.PLATFORM_VERSION): "LATEST",
        (CONFIG_GROUP_NAME, AllEcsConfigKeys.ASSIGN_PUBLIC_IP): "False",
        (CONFIG_GROUP_NAME, AllEcsConfigKeys.SECURITY_GROUPS): "sg1,sg2",
        (CONFIG_GROUP_NAME, AllEcsConfigKeys.SUBNETS): "sub1,sub2",
        (CONFIG_GROUP_NAME, AllEcsConfigKeys.MAX_RUN_TASK_ATTEMPTS): "3",
    }
    with conf_vars(overrides):
        yield


class TestEcsExecutorConfig:
    @pytest.fixture
    def assign_subnets(self):
        with conf_vars({(CONFIG_GROUP_NAME, AllEcsConfigKeys.SUBNETS): "sub1,sub2"}):
            yield

    @pytest.fixture
    def assign_container_name(self):
        with conf_vars({(CONFIG_GROUP_NAME, AllEcsConfigKeys.CONTAINER_NAME): "foobar"}):
            yield

    def test_subnets_required(self):
        conf_overrides = {
            (CONFIG_GROUP_NAME, AllEcsConfigKeys.SUBNETS): None,
            (CONFIG_GROUP_NAME, AllEcsConfigKeys.REGION_NAME): "us-west-1",
            (CONFIG_GROUP_NAME, AllEcsConfigKeys.CLUSTER): "some-cluster",
            (CONFIG_GROUP_NAME, AllEcsConfigKeys.CONTAINER_NAME): "container-name",
            (CONFIG_GROUP_NAME, AllEcsConfigKeys.TASK_DEFINITION): "some-task-def",
            (CONFIG_GROUP_NAME, AllEcsConfigKeys.LAUNCH_TYPE): "FARGATE",
            (CONFIG_GROUP_NAME, AllEcsConfigKeys.PLATFORM_VERSION): "LATEST",
            (CONFIG_GROUP_NAME, AllEcsConfigKeys.ASSIGN_PUBLIC_IP): "False",
            (CONFIG_GROUP_NAME, AllEcsConfigKeys.SECURITY_GROUPS): "sg1,sg2",
        }
        with conf_vars(conf_overrides):
            with pytest.raises(ValueError, match="At least one subnet is required to run a task"):
                ecs_executor_config.build_task_kwargs(conf)

    @conf_vars({(CONFIG_GROUP_NAME, AllEcsConfigKeys.CONTAINER_NAME): "container-name"})
    def test_config_defaults_are_applied(self, assign_subnets):
        task_kwargs = _recursive_flatten_dict(ecs_executor_config.build_task_kwargs(conf))
        found_keys = {convert_camel_to_snake(key): key for key in task_kwargs.keys()}

        for expected_key, expected_value_raw in CONFIG_DEFAULTS.items():
            # conn_id, max_run_task_attempts, check_health_on_startup and run_task_concurrency are used by
            # the executor, but are not expected to appear in the task_kwargs.
            if expected_key in [
                AllEcsConfigKeys.AWS_CONN_ID,
                AllEcsConfigKeys.MAX_RUN_TASK_ATTEMPTS,
                AllEcsConfigKeys.CHECK_HEALTH_ON_STARTUP,
                AllEcsConfigKeys.RUN_TASK_CONCURRENCY,
            ]:
                assert expected_key not in found_keys.keys()
            else:
                assert expected_key in found_keys.keys()
                # Make sure to convert "assign_public_ip" from True/False to ENABLE/DISABLE.
                expected_value = (
                    parse_assign_public_ip(expected_value_raw)
                    if expected_key is AllEcsConfigKeys.ASSIGN_PUBLIC_IP
                    else expected_value_raw
                )
                assert expected_value == task_kwargs[found_keys[expected_key]]

    def test_provided_values_override_defaults(self, assign_subnets, assign_container_name, monkeypatch):
        """
        Expected precedence is default values are overwritten by values provided explicitly,
        and those values are overwritten by those provided in run_task_kwargs.
        """
        run_task_kwargs_env_key = f"AIRFLOW__{CONFIG_GROUP_NAME}__{AllEcsConfigKeys.RUN_TASK_KWARGS}".upper()
        platform_version_env_key = (
            f"AIRFLOW__{CONFIG_GROUP_NAME}__{AllEcsConfigKeys.PLATFORM_VERSION}".upper()
        )
        default_version = CONFIG_DEFAULTS[AllEcsConfigKeys.PLATFORM_VERSION]
        templated_version = "1"
        first_explicit_version = "2"
        second_explicit_version = "3"

        # Confirm the default value is applied when no value is provided.
        monkeypatch.delenv(platform_version_env_key, raising=False)
        monkeypatch.delenv(run_task_kwargs_env_key, raising=False)

        task_kwargs = ecs_executor_config.build_task_kwargs(conf)
        assert task_kwargs["platformVersion"] == default_version

        # Provide a new value explicitly and assert that it is applied over the default.
        monkeypatch.setenv(platform_version_env_key, first_explicit_version)
        task_kwargs = ecs_executor_config.build_task_kwargs(conf)
        assert task_kwargs["platformVersion"] == first_explicit_version

        # Provide a value via template and assert that it is applied over the explicit value.
        monkeypatch.setenv(
            run_task_kwargs_env_key,
            json.dumps({AllEcsConfigKeys.PLATFORM_VERSION: templated_version}),
        )
        task_kwargs = ecs_executor_config.build_task_kwargs(conf)
        assert task_kwargs["platformVersion"] == templated_version

        # Provide a new value explicitly and assert it is not applied over the templated values.
        monkeypatch.setenv(platform_version_env_key, second_explicit_version)
        task_kwargs = ecs_executor_config.build_task_kwargs(conf)
        assert task_kwargs["platformVersion"] == templated_version

    @mock.patch.object(EcsHook, "conn")
    def test_count_can_not_be_modified_by_the_user(
        self, _, assign_subnets, assign_container_name, monkeypatch
    ):
        """The ``count`` parameter must always be 1; verify that the user can not override this value."""
        templated_version = "1"
        templated_cluster = "templated_cluster_name"
        provided_run_task_kwargs = {
            AllEcsConfigKeys.PLATFORM_VERSION: templated_version,
            AllEcsConfigKeys.CLUSTER: templated_cluster,
            "count": 2,  # The user should not be allowed to overwrite count, it must be value of 1
        }

        # Provide values via task run kwargs template and assert that they are applied,
        # which verifies that the OTHER values were changed.
        monkeypatch.setenv(
            f"AIRFLOW__{CONFIG_GROUP_NAME}__{AllEcsConfigKeys.RUN_TASK_KWARGS}".upper(),
            json.dumps(provided_run_task_kwargs),
        )
        task_kwargs = ecs_executor_config.build_task_kwargs(conf)
        assert task_kwargs["platformVersion"] == templated_version
        assert task_kwargs["cluster"] == templated_cluster

        # Assert that count was NOT overridden when the others were applied.
        assert task_kwargs["count"] == 1

    def test_verify_tags_are_used_as_provided(self, assign_subnets, assign_container_name, monkeypatch):
        """Confirm that the ``tags`` provided are not converted to camelCase."""
        templated_tags = {"Apache": "Airflow"}

        provided_run_task_kwargs = {
            "tags": templated_tags,  # The user should be allowed to pass arbitrary run task args
        }

        run_task_kwargs_env_key = f"AIRFLOW__{CONFIG_GROUP_NAME}__{AllEcsConfigKeys.RUN_TASK_KWARGS}".upper()
        monkeypatch.setenv(run_task_kwargs_env_key, json.dumps(provided_run_task_kwargs))
        task_kwargs = ecs_executor_config.build_task_kwargs(conf)

        # Verify that tag names are exempt from the camel-case conversion.
        assert task_kwargs["tags"] == templated_tags

    def test_that_provided_kwargs_are_moved_to_correct_nesting(self, monkeypatch):
        """
        kwargs such as subnets, security groups,  public ip, and container name are valid run task kwargs,
        but they are not placed at the root of the kwargs dict, they should be nested in various sub dicts.
        Ensure we don't leave any behind in the wrong location.
        """
        kwargs_to_test = {
            AllEcsConfigKeys.CONTAINER_NAME: "foobar",
            AllEcsConfigKeys.ASSIGN_PUBLIC_IP: "True",
            AllEcsConfigKeys.SECURITY_GROUPS: "sg1,sg2",
            AllEcsConfigKeys.SUBNETS: "sub1,sub2",
        }
        for key, value in kwargs_to_test.items():
            monkeypatch.setenv(f"AIRFLOW__{CONFIG_GROUP_NAME}__{key}".upper(), value)

        run_task_kwargs = ecs_executor_config.build_task_kwargs(conf)
        run_task_kwargs_network_config = run_task_kwargs["networkConfiguration"]["awsvpcConfiguration"]
        for key, value in kwargs_to_test.items():
            # Assert that the values are not at the root of the kwargs
            camelized_key = camelize(key, uppercase_first_letter=False)

            assert key not in run_task_kwargs
            assert camelized_key not in run_task_kwargs
            if key == AllEcsConfigKeys.CONTAINER_NAME:
                # The actual ECS run_task_kwarg is "name" not "containerName"
                assert run_task_kwargs["overrides"]["containerOverrides"][0]["name"] == value
            elif key == AllEcsConfigKeys.ASSIGN_PUBLIC_IP:
                # The value for this kwarg is cast from bool to enabled/disabled
                assert run_task_kwargs_network_config[camelized_key] == "ENABLED"
            else:
                assert run_task_kwargs_network_config[camelized_key] == value.split(",")

    def test_providing_both_capacity_provider_and_launch_type_fails(self, set_env_vars):
        cps = "[{'capacityProvider': 'cp1', 'weight': 5}, {'capacityProvider': 'cp2', 'weight': 1}]"
        expected_error = re.escape(
            "capacity_provider_strategy and launch_type are mutually exclusive, you can not provide both."
        )
        with conf_vars({(CONFIG_GROUP_NAME, AllEcsConfigKeys.CAPACITY_PROVIDER_STRATEGY): cps}):
            with pytest.raises(ValueError, match=expected_error):
                ecs_executor_config.build_task_kwargs(conf)

    def test_providing_capacity_provider(self, set_env_vars):
        # If a capacity provider strategy is supplied without a launch type, use the strategy.
        valid_capacity_provider = (
            "[{'capacityProvider': 'cp1', 'weight': 5}, {'capacityProvider': 'cp2', 'weight': 1}]"
        )
        conf_overrides = {
            (CONFIG_GROUP_NAME, AllEcsConfigKeys.CAPACITY_PROVIDER_STRATEGY): valid_capacity_provider,
            (CONFIG_GROUP_NAME, AllEcsConfigKeys.LAUNCH_TYPE): None,
        }
        with conf_vars(conf_overrides):
            task_kwargs = ecs_executor_config.build_task_kwargs(conf)
            assert "launchType" not in task_kwargs
            assert task_kwargs["capacityProviderStrategy"] == valid_capacity_provider

    @mock.patch.object(EcsHook, "conn")
    def test_providing_no_capacity_provider_no_lunch_type_with_cluster_default(self, mock_conn, set_env_vars):
        # If no capacity provider strategy is supplied and no launch type, but the
        # cluster has a default capacity provider strategy, use the cluster's default.
        mock_conn.describe_clusters.return_value = {
            "clusters": [{"defaultCapacityProviderStrategy": ["some_strategy"]}]
        }
        with conf_vars({(CONFIG_GROUP_NAME, AllEcsConfigKeys.LAUNCH_TYPE): None}):
            task_kwargs = ecs_executor_config.build_task_kwargs(conf)
            assert "launchType" not in task_kwargs
            assert "capacityProviderStrategy" not in task_kwargs
            mock_conn.describe_clusters.assert_called_once()

    @mock.patch.object(EcsHook, "conn")
    def test_providing_no_capacity_provider_no_lunch_type_no_cluster_default(self, mock_conn, set_env_vars):
        # If no capacity provider strategy is supplied and no launch type, and the cluster
        # does not have a default capacity provider strategy, use the FARGATE launch type.
        mock_conn.describe_clusters.return_value = {"clusters": [{"status": "ACTIVE"}]}
        with conf_vars({(CONFIG_GROUP_NAME, AllEcsConfigKeys.LAUNCH_TYPE): None}):
            task_kwargs = ecs_executor_config.build_task_kwargs(conf)
            assert task_kwargs["launchType"] == "FARGATE"

    def test_ec2_launch_type_drops_platform_version(self):
        conf_overrides = {
            (CONFIG_GROUP_NAME, AllEcsConfigKeys.CONTAINER_NAME): "my-ec2-container",
            (CONFIG_GROUP_NAME, AllEcsConfigKeys.LAUNCH_TYPE): ECS_LAUNCH_TYPE_EC2,
            (CONFIG_GROUP_NAME, AllEcsConfigKeys.PLATFORM_VERSION): "LATEST",
            (CONFIG_GROUP_NAME, AllEcsConfigKeys.ASSIGN_PUBLIC_IP): "False",
            (CONFIG_GROUP_NAME, AllEcsConfigKeys.SUBNETS): None,
            (CONFIG_GROUP_NAME, AllEcsConfigKeys.SECURITY_GROUPS): None,
        }
        with conf_vars(conf_overrides):
            result = ecs_executor_config.build_task_kwargs(conf)

        assert result["launchType"] == ECS_LAUNCH_TYPE_EC2
        assert "platformVersion" not in result
        assert "networkConfiguration" not in result
