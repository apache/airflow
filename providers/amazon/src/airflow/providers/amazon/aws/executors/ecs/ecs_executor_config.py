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

"""
AWS ECS Executor configuration.

This is the configuration for calling the ECS ``run_task`` function. The AWS ECS Executor calls
Boto3's ``run_task(**kwargs)`` function with the kwargs templated by this dictionary. See the URL
below for documentation on the parameters accepted by the Boto3 run_task function.

.. seealso::
    https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ecs.html#ECS.Client.run_task

"""

from __future__ import annotations

import json
from json import JSONDecodeError

from airflow.providers.amazon.aws.executors.ecs.utils import (
    CONFIG_GROUP_NAME,
    ECS_LAUNCH_TYPE_EC2,
    ECS_LAUNCH_TYPE_FARGATE,
    AllEcsConfigKeys,
    RunTaskKwargsConfigKeys,
    camelize_dict_keys,
    parse_assign_public_ip,
)
from airflow.providers.amazon.aws.hooks.ecs import EcsHook
from airflow.utils.helpers import prune_dict


def _fetch_templated_kwargs(conf) -> dict[str, str]:
    run_task_kwargs_value = conf.get(
        CONFIG_GROUP_NAME,
        AllEcsConfigKeys.RUN_TASK_KWARGS,
        fallback=dict(),
    )
    return json.loads(str(run_task_kwargs_value))


def _fetch_config_values(conf) -> dict[str, str]:
    return prune_dict(
        {key: conf.get(CONFIG_GROUP_NAME, key, fallback=None) for key in RunTaskKwargsConfigKeys()}
    )


def build_task_kwargs(conf) -> dict:
    all_config_keys = AllEcsConfigKeys()
    # This will put some kwargs at the root of the dictionary that do NOT belong there. However,
    # the code below expects them to be there and will rearrange them as necessary.
    task_kwargs = _fetch_config_values(conf)
    task_kwargs.update(_fetch_templated_kwargs(conf))

    has_launch_type: bool = all_config_keys.LAUNCH_TYPE in task_kwargs
    has_capacity_provider: bool = all_config_keys.CAPACITY_PROVIDER_STRATEGY in task_kwargs
    is_launch_type_ec2: bool = task_kwargs.get(all_config_keys.LAUNCH_TYPE, None) == ECS_LAUNCH_TYPE_EC2

    if has_capacity_provider and has_launch_type:
        raise ValueError(
            "capacity_provider_strategy and launch_type are mutually exclusive, you can not provide both."
        )
    if "cluster" in task_kwargs and not (has_capacity_provider or has_launch_type):
        # Default API behavior if neither is provided is to fall back on the default capacity
        # provider if it exists. Since it is not a required value, check if there is one
        # before using it, and if there is not then use the FARGATE launch_type as
        # the final fallback.
        cluster = EcsHook().conn.describe_clusters(clusters=[task_kwargs["cluster"]])["clusters"][0]
        if not cluster.get("defaultCapacityProviderStrategy"):
            task_kwargs[all_config_keys.LAUNCH_TYPE] = ECS_LAUNCH_TYPE_FARGATE

    # If you're using the EC2 launch type, you should not/can not provide the platform_version. In this
    # case we'll drop it on the floor on behalf of the user, instead of throwing an exception.
    if is_launch_type_ec2:
        task_kwargs.pop(all_config_keys.PLATFORM_VERSION, None)

    # There can only be 1 count of these containers
    task_kwargs["count"] = 1  # type: ignore
    # There could be a generic approach to the below, but likely more convoluted then just manually ensuring
    # the one nested config we need to update is present. If we need to override more options in the future we
    # should revisit this.
    if "overrides" not in task_kwargs:
        task_kwargs["overrides"] = {}  # type: ignore
    if "containerOverrides" not in task_kwargs["overrides"]:
        task_kwargs["overrides"]["containerOverrides"] = [{}]  # type: ignore
    task_kwargs["overrides"]["containerOverrides"][0]["name"] = task_kwargs.pop(  # type: ignore
        AllEcsConfigKeys.CONTAINER_NAME
    )
    # The executor will overwrite the 'command' property during execution. Must always be the first container!
    task_kwargs["overrides"]["containerOverrides"][0]["command"] = []  # type: ignore

    subnets = task_kwargs.pop(AllEcsConfigKeys.SUBNETS, None)
    security_groups = task_kwargs.pop(AllEcsConfigKeys.SECURITY_GROUPS, None)
    assign_public_ip = task_kwargs.pop(AllEcsConfigKeys.ASSIGN_PUBLIC_IP, None)
    if subnets or security_groups or assign_public_ip != "False":
        network_config = prune_dict(
            {
                "awsvpcConfiguration": {
                    "subnets": str(subnets).split(",") if subnets else None,
                    "securityGroups": str(security_groups).split(",") if security_groups else None,
                    "assignPublicIp": parse_assign_public_ip(assign_public_ip, is_launch_type_ec2),
                }
            }
        )

        if "subnets" not in network_config["awsvpcConfiguration"]:
            raise ValueError("At least one subnet is required to run a task.")

        task_kwargs["networkConfiguration"] = network_config

    task_kwargs = camelize_dict_keys(task_kwargs)

    try:
        json.loads(json.dumps(task_kwargs))
    except JSONDecodeError:
        raise ValueError("AWS ECS Executor config values must be JSON serializable.")

    return task_kwargs
