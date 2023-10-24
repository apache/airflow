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

from airflow.configuration import conf
from airflow.providers.amazon.aws.executors.ecs.utils import (
    CONFIG_GROUP_NAME,
    AllEcsConfigKeys,
    RunTaskKwargsConfigKeys,
    camelize_dict_keys,
    parse_assign_public_ip,
)
from airflow.utils.helpers import prune_dict


def _fetch_templated_kwargs() -> dict[str, str]:
    run_task_kwargs_value = conf.get(CONFIG_GROUP_NAME, AllEcsConfigKeys.RUN_TASK_KWARGS, fallback=dict())
    return json.loads(str(run_task_kwargs_value))


def _fetch_config_values() -> dict[str, str]:
    return prune_dict(
        {key: conf.get(CONFIG_GROUP_NAME, key, fallback=None) for key in RunTaskKwargsConfigKeys()}
    )


def build_task_kwargs() -> dict:
    # This will put some kwargs at the root of the dictionary that do NOT belong there. However,
    # the code below expects them to be there and will rearrange them as necessary.
    task_kwargs = _fetch_config_values()
    task_kwargs.update(_fetch_templated_kwargs())

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

    if any(
        [
            subnets := task_kwargs.pop(AllEcsConfigKeys.SUBNETS, None),
            security_groups := task_kwargs.pop(AllEcsConfigKeys.SECURITY_GROUPS, None),
            # Surrounding parens are for the walrus operator to function correctly along with the None check
            (assign_public_ip := task_kwargs.pop(AllEcsConfigKeys.ASSIGN_PUBLIC_IP, None)) is not None,
        ]
    ):
        network_config = prune_dict(
            {
                "awsvpcConfiguration": {
                    "subnets": str(subnets).split(",") if subnets else None,
                    "securityGroups": str(security_groups).split(",") if security_groups else None,
                    "assignPublicIp": parse_assign_public_ip(assign_public_ip),
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
