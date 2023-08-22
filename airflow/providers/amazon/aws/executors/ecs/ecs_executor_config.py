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

This is the configuration for calling the ECS `run_task` function. The AWS ECS Executor calls
Boto3's run_task(**kwargs) function with the kwargs templated by this dictionary. See the URL
below for documentation on the parameters accepted by the Boto3 run_task function.

.. seealso::
https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ecs.html#ECS.Client.run_task
"""

from __future__ import annotations

import json
from copy import deepcopy
from json import JSONDecodeError

from airflow.configuration import conf
from airflow.providers.amazon.aws.executors.ecs.utils import (
    CONFIG_GROUP_NAME,
    RUN_TASK_KWARG_DEFAULTS,
    EcsConfigKeys,
    convert_dict_keys_camel_case,
    parse_assign_public_ip,
)
from airflow.utils.helpers import prune_dict


def has_section(section):
    """
    Check if the config section exists.

    Config options which are set via environment variables do not get written to conf, which
    means ``conf.has_section()`` will return False if all "aws_ecs_executor" options are set
    that way, as they are in the unit tests.

    ``conf.has_option()`` calls ``conf.get()`` which checks the Airflow config and the
    environment variables to see if the value is set, meaning it will detect the options
    set that way.
    """
    return any([conf.has_option(section, key) for key in EcsConfigKeys()])


def _fetch_templated_kwargs() -> dict[str, str]:
    run_task_kwargs_value = conf.get(CONFIG_GROUP_NAME, EcsConfigKeys.RUN_TASK_KWARGS, fallback=dict())
    return json.loads(str(run_task_kwargs_value))


def _fetch_explicit_kwargs() -> dict[str, str]:
    return prune_dict({key: conf.get(CONFIG_GROUP_NAME, key, fallback=None) for key in EcsConfigKeys()})


def _build_task_kwargs() -> dict:
    task_kwargs = deepcopy(RUN_TASK_KWARG_DEFAULTS)
    if has_section(CONFIG_GROUP_NAME):
        task_kwargs.update(_fetch_templated_kwargs())
        task_kwargs.update(_fetch_explicit_kwargs())
    task_kwargs.update(
        prune_dict(
            {
                "overrides": {
                    "containerOverrides": [
                        {
                            "name": task_kwargs.get(EcsConfigKeys.CONTAINER_NAME),
                            # The executor will overwrite the 'command' property during execution.
                            # Must always be the first container!
                            "command": [],
                        }
                    ]
                },
                "count": 1,
            }
        )
    )

    if any(
        [
            subnets := task_kwargs.get(EcsConfigKeys.SUBNETS),
            security_groups := task_kwargs.get(EcsConfigKeys.SECURITY_GROUPS),
            (assign_public_ip := task_kwargs.get(EcsConfigKeys.ASSIGN_PUBLIC_IP)) is not None,
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

    task_kwargs = convert_dict_keys_camel_case(task_kwargs)

    try:
        json.loads(json.dumps(task_kwargs))
    except JSONDecodeError:
        raise ValueError(f"AWS ECS Executor config values must be JSON serializable. Got {task_kwargs}")

    return task_kwargs


ECS_EXECUTOR_RUN_TASK_KWARGS = _build_task_kwargs()
