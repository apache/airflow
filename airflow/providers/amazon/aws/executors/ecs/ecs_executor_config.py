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
Default AWS ECS Executor configuration.

This is the default configuration for calling the ECS `run_task` function.
The AWS ECS Executor calls Boto3's run_task(**kwargs) function with the kwargs templated by this
dictionary. See the URL below for documentation on the parameters accepted by the Boto3 run_task
function. In other words, if you don't like the way Airflow calls the Boto3 RunTask API, then
send your own kwargs by overriding the airflow config file.

.. seealso::
https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ecs.html#ECS.Client.run_task
:return: Dictionary kwargs to be used by ECS run_task() function.
"""

from __future__ import annotations

import json
from json import JSONDecodeError

from airflow.configuration import conf
from airflow.providers.amazon.aws.executors.ecs.utils import CONFIG_DEFAULTS, CONFIG_GROUP_NAME, EcsConfigKeys
from airflow.utils.helpers import prune_dict

base_run_task_kwargs = str(conf.get(CONFIG_GROUP_NAME, EcsConfigKeys.RUN_TASK_KWARGS, fallback=dict()))
ECS_EXECUTOR_RUN_TASK_KWARGS = json.loads(base_run_task_kwargs)

if conf.has_option(CONFIG_GROUP_NAME, EcsConfigKeys.REGION):
    ECS_EXECUTOR_RUN_TASK_KWARGS = {
        "cluster": conf.get(CONFIG_GROUP_NAME, EcsConfigKeys.CLUSTER),
        "taskDefinition": conf.get(CONFIG_GROUP_NAME, EcsConfigKeys.TASK_DEFINITION),
        "platformVersion": conf.get(
            CONFIG_GROUP_NAME,
            EcsConfigKeys.PLATFORM_VERSION,
            fallback=CONFIG_DEFAULTS[EcsConfigKeys.PLATFORM_VERSION],
        ),
        "overrides": {
            "containerOverrides": [
                {
                    "name": conf.get(CONFIG_GROUP_NAME, EcsConfigKeys.CONTAINER_NAME),
                    # The executor will overwrite the 'command' property during execution.
                    # Must always be the first container!
                    "command": [],
                }
            ]
        },
        "count": 1,
        "launchType": conf.get(
            CONFIG_GROUP_NAME, EcsConfigKeys.LAUNCH_TYPE, fallback=CONFIG_DEFAULTS[EcsConfigKeys.LAUNCH_TYPE]
        ),
    }

    if any(
        [
            subnets := conf.get(CONFIG_GROUP_NAME, EcsConfigKeys.SUBNETS, fallback=None),
            security_groups := conf.get(CONFIG_GROUP_NAME, EcsConfigKeys.SECURITY_GROUPS, fallback=None),
            assign_public_ip := conf.getboolean(
                CONFIG_GROUP_NAME,
                EcsConfigKeys.ASSIGN_PUBLIC_IP,
                fallback=CONFIG_DEFAULTS[EcsConfigKeys.ASSIGN_PUBLIC_IP],
            ),
        ]
    ):
        network_config = prune_dict(
            {
                "awsvpcConfiguration": {
                    "subnets": str(subnets).split(",") if subnets else subnets,
                    "securityGroups": str(security_groups).split(",") if security_groups else security_groups,
                    "assignPublicIp": "ENABLED" if assign_public_ip else "DISABLED",
                }
            }
        )

        if "subnets" not in network_config["awsvpcConfiguration"]:
            raise ValueError("At least one subnet is required to run a task.")

        ECS_EXECUTOR_RUN_TASK_KWARGS["networkConfiguration"] = network_config

    try:
        json.loads(json.dumps(ECS_EXECUTOR_RUN_TASK_KWARGS))
    except JSONDecodeError:
        raise ValueError(
            f"AWS ECS Executor config values must be JSON serializable. Got {ECS_EXECUTOR_RUN_TASK_KWARGS}"
        )
