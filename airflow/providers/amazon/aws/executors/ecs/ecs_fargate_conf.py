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

"""**Default AWS ECS configuration**
This is the default configuration for calling the ECS `run_task` function.
The AWS ECS-Fargate Executor calls Boto3's run_task(**kwargs) function with the kwargs templated by this
dictionary. See the URL below for documentation on the parameters accepted by the Boto3 run_task function.
In other words, if you don't like the way Airflow calls the Boto3 RunTask API, then send your own kwargs
by overriding the airflow config file.

.. seealso::
https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ecs.html#ECS.Client.run_task
:return: Dictionary kwargs to be used by ECS run_task() function.
"""
from __future__ import annotations

from airflow.configuration import conf


def has_option(section, config_name) -> bool:
    """Returns True if configuration has a section and an option."""
    if conf.has_option(section, config_name):
        config_val = conf.get(section, config_name)
        return config_val is not None and config_val != ""
    return False


ECS_FARGATE_RUN_TASK_KWARGS = {}
if conf.has_option("ecs_fargate", "region"):
    ECS_FARGATE_RUN_TASK_KWARGS = {
        "cluster": conf.get("ecs_fargate", "cluster"),
        "taskDefinition": conf.get("ecs_fargate", "task_definition"),
        "platformVersion": "LATEST",
        "overrides": {
            "containerOverrides": [
                {
                    "name": conf.get("ecs_fargate", "container_name"),
                    # The executor will overwrite the 'command' property during execution.
                    # Must always be the first container!
                    "command": [],
                }
            ]
        },
        "count": 1,
    }

    if has_option("ecs_fargate", "launch_type"):
        ECS_FARGATE_RUN_TASK_KWARGS["launchType"] = conf.get("ecs_fargate", "launch_type")

    # Only build this section if 'subnets', 'security_groups', and 'assign_public_ip' are populated
    if (
        has_option("ecs_fargate", "subnets")
        and has_option("ecs_fargate", "security_groups")
        and conf.has_option("ecs_fargate", "assign_public_ip")
    ):
        ECS_FARGATE_RUN_TASK_KWARGS["networkConfiguration"] = {
            "awsvpcConfiguration": {
                "subnets": conf.get("ecs_fargate", "subnets").split(","),
                "securityGroups": conf.get("ecs_fargate", "security_groups").split(","),
                "assignPublicIp": conf.get("ecs_fargate", "assign_public_ip"),
            }
        }
