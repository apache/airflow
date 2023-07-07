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

from collections import namedtuple
from typing import Any, Callable, Dict, List

CommandType = List[str]
ExecutorConfigFunctionType = Callable[[CommandType], dict]
EcsQueuedTask = namedtuple("EcsQueuedTask", ("key", "command", "queue", "executor_config"))
ExecutorConfigType = Dict[str, Any]
EcsTaskInfo = namedtuple("EcsTaskInfo", ("cmd", "queue", "config"))

CONFIG_GROUP_NAME = "aws_ecs_executor"

CONFIG_DEFAULTS = {
    "assign_public_ip": False,
    "conn_id": "aws_default",
    "launch_type": "FARGATE",
    "platform_version": "LATEST",
}


class EcsConfigKeys:
    """Keys loaded into the config which are related to the ECS Executor."""

    ASSIGN_PUBLIC_IP = "assign_public_ip"
    CLUSTER = "cluster"
    CONTAINER_NAME = "container_name"
    LAUNCH_TYPE = "launch_type"
    PLATFORM_VERSION = "platform_version"
    REGION = "region"
    RUN_TASK_KWARGS = "run_task_kwargs"
    SECURITY_GROUPS = "security_groups"
    SUBNETS = "subnets"
    TASK_DEFINITION = "task_definition"


class EcsExecutorException(Exception):
    """Thrown when something unexpected has occurred within the ECS ecosystem."""
