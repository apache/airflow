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

import datetime
from collections.abc import Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from airflow.providers.amazon.aws.executors.utils.base_config_keys import BaseConfigKeys

if TYPE_CHECKING:
    from airflow.models.taskinstancekey import TaskInstanceKey


CONFIG_GROUP_NAME = "aws_lambda_executor"
INVALID_CREDENTIALS_EXCEPTIONS = [
    "ExpiredTokenException",
    "InvalidClientTokenId",
    "UnrecognizedClientException",
]


@dataclass
class LambdaQueuedTask:
    """Represents a Lambda task that is queued. The task will be run in the next heartbeat."""

    key: TaskInstanceKey
    command: CommandType
    queue: str
    executor_config: ExecutorConfigType
    attempt_number: int
    next_attempt_time: datetime.datetime


class InvokeLambdaKwargsConfigKeys(BaseConfigKeys):
    """Config keys loaded which are valid lambda invoke args."""

    FUNCTION_NAME = "function_name"
    QUALIFIER = "function_qualifier"


class AllLambdaConfigKeys(InvokeLambdaKwargsConfigKeys):
    """All config keys which are related to the Lambda Executor."""

    AWS_CONN_ID = "conn_id"
    CHECK_HEALTH_ON_STARTUP = "check_health_on_startup"
    MAX_INVOKE_ATTEMPTS = "max_run_task_attempts"
    REGION_NAME = "region_name"
    QUEUE_URL = "queue_url"
    DLQ_URL = "dead_letter_queue_url"
    END_WAIT_TIMEOUT = "end_wait_timeout"


CommandType = Sequence[str]
ExecutorConfigType = dict[str, Any]
