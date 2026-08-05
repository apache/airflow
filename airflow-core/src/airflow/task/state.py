#
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

from enum import Enum


class TerminalTIState(str, Enum):
    """States that a Task Instance can be in that indicate it has reached a terminal state."""

    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"  # A user can raise a AirflowSkipException from a task & it will be marked as skipped
    UPSTREAM_FAILED = "upstream_failed"
    REMOVED = "removed"

    def __str__(self) -> str:
        return self.value


class IntermediateTIState(str, Enum):
    """States that a Task Instance can be in that indicate it is not yet in a terminal or running state."""

    SCHEDULED = "scheduled"
    QUEUED = "queued"
    RESTARTING = "restarting"
    UP_FOR_RETRY = "up_for_retry"
    UP_FOR_RESCHEDULE = "up_for_reschedule"
    DEFERRED = "deferred"
    AWAITING_INPUT = "awaiting_input"

    def __str__(self) -> str:
        return self.value
