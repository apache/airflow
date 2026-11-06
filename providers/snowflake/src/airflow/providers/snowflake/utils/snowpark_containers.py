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


class SnowparkContainerJobStatus(str, Enum):
    """Statuses of a Snowpark Container Services job service."""

    PENDING = "PENDING"
    RUNNING = "RUNNING"
    CANCELLING = "CANCELLING"
    SUSPENDING = "SUSPENDING"
    DELETING = "DELETING"
    DONE = "DONE"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"
    INTERNAL_ERROR = "INTERNAL_ERROR"


TERMINAL_STATUSES: frozenset[SnowparkContainerJobStatus] = frozenset(
    {
        SnowparkContainerJobStatus.DONE,
        SnowparkContainerJobStatus.FAILED,
        SnowparkContainerJobStatus.CANCELLED,
        SnowparkContainerJobStatus.INTERNAL_ERROR,
    }
)
NON_TERMINAL_STATUSES: frozenset[SnowparkContainerJobStatus] = frozenset(
    {
        SnowparkContainerJobStatus.PENDING,
        SnowparkContainerJobStatus.RUNNING,
        SnowparkContainerJobStatus.CANCELLING,
        SnowparkContainerJobStatus.SUSPENDING,
        SnowparkContainerJobStatus.DELETING,
    }
)
