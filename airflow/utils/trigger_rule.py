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


class TriggerRule(str, Enum):
    """Class with task's trigger rules."""

    ALL_SUCCESS = 'all_success'
    ALL_FAILED = 'all_failed'
    ALL_DONE = 'all_done'
    ONE_SUCCESS = 'one_success'
    ONE_FAILED = 'one_failed'
    ONE_DONE = 'one_done'
    NONE_FAILED = 'none_failed'
    NONE_FAILED_OR_SKIPPED = 'none_failed_or_skipped'
    NONE_SKIPPED = 'none_skipped'
    DUMMY = 'dummy'
    ALWAYS = 'always'
    NONE_FAILED_MIN_ONE_SUCCESS = "none_failed_min_one_success"
    ALL_SKIPPED = 'all_skipped'

    @classmethod
    def is_valid(cls, trigger_rule: str) -> bool:
        """Validates a trigger rule."""
        return trigger_rule in cls.all_triggers()

    @classmethod
    def all_triggers(cls) -> set[str]:
        """Returns all trigger rules."""
        return set(cls.__members__.values())

    def __str__(self) -> str:
        return self.value
