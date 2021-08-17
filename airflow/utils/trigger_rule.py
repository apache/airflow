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

from typing import Set


class TriggerRule:
    """Class with task's trigger rules."""

    ALL_SUCCESS = 'all_success'
    ALL_FAILED = 'all_failed'
    ALL_DONE = 'all_done'
    ONE_SUCCESS = 'one_success'
    ONE_FAILED = 'one_failed'
    NONE_FAILED = 'none_failed'
    NONE_FAILED_OR_SKIPPED = 'none_failed_or_skipped'
    NONE_SKIPPED = 'none_skipped'
    DUMMY = 'dummy'

    _ALL_TRIGGER_RULES: Set[str] = set()

    @classmethod
    def is_valid(cls, trigger_rule):
        """Validates a trigger rule."""
        return trigger_rule in cls.all_triggers()

    @classmethod
    def all_triggers(cls):
        """Returns all trigger rules."""
        if not cls._ALL_TRIGGER_RULES:
            cls._ALL_TRIGGER_RULES = {
                getattr(cls, attr)
                for attr in dir(cls)
                if not attr.startswith("_") and not callable(getattr(cls, attr))
            }
        return cls._ALL_TRIGGER_RULES
