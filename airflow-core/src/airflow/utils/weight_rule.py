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

import methodtools

# Databases do not support arbitrary precision integers, so we need to limit the range of priority weights.
# postgres: -2147483648 to +2147483647 (see https://www.postgresql.org/docs/current/datatype-numeric.html)
# mysql: -2147483648 to +2147483647 (see https://dev.mysql.com/doc/refman/8.4/en/integer-types.html)
# sqlite: -9223372036854775808 to +9223372036854775807 (see https://sqlite.org/datatype3.html)
DB_SAFE_MINIMUM = -2147483648
DB_SAFE_MAXIMUM = 2147483647


def db_safe_priority(priority_weight: int) -> int:
    """Convert priority weight to a safe value for the database."""
    return max(DB_SAFE_MINIMUM, min(DB_SAFE_MAXIMUM, priority_weight))


class WeightRule(str, Enum):
    """Weight rules."""

    DOWNSTREAM = "downstream"
    UPSTREAM = "upstream"
    ABSOLUTE = "absolute"

    @classmethod
    def is_valid(cls, weight_rule: str) -> bool:
        """Check if weight rule is valid."""
        return weight_rule in cls.all_weight_rules()

    @methodtools.lru_cache(maxsize=None)
    @classmethod
    def all_weight_rules(cls) -> set[str]:
        """Return all weight rules."""
        return set(cls.__members__.values())

    def __str__(self) -> str:
        return self.value
