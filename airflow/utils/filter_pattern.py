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

from fastapi import HTTPException, status
from pydantic import BaseModel


class FilterPatternType(str, Enum):
    """
    Enum representing the types of patterns that can be used for advanced search queries.

    STARTS_WITH: Match strings that start with the given value.
    ENDS_WITH: Match strings that end with the given value.
    CONTAINS: Match strings that contain the given value.
    EQUALS: Match strings that exactly match the given value.
    NOT_STARTS_WITH: Exclude strings that start with the given value.
    NOT_ENDS_WITH: Exclude strings that end with the given value.
    NOT_CONTAINS: Exclude strings that contain the given value.
    """

    STARTS_WITH = "starts_with"
    ENDS_WITH = "ends_with"
    CONTAINS = "contains"
    EQUALS = "equals"
    NOT_STARTS_WITH = "not_starts_with"
    NOT_ENDS_WITH = "not_ends_with"
    NOT_CONTAINS = "not_contains"

    @classmethod
    def validate(cls, pattern_type: str):
        """Validate the pattern type and raise a ValueError if invalid."""
        if pattern_type not in cls._value2member_map_:
            valid_types = ", ".join(cls._value2member_map_)
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=f"Invalid value for pattern type. Valid values are {valid_types}",
            )


class Filter(BaseModel):
    """
    Represents a filter used to match data based on specific criteria.

    field (str): The field to filter by, e.g., 'key' or 'value'.
    pattern (FilterPatternType): The type of pattern to apply (e.g., 'starts_with', 'equals').
    value (str): The value to match in the filter.
    """

    field: str
    pattern: FilterPatternType | None = FilterPatternType.CONTAINS
    value: str | None
