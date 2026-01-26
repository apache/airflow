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
"""Parameters and validators for FastAPI endpoints."""
from __future__ import annotations

import re
from typing import Annotated

from fastapi import Query
from pendulum.parsing import ParserError
from pendulum.tz.timezone import Timezone

from airflow.providers.fab.www.api_fastapi.exceptions import BadRequest


def validate_istimezone(value: str) -> str:
    """
    Validate timezone for Query parameter.

    :param value: Timezone string to validate
    :return: Validated timezone string
    :raises BadRequest: If timezone is invalid
    """
    from pendulum import timezone

    try:
        timezone(value)
        return value
    except ParserError:
        raise BadRequest(detail=f"Invalid timezone: {value}")


def check_limit(value: int) -> int:
    """
    Check the limit does not exceed Max limit.

    :param value: The limit value
    :return: Validated limit
    :raises BadRequest: If limit exceeds maximum
    """
    from airflow.api_connexion.parameters import RESOURCE_EVENT_PREFIX

    max_val = 100
    fallback = max_val

    if value < 0:
        raise BadRequest(
            detail=f"Limit cannot be negative. Got {value}",
        )
    if value == 0:
        return fallback
    if value > max_val:
        raise BadRequest(
            detail=f"The limit should not exceed {max_val}. Got {value}",
        )
    return value


def format_datetime_with_timezone_parameter() -> str | None:
    """Return expected datetime format for API parameters."""
    return "%Y-%m-%dT%H:%M:%SZ"


def format_min_start_date_parameter() -> str | None:
    """Return expected minimum start date format."""
    return "1900-01-01T00:00:00Z"


# Common query parameter types
LimitQueryParam = Annotated[int, Query(ge=1, le=100, description="The maximum number of items to return")]
OffsetQueryParam = Annotated[int, Query(ge=0, description="The number of items to skip")]
