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

import logging
from collections.abc import Callable
from functools import wraps
from typing import TYPE_CHECKING, Any, TypeVar, cast

from airflow.configuration import conf
from airflow.providers.fab.www.api_connexion.exceptions import BadRequest

if TYPE_CHECKING:
    from datetime import datetime


log = logging.getLogger(__name__)


def validate_istimezone(value: datetime) -> None:
    """Validate that a datetime is not naive."""
    if not value.tzinfo:
        raise BadRequest("Invalid datetime format", detail="Naive datetime is disallowed")


def check_limit(value: int) -> int:
    """
    Check the limit does not exceed configured value.

    This checks the limit passed to view and raises BadRequest if
    limit exceed user configured value
    """
    max_val = conf.getint("api", "maximum_page_limit")  # user configured max page limit
    fallback = conf.getint("api", "fallback_page_limit")

    if value > max_val:
        log.warning(
            "The limit param value %s passed in API exceeds the configured maximum page limit %s",
            value,
            max_val,
        )
        return max_val
    if value == 0:
        return fallback
    if value < 0:
        raise BadRequest("Page limit must be a positive integer")
    return value


T = TypeVar("T", bound=Callable)


def format_parameters(params_formatters: dict[str, Callable[[Any], Any]]) -> Callable[[T], T]:
    """
    Create a decorator to convert parameters using given formatters.

    Using it allows you to separate parameter formatting from endpoint logic.

    :param params_formatters: Map of key name and formatter function
    """

    def format_parameters_decorator(func: T) -> T:
        @wraps(func)
        def wrapped_function(*args, **kwargs):
            for key, formatter in params_formatters.items():
                if key in kwargs:
                    kwargs[key] = formatter(kwargs[key])
            return func(*args, **kwargs)

        return cast("T", wrapped_function)

    return format_parameters_decorator
