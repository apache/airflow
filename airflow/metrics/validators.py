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

# Only characters in the character set are considered valid
# for the stat_name if stat_name_default_handler is used.
from __future__ import annotations

import abc
import logging
import string
from functools import partial, wraps
from typing import Callable, Iterable, TypeVar, cast

from airflow.configuration import conf
from airflow.exceptions import InvalidStatsNameException

# Only characters in the character set are considered valid
# for the stat_name if stat_name_default_handler is used.
ALLOWED_CHARACTERS = frozenset(string.ascii_letters + string.digits + "_.-")

T = TypeVar("T", bound=Callable)

log = logging.getLogger(__name__)


def validate_stat(fn: T) -> T:
    """
    Check if stat name contains invalid characters.
    Log and not emit stats if name is invalid.
    """

    @wraps(fn)
    def wrapper(self, stat: str | None = None, *args, **kwargs) -> T | None:
        try:
            if stat is not None:
                handler_stat_name_func = get_current_handler_stat_name_func()
                stat = handler_stat_name_func(stat)
            return fn(self, stat, *args, **kwargs)
        except InvalidStatsNameException:
            log.exception("Invalid stat name: %s.", stat)
            return None

    return cast(T, wrapper)


def stat_name_default_handler(
    stat_name: str, max_length: int = 250, allowed_chars: Iterable[str] = ALLOWED_CHARACTERS
) -> str:
    """
    Validate the metric stat name.

    Apply changes when necessary and return the transformed stat name.
    """
    if not isinstance(stat_name, str):
        raise InvalidStatsNameException("The stat_name has to be a string")
    if len(stat_name) > max_length:
        raise InvalidStatsNameException(
            f"The stat_name ({stat_name}) has to be less than {max_length} characters."
        )
    if not all((c in allowed_chars) for c in stat_name):
        raise InvalidStatsNameException(
            f"The stat name ({stat_name}) has to be composed of ASCII "
            f"alphabets, numbers, or the underscore, dot, or dash characters."
        )
    return stat_name


def get_current_handler_stat_name_func() -> Callable[[str], str]:
    """Get Stat Name Handler from airflow.cfg."""
    handler = conf.getimport("metrics", "stat_name_handler")
    if handler is None:
        if conf.get("metrics", "statsd_influxdb_enabled", fallback=False):
            handler = partial(stat_name_default_handler, allowed_chars={*ALLOWED_CHARACTERS, ",", "="})
        else:
            handler = stat_name_default_handler
    return handler


class ListValidator(metaclass=abc.ABCMeta):
    """
    ListValidator metaclass that can be implemented as a AllowListValidator
    or BlockListValidator. The test method must be overridden by its subclass.
    """

    def __init__(self, validate_list: str | None = None) -> None:
        self.validate_list: tuple[str, ...] | None = (
            tuple(item.strip().lower() for item in validate_list.split(",")) if validate_list else None
        )

    @classmethod
    def __subclasshook__(cls, subclass: Callable[[str], str]) -> bool:
        return hasattr(subclass, "test") and callable(subclass.test) or NotImplemented

    @abc.abstractmethod
    def test(self, name: str) -> bool:
        """Test if name is allowed"""
        raise NotImplementedError


class AllowListValidator(ListValidator):
    """AllowListValidator only allows names that match the allowed prefixes."""

    def test(self, name: str) -> bool:
        if self.validate_list is not None:
            return name.strip().lower().startswith(self.validate_list)
        else:
            return True  # default is all metrics are allowed


class BlockListValidator(ListValidator):
    """BlockListValidator only allows names that do not match the blocked prefixes."""

    def test(self, name: str) -> bool:
        if self.validate_list is not None:
            return not name.strip().lower().startswith(self.validate_list)
        else:
            return True  # default is all metrics are allowed
