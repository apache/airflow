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
import re
from datetime import datetime, timezone
from enum import Enum
from importlib import metadata
from typing import Any

from airflow.providers.common.compat.sdk import AirflowException
from airflow.utils.helpers import prune_dict
from airflow.version import version

log = logging.getLogger(__name__)


def trim_none_values(obj: dict):
    return prune_dict(obj)


def datetime_to_epoch(date_time: datetime) -> int:
    """Convert a datetime object to an epoch integer (seconds)."""
    return int(date_time.timestamp())


def datetime_to_epoch_ms(date_time: datetime) -> int:
    """Convert a datetime object to an epoch integer (milliseconds)."""
    return int(date_time.timestamp() * 1_000)


def datetime_to_epoch_utc_ms(date_time: datetime) -> int:
    """Convert a datetime object to an epoch integer (milliseconds) in UTC timezone."""
    return int(date_time.replace(tzinfo=timezone.utc).timestamp() * 1_000)


def datetime_to_epoch_us(date_time: datetime) -> int:
    """Convert a datetime object to an epoch integer (microseconds)."""
    return int(date_time.timestamp() * 1_000_000)


def get_airflow_version() -> tuple[int, ...]:
    match = re.match(r"(\d+)\.(\d+)\.(\d+)", version)
    if match is None:  # Not theoratically possible.
        raise RuntimeError(f"Broken Airflow version: {version}")
    return tuple(int(x) for x in match.groups())


def get_botocore_version() -> tuple[int, ...]:
    """Return the version number of the installed botocore package in the form of a tuple[int,...]."""
    return tuple(map(int, metadata.version("botocore").split(".")[:3]))


def validate_execute_complete_event(event: dict[str, Any] | None = None) -> dict[str, Any]:
    if event is None:
        err_msg = "Trigger error: event is None"
        log.error(err_msg)
        raise AirflowException(err_msg)
    return event


class _StringCompareEnum(Enum):
    """
    An Enum class which can be compared with regular `str` and subclasses.

    This class avoids multiple inheritance such as AwesomeEnum(str, Enum)
    which does not work well with templated_fields and Jinja templates.
    """

    def __eq__(self, other):
        if isinstance(other, str):
            return self.value == other
        return super().__eq__(other)

    def __hash__(self):
        return super().__hash__()  # Need to set because we redefine __eq__
