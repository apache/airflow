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
import warnings
from datetime import datetime
from enum import Enum

from airflow.exceptions import AirflowProviderDeprecationWarning
from airflow.utils.helpers import prune_dict
from airflow.version import version

log = logging.getLogger(__name__)


def trim_none_values(obj: dict):
    from packaging.version import Version

    from airflow.version import version

    if Version(version) < Version("2.7"):
        return {key: val for key, val in obj.items() if val is not None}
    else:
        warnings.warn(
            "use airflow.utils.helpers.prune_dict() instead", AirflowProviderDeprecationWarning, stacklevel=2
        )
        return prune_dict(obj)


def datetime_to_epoch(date_time: datetime) -> int:
    """Convert a datetime object to an epoch integer (seconds)."""
    return int(date_time.timestamp())


def datetime_to_epoch_ms(date_time: datetime) -> int:
    """Convert a datetime object to an epoch integer (milliseconds)."""
    return int(date_time.timestamp() * 1_000)


def datetime_to_epoch_us(date_time: datetime) -> int:
    """Convert a datetime object to an epoch integer (microseconds)."""
    return int(date_time.timestamp() * 1_000_000)


def get_airflow_version() -> tuple[int, ...]:
    val = re.sub(r"(\d+\.\d+\.\d+).*", lambda x: x.group(1), version)
    return tuple(int(x) for x in val.split("."))


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
