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

import datetime

import jinja2.nativetypes
import jinja2.sandbox


class _AirflowEnvironmentMixin:
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        self.filters.update(FILTERS)

    def is_safe_attribute(self, obj, attr, value):
        """
        Allow access to ``_`` prefix vars (but not ``__``).

        Unlike the stock SandboxedEnvironment, we allow access to "private" attributes (ones starting with
        ``_``) whilst still blocking internal or truly private attributes (``__`` prefixed ones).
        """
        return not jinja2.sandbox.is_internal_attribute(obj, attr)


class NativeEnvironment(_AirflowEnvironmentMixin, jinja2.nativetypes.NativeEnvironment):
    """NativeEnvironment for Airflow task templates."""


class SandboxedEnvironment(_AirflowEnvironmentMixin, jinja2.sandbox.SandboxedEnvironment):
    """SandboxedEnvironment for Airflow task templates."""


def ds_filter(value: datetime.date | datetime.time | None) -> str | None:
    """Date filter."""
    if value is None:
        return None
    return value.strftime("%Y-%m-%d")


def ds_nodash_filter(value: datetime.date | datetime.time | None) -> str | None:
    """Date filter without dashes."""
    if value is None:
        return None
    return value.strftime("%Y%m%d")


def ts_filter(value: datetime.date | datetime.time | None) -> str | None:
    """Timestamp filter."""
    if value is None:
        return None
    return value.isoformat()


def ts_nodash_filter(value: datetime.date | datetime.time | None) -> str | None:
    """Timestamp filter without dashes."""
    if value is None:
        return None
    return value.strftime("%Y%m%dT%H%M%S")


def ts_nodash_with_tz_filter(value: datetime.date | datetime.time | None) -> str | None:
    """Timestamp filter with timezone."""
    if value is None:
        return None
    return value.isoformat().replace("-", "").replace(":", "")


FILTERS = {
    "ds": ds_filter,
    "ds_nodash": ds_nodash_filter,
    "ts": ts_filter,
    "ts_nodash": ts_nodash_filter,
    "ts_nodash_with_tz": ts_nodash_with_tz_filter,
}
