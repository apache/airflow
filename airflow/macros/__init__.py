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

from datetime import datetime
from typing import TYPE_CHECKING, Any

import dateutil  # noqa: F401
from babel import Locale
from babel.dates import LC_TIME, format_datetime

import airflow.utils.yaml as yaml  # noqa: F401
from airflow.sdk.definitions.macros import ds_add, ds_format, json, time, uuid  # noqa: F401

if TYPE_CHECKING:
    from pendulum import DateTime


def ds_format_locale(
    ds: str, input_format: str, output_format: str, locale: Locale | str | None = None
) -> str:
    """
    Output localized datetime string in a given Babel format.

    :param ds: Input string which contains a date.
    :param input_format: Input string format (e.g., '%Y-%m-%d').
    :param output_format: Output string Babel format (e.g., `yyyy-MM-dd`).
    :param locale: Locale used to format the output string (e.g., 'en_US').
                   If locale not specified, default LC_TIME will be used and if that's also not available,
                   'en_US' will be used.

    >>> ds_format("2015-01-01", "%Y-%m-%d", "MM-dd-yy")
    '01-01-15'
    >>> ds_format("1/5/2015", "%m/%d/%Y", "yyyy-MM-dd")
    '2015-01-05'
    >>> ds_format("12/07/2024", "%d/%m/%Y", "EEEE dd MMMM yyyy", "en_US")
    'Friday 12 July 2024'

    .. versionadded:: 2.10.0
    """
    return format_datetime(
        datetime.strptime(str(ds), input_format),
        format=output_format,
        locale=locale or LC_TIME or Locale("en_US"),
    )


# TODO: Task SDK: Move this to the Task SDK once we evaluate "pendulum"'s dependency
def datetime_diff_for_humans(dt: Any, since: DateTime | None = None) -> str:
    """
    Return a human-readable/approximate difference between datetimes.

    When only one datetime is provided, the comparison will be based on now.

    :param dt: The datetime to display the diff for
    :param since: When to display the date from. If ``None`` then the diff is
        between ``dt`` and now.
    """
    import pendulum

    return pendulum.instance(dt).diff_for_humans(since)
