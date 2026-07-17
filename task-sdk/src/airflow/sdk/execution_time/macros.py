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

import json  # noqa: F401
import time  # noqa: F401
import uuid  # noqa: F401
from datetime import datetime, timedelta
from random import random  # noqa: F401
from typing import TYPE_CHECKING, Any

import dateutil  # noqa: F401

import airflow.sdk.yaml as yaml  # noqa: F401

if TYPE_CHECKING:
    from babel import Locale
    from pendulum import DateTime


def ds_add(ds: str, days: int) -> str:
    """
    Add or subtract days from a YYYY-MM-DD.

    :param ds: anchor date in ``YYYY-MM-DD`` format to add to
    :param days: number of days to add to the ds, you can use negative values

    >>> ds_add("2015-01-01", 5)
    '2015-01-06'
    >>> ds_add("2015-01-06", -5)
    '2015-01-01'
    """
    if not days:
        return str(ds)
    dt = datetime.strptime(str(ds), "%Y-%m-%d") + timedelta(days=days)
    return dt.strftime("%Y-%m-%d")


def ds_format(ds: str, input_format: str, output_format: str) -> str:
    """
    Output datetime string in a given format.

    :param ds: Input string which contains a date.
    :param input_format: Input string format (e.g., '%Y-%m-%d').
    :param output_format: Output string format (e.g., '%Y-%m-%d').

    >>> ds_format("2015-01-01", "%Y-%m-%d", "%m-%d-%y")
    '01-01-15'
    >>> ds_format("1/5/2015", "%m/%d/%Y", "%Y-%m-%d")
    '2015-01-05'
    >>> ds_format("12/07/2024", "%d/%m/%Y", "%A %d %B %Y", "en_US")
    'Friday 12 July 2024'
    """
    return datetime.strptime(str(ds), input_format).strftime(output_format)


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
    from babel import Locale
    from babel.dates import LC_TIME, format_datetime

    return format_datetime(
        datetime.strptime(str(ds), input_format),
        format=output_format,
        locale=locale or LC_TIME or Locale("en_US"),
    )
