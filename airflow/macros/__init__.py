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

import json  # noqa
import time  # noqa
import uuid  # noqa
from datetime import datetime, timedelta
from random import random  # noqa
from typing import Any

import dateutil  # noqa
from pendulum import DateTime

import airflow.utils.yaml as yaml  # noqa
from airflow.utils.deprecation_tools import add_deprecated_classes

__deprecated_classes = {
    "hive": {
        "closest_ds_partition": "airflow.providers.apache.hive.macros.hive.closest_ds_partition",
        "max_partition": "airflow.providers.apache.hive.macros.hive.max_partition",
    },
}

add_deprecated_classes(__deprecated_classes, __name__)


def ds_add(ds: str, days: int) -> str:
    """
    Add or subtract days from a YYYY-MM-DD.

    :param ds: anchor date in ``YYYY-MM-DD`` format to add to
    :param days: number of days to add to the ds, you can use negative values

    >>> ds_add('2015-01-01', 5)
    '2015-01-06'
    >>> ds_add('2015-01-06', -5)
    '2015-01-01'
    """
    if not days:
        return str(ds)
    dt = datetime.strptime(str(ds), "%Y-%m-%d") + timedelta(days=days)
    return dt.strftime("%Y-%m-%d")


def ds_format(ds: str, input_format: str, output_format: str) -> str:
    """
    Output datetime string in a given format.

    :param ds: input string which contains a date
    :param input_format: input string format. E.g. %Y-%m-%d
    :param output_format: output string format  E.g. %Y-%m-%d

    >>> ds_format('2015-01-01', "%Y-%m-%d", "%m-%d-%y")
    '01-01-15'
    >>> ds_format('1/5/2015', "%m/%d/%Y",  "%Y-%m-%d")
    '2015-01-05'
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
