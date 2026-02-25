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

import re
from datetime import datetime

import click


def validate_release_date(ctx: click.core.Context, param: click.core.Option, value: str) -> str:
    """
    Validate that the date follows YYYY-MM-DD[_NN] format.

    :param ctx: Click context
    :param param: Click parameter
    :param value: The value to validate
    :return: The validated value
    :raises click.BadParameter: If the value doesn't match the required format
    """
    if not value:
        return value

    # Check if the format matches YYYY-MM-DD or YYYY-MM-DD_NN
    pattern = r"^\d{4}-\d{2}-\d{2}(_\d{2})?$"
    if not re.match(pattern, value):
        raise click.BadParameter(
            "Date must be in YYYY-MM-DD or YYYY-MM-DD_NN format (e.g., 2025-11-16 or 2025-11-16_01)"
        )

    # Validate that the date part (YYYY-MM-DD) is a valid date
    date_part = value.split("_")[0]
    try:
        datetime.strptime(date_part, "%Y-%m-%d")
    except ValueError:
        raise click.BadParameter(f"Invalid date: {date_part}. Please provide a valid date.")

    return value
