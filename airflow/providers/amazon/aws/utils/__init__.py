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

import re
from datetime import datetime
from typing import Tuple

from airflow.version import version


def trim_none_values(obj: dict):
    return {key: val for key, val in obj.items() if val is not None}


def datetime_to_epoch(date_time: datetime) -> int:
    """Convert a datetime object to an epoch integer (seconds)."""
    return int(date_time.timestamp())


def datetime_to_epoch_ms(date_time: datetime) -> int:
    """Convert a datetime object to an epoch integer (milliseconds)."""
    return int(date_time.timestamp() * 1_000)


def datetime_to_epoch_us(date_time: datetime) -> int:
    """Convert a datetime object to an epoch integer (microseconds)."""
    return int(date_time.timestamp() * 1_000_000)


def get_airflow_version() -> Tuple[int, ...]:
    val = re.sub(r'(\d+\.\d+\.\d+).*', lambda x: x.group(1), version)
    return tuple(int(x) for x in val.split('.'))
