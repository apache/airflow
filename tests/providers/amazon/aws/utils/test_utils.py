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

from airflow.providers.amazon.aws.utils import (
    _StringCompareEnum,
    datetime_to_epoch,
    datetime_to_epoch_ms,
    datetime_to_epoch_us,
    get_airflow_version,
)

DT = datetime.datetime(2000, 1, 1, tzinfo=datetime.timezone.utc)
EPOCH = 946_684_800


class EnumTest(_StringCompareEnum):
    FOO = "FOO"


def test_datetime_to_epoch():
    assert datetime_to_epoch(DT) == EPOCH


def test_datetime_to_epoch_ms():
    assert datetime_to_epoch_ms(DT) == EPOCH * 1000


def test_datetime_to_epoch_us():
    assert datetime_to_epoch_us(DT) == EPOCH * 1_000_000


def test_get_airflow_version():
    assert len(get_airflow_version()) == 3


def test_str_enum():
    assert EnumTest.FOO == "FOO"
    assert EnumTest.FOO.value == "FOO"
