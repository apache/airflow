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
# Note: Any AirflowException raised is expected to cause the TaskInstance
#       to be marked in an ERROR state
from __future__ import annotations

from datetime import datetime, timedelta

from performance_dags.performance_dag.performance_dag_utils import (
    parse_start_date,
    parse_time_delta,
)


def test_parse_time_delta():
    assert parse_time_delta("1h") == timedelta(hours=1)


def test_parse_start_date_from_date():
    now = datetime.now()
    formatted_time = now.strftime("%Y-%m-%d %H:%M:%S.%f")
    date, dag_id_component = parse_start_date(formatted_time, "1h")
    assert date == now
    assert dag_id_component == str(int(now.timestamp()))


def test_parse_start_date_from_offset():
    now = datetime.now()
    one_hour_ago = now - timedelta(hours=1)
    date, dag_id_component = parse_start_date(None, "1h")
    one_hour_ago_timestamp = int(one_hour_ago.timestamp())
    one_hour_ago_result = int(date.timestamp())
    assert abs(one_hour_ago_result - one_hour_ago_timestamp) < 1000
    assert dag_id_component == "1h"
