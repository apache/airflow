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

import json

from airflow.models import Log
from airflow.sdk.execution_time.secrets_masker import DEFAULT_SENSITIVE_FIELDS


def _masked_value_check(data, sensitive_fields):
    """
    Recursively check if sensitive fields are properly masked.

    :param data: JSON object (dict, list, or value)
    :param sensitive_fields: Set of sensitive field names
    """
    if isinstance(data, dict):
        for key, value in data.items():
            if key in sensitive_fields:
                assert value == "***", f"Expected masked value for {key}, but got {value}"
            else:
                _masked_value_check(value, sensitive_fields)
    elif isinstance(data, list):
        for item in data:
            _masked_value_check(item, sensitive_fields)


def check_last_log(session, dag_id, event, logical_date, expected_extra=None, check_masked=False):
    logs = (
        session.query(
            Log.dag_id,
            Log.task_id,
            Log.event,
            Log.logical_date,
            Log.owner,
            Log.extra,
        )
        .filter(
            Log.dag_id == dag_id,
            Log.event == event,
            Log.logical_date == logical_date,
        )
        .order_by(Log.dttm.desc())
        .limit(5)
        .all()
    )
    assert len(logs) >= 1
    assert logs[0].extra
    if expected_extra:
        assert json.loads(logs[0].extra) == expected_extra
    if check_masked:
        extra_json = json.loads(logs[0].extra)
        _masked_value_check(extra_json, DEFAULT_SENSITIVE_FIELDS)

    session.query(Log).delete()
