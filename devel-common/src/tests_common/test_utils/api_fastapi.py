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

from airflow.models import DagRun, Log
from airflow.models.dagrun import DagRunNote
from airflow.models.taskinstance import TaskInstanceNote

try:
    from airflow.sdk._shared.secrets_masker import DEFAULT_SENSITIVE_FIELDS
except ImportError:
    from airflow.sdk.execution_time.secrets_masker import DEFAULT_SENSITIVE_FIELDS  # type:ignore[no-redef]

sensitive_fields = DEFAULT_SENSITIVE_FIELDS


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


def _check_last_log(session, dag_id, event, logical_date, expected_extra=None, check_masked=False):
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
        .limit(1)
        .all()
    )
    assert len(logs) == 1
    assert logs[0].extra
    if expected_extra:
        assert json.loads(logs[0].extra) == expected_extra
    if check_masked:
        extra_json = json.loads(logs[0].extra)
        _masked_value_check(extra_json, sensitive_fields)


def _check_dag_run_note(session, dr_id, note_data):
    dr_note = (
        session.query(DagRunNote)
        .join(DagRun, DagRunNote.dag_run_id == DagRun.id)
        .filter(DagRun.run_id == dr_id)
        .one_or_none()
    )
    if note_data is None:
        assert dr_note is None
    else:
        assert dr_note.user_id == note_data.get("user_id")
        assert dr_note.content == note_data.get("content")


def _check_task_instance_note(session, ti_id, note_data):
    ti_note = session.query(TaskInstanceNote).filter_by(ti_id=ti_id).one_or_none()
    if note_data is None:
        assert ti_note is None
    else:
        # Had to add this refresh because TestPatchTaskInstance::test_set_note_should_respond_200_mapped_task_instance_with_rtif
        # was failing for map index = 2. Unless I force refresh ti_note, it was returning the old value.
        # Even if I reverse the order of map indexes, only the map index 2 was returning older value.
        session.refresh(ti_note)
        assert ti_note.content == note_data["content"]
        assert ti_note.user_id == note_data["user_id"]
