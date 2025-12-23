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

from uuid6 import uuid7

from airflow.sdk import timezone
from airflow.sdk.api.datamodels._generated import HITLDetailResponse, HITLUser as APIHITLUser
from airflow.sdk.execution_time.comms import CreateHITLDetailPayload
from airflow.sdk.execution_time.hitl import (
    HITLUser,
    get_hitl_detail_content_detail,
    update_hitl_detail_response,
    upsert_hitl_detail,
)

TI_ID = uuid7()


def test_upsert_hitl_detail(mock_supervisor_comms) -> None:
    upsert_hitl_detail(
        ti_id=TI_ID,
        options=["Approve", "Reject"],
        subject="Subject",
        body="Optional body",
        defaults=["Approve", "Reject"],
        params={"input_1": {"value": 1, "description": None, "schema": {}}},
        assigned_users=[HITLUser(id="test", name="test")],
        multiple=False,
    )
    mock_supervisor_comms.send.assert_called_with(
        msg=CreateHITLDetailPayload(
            ti_id=TI_ID,
            options=["Approve", "Reject"],
            subject="Subject",
            body="Optional body",
            defaults=["Approve", "Reject"],
            params={"input_1": {"value": 1, "description": None, "schema": {}}},
            assigned_users=[APIHITLUser(id="test", name="test")],
            multiple=False,
        )
    )


def test_update_hitl_detail_response(mock_supervisor_comms) -> None:
    timestamp = timezone.utcnow()
    mock_supervisor_comms.send.return_value = HITLDetailResponse(
        response_received=True,
        chosen_options=["Approve"],
        responded_at=timestamp,
        responded_by_user=APIHITLUser(id="admin", name="admin"),
        params_input={"input_1": 1},
    )
    resp = update_hitl_detail_response(
        ti_id=TI_ID,
        chosen_options=["Approve"],
        params_input={"input_1": 1},
    )
    assert resp == HITLDetailResponse(
        response_received=True,
        chosen_options=["Approve"],
        responded_at=timestamp,
        responded_by_user=APIHITLUser(id="admin", name="admin"),
        params_input={"input_1": 1},
    )


def test_get_hitl_detail_content_detail(mock_supervisor_comms) -> None:
    mock_supervisor_comms.send.return_value = HITLDetailResponse(
        response_received=False,
        chosen_options=None,
        responded_at=None,
        responded_by_user=None,
        params_input={},
    )
    resp = get_hitl_detail_content_detail(TI_ID)
    assert resp == HITLDetailResponse(
        response_received=False,
        chosen_options=None,
        responded_at=None,
        responded_by_user=None,
        params_input={},
    )
