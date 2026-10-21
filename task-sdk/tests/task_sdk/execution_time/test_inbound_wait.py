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

from datetime import timedelta

import pytest
from uuid6 import uuid7

from airflow.sdk import timezone
from airflow.sdk.api.datamodels._generated import WaitStatus
from airflow.sdk.exceptions import ErrorType, WebhookWaitUnsupported
from airflow.sdk.execution_time.comms import ErrorResponse, InboundWaitResult, RegisterInboundWait
from airflow.sdk.execution_time.inbound_wait import register_inbound_wait

WAIT = InboundWaitResult(
    id=uuid7(), status=WaitStatus.REGISTERED, deadline=timezone.datetime(2026, 10, 20, 13)
)


@pytest.mark.parametrize(
    ("events", "match", "sent_events", "sent_match"),
    [
        pytest.param(
            ["pull_request"],
            {"repository": "my-org/configs", "number": 4242},
            ["pull_request"],
            {"repository": "my-org/configs", "number": 4242},
            id="lists-and-strings-as-given",
        ),
        pytest.param(
            ("pull_request",),
            {"action": ("closed", "reopened")},
            ["pull_request"],
            {"action": ["closed", "reopened"]},
            id="tuples-as-lists",
        ),
        pytest.param(
            range(1, 3), {"number": range(4242, 4244)}, [1, 2], {"number": [4242, 4243]}, id="ranges"
        ),
        pytest.param(None, {"number": {"gt": 1}}, None, {"number": {"gt": 1}}, id="grammar-left-to-server"),
    ],
)
def test_register_inbound_wait_sends_the_wait(mock_supervisor_comms, events, match, sent_events, sent_match):
    """Sequences are sent as lists, and the wait the supervisor answers with is returned."""
    mock_supervisor_comms.send.return_value = WAIT

    wait = register_inbound_wait(webhook="github", events=events, match=match, timeout=timedelta(days=1))

    assert wait == WAIT
    mock_supervisor_comms.send.assert_called_once_with(
        RegisterInboundWait(webhook="github", events=sent_events, match=sent_match, timeout=timedelta(days=1))
    )


@pytest.mark.parametrize(
    ("kwargs", "error", "message"),
    [
        pytest.param({"events": "pull_request"}, ValueError, "valid list", id="events-string"),
        pytest.param({"timeout": 3600}, TypeError, "timeout must be a timedelta", id="timeout-seconds"),
        pytest.param({"timeout": "PT1H"}, TypeError, "timeout must be a timedelta", id="timeout-duration"),
    ],
)
def test_register_inbound_wait_refuses_a_wait_it_cannot_send(mock_supervisor_comms, kwargs, error, message):
    with pytest.raises(error, match=message):
        register_inbound_wait(webhook="github", **kwargs)

    mock_supervisor_comms.send.assert_not_called()


def test_register_inbound_wait_on_server_without_waits_raises_webhook_wait_unsupported(mock_supervisor_comms):
    mock_supervisor_comms.send.return_value = ErrorResponse(
        error=ErrorType.WEBHOOK_WAIT_UNSUPPORTED,
        detail={"message": "The API server does not support webhook waits"},
    )

    with pytest.raises(WebhookWaitUnsupported, match="^The API server does not support webhook waits$"):
        register_inbound_wait(webhook="github", timeout=timedelta(hours=1))
