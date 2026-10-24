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

from datetime import UTC, date, datetime
from uuid import UUID

import pytest

from airflow._shared.serialization import FORBIDDEN_XCOM_KEYS
from airflow.sdk.serde import deserialize, serialize
from airflow.webhooks.waits import build_resume_event, build_timeout_event

RECEIVED_AT = datetime(2026, 10, 20, 12, 30, tzinfo=UTC)
ATTRIBUTES = {"event": "pull_request", "action": "closed", "repository": "my-org/configs", "number": 42}


class TestBuildResumeEvent:
    def test_has_exactly_the_documented_keys(self):
        event = build_resume_event(
            webhook="github",
            event="pull_request",
            delivery_id="d-1",
            received_at=RECEIVED_AT,
            attributes=ATTRIBUTES,
            payload={"pull_request": {"number": 42, "merged": True}},
        )

        assert event == {
            "webhook": "github",
            "event": "pull_request",
            "delivery_id": "d-1",
            "received_at": "2026-10-20T12:30:00+00:00",
            "attributes": ATTRIBUTES,
            "payload": {"pull_request": {"number": 42, "merged": True}},
        }

    def test_keeps_missing_values_as_none(self):
        event = build_resume_event(
            webhook="github",
            event=None,
            delivery_id=None,
            received_at=RECEIVED_AT,
            attributes={},
            payload=None,
        )

        assert (event["event"], event["delivery_id"], event["payload"]) == (None, None, None)

    def test_normalizes_values_json_cannot_hold(self):
        event = build_resume_event(
            webhook="github",
            event="pull_request",
            delivery_id="d-1",
            received_at=RECEIVED_AT,
            attributes={"number": 42, "merged_at": RECEIVED_AT},
            payload={
                "day": date(2026, 10, 20),
                "labels": ("deploy", "preview"),
                "author": UUID("01960000-0000-7000-8000-000000000002"),
                "counts": {1: "one"},
            },
        )

        assert event["attributes"] == {"number": 42, "merged_at": "2026-10-20T12:30:00+00:00"}
        assert event["payload"] == {
            "day": "2026-10-20",
            "labels": ["deploy", "preview"],
            "author": "01960000-0000-7000-8000-000000000002",
            "counts": {"1": "one"},
        }

    @pytest.mark.parametrize("reserved", sorted(FORBIDDEN_XCOM_KEYS))
    def test_escapes_the_keys_serde_reserves(self, reserved):
        event = build_resume_event(
            webhook="github",
            event="workflow_dispatch",
            delivery_id="d-1",
            received_at=RECEIVED_AT,
            attributes={reserved: "attribute"},
            payload={"inputs": [{reserved: "sent", f"_{reserved}": "sent escaped", "ref": "main"}]},
        )

        assert event["attributes"] == {f"_{reserved}": "attribute"}
        assert event["payload"] == {
            "inputs": [{f"_{reserved}": "sent", f"__{reserved}": "sent escaped", "ref": "main"}]
        }

    def test_survives_serde_whatever_keys_the_payload_holds(self):
        event = build_resume_event(
            webhook="github",
            event="workflow_dispatch",
            delivery_id="d-1",
            received_at=RECEIVED_AT,
            attributes=ATTRIBUTES,
            payload={"inputs": {key: "value" for key in FORBIDDEN_XCOM_KEYS}},
        )

        assert deserialize(serialize(event)) == event


def test_build_timeout_event_has_exactly_the_documented_keys():
    assert build_timeout_event(datetime(2026, 10, 21, tzinfo=UTC)) == {
        "error": "No matching delivery arrived before 2026-10-21T00:00:00+00:00",
        "error_type": "timeout",
    }
