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

import hashlib
from datetime import UTC, date, datetime
from uuid import UUID

import pytest

from airflow._shared.serialization import FORBIDDEN_XCOM_KEYS
from airflow.sdk.serde import deserialize, serialize
from airflow.webhooks.waits import (
    MatchResult,
    build_resume_event,
    build_timeout_event,
    compute_key_digest,
    compute_match_digest,
    evaluate,
    validate_wait,
)

RECEIVED_AT = datetime(2026, 10, 20, 12, 30, tzinfo=UTC)
ATTRIBUTES = {"event": "pull_request", "action": "closed", "repository": "my-org/configs", "number": 42}


class TestValidateWait:
    @pytest.mark.parametrize(
        ("events", "match"),
        [
            pytest.param(None, None, id="anything"),
            pytest.param(["pull_request"], {}, id="events-only"),
            pytest.param(None, {"number": 42, "repository": "my-org/configs"}, id="scalars"),
            pytest.param(("push", "pull_request"), {"action": ("opened", "closed")}, id="tuples"),
            pytest.param(["pull_request"], {"action": ["closed"], "number": "42"}, id="list-and-scalar"),
        ],
    )
    def test_accepts_the_grammar(self, events, match):
        validate_wait(events, match)

    @pytest.mark.parametrize(
        ("events", "match", "message"),
        [
            pytest.param("push", None, "events must be a list", id="events-string"),
            pytest.param([], None, "events must not be empty", id="events-empty"),
            pytest.param(
                [None], None, "events must be a string or an integer, not NoneType", id="events-none"
            ),
            pytest.param([True], None, "events must be a string or an integer, not bool", id="events-bool"),
            pytest.param(None, [("number", 42)], "match must be a mapping", id="match-not-mapping"),
            pytest.param(None, {"": "x"}, "match keys must be non-empty strings", id="key-empty"),
            pytest.param(None, {1: "x"}, "match keys must be non-empty strings", id="key-not-string"),
            pytest.param(None, {"number": {"gt": 4}}, "reserved for operators", id="dict-value"),
            pytest.param(None, {"action": []}, "must not be an empty list", id="empty-list"),
            pytest.param(None, {"number": None}, "not NoneType", id="none-value"),
            pytest.param(None, {"merged": True}, "not bool", id="bool-value"),
            pytest.param(None, {"ratio": 1.5}, "not float", id="float-value"),
            pytest.param(None, {"action": ["closed", {"in": 1}]}, "not dict", id="dict-in-list"),
            pytest.param(None, {"action": [["closed"]]}, "not list", id="list-in-list"),
        ],
    )
    def test_refuses_what_the_grammar_does_not_allow(self, events, match, message):
        with pytest.raises(ValueError, match=message):
            validate_wait(events, match)

    @pytest.mark.parametrize(
        "check",
        [
            pytest.param(lambda match: compute_key_digest(match), id="key-digest"),
            pytest.param(lambda match: compute_match_digest(None, match), id="match-digest"),
            pytest.param(
                lambda match: evaluate(events=None, match=match, event="push", attributes={}), id="evaluate"
            ),
        ],
    )
    def test_every_entry_point_refuses_a_dict_value(self, check):
        with pytest.raises(ValueError, match="reserved for operators"):
            check({"number": {"in": [1, 2]}})


class TestDigests:
    def test_key_digest_is_the_sha256_of_the_sorted_key_names(self):
        assert (
            compute_key_digest({"repository": "my-org/configs", "number": 42})
            == hashlib.sha256(b'["number","repository"]').hexdigest()
        )

    def test_key_digest_depends_only_on_the_key_set(self):
        assert compute_key_digest({"a": 1, "b": "x"}) == compute_key_digest({"b": ["y", "z"], "a": "2"})
        assert compute_key_digest({"a": 1}) != compute_key_digest({"a": 1, "b": 1})

    def test_match_digest_is_the_sha256_of_the_canonical_json(self):
        assert (
            compute_match_digest(["pull_request"], {"number": 42, "action": ["opened", "closed"]})
            == hashlib.sha256(
                b'{"events":["pull_request"],"match":{"action":["closed","opened"],"number":["42"]}}'
            ).hexdigest()
        )

    @pytest.mark.parametrize(
        ("first", "second"),
        [
            pytest.param(
                (None, {"repository": "my-org/configs", "number": 42}),
                (None, {"number": 42, "repository": "my-org/configs"}),
                id="key-order",
            ),
            pytest.param(
                (["push", "pull_request"], {"action": ["opened", "closed"]}),
                (["pull_request", "push"], {"action": ["closed", "opened"]}),
                id="list-order",
            ),
            pytest.param(
                (["push", "push"], {"action": ["closed", "closed"]}),
                (["push"], {"action": ["closed"]}),
                id="repeated-values",
            ),
            pytest.param((None, {"number": 42}), (None, {"number": "42"}), id="int-and-string"),
            pytest.param((None, {"action": "closed"}), (None, {"action": ["closed"]}), id="scalar-and-list"),
            pytest.param((None, None), (None, {}), id="no-match"),
        ],
    )
    def test_match_digest_is_the_same_for_the_same_key_written_differently(self, first, second):
        assert compute_match_digest(*first) == compute_match_digest(*second)

    @pytest.mark.parametrize(
        ("first", "second"),
        [
            pytest.param((None, {"number": 42}), (["pull_request"], {"number": 42}), id="events-added"),
            pytest.param((["push"], {"number": 42}), (["pull_request"], {"number": 42}), id="other-events"),
            pytest.param((None, {"number": 42}), (None, {"number": 43}), id="other-value"),
            pytest.param((None, {"number": 42}), (None, {"issue": 42}), id="other-key"),
            pytest.param((None, {"action": "closed"}), (None, {"action": ["closed", "opened"]}), id="any-of"),
        ],
    )
    def test_match_digest_differs_for_different_keys(self, first, second):
        assert compute_match_digest(*first) != compute_match_digest(*second)


class TestEvaluate:
    @pytest.mark.parametrize(
        ("events", "match"),
        [
            pytest.param(None, None, id="anything"),
            pytest.param(["push", "pull_request"], {}, id="event-in-events"),
            pytest.param(None, {"repository": "my-org/configs", "action": "closed"}, id="all-keys-equal"),
            pytest.param(None, {"number": "42"}, id="string-against-int-attribute"),
            pytest.param(None, {"number": 42}, id="int-against-int-attribute"),
            pytest.param(None, {"action": ["opened", "closed"]}, id="any-of"),
            pytest.param(
                ["pull_request"], {"event": "pull_request", "number": [41, 42]}, id="events-and-match"
            ),
        ],
    )
    def test_matches(self, events, match):
        assert evaluate(
            events=events, match=match, event="pull_request", attributes=ATTRIBUTES
        ) == MatchResult(matched=True)

    @pytest.mark.parametrize(
        ("events", "match", "event", "expected"),
        [
            pytest.param(
                ["push", "issues"],
                {},
                "pull_request",
                MatchResult(matched=False, key="event", expected=["issues", "push"], actual="pull_request"),
                id="event-not-in-events",
            ),
            pytest.param(
                ["push"],
                {},
                None,
                MatchResult(matched=False, key="event", expected=["push"], actual=None),
                id="delivery-without-event",
            ),
            pytest.param(
                None,
                {"number": 43},
                "pull_request",
                MatchResult(matched=False, key="number", expected=["43"], actual="42"),
                id="other-value",
            ),
            pytest.param(
                None,
                {"branch": "main"},
                "pull_request",
                MatchResult(matched=False, key="branch", expected=["main"], actual=None),
                id="missing-attribute",
            ),
            pytest.param(
                None,
                {"repository": "my-org/configs", "number": 43, "action": ["opened", "reopened"]},
                "pull_request",
                MatchResult(matched=False, key="action", expected=["opened", "reopened"], actual="closed"),
                id="first-failing-key-in-sorted-order",
            ),
            pytest.param(
                ["push"],
                {"number": 43},
                "pull_request",
                MatchResult(matched=False, key="event", expected=["push"], actual="pull_request"),
                id="events-before-match",
            ),
        ],
    )
    def test_names_the_first_failing_key(self, events, match, event, expected):
        assert evaluate(events=events, match=match, event=event, attributes=ATTRIBUTES) == expected

    def test_a_delivery_without_attributes_fails_any_key(self):
        assert evaluate(events=None, match={"number": 42}, event="push", attributes=None) == MatchResult(
            matched=False, key="number", expected=["42"], actual=None
        )


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
