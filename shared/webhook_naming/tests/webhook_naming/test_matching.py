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

import pytest

from airflow_shared.webhook_naming.matching import (
    MatchResult,
    compute_covered_key_digests,
    compute_key_digest,
    compute_match_digest,
    evaluate,
    normalize_events,
    normalize_match,
    validate_match,
)

ATTRIBUTES = {"event": "pull_request", "action": "closed", "repository": "my-org/configs", "number": 42}


class TestValidateMatch:
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
        validate_match(events, match)

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
            validate_match(events, match)

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

    def test_covered_key_digests_are_those_of_every_key_set_within_the_names(self):
        assert sorted(compute_covered_key_digests(["repository", "event", "number"])) == sorted(
            compute_key_digest(dict.fromkeys(keys, "x"))
            for keys in [
                (),
                ("event",),
                ("number",),
                ("repository",),
                ("event", "number"),
                ("event", "repository"),
                ("number", "repository"),
                ("event", "number", "repository"),
            ]
        )


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


class TestNormalize:
    def test_normalize_events_returns_sorted_distinct_strings(self):
        assert normalize_events(("push", 42, "push")) == ["42", "push"]
        assert normalize_events(None) is None

    def test_normalize_match_returns_sorted_keys_and_values(self):
        assert normalize_match({"b": 1, "a": ["y", "x", "y"]}) == {"a": ["x", "y"], "b": ["1"]}
        assert normalize_match(None) == {}
