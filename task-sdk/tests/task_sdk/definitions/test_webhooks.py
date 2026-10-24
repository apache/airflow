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

import copy

import attrs
import pytest

from airflow.sdk import TaskInstanceState, WebhookMatch, WebhookTopic
from airflow.sdk.definitions.webhooks import coerce_cancel_on, coerce_cancel_state

CLOSED = WebhookMatch("github", events=["pull_request"], match={"action": "closed"})
CLOSED_BY_CONF = WebhookMatch(
    "github", events=["pull_request"], match={"action": "closed"}, correlation_key_conf="pr"
)
PUSHED = WebhookMatch("github", events=["push"])
SLACK = WebhookMatch("slack")


class TestWebhookMatch:
    def test_stores_events_and_match_in_canonical_form(self):
        condition = WebhookMatch(
            "github", events=("push", "issues"), match={"number": 42, "action": ["opened", "closed"]}
        )

        assert condition.events == ["issues", "push"]
        assert condition.match == {"action": ["closed", "opened"], "number": ["42"]}
        assert WebhookMatch("github").events is None
        assert WebhookMatch("github").match == {}

    def test_conditions_written_differently_are_equal(self):
        a = WebhookMatch("github", events=["push", "issues"], match={"number": 42, "action": "closed"})
        b = WebhookMatch(
            webhook="github", events=("issues", "push", "push"), match={"action": ["closed"], "number": "42"}
        )

        assert a == b
        assert hash(a) == hash(b)
        assert len({a, b}) == 1

    @pytest.mark.parametrize("webhook", ["", "GitHub", "team:github", "_health", "a" * 65, 1])
    def test_refuses_an_invalid_webhook_name(self, webhook):
        with pytest.raises(ValueError, match="Webhook name must match"):
            WebhookMatch(webhook)

    @pytest.mark.parametrize(
        "kwargs",
        [
            pytest.param({"events": "push"}, id="events-string"),
            pytest.param({"events": []}, id="events-empty"),
            pytest.param({"match": {"n": {"gt": 1}}}, id="match-operator"),
            pytest.param({"match": {"merged": True}}, id="match-bool"),
        ],
    )
    def test_refuses_what_the_match_grammar_does_not_allow(self, kwargs):
        with pytest.raises(ValueError, match="events|match"):
            WebhookMatch("github", **kwargs)

    @pytest.mark.parametrize(
        ("key", "message"),
        [
            pytest.param("", "must name a top-level conf key", id="empty"),
            pytest.param("  ", "must name a top-level conf key", id="blank"),
            pytest.param(1, "must name a top-level conf key", id="int"),
            pytest.param("k" * 251, "cannot be longer than 250 characters", id="too-long"),
        ],
    )
    def test_refuses_an_invalid_correlation_key_conf(self, key, message):
        with pytest.raises(ValueError, match=message):
            WebhookMatch("github", correlation_key_conf=key)

    def test_accepts_the_longest_correlation_key_conf(self):
        assert WebhookMatch("github", correlation_key_conf="k" * 250).correlation_key_conf == "k" * 250

    def test_events_match_and_correlation_key_conf_are_keyword_only(self):
        with pytest.raises(TypeError):
            WebhookMatch("github", ["push"])  # type: ignore[misc]

    def test_is_frozen(self):
        with pytest.raises(attrs.exceptions.FrozenInstanceError):
            CLOSED.webhook = "slack"  # type: ignore[misc]

    def test_survives_deepcopy(self):
        copied = copy.deepcopy(CLOSED_BY_CONF)

        assert copied == CLOSED_BY_CONF
        assert copied.match is not CLOSED_BY_CONF.match


class TestCoerceCancelOn:
    @pytest.mark.parametrize(
        ("cancel_on", "expected"),
        [
            pytest.param(CLOSED, [CLOSED], id="single"),
            pytest.param(
                [SLACK, PUSHED, CLOSED_BY_CONF, CLOSED], [CLOSED, PUSHED, CLOSED_BY_CONF, SLACK], id="list"
            ),
            pytest.param({SLACK, CLOSED, PUSHED}, [CLOSED, PUSHED, SLACK], id="set"),
            pytest.param(
                (
                    PUSHED,
                    CLOSED,
                    WebhookMatch("github", events=("pull_request",), match={"action": ["closed"]}),
                ),
                [CLOSED, PUSHED],
                id="tuple-with-duplicate",
            ),
        ],
    )
    def test_returns_a_sorted_list_without_repeats(self, cancel_on, expected):
        assert coerce_cancel_on(cancel_on) == expected

    @pytest.mark.parametrize("cancel_on", [None, [], ()])
    def test_holds_no_condition(self, cancel_on):
        assert coerce_cancel_on(cancel_on) is None

    @pytest.mark.parametrize(
        ("cancel_on", "message"),
        [
            pytest.param("github", "cancel_on must be a WebhookMatch or a list of them, not str", id="str"),
            pytest.param(
                {"webhook": "github"},
                "cancel_on must be a WebhookMatch or a list of them, not dict",
                id="dict",
            ),
            pytest.param(
                [WebhookTopic(webhook="github", event="push")],
                "cancel_on takes WebhookMatch conditions, not WebhookTopic",
                id="topic",
            ),
            pytest.param(
                [CLOSED, object()], "cancel_on takes WebhookMatch conditions, not object", id="object"
            ),
        ],
    )
    def test_refuses_what_is_not_a_webhook_match(self, cancel_on, message):
        with pytest.raises(TypeError, match=message):
            coerce_cancel_on(cancel_on)


class TestCoerceCancelState:
    @pytest.mark.parametrize(
        ("cancel_state", "expected"),
        [
            pytest.param("failed", "failed", id="failed"),
            pytest.param("skipped", "skipped", id="skipped"),
            pytest.param(TaskInstanceState.SKIPPED, "skipped", id="enum"),
            pytest.param(None, None, id="none"),
        ],
    )
    def test_accepts(self, cancel_state, expected):
        result = coerce_cancel_state(cancel_state)

        assert result == expected
        assert type(result) is type(expected)

    @pytest.mark.parametrize("cancel_state", ["success", "FAILED", 1])
    def test_refuses_an_unknown_state(self, cancel_state):
        with pytest.raises(ValueError, match="cancel_state must be 'failed' or 'skipped'"):
            coerce_cancel_state(cancel_state)
