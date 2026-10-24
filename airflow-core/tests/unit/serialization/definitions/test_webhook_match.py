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

import pytest

from airflow.serialization.definitions.webhook_match import SerializedWebhookMatch


class TestSerializedWebhookMatch:
    @pytest.mark.parametrize(
        ("kwargs", "expected"),
        [
            pytest.param(
                {"events": ["push", "pull_request", "push"]},
                SerializedWebhookMatch(webhook="github", events=("pull_request", "push")),
                id="events-sorted-without-repeats",
            ),
            pytest.param(
                {"match": {"number": 42, "action": ["opened", "closed", "opened"]}},
                SerializedWebhookMatch(
                    webhook="github", match=(("action", ("closed", "opened")), ("number", ("42",)))
                ),
                id="match-keys-sorted-values-as-sorted-strings",
            ),
            pytest.param(
                {"match": {"action": "closed"}},
                SerializedWebhookMatch(webhook="github", match=(("action", ("closed",)),)),
                id="a-value-is-a-list-of-one",
            ),
            pytest.param({"match": None}, SerializedWebhookMatch(webhook="github"), id="no-match"),
            pytest.param(
                {"correlation_key_conf": "pr"},
                SerializedWebhookMatch(webhook="github", correlation_key_conf="pr"),
                id="conf-key",
            ),
        ],
    )
    def test_build_normalizes_to_the_canonical_form(self, kwargs, expected):
        assert SerializedWebhookMatch.build(webhook="github", **kwargs) == expected

    @pytest.mark.parametrize(
        "kwargs",
        [
            pytest.param({"webhook": "GitHub"}, id="webhook-uppercase"),
            pytest.param({"webhook": ""}, id="webhook-empty"),
            pytest.param({"webhook": "a" * 65}, id="webhook-too-long"),
            pytest.param({"webhook": 5}, id="webhook-not-a-string"),
            pytest.param({"correlation_key_conf": ""}, id="conf-key-empty"),
            pytest.param({"correlation_key_conf": "  "}, id="conf-key-blank"),
            pytest.param({"correlation_key_conf": "k" * 251}, id="conf-key-too-long"),
            pytest.param({"correlation_key_conf": 5}, id="conf-key-not-a-string"),
            pytest.param({"events": []}, id="events-empty"),
            pytest.param({"match": {"a": {"b": 1}}}, id="match-operator"),
            pytest.param({"match": {"a": []}}, id="match-empty-list"),
        ],
    )
    def test_build_refuses_a_condition_outside_the_grammar(self, kwargs):
        with pytest.raises(ValueError, match="."):
            SerializedWebhookMatch.build(**{"webhook": "github", **kwargs})

    def test_build_accepts_the_longest_conf_key(self):
        assert SerializedWebhookMatch.build(webhook="github", correlation_key_conf="k" * 250)

    @pytest.mark.parametrize(
        ("match", "expected"),
        [
            pytest.param(SerializedWebhookMatch(webhook="github"), {"webhook": "github"}, id="only-webhook"),
            pytest.param(
                SerializedWebhookMatch.build(
                    webhook="github",
                    events=["pull_request"],
                    match={"action": "closed"},
                    correlation_key_conf="pr",
                ),
                {
                    "webhook": "github",
                    "events": ["pull_request"],
                    "match": {"action": ["closed"]},
                    "correlation_key_conf": "pr",
                },
                id="every-field",
            ),
        ],
    )
    def test_to_json_omits_unset_fields(self, match, expected):
        assert match.to_json() == expected

    def test_digest_ignores_how_a_condition_is_written(self):
        written = SerializedWebhookMatch.build(
            webhook="github", events=["push", "pull_request"], match={"number": 42, "action": "closed"}
        )
        rewritten = SerializedWebhookMatch.build(
            webhook="github",
            events=["pull_request", "push", "push"],
            match={"action": ["closed"], "number": "42"},
        )
        by_conf = SerializedWebhookMatch.build(
            webhook="github",
            events=["pull_request", "push"],
            match={"action": "closed", "number": 42},
            correlation_key_conf="pr",
        )

        assert written.digest == rewritten.digest
        assert by_conf.digest != written.digest
        assert len(written.digest) == 64
