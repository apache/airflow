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

from dataclasses import dataclass
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from pydantic import TypeAdapter
from pydantic_ai.messages import ModelResponse

from airflow.providers.common.ai.exceptions import LowConfidenceError as ExportedLowConfidenceError
from airflow.providers.common.ai.policies.decision import (
    BranchOption,
    DecisionPolicy as ExportedDecisionPolicy,
)
from airflow.providers.common.ai.utils.decision import (
    DecisionPolicy,
    LowConfidenceError,
    ModelConfidence,
    check_uncertain_action,
    decision_record,
    describe_confidence,
    described_choices,
    finalize_record,
    initial_decided_by,
    picked_key,
    policy_fails,
    policy_record,
    review_reason,
    threshold_for,
    timed_out_record,
)


def _result(provider_details=None, model_name="jev-1.13.0"):
    response = ModelResponse(parts=[], model_name=model_name, provider_details=provider_details)
    result = MagicMock(spec=["response", "output"])
    result.response = response
    return result


class TestModelConfidence:
    def test_reads_confidence_probabilities_and_model(self):
        details = {
            "confidence": {"response": 0.52},
            "probabilities": {"response": {"handle_auth": 0.52, "handle_billing": 0.48}},
            "scores": {},
        }
        mc = ModelConfidence.from_result(_result(details))
        assert mc.model == "jev-1.13.0"
        assert mc.confidence == {"response": 0.52}
        assert mc.probabilities == {"response": {"handle_auth": 0.52, "handle_billing": 0.48}}

    def test_text_model_reports_nothing(self):
        mc = ModelConfidence.from_result(_result(None, model_name="claude-sonnet-5"))
        assert mc.model == "claude-sonnet-5"
        assert mc.confidence == {}
        assert mc.probabilities == {}
        assert mc.lowest() is None

    def test_non_finite_confidence_is_not_reported(self):
        details = {"confidence": {"a": float("nan"), "b": float("inf"), "c": 0.5}, "probabilities": {}}
        assert ModelConfidence.from_result(_result(details)).confidence == {"c": 0.5}

    def test_mock_result_without_details_is_empty(self):
        """The test fixtures elsewhere use MagicMock results; a MagicMock provider_details is not a mapping."""
        result = MagicMock(spec=["response", "output"])
        result.response = MagicMock(spec=["model_name", "provider_details"], model_name="test-model")
        mc = ModelConfidence.from_result(result)
        assert mc.model == "test-model"
        assert mc.confidence == {}

    def test_lowest_over_selected_fields(self):
        mc = ModelConfidence(confidence={"category": 0.9, "severity": 0.4, "urgent": 0.7})
        assert mc.lowest() == 0.4
        assert mc.lowest(["category", "urgent"]) == 0.7
        assert mc.lowest(["absent"]) is None


class TestThresholdFor:
    def test_policy_bar_applies_to_everything(self):
        assert threshold_for(0.7, None, ["anything"]) == 0.7

    def test_branch_bar_applies_to_the_picked_branch(self):
        bars = {"page_oncall": 0.85}
        assert threshold_for(0.6, bars, ["rerun"]) == 0.6
        assert threshold_for(0.6, bars, ["page_oncall"]) == 0.85

    def test_several_picks_use_the_strictest(self):
        assert threshold_for(0.5, {"b": 0.9}, ["a", "b"]) == 0.9
        assert threshold_for(0.5, {"b": 0.3}, ["a", "b"]) == 0.5

    def test_unlisted_pick_inherits_the_policy_bar(self):
        assert threshold_for(0.6, {"page_oncall": 0.85}, ["ignore"]) == 0.6

    def test_no_policy_bar_means_no_bar(self):
        assert threshold_for(None, None, ["a"]) is None
        assert threshold_for(None, {"a": 0.9}, ["a"]) is None

    def test_no_picks_means_the_policy_bar(self):
        assert threshold_for(0.6, {"a": 0.9}, []) == 0.6


class TestReviewReason:
    def test_require_approval_wins_over_everything(self):
        assert review_reason(require_approval=True, threshold=None, confidence=0.99) == "require_approval"

    def test_no_bar_means_no_review(self):
        assert review_reason(require_approval=False, threshold=None, confidence=None) is None

    @pytest.mark.parametrize(
        ("confidence", "expected"), [(0.69, "below_threshold"), (0.7, None), (0.71, None)]
    )
    def test_boundary_is_inclusive_at_the_bar(self, confidence, expected):
        assert review_reason(require_approval=False, threshold=0.7, confidence=confidence) == expected

    def test_missing_confidence_counts_as_uncertain(self):
        assert review_reason(require_approval=False, threshold=0.7, confidence=None) == "missing_confidence"


class TestPolicyFails:
    @pytest.mark.parametrize(
        ("review", "action", "expected"),
        [
            (None, "fail", False),
            ("require_approval", "fail", False),
            ("below_threshold", "review", False),
            ("below_threshold", "fail", True),
            ("missing_confidence", "fail", True),
        ],
    )
    def test_predicate(self, review, action, expected):
        assert policy_fails(review, action) is expected


class TestInitialDecidedBy:
    @pytest.mark.parametrize(
        ("review", "action", "expected"),
        [
            (None, "review", "model"),
            (None, "fail", "model"),
            ("require_approval", "fail", None),
            ("below_threshold", "review", None),
            ("missing_confidence", "review", None),
            ("below_threshold", "fail", "policy"),
            ("missing_confidence", "fail", "policy"),
        ],
    )
    def test_who_decides_at_first_write(self, review, action, expected):
        assert initial_decided_by(review, action) == expected


class TestCheckUncertainAction:
    @pytest.mark.parametrize("review", [None, "require_approval", "below_threshold", "missing_confidence"])
    def test_review_action_never_raises(self, review):
        check_uncertain_action(review, "review", what="t", confidence=0.1, threshold=0.7)

    @pytest.mark.parametrize("review", [None, "require_approval"])
    def test_fail_action_only_fires_under_the_bar(self, review):
        check_uncertain_action(review, "fail", what="t", confidence=0.9, threshold=0.7)

    def test_fail_on_below_threshold(self):
        with pytest.raises(LowConfidenceError, match="Confidence 0.40 for t is below min_confidence=0.70"):
            check_uncertain_action("below_threshold", "fail", what="t", confidence=0.4, threshold=0.7)

    def test_fail_on_missing_confidence(self):
        with pytest.raises(LowConfidenceError, match="reported no confidence"):
            check_uncertain_action("missing_confidence", "fail", what="t", confidence=None, threshold=0.7)


class TestDescribeConfidence:
    def test_lists_probabilities_ranked(self):
        mc = ModelConfidence(
            confidence={"response": 0.52},
            probabilities={"response": {"handle_auth": 0.46, "handle_billing": 0.02, "page_oncall": 0.52}},
        )
        text = describe_confidence(mc, "response", 0.7)
        assert text.startswith("Confidence: 0.52 (minimum 0.70)")
        assert "page_oncall 0.52, handle_auth 0.46, handle_billing 0.02" in text

    def test_says_when_nothing_was_reported(self):
        assert (
            describe_confidence(ModelConfidence(), "response", 0.7)
            == "Confidence: not reported by the model (minimum 0.70)"
        )


class TestRecords:
    def test_policy_record_carries_the_policy_and_the_branch_bars(self):
        assert policy_record(DecisionPolicy(min_confidence=0.6, on_uncertain="fail"), {"rerun": 0.8}) == {
            "min_confidence": 0.6,
            "on_uncertain": "fail",
            "branches": {"rerun": 0.8},
        }
        assert policy_record(DecisionPolicy()) == {
            "min_confidence": None,
            "on_uncertain": "review",
            "branches": None,
        }

    def test_pending_record_carries_the_bar_and_the_policy(self):
        record = decision_record(
            model_confidence=ModelConfidence(model="jev-1.13.0", confidence={"response": 0.5}),
            proposed="rerun",
            action=None,
            threshold=0.6,
            review="below_threshold",
            decided_by=None,
            policy=policy_record(DecisionPolicy(min_confidence=0.6)),
        )
        assert record["min_confidence"] == 0.6
        assert record["policy"]["on_uncertain"] == "review"
        assert record["action"] is None
        assert record["decided_by"] is None

    @pytest.mark.parametrize(
        ("event", "expected"),
        [
            pytest.param({"responded_by_user": {"id": "u1", "name": "Sam"}}, "human", id="responder"),
            pytest.param({"timedout": True, "responded_by_user": None}, "timeout_default", id="timeout"),
            pytest.param({}, "timeout_default", id="no-responder"),
        ],
    )
    def test_finalize_sets_action_and_who_decided(self, event, expected):
        pending = {"proposed": "rerun", "action": None, "decided_by": None, "review": "below_threshold"}

        final = finalize_record(pending, event, action="page_oncall")

        assert final == {**pending, "action": "page_oncall", "decided_by": expected}
        assert pending["action"] is None

    def test_timed_out_record_says_nobody_decided(self):
        pending = {"proposed": "rerun", "action": None, "decided_by": None, "review": "below_threshold"}
        assert timed_out_record(pending) == {**pending, "action": None, "decided_by": "timeout"}

    def test_public_objects_live_in_policies_and_exceptions(self):
        assert ExportedLowConfidenceError is LowConfidenceError
        assert ExportedDecisionPolicy is DecisionPolicy
        assert BranchOption("x").template_fields == ("description",)


@dataclass(frozen=True)
class _FakeChoice:
    """pydantic-ai 2.46's ``Choice(description=None, *, value=UNSET)``, as far as the builder uses it."""

    description: str | None = None


@dataclass(frozen=True)
class _FakeChoices:
    """pydantic-ai 2.46's ``Choices(choices, *, name=None, description=None)``, recording the call shape."""

    choices: Any
    name: str | None = None


class TestDescribedChoices:
    """CI runs on a pydantic-ai without ``Choices``, so the 2.46+ call shape is pinned against a stand-in."""

    @patch("airflow.providers.common.ai.utils.decision.Choices", _FakeChoices)
    @patch("airflow.providers.common.ai.utils.decision.Choice", _FakeChoice)
    def test_choices_gets_the_keys_with_descriptions_in_order(self):
        built = described_choices("Options", {"b": "Second.", "a": None})

        assert built == _FakeChoices({"b": _FakeChoice("Second."), "a": _FakeChoice(None)}, name="Options")

    @patch("airflow.providers.common.ai.utils.decision.Choices", _FakeChoices)
    @patch("airflow.providers.common.ai.utils.decision.Choice", _FakeChoice)
    def test_choices_gets_a_plain_list_when_nothing_is_described(self):
        assert described_choices("Options", {"b": None, "a": None}) == _FakeChoices(
            ["b", "a"], name="Options"
        )

    @patch("airflow.providers.common.ai.utils.decision.Choices", None)
    @patch("airflow.providers.common.ai.utils.decision.Choice", None)
    def test_enum_fallback_keeps_keys_as_values_and_emits_the_described_schema(self):
        built = described_choices(
            "Options", {"_sunder_": "Reserved by Enum.", "mro": None, "plain key": None}
        )

        assert [member.value for member in built] == ["_sunder_", "mro", "plain key"]
        assert picked_key(built("mro")) == "mro"
        assert TypeAdapter(built).json_schema()["anyOf"] == [
            {"const": "_sunder_", "type": "string", "description": "Reserved by Enum."},
            {"const": "mro", "type": "string"},
            {"const": "plain key", "type": "string"},
        ]

    @patch("airflow.providers.common.ai.utils.decision.Choices", None)
    @patch("airflow.providers.common.ai.utils.decision.Choice", None)
    def test_enum_fallback_without_descriptions_is_a_plain_enum(self):
        built = described_choices("Options", {"x": None, "y": None})

        assert TypeAdapter(built).json_schema()["enum"] == ["x", "y"]
        assert picked_key("x") == "x"
