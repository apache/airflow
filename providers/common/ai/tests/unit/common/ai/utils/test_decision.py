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

from unittest.mock import MagicMock

import pytest
from pydantic_ai.messages import ModelResponse

from airflow.providers.common.ai.utils.decision import (
    MissingConfidenceError,
    ModelConfidence,
    describe_confidence,
    review_reason,
    threshold_for,
    validate_missing_confidence_policy,
    validate_review_below,
)


def _result(provider_details=None, model_name="jev-1.13.0"):
    response = ModelResponse(parts=[], model_name=model_name, provider_details=provider_details)
    result = MagicMock(spec=["response", "output"])
    result.response = response
    return result


class TestValidation:
    @pytest.mark.parametrize(("value", "expected"), [(0.7, 0.7), (1, 1.0), (0, 0.0), (None, None)])
    def test_review_below_number(self, value, expected):
        assert validate_review_below(value) == expected

    def test_review_below_mapping_is_normalised_to_floats(self):
        assert validate_review_below({"permanent": 0.85, "transient": 1}) == {
            "permanent": 0.85,
            "transient": 1.0,
        }

    @pytest.mark.parametrize("value", [1.5, -0.1, {"a": 2}])
    def test_review_below_out_of_range(self, value):
        with pytest.raises(ValueError, match="between 0 and 1"):
            validate_review_below(value)

    @pytest.mark.parametrize("value", ["0.7", True, {"a": "high"}])
    def test_review_below_wrong_type(self, value):
        with pytest.raises(TypeError, match="must be a number"):
            validate_review_below(value)

    def test_empty_mapping_is_rejected(self):
        with pytest.raises(ValueError, match="at least one option"):
            validate_review_below({})

    def test_missing_confidence_policy(self):
        assert validate_missing_confidence_policy("proceed") == "proceed"
        with pytest.raises(ValueError, match="on_missing_confidence must be one of"):
            validate_missing_confidence_policy("ignore")


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
    def test_number_applies_to_everything(self):
        assert threshold_for(0.7, ["anything"]) == 0.7

    def test_mapping_uses_the_picked_branch(self):
        bars = {"page_oncall": 0.85, "rerun": 0.6}
        assert threshold_for(bars, ["rerun"]) == 0.6
        assert threshold_for(bars, ["page_oncall"]) == 0.85

    def test_mapping_uses_the_strictest_of_several_picks(self):
        assert threshold_for({"a": 0.5, "b": 0.9}, ["a", "b"]) == 0.9

    def test_mapping_without_the_picked_branch_means_no_bar(self):
        assert threshold_for({"page_oncall": 0.85}, ["ignore"]) is None

    def test_none_means_no_bar(self):
        assert threshold_for(None, ["a"]) is None


class TestReviewReason:
    def test_require_approval_wins_over_everything(self):
        assert (
            review_reason(
                require_approval=True,
                threshold=None,
                confidence=0.99,
                on_missing_confidence="proceed",
                what="t",
            )
            == "require_approval"
        )

    def test_no_bar_means_no_review(self):
        assert (
            review_reason(
                require_approval=False,
                threshold=None,
                confidence=None,
                on_missing_confidence="review",
                what="t",
            )
            is None
        )

    @pytest.mark.parametrize(
        ("confidence", "expected"), [(0.69, "below_threshold"), (0.7, None), (0.71, None)]
    )
    def test_boundary_is_inclusive_at_the_bar(self, confidence, expected):
        assert (
            review_reason(
                require_approval=False,
                threshold=0.7,
                confidence=confidence,
                on_missing_confidence="review",
                what="t",
            )
            == expected
        )

    def test_missing_confidence_reviews_by_default(self):
        assert (
            review_reason(
                require_approval=False,
                threshold=0.7,
                confidence=None,
                on_missing_confidence="review",
                what="t",
            )
            == "missing_confidence"
        )

    def test_missing_confidence_can_proceed(self):
        assert (
            review_reason(
                require_approval=False,
                threshold=0.7,
                confidence=None,
                on_missing_confidence="proceed",
                what="t",
            )
            is None
        )

    def test_missing_confidence_can_fail(self):
        with pytest.raises(MissingConfidenceError, match="reported no confidence"):
            review_reason(
                require_approval=False, threshold=0.7, confidence=None, on_missing_confidence="fail", what="t"
            )


class TestDescribeConfidence:
    def test_lists_probabilities_ranked(self):
        mc = ModelConfidence(
            confidence={"response": 0.52},
            probabilities={"response": {"handle_auth": 0.46, "handle_billing": 0.02, "page_oncall": 0.52}},
        )
        text = describe_confidence(mc, "response", 0.7)
        assert text.startswith("Confidence: 0.52 (review below 0.70)")
        assert "page_oncall 0.52, handle_auth 0.46, handle_billing 0.02" in text

    def test_says_when_nothing_was_reported(self):
        assert (
            describe_confidence(ModelConfidence(), "response", 0.7)
            == "Confidence: not reported by the model (review below 0.70)"
        )
