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

import dataclasses

import pytest

from airflow.providers.common.ai.policies.decision import BranchOption, DecisionPolicy, check_bar
from airflow.providers.common.ai.utils.decision import validate_decision_policy


class TestDecisionPolicy:
    def test_defaults_are_no_gate_and_review(self):
        policy = DecisionPolicy()
        assert policy.min_confidence is None
        assert policy.on_uncertain == "review"

    @pytest.mark.parametrize(("value", "expected"), [(0.7, 0.7), (1, 1.0), (0, 0.0)])
    def test_min_confidence_is_normalised_to_a_float(self, value, expected):
        assert DecisionPolicy(min_confidence=value).min_confidence == expected

    @pytest.mark.parametrize("value", [1.5, -0.1])
    def test_min_confidence_out_of_range(self, value):
        with pytest.raises(ValueError, match="between 0 and 1"):
            DecisionPolicy(min_confidence=value)

    @pytest.mark.parametrize("value", ["0.7", True, {"a": 0.5}])
    def test_min_confidence_wrong_type(self, value):
        with pytest.raises(TypeError, match="must be a number"):
            DecisionPolicy(min_confidence=value)

    def test_on_uncertain_is_validated(self):
        assert DecisionPolicy(on_uncertain="fail").on_uncertain == "fail"
        with pytest.raises(ValueError, match="on_uncertain must be one of"):
            DecisionPolicy(on_uncertain="proceed")

    def test_is_frozen(self):
        with pytest.raises(AttributeError):
            DecisionPolicy().min_confidence = 0.5  # type: ignore[misc]

    def test_validate_accepts_none_and_a_policy_only(self):
        assert validate_decision_policy(None) == DecisionPolicy()
        policy = DecisionPolicy(min_confidence=0.6)
        assert validate_decision_policy(policy) is policy
        with pytest.raises(TypeError, match="decision_policy must be a DecisionPolicy"):
            validate_decision_policy({"min_confidence": 0.6})


class TestBranchOption:
    def test_string_shorthand_and_defaults(self):
        option = BranchOption("Owner: {{ ds }}")
        assert option.description == "Owner: {{ ds }}"
        assert option.min_confidence is None

    def test_bar_is_normalised_and_validated(self):
        assert BranchOption("x", min_confidence=1).min_confidence == 1.0
        with pytest.raises(ValueError, match="BranchOption.min_confidence must be between 0 and 1"):
            BranchOption("x", min_confidence=1.5)

    def test_template_fields_is_a_class_attribute_not_a_field(self):
        """The templater reads ``template_fields`` off the instance; a dataclass field there would be a constructor argument."""
        assert BranchOption.template_fields == ("description",)
        assert "template_fields" not in {f.name for f in dataclasses.fields(BranchOption)}
        assert BranchOption("a") == BranchOption("a")


class TestCheckBar:
    @pytest.mark.parametrize(("value", "expected"), [(0, 0.0), (1, 1.0), (0.25, 0.25)])
    def test_accepts_numbers_in_range(self, value, expected):
        assert check_bar(value, "x") == expected

    @pytest.mark.parametrize("value", [True, "0.5", None])
    def test_rejects_non_numbers(self, value):
        with pytest.raises(TypeError, match="must be a number"):
            check_bar(value, "x")
