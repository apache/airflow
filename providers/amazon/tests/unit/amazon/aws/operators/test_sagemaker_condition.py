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

from unittest.mock import MagicMock

import pytest

from airflow.providers.amazon.aws.operators.sagemaker import SageMakerConditionOperator
from airflow.providers.common.compat.sdk import AirflowFailException

from unit.amazon.aws.utils.test_template_fields import validate_template_fields


def _choose(conditions, if_ids="if_task", else_ids="else_task"):
    """Instantiate with conditions list and call choose_branch."""
    op = SageMakerConditionOperator(
        task_id="test",
        conditions=conditions,
        if_task_ids=if_ids,
        else_task_ids=else_ids,
    )
    return op.choose_branch(context=MagicMock(spec=dict))


def _choose_flat(condition_type, left, right, if_ids="if_task", else_ids="else_task"):
    """Instantiate with flat params and call choose_branch."""
    op = SageMakerConditionOperator(
        task_id="test",
        condition_type=condition_type,
        left_value=left,
        right_value=right,
        if_task_ids=if_ids,
        else_task_ids=else_ids,
    )
    return op.choose_branch(context=MagicMock(spec=dict))


def test_template_fields():
    op = SageMakerConditionOperator(
        task_id="test",
        conditions=[{"type": "Equals", "left_value": 1, "right_value": 1}],
        if_task_ids=["a"],
        else_task_ids=["b"],
    )
    validate_template_fields(op)


class TestConditionTypes:
    """One true + one false per condition type, plus logical combinators."""

    @pytest.mark.parametrize(
        ("cond_type", "left", "right", "expected"),
        [
            ("Equals", 1, 1, "if"),
            ("Equals", 1, 2, "else"),
            ("GreaterThan", 5, 3, "if"),
            ("GreaterThan", 3, 5, "else"),
            ("GreaterThanOrEqualTo", 3, 3, "if"),
            ("GreaterThanOrEqualTo", 2, 3, "else"),
            ("LessThan", 3, 5, "if"),
            ("LessThan", 5, 3, "else"),
            ("LessThanOrEqualTo", 3, 3, "if"),
            ("LessThanOrEqualTo", 5, 3, "else"),
        ],
    )
    def test_comparison(self, cond_type, left, right, expected):
        result = _choose([{"type": cond_type, "left_value": left, "right_value": right}])
        assert result == (["if_task"] if expected == "if" else ["else_task"])

    def test_in_true(self):
        assert _choose([{"type": "In", "value": 1, "in_values": [1, 2]}]) == ["if_task"]

    def test_in_false(self):
        assert _choose([{"type": "In", "value": 4, "in_values": [1, 2]}]) == ["else_task"]

    def test_not_negates(self):
        cond = [{"type": "Not", "condition": {"type": "Equals", "left_value": 1, "right_value": 1}}]
        assert _choose(cond) == ["else_task"]

    def test_or_any_true(self):
        cond = [
            {
                "type": "Or",
                "conditions": [
                    {"type": "Equals", "left_value": 1, "right_value": 2},
                    {"type": "Equals", "left_value": 1, "right_value": 1},
                ],
            }
        ]
        assert _choose(cond) == ["if_task"]


class TestAndSemantics:
    def test_multiple_conditions_and(self):
        """All true -> if, one false -> else."""
        assert _choose(
            [
                {"type": "Equals", "left_value": 1, "right_value": 1},
                {"type": "GreaterThan", "left_value": 5, "right_value": 3},
            ]
        ) == ["if_task"]
        assert _choose(
            [
                {"type": "Equals", "left_value": 1, "right_value": 1},
                {"type": "GreaterThan", "left_value": 2, "right_value": 10},
            ]
        ) == ["else_task"]


class TestValueCasting:
    def test_cast_numeric_and_passthrough(self):
        """int string, float string, bool string, non-numeric string, non-string type."""
        assert SageMakerConditionOperator._cast("42") == 42
        assert SageMakerConditionOperator._cast("0.9") == 0.9
        assert SageMakerConditionOperator._cast("true") is True
        assert SageMakerConditionOperator._cast("us-east-1") == "us-east-1"
        assert SageMakerConditionOperator._cast(42) == 42


class TestValidation:
    def test_empty_conditions_raises(self):
        with pytest.raises(ValueError, match="At least 1 condition is required"):
            SageMakerConditionOperator(task_id="t", conditions=[], if_task_ids="a", else_task_ids="b")

    def test_unknown_type_raises(self):
        with pytest.raises(ValueError, match="Unknown condition type"):
            _choose([{"type": "FooBar", "left_value": 1, "right_value": 2}])

    def test_type_mismatch_raises(self):
        with pytest.raises(TypeError, match="Cannot compare"):
            _choose([{"type": "GreaterThanOrEqualTo", "left_value": "hello", "right_value": 0.9}])

    def test_none_operand_raises(self):
        """Covers both Python None and Jinja-rendered string 'None'."""
        with pytest.raises(TypeError, match="received None"):
            _choose([{"type": "Equals", "left_value": None, "right_value": 1}])
        with pytest.raises(TypeError, match="received None"):
            _choose([{"type": "GreaterThanOrEqualTo", "left_value": "None", "right_value": 0.9}])

    @pytest.mark.parametrize(
        ("condition", "match_pattern"),
        [
            ({}, "missing required key 'type'"),
            ({"type": "Equals", "left_value": 1}, "missing required key"),
            ({"type": "Not"}, "missing required key"),
            ({"type": "Or"}, "missing required key"),
        ],
    )
    def test_missing_key_raises(self, condition, match_pattern):
        with pytest.raises(ValueError, match=match_pattern):
            _choose([condition])


class TestFlatInterface:
    def test_flat_condition(self):
        """Flat params work for comparison and In types."""
        assert _choose_flat("Equals", 1, 1) == ["if_task"]
        assert _choose_flat("Equals", 1, 2) == ["else_task"]
        assert _choose_flat("In", 1, [1, 2, 3]) == ["if_task"]
        assert _choose_flat("In", 99, [1, 2, 3]) == ["else_task"]

    def test_invalid_condition_type_raises(self):
        with pytest.raises(ValueError, match="Unknown condition_type"):
            SageMakerConditionOperator(
                task_id="t",
                condition_type="NotEquals",
                left_value=1,
                right_value=1,
                if_task_ids="a",
                else_task_ids="b",
            )

    def test_mutual_exclusion_raises(self):
        with pytest.raises(ValueError, match="Cannot use 'condition_type' and 'conditions' together"):
            SageMakerConditionOperator(
                task_id="t",
                condition_type="Equals",
                left_value=1,
                right_value=1,
                conditions=[{"type": "Equals", "left_value": 1, "right_value": 1}],
                if_task_ids="a",
                else_task_ids="b",
            )

    def test_neither_provided_raises(self):
        with pytest.raises(ValueError, match="Missing condition"):
            SageMakerConditionOperator(task_id="t", if_task_ids="a", else_task_ids="b")

    def test_optional_else_task_ids(self):
        """else_task_ids defaults to empty list when omitted."""
        op = SageMakerConditionOperator(
            task_id="t",
            conditions=[{"type": "Equals", "left_value": 1, "right_value": 1}],
            if_task_ids=["deploy"],
        )
        assert op.else_task_ids == []

    def test_no_else_branch_raises_on_false(self):
        """When else_task_ids is empty and conditions are false, raises AirflowFailException."""
        op = SageMakerConditionOperator(
            task_id="check",
            conditions=[{"type": "Equals", "left_value": 1, "right_value": 2}],
            if_task_ids=["deploy"],
        )
        with pytest.raises(AirflowFailException, match="Condition check failed in task 'check'"):
            op.choose_branch(context=MagicMock(spec=dict))
