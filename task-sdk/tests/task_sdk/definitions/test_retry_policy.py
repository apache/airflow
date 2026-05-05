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
from unittest.mock import MagicMock

import pytest
import structlog

from airflow.sdk.api.datamodels._generated import TaskInstanceState
from airflow.sdk.definitions.retry_policy import (
    ExceptionRetryPolicy,
    RetryAction,
    RetryDecision,
    RetryPolicy,
    RetryRule,
)
from airflow.sdk.execution_time.task_runner import (
    _apply_retry_policy_or_default,
    _evaluate_retry_policy,
)

log = structlog.get_logger("test")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_mock_ti(policy=None, try_number=1, max_tries=3, should_retry=True, ti_context=True):
    """Create a mock RuntimeTaskInstance for retry policy tests."""
    ti = MagicMock()
    ti.task.retry_policy = policy
    ti.try_number = try_number
    ti.rendered_map_index = None
    ti.dag_id = "test_dag"
    ti.task_id = "test_task"
    ti.task.__class__.__name__ = "PythonOperator"
    if ti_context:
        ti._ti_context_from_server = MagicMock()
        ti._ti_context_from_server.max_tries = max_tries
        ti._ti_context_from_server.should_retry = should_retry
    else:
        ti._ti_context_from_server = None
    return ti


# ---------------------------------------------------------------------------
# RetryRule matching behaviour
# ---------------------------------------------------------------------------


class TestRetryRuleMatching:
    def test_matches_by_class(self):
        rule = RetryRule(exception=ValueError)
        assert rule.matches(ValueError("test"))
        assert not rule.matches(TypeError("test"))

    def test_matches_subclass_by_default(self):
        """OSError rule matches ConnectionError (a subclass)."""
        rule = RetryRule(exception=OSError)
        assert rule.matches(ConnectionError("test"))

    def test_exact_match_rejects_subclass(self):
        rule = RetryRule(exception=OSError, match_subclasses=False)
        assert rule.matches(OSError("test"))
        assert not rule.matches(ConnectionError("test"))

    def test_matches_by_dotted_string_path(self):
        rule = RetryRule(exception="builtins.ValueError")
        assert rule.matches(ValueError("test"))
        assert not rule.matches(TypeError("test"))

    def test_unresolvable_string_does_not_match(self):
        rule = RetryRule(exception="nonexistent.module.SomeError")
        assert not rule.matches(ValueError("test"))

    def test_invalid_string_without_dot_raises(self):
        with pytest.raises(ValueError, match="dotted import path"):
            RetryRule(exception="ValueError")

    def test_list_of_exceptions_matches_any(self):
        """A rule with a list of exceptions matches if any entry matches."""
        rule = RetryRule(exception=[ConnectionError, TimeoutError], action=RetryAction.RETRY)
        assert rule.matches(ConnectionError("refused"))
        assert rule.matches(TimeoutError("timed out"))
        assert not rule.matches(ValueError("bad"))

    def test_list_with_string_paths(self):
        rule = RetryRule(
            exception=["builtins.ValueError", "builtins.TypeError"],
            action=RetryAction.FAIL,
        )
        assert rule.matches(ValueError("x"))
        assert rule.matches(TypeError("x"))
        assert not rule.matches(ConnectionError("x"))

    def test_list_serialization_roundtrip(self):
        rule = RetryRule(
            exception=[ConnectionError, TimeoutError],
            action=RetryAction.RETRY,
            retry_delay=timedelta(seconds=10),
        )
        data = rule.serialize()
        assert isinstance(data["exception"], list)
        assert len(data["exception"]) == 2

        restored = RetryRule.deserialize(data)
        assert restored.matches(ConnectionError("x"))
        assert restored.matches(TimeoutError("x"))
        assert not restored.matches(ValueError("x"))

    def test_invalid_string_in_list_raises(self):
        with pytest.raises(ValueError, match="dotted import path"):
            RetryRule(exception=[ConnectionError, "ValueError"])

    def test_resolved_classes_are_cached(self):
        """Second call to matches() should not re-import."""
        rule = RetryRule(exception="builtins.ValueError")
        rule.matches(ValueError("first"))
        assert rule._resolved_classes is not object
        assert ValueError in rule._resolved_classes

    def test_failed_resolution_is_cached_as_none(self):
        rule = RetryRule(exception="nonexistent.module.Error")
        rule.matches(ValueError("x"))
        assert rule._resolved_classes is None
        assert not rule.matches(ValueError("x"))


class TestRetryRuleSerialization:
    def test_roundtrip_preserves_behaviour(self):
        rule = RetryRule(
            exception=ValueError,
            action=RetryAction.FAIL,
            retry_delay=timedelta(seconds=30),
            reason="bad data",
            match_subclasses=False,
        )
        restored = RetryRule.deserialize(rule.serialize())
        # Restored rule should match the same exceptions
        assert restored.matches(ValueError("test"))
        assert not restored.matches(TypeError("test"))
        assert restored.action == RetryAction.FAIL
        assert restored.retry_delay == timedelta(seconds=30)

    def test_minimal_roundtrip(self):
        rule = RetryRule(exception=ValueError)
        data = rule.serialize()
        assert "retry_delay" not in data
        assert "reason" not in data


# ---------------------------------------------------------------------------
# ExceptionRetryPolicy evaluation
# ---------------------------------------------------------------------------


class TestExceptionRetryPolicy:
    def test_first_matching_rule_wins(self):
        policy = ExceptionRetryPolicy(
            rules=[
                RetryRule(exception=ValueError, action=RetryAction.FAIL, reason="bad value"),
                RetryRule(exception=Exception, action=RetryAction.RETRY, reason="generic"),
            ]
        )
        decision = policy.evaluate(ValueError("test"), try_number=1, max_tries=3)
        assert decision.action == RetryAction.FAIL
        assert decision.reason == "bad value"

    def test_no_match_returns_configured_default(self):
        policy = ExceptionRetryPolicy(
            rules=[RetryRule(exception=ValueError, action=RetryAction.FAIL)],
            default=RetryAction.RETRY,
        )
        decision = policy.evaluate(TypeError("test"), try_number=1, max_tries=3)
        assert decision.action == RetryAction.RETRY

    def test_empty_rules_returns_default(self):
        policy = ExceptionRetryPolicy(rules=[])
        decision = policy.evaluate(ValueError("test"), try_number=1, max_tries=3)
        assert decision.action == RetryAction.DEFAULT

    def test_custom_delay_on_rule(self):
        policy = ExceptionRetryPolicy(
            rules=[
                RetryRule(
                    exception=ConnectionError, action=RetryAction.RETRY, retry_delay=timedelta(seconds=10)
                ),
            ]
        )
        decision = policy.evaluate(ConnectionError("test"), try_number=1, max_tries=3)
        assert decision.retry_delay == timedelta(seconds=10)

    def test_serialization_roundtrip_preserves_evaluate_behaviour(self):
        policy = ExceptionRetryPolicy(
            rules=[
                RetryRule(exception=ValueError, action=RetryAction.FAIL, reason="bad data"),
                RetryRule(
                    exception=ConnectionError, action=RetryAction.RETRY, retry_delay=timedelta(seconds=30)
                ),
            ]
        )
        restored = ExceptionRetryPolicy.deserialize(policy.serialize())
        assert restored.evaluate(ValueError("x"), 1, 3).action == RetryAction.FAIL
        assert restored.evaluate(ConnectionError("x"), 1, 3).action == RetryAction.RETRY
        assert restored.evaluate(TypeError("x"), 1, 3).action == RetryAction.DEFAULT


# ---------------------------------------------------------------------------
# Custom RetryPolicy subclass
# ---------------------------------------------------------------------------


class TestCustomRetryPolicy:
    def test_context_aware_policy(self):
        """Policy can use context to make decisions."""

        class ContextPolicy(RetryPolicy):
            def evaluate(self, exception, try_number, max_tries, context=None):
                if context and context.get("params", {}).get("skip_retries"):
                    return RetryDecision.fail(reason="skip_retries param set")
                return RetryDecision.default()

        policy = ContextPolicy()
        assert policy.evaluate(ValueError("x"), 1, 3).action == RetryAction.DEFAULT
        assert (
            policy.evaluate(ValueError("x"), 1, 3, context={"params": {"skip_retries": True}}).action
            == RetryAction.FAIL
        )


# ---------------------------------------------------------------------------
# MappedOperator carries retry_policy
# ---------------------------------------------------------------------------


class TestMappedOperatorRetryPolicy:
    def test_partial_expand_carries_policy(self):
        from airflow.sdk import DAG, BaseOperator
        from airflow.sdk.definitions.mappedoperator import MappedOperator

        policy = ExceptionRetryPolicy(rules=[RetryRule(exception=ValueError, action=RetryAction.FAIL)])
        with DAG(dag_id="test_mapped", schedule=None):
            op = BaseOperator.partial(task_id="mapped", retry_policy=policy).expand(
                params=[{"a": 1}, {"a": 2}]
            )
        assert isinstance(op, MappedOperator)
        assert op.retry_policy is policy


# ---------------------------------------------------------------------------
# _evaluate_retry_policy (task_runner.py)
# ---------------------------------------------------------------------------


class TestEvaluateRetryPolicy:
    def test_no_policy_returns_none(self):
        ti = _make_mock_ti(policy=None)
        assert _evaluate_retry_policy(ti, ValueError("x"), log) is None

    def test_fail_decision(self):
        policy = ExceptionRetryPolicy(
            rules=[RetryRule(exception=PermissionError, action=RetryAction.FAIL, reason="auth")]
        )
        ti = _make_mock_ti(policy=policy)
        result = _evaluate_retry_policy(ti, PermissionError("403"), log)
        assert result.action == RetryAction.FAIL

    def test_retry_decision_with_delay(self):
        policy = ExceptionRetryPolicy(
            rules=[
                RetryRule(
                    exception=ConnectionError, action=RetryAction.RETRY, retry_delay=timedelta(seconds=5)
                ),
            ]
        )
        ti = _make_mock_ti(policy=policy)
        result = _evaluate_retry_policy(ti, ConnectionError("refused"), log)
        assert result.action == RetryAction.RETRY
        assert result.retry_delay == timedelta(seconds=5)

    def test_broken_policy_returns_none(self):
        """If policy.evaluate() raises, fallback to None (standard retry)."""

        class BrokenPolicy(RetryPolicy):
            def evaluate(self, exception, try_number, max_tries, context=None):
                raise RuntimeError("policy crashed")

        ti = _make_mock_ti(policy=BrokenPolicy())
        assert _evaluate_retry_policy(ti, ValueError("x"), log) is None

    def test_context_forwarded_to_policy(self):
        received = {}

        class CapturePolicy(RetryPolicy):
            def evaluate(self, exception, try_number, max_tries, context=None):
                received["ctx"] = context
                return RetryDecision.default()

        ti = _make_mock_ti(policy=CapturePolicy())
        ctx = {"params": {"key": "val"}}
        _evaluate_retry_policy(ti, ValueError("x"), log, context=ctx)
        assert received["ctx"] is ctx

    def test_ti_context_from_server_none_uses_zero_max_tries(self):
        """When _ti_context_from_server is None, max_tries defaults to 0."""
        received = {}

        class CaptureMaxTries(RetryPolicy):
            def evaluate(self, exception, try_number, max_tries, context=None):
                received["max_tries"] = max_tries
                return RetryDecision.default()

        ti = _make_mock_ti(policy=CaptureMaxTries(), ti_context=False)
        _evaluate_retry_policy(ti, ValueError("x"), log)
        assert received["max_tries"] == 0


# ---------------------------------------------------------------------------
# _apply_retry_policy_or_default (task_runner.py)
# ---------------------------------------------------------------------------


class TestApplyRetryPolicyOrDefault:
    def test_fail_bypasses_retry_count(self):
        """Policy FAIL overrides should_retry=True -- task fails immediately."""
        policy = ExceptionRetryPolicy(rules=[RetryRule(exception=PermissionError, action=RetryAction.FAIL)])
        ti = _make_mock_ti(policy=policy, should_retry=True)
        msg, state = _apply_retry_policy_or_default(ti, PermissionError("denied"), log)
        assert state == TaskInstanceState.FAILED

    def test_retry_with_delay_and_reason(self):
        policy = ExceptionRetryPolicy(
            rules=[
                RetryRule(
                    exception=ConnectionError,
                    action=RetryAction.RETRY,
                    retry_delay=timedelta(seconds=42),
                    reason="net error",
                ),
            ]
        )
        ti = _make_mock_ti(policy=policy)
        msg, state = _apply_retry_policy_or_default(ti, ConnectionError("refused"), log)
        assert state == TaskInstanceState.UP_FOR_RETRY
        assert msg.retry_delay_seconds == 42.0
        assert msg.retry_reason == "net error"

    def test_retry_reason_truncated_to_500_chars(self):
        """Long reasons are truncated to fit the DB column."""
        long_reason = "x" * 1000
        policy = ExceptionRetryPolicy(
            rules=[
                RetryRule(exception=ValueError, action=RetryAction.RETRY, reason=long_reason),
            ]
        )
        ti = _make_mock_ti(policy=policy)
        msg, state = _apply_retry_policy_or_default(ti, ValueError("bad"), log)
        assert state == TaskInstanceState.UP_FOR_RETRY
        assert len(msg.retry_reason) == 500

    def test_default_falls_through_to_standard_retry(self):
        policy = ExceptionRetryPolicy(rules=[])  # no match -> DEFAULT
        ti = _make_mock_ti(policy=policy, should_retry=True)
        msg, state = _apply_retry_policy_or_default(ti, ValueError("bad"), log)
        assert state == TaskInstanceState.UP_FOR_RETRY
        assert msg.retry_delay_seconds is None  # no override

    def test_retry_when_retries_exhausted_still_fails(self):
        """Policy says RETRY but should_retry=False (retries used up) -> FAILED."""
        policy = ExceptionRetryPolicy(
            rules=[
                RetryRule(exception=ConnectionError, action=RetryAction.RETRY),
            ]
        )
        ti = _make_mock_ti(policy=policy, should_retry=False)
        msg, state = _apply_retry_policy_or_default(ti, ConnectionError("x"), log)
        assert state == TaskInstanceState.FAILED

    def test_no_policy_with_should_retry_true(self):
        ti = _make_mock_ti(policy=None, should_retry=True)
        msg, state = _apply_retry_policy_or_default(ti, ValueError("bad"), log)
        assert state == TaskInstanceState.UP_FOR_RETRY

    def test_no_policy_with_should_retry_false(self):
        ti = _make_mock_ti(policy=None, should_retry=False)
        msg, state = _apply_retry_policy_or_default(ti, ValueError("bad"), log)
        assert state == TaskInstanceState.FAILED
