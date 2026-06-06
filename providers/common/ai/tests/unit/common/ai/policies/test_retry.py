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
from unittest.mock import MagicMock, patch

import pytest

# LLMRetryPolicy depends on the RetryPolicy ABC introduced in Airflow 3.3 (AIP-105).
# Skip the entire test module on older Airflow versions tested in compat CI.
pytest.importorskip("airflow.sdk.definitions.retry_policy", reason="RetryPolicy requires Airflow 3.3+")

from airflow.providers.common.ai.hooks.base import AgentRunResult, AgentUsage, BaseAIHook
from airflow.providers.common.ai.policies.retry import (
    ErrorClassification,
    LLMRetryPolicy,
)
from airflow.sdk.definitions.retry_policy import RetryAction, RetryRule


def _make_run_result(output):
    return AgentRunResult(
        output=output,
        model_name="test-model",
        usage=AgentUsage(requests=1),
    )


def _make_mock_hook(run_result):
    mock_hook = MagicMock()
    mock_hook.create_agent.return_value = MagicMock()
    mock_hook.run_agent.return_value = run_result
    return mock_hook


class TestLLMClassifyDecisions:
    """Test that _classify maps LLM classification to correct RetryDecisions."""

    @patch.object(BaseAIHook, "get_agent_hook")
    def test_auth_error_returns_fail(self, mock_get_hook):
        mock_get_hook.return_value = _make_mock_hook(
            _make_run_result(
                ErrorClassification(category="auth", should_retry=False, reasoning="API key expired")
            )
        )
        policy = LLMRetryPolicy(llm_conn_id="test")
        decision = policy.evaluate(PermissionError("403"), try_number=1, max_tries=3)

        assert decision.action == RetryAction.FAIL
        assert "auth" in decision.reason
        assert "API key expired" in decision.reason

    @patch.object(BaseAIHook, "get_agent_hook")
    def test_rate_limit_returns_retry_with_delay(self, mock_get_hook):
        mock_get_hook.return_value = _make_mock_hook(
            _make_run_result(
                ErrorClassification(
                    category="rate_limit", should_retry=True, suggested_delay_seconds=60, reasoning="429"
                )
            )
        )
        policy = LLMRetryPolicy(llm_conn_id="test")
        decision = policy.evaluate(RuntimeError("429"), try_number=1, max_tries=3)

        assert decision.action == RetryAction.RETRY
        assert decision.retry_delay == timedelta(seconds=60)

    @patch.object(BaseAIHook, "get_agent_hook")
    def test_transient_retry_with_zero_delay_uses_default(self, mock_get_hook):
        """suggested_delay_seconds=0 means use the task's default delay, not override."""
        mock_get_hook.return_value = _make_mock_hook(
            _make_run_result(
                ErrorClassification(
                    category="transient", should_retry=True, suggested_delay_seconds=0, reasoning="glitch"
                )
            )
        )
        policy = LLMRetryPolicy(llm_conn_id="test")
        decision = policy.evaluate(RuntimeError("glitch"), try_number=1, max_tries=3)

        assert decision.action == RetryAction.RETRY
        assert decision.retry_delay is None  # None = use task's default

    @patch.object(BaseAIHook, "get_agent_hook")
    def test_negative_delay_treated_as_no_override(self, mock_get_hook):
        """Negative delay from LLM should not produce a negative timedelta."""
        mock_get_hook.return_value = _make_mock_hook(
            _make_run_result(
                ErrorClassification(
                    category="transient", should_retry=True, suggested_delay_seconds=-5, reasoning="x"
                )
            )
        )
        policy = LLMRetryPolicy(llm_conn_id="test")
        decision = policy.evaluate(RuntimeError("x"), try_number=1, max_tries=3)

        assert decision.action == RetryAction.RETRY
        assert decision.retry_delay is None

    @patch.object(BaseAIHook, "get_agent_hook")
    def test_prompt_includes_exception_type_and_message(self, mock_get_hook):
        mock_hook = _make_mock_hook(
            _make_run_result(ErrorClassification(category="data", should_retry=False, reasoning="test"))
        )
        mock_get_hook.return_value = mock_hook

        policy = LLMRetryPolicy(llm_conn_id="test")
        policy.evaluate(ValueError("bad column type"), try_number=2, max_tries=5)

        request = mock_hook.create_agent.call_args[0][0]
        assert "ValueError: bad column type" in request.prompt
        assert "attempt 2 of 5" in request.prompt

    @patch.object(BaseAIHook, "get_agent_hook")
    def test_custom_instructions_forwarded_to_agent(self, mock_get_hook):
        mock_hook = _make_mock_hook(
            _make_run_result(ErrorClassification(category="x", should_retry=False, reasoning="test"))
        )
        mock_get_hook.return_value = mock_hook

        policy = LLMRetryPolicy(llm_conn_id="test", instructions="My custom prompt")
        policy.evaluate(ValueError("x"), try_number=1, max_tries=3)

        request = mock_hook.create_agent.call_args[0][0]
        assert request.instructions == "My custom prompt"
        assert request.output_type is ErrorClassification

    @patch.object(BaseAIHook, "get_agent_hook")
    def test_timeout_passed_via_model_settings(self, mock_get_hook):
        mock_hook = _make_mock_hook(
            _make_run_result(ErrorClassification(category="auth", should_retry=False, reasoning="test"))
        )
        mock_get_hook.return_value = mock_hook

        policy = LLMRetryPolicy(llm_conn_id="test", timeout=15.0)
        policy.evaluate(ValueError("x"), try_number=1, max_tries=3)

        request = mock_hook.create_agent.call_args[0][0]
        model_settings = request.agent_params["model_settings"]
        assert model_settings["timeout"] == 15.0


class TestLLMFallbackBehaviour:
    """Test fallback when the LLM call itself fails."""

    def test_falls_back_to_rules_when_connection_missing(self):
        policy = LLMRetryPolicy(
            llm_conn_id="nonexistent",
            fallback_rules=[
                RetryRule(
                    exception=ConnectionError, action=RetryAction.RETRY, retry_delay=timedelta(seconds=10)
                ),
                RetryRule(exception=PermissionError, action=RetryAction.FAIL, reason="auth fallback"),
            ],
        )
        d = policy.evaluate(ConnectionError("refused"), try_number=1, max_tries=3)
        assert d.action == RetryAction.RETRY
        assert d.retry_delay == timedelta(seconds=10)

        d = policy.evaluate(PermissionError("denied"), try_number=1, max_tries=3)
        assert d.action == RetryAction.FAIL

    def test_falls_back_to_default_when_no_rules(self):
        policy = LLMRetryPolicy(llm_conn_id="nonexistent")
        d = policy.evaluate(ValueError("bad"), try_number=1, max_tries=3)
        assert d.action == RetryAction.DEFAULT

    def test_fallback_rules_no_match_returns_default(self):
        """When fallback rules exist but none match, DEFAULT is returned."""
        policy = LLMRetryPolicy(
            llm_conn_id="nonexistent",
            fallback_rules=[
                RetryRule(exception=PermissionError, action=RetryAction.FAIL),
            ],
        )
        # ValueError doesn't match the PermissionError rule
        d = policy.evaluate(ValueError("bad"), try_number=1, max_tries=3)
        assert d.action == RetryAction.DEFAULT

    @patch.object(BaseAIHook, "get_agent_hook")
    def test_run_agent_failure_triggers_fallback(self, mock_get_hook):
        """Failure during run_agent (not hook creation) still triggers fallback."""
        mock_hook = MagicMock()
        mock_hook.create_agent.return_value = MagicMock()
        mock_hook.run_agent.side_effect = RuntimeError("network error mid-call")
        mock_get_hook.return_value = mock_hook

        policy = LLMRetryPolicy(
            llm_conn_id="test",
            fallback_rules=[RetryRule(exception=ValueError, action=RetryAction.FAIL, reason="fallback")],
        )
        d = policy.evaluate(ValueError("x"), try_number=1, max_tries=3)
        assert d.action == RetryAction.FAIL
        assert d.reason == "fallback"

    @patch.object(BaseAIHook, "get_agent_hook")
    def test_hook_creation_failure_triggers_fallback(self, mock_get_hook):
        """Failure during hook.create_agent still triggers fallback."""
        mock_hook = MagicMock()
        mock_hook.create_agent.side_effect = RuntimeError("unexpected")
        mock_get_hook.return_value = mock_hook

        policy = LLMRetryPolicy(
            llm_conn_id="test",
            fallback_rules=[RetryRule(exception=ValueError, action=RetryAction.FAIL, reason="caught")],
        )
        d = policy.evaluate(ValueError("x"), try_number=1, max_tries=3)
        assert d.action == RetryAction.FAIL
