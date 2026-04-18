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

from airflow.providers.common.ai.policies.retry import (
    ErrorClassification,
    LLMRetryPolicy,
)
from airflow.sdk.definitions.retry_policy import RetryAction, RetryRule


def _make_mock_agent(category, should_retry, delay=0, reasoning="test"):
    """Create a mock agent that returns a canned ErrorClassification."""
    mock_result = MagicMock()
    mock_result.output = ErrorClassification(
        category=category,
        should_retry=should_retry,
        suggested_delay_seconds=delay,
        reasoning=reasoning,
    )
    mock_agent = MagicMock()
    mock_agent.run_sync.return_value = mock_result
    return mock_agent


class TestLLMClassifyDecisions:
    """Test that _classify maps LLM classification to correct RetryDecisions."""

    @patch("airflow.providers.common.ai.hooks.pydantic_ai.PydanticAIHook", autospec=True)
    def test_auth_error_returns_fail(self, mock_hook_cls):
        mock_hook_cls.return_value.create_agent.return_value = _make_mock_agent(
            "auth", should_retry=False, reasoning="API key expired"
        )
        policy = LLMRetryPolicy(llm_conn_id="test")
        decision = policy.evaluate(PermissionError("403"), try_number=1, max_tries=3)

        assert decision.action == RetryAction.FAIL
        assert "auth" in decision.reason
        assert "API key expired" in decision.reason

    @patch("airflow.providers.common.ai.hooks.pydantic_ai.PydanticAIHook", autospec=True)
    def test_rate_limit_returns_retry_with_delay(self, mock_hook_cls):
        mock_hook_cls.return_value.create_agent.return_value = _make_mock_agent(
            "rate_limit", should_retry=True, delay=60, reasoning="429"
        )
        policy = LLMRetryPolicy(llm_conn_id="test")
        decision = policy.evaluate(RuntimeError("429"), try_number=1, max_tries=3)

        assert decision.action == RetryAction.RETRY
        assert decision.retry_delay == timedelta(seconds=60)

    @patch("airflow.providers.common.ai.hooks.pydantic_ai.PydanticAIHook", autospec=True)
    def test_transient_retry_with_zero_delay_uses_default(self, mock_hook_cls):
        """suggested_delay_seconds=0 means use the task's default delay, not override."""
        mock_hook_cls.return_value.create_agent.return_value = _make_mock_agent(
            "transient", should_retry=True, delay=0
        )
        policy = LLMRetryPolicy(llm_conn_id="test")
        decision = policy.evaluate(RuntimeError("glitch"), try_number=1, max_tries=3)

        assert decision.action == RetryAction.RETRY
        assert decision.retry_delay is None  # None = use task's default

    @patch("airflow.providers.common.ai.hooks.pydantic_ai.PydanticAIHook", autospec=True)
    def test_negative_delay_treated_as_no_override(self, mock_hook_cls):
        """Negative delay from LLM should not produce a negative timedelta."""
        mock_hook_cls.return_value.create_agent.return_value = _make_mock_agent(
            "transient", should_retry=True, delay=-5
        )
        policy = LLMRetryPolicy(llm_conn_id="test")
        decision = policy.evaluate(RuntimeError("x"), try_number=1, max_tries=3)

        assert decision.action == RetryAction.RETRY
        assert decision.retry_delay is None

    @patch("airflow.providers.common.ai.hooks.pydantic_ai.PydanticAIHook", autospec=True)
    def test_prompt_includes_exception_type_and_message(self, mock_hook_cls):
        mock_agent = _make_mock_agent("data", should_retry=False)
        mock_hook_cls.return_value.create_agent.return_value = mock_agent

        policy = LLMRetryPolicy(llm_conn_id="test")
        policy.evaluate(ValueError("bad column type"), try_number=2, max_tries=5)

        prompt = mock_agent.run_sync.call_args[0][0]
        assert "ValueError: bad column type" in prompt
        assert "attempt 2 of 5" in prompt

    @patch("airflow.providers.common.ai.hooks.pydantic_ai.PydanticAIHook", autospec=True)
    def test_custom_instructions_forwarded_to_agent(self, mock_hook_cls):
        mock_hook_cls.return_value.create_agent.return_value = _make_mock_agent("x", False)

        policy = LLMRetryPolicy(llm_conn_id="test", instructions="My custom prompt")
        policy.evaluate(ValueError("x"), try_number=1, max_tries=3)

        mock_hook_cls.return_value.create_agent.assert_called_once_with(
            output_type=ErrorClassification,
            instructions="My custom prompt",
        )

    @patch("airflow.providers.common.ai.hooks.pydantic_ai.PydanticAIHook", autospec=True)
    def test_timeout_passed_via_model_settings(self, mock_hook_cls):
        mock_agent = _make_mock_agent("auth", False)
        mock_hook_cls.return_value.create_agent.return_value = mock_agent

        policy = LLMRetryPolicy(llm_conn_id="test", timeout=15.0)
        policy.evaluate(ValueError("x"), try_number=1, max_tries=3)

        model_settings = mock_agent.run_sync.call_args.kwargs["model_settings"]
        assert model_settings["timeout"] == 15.0


class TestLLMFallbackBehaviour:
    """Test fallback when the LLM call itself fails."""

    def test_falls_back_to_rules_when_connection_missing(self):
        policy = LLMRetryPolicy(
            llm_conn_id="nonexistent",
            fallback_rules=[
                RetryRule(exception=ConnectionError, action=RetryAction.RETRY, retry_delay=timedelta(seconds=10)),
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

    @patch("airflow.providers.common.ai.hooks.pydantic_ai.PydanticAIHook", autospec=True)
    def test_agent_run_sync_failure_triggers_fallback(self, mock_hook_cls):
        """Failure during run_sync (not hook creation) still triggers fallback."""
        mock_agent = MagicMock()
        mock_agent.run_sync.side_effect = RuntimeError("network error mid-call")
        mock_hook_cls.return_value.create_agent.return_value = mock_agent

        policy = LLMRetryPolicy(
            llm_conn_id="test",
            fallback_rules=[RetryRule(exception=ValueError, action=RetryAction.FAIL, reason="fallback")],
        )
        d = policy.evaluate(ValueError("x"), try_number=1, max_tries=3)
        assert d.action == RetryAction.FAIL
        assert d.reason == "fallback"

    @patch("airflow.providers.common.ai.hooks.pydantic_ai.PydanticAIHook", autospec=True)
    def test_hook_creation_failure_triggers_fallback(self, mock_hook_cls):
        """Failure during hook.create_agent still triggers fallback."""
        mock_hook_cls.return_value.create_agent.side_effect = RuntimeError("unexpected")

        policy = LLMRetryPolicy(
            llm_conn_id="test",
            fallback_rules=[RetryRule(exception=ValueError, action=RetryAction.FAIL, reason="caught")],
        )
        d = policy.evaluate(ValueError("x"), try_number=1, max_tries=3)
        assert d.action == RetryAction.FAIL
