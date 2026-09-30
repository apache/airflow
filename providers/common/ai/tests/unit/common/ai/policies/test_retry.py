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
import logging
import math
import warnings
from datetime import timedelta
from unittest.mock import MagicMock, patch

import pytest
from pydantic import TypeAdapter, ValidationError
from pydantic_ai import Agent
from pydantic_ai.agent import AgentRunResult
from pydantic_ai.exceptions import UnexpectedModelBehavior
from pydantic_ai.messages import ModelResponse

# LLMRetryPolicy depends on the RetryPolicy ABC introduced in Airflow 3.3 (AIP-105).
# Skip the entire test module on older Airflow versions tested in compat CI.
pytest.importorskip("airflow.sdk.definitions.retry_policy", reason="RetryPolicy requires Airflow 3.3+")

from airflow.providers.common.ai.policies.retry import (
    CLASSIFIER_INSTRUCTIONS,
    DEFAULT_CATEGORIES,
    DEFAULT_INSTRUCTIONS,
    ClassifierRetryPolicy,
    ErrorCategory,
    ErrorClassification,
    LLMRetryPolicy,
    redact_registered_secrets,
)
from airflow.providers.common.ai.utils.decision import picked_key
from airflow.sdk._shared.secrets_masker import reset_secrets_masker
from airflow.sdk.definitions.retry_policy import RetryAction, RetryDecision, RetryPolicy, RetryRule
from airflow.sdk.log import mask_secret

HOOK = "airflow.providers.common.ai.hooks.pydantic_ai.PydanticAIHook"


def _run_result(output, *, confidence=None, probabilities=None, model_name="test-model"):
    """A run result as pydantic-ai returns it: the pick in ``output``, the confidence in ``provider_details``."""
    details = {}
    if confidence is not None:
        details["confidence"] = {"response": confidence}
    if probabilities is not None:
        details["probabilities"] = {"response": probabilities}
    result = MagicMock(spec=AgentRunResult)
    result.output = output
    result.response = MagicMock(spec=ModelResponse, model_name=model_name, provider_details=details or None)
    return result


def _agent(output, **kwargs):
    """A mock agent whose ``run_sync`` returns ``output``; ``kwargs`` go to :func:`_run_result`."""
    agent = MagicMock(spec=Agent)
    agent.run_sync.return_value = _run_result(output, **kwargs)
    return agent


def _install(mock_hook_cls, agent):
    """Wire a mock agent behind the patched hook class and return the mock agent."""
    mock_hook_cls.return_value.create_agent.return_value = agent
    return agent


def _output_type(mock_hook_cls):
    return mock_hook_cls.return_value.create_agent.call_args.kwargs["output_type"]


def _options(output_type):
    """The ``(name, description)`` pairs the model is offered, as they appear in the schema."""
    schema = TypeAdapter(output_type).json_schema()
    if "anyOf" in schema:
        return [(o["const"], o.get("description")) for o in schema["anyOf"]]
    return [(name, None) for name in schema["enum"]]


@pytest.mark.enable_redact
def test_redact_registered_secrets_masks_only_registered_values():
    """Docs tell Dag authors to import and wrap this, so both the name and its narrow scope are contracts."""
    reset_secrets_masker()
    mask_secret("super-secret-conn-password")

    assert (
        redact_registered_secrets("contact user@example.com with super-secret-conn-password")
        == "contact user@example.com with ***"
    )


class TestErrorCategory:
    @pytest.mark.parametrize("description", ["", "   ", None, 3])
    def test_description_must_be_a_non_empty_string(self, description):
        with pytest.raises(ValueError, match="description must be a non-empty string"):
            ErrorCategory(description)

    def test_int_seconds_delay_is_rejected(self):
        """Int seconds was the shape of the removed ``suggested_delay_seconds`` field."""
        with pytest.raises(TypeError, match="must be a timedelta or None, got 60"):
            ErrorCategory("x", delay=60)  # type: ignore[arg-type]

    @pytest.mark.parametrize("retry", ["false", None, 0])
    def test_retry_must_be_a_bool(self, retry):
        """A truthy string here would retry a category the author meant to fail."""
        with pytest.raises(TypeError, match="retry must be True or False"):
            ErrorCategory("x", retry=retry)  # type: ignore[arg-type]

    def test_negative_delay_is_rejected(self):
        with pytest.raises(ValueError, match="must not be negative"):
            ErrorCategory("x", delay=timedelta(seconds=-1))

    def test_delay_without_retry_is_a_contradiction(self):
        with pytest.raises(ValueError, match="retry=False; a failed task has no retry to delay"):
            ErrorCategory("x", retry=False, delay=timedelta(seconds=5))

    def test_bar_is_normalized_and_validated(self):
        assert ErrorCategory("x", min_confidence=1).min_confidence == 1.0
        with pytest.raises(ValueError, match="ErrorCategory.min_confidence must be between 0 and 1"):
            ErrorCategory("x", min_confidence=1.5)
        with pytest.raises(TypeError, match="ErrorCategory.min_confidence must be a number"):
            ErrorCategory("x", min_confidence="0.5")  # type: ignore[arg-type]


class TestDefaultCategories:
    def test_cannot_be_mutated(self):
        """The default table is shared by every policy instance, so it must be read-only."""
        with pytest.raises(TypeError):
            DEFAULT_CATEGORIES["auth"] = ErrorCategory("x")  # type: ignore[index]

    def test_documented_split(self):
        """Spelled out here on purpose: the docs list this table, so a drift shows up as a test failure."""
        assert {name: (c.retry, c.delay, c.min_confidence) for name, c in DEFAULT_CATEGORIES.items()} == {
            "rate_limit": (True, timedelta(seconds=60), None),
            "auth": (False, None, None),
            "network": (True, timedelta(seconds=10), None),
            "data": (False, None, None),
            "resource": (False, None, None),
            "transient": (True, timedelta(seconds=30), None),
            "permanent": (False, None, None),
        }

    def test_category_instructions_do_not_recite_the_taxonomy(self):
        """With categories the taxonomy travels in the schema, so the categories prompt must not pin it."""
        for name in DEFAULT_CATEGORIES:
            assert f"- {name}:" not in CLASSIFIER_INSTRUCTIONS
            assert f"- {name}:" in DEFAULT_INSTRUCTIONS


class TestClassifierRetryPolicyConstruction:
    @patch(HOOK, autospec=True)
    def test_construction_makes_no_connection_or_network_call(self, mock_hook_cls):
        """The policy is instantiated at Dag parse time, so everything is validated without a hook."""
        ClassifierRetryPolicy(
            llm_conn_id="test",
            categories={"a": ErrorCategory("A", min_confidence=0.9), "b": ErrorCategory("B", retry=False)},
            min_confidence=0.6,
        )

        mock_hook_cls.assert_not_called()

    def test_categories_must_be_a_mapping(self):
        with pytest.raises(TypeError, match="categories must be a mapping"):
            ClassifierRetryPolicy(llm_conn_id="test", categories=[ErrorCategory("x")])  # type: ignore[arg-type]

    @pytest.mark.parametrize("categories", [{}, {"only": ErrorCategory("x")}])
    def test_fewer_than_two_categories_is_rejected(self, categories):
        with pytest.raises(ValueError, match="at least two entries"):
            ClassifierRetryPolicy(llm_conn_id="test", categories=categories)

    @pytest.mark.parametrize("name", ["", 3])
    def test_category_names_must_be_non_empty_strings(self, name):
        with pytest.raises(ValueError, match="keys must be non-empty strings"):
            ClassifierRetryPolicy(
                llm_conn_id="test", categories={name: ErrorCategory("x"), "b": ErrorCategory("y")}
            )

    def test_category_values_must_be_error_categories(self):
        """A bare string is not a description here: retry and delay would have nowhere to live."""
        with pytest.raises(TypeError, match=r"categories\['a'\] must be an ErrorCategory, got str"):
            ClassifierRetryPolicy(llm_conn_id="test", categories={"a": "flaky", "b": ErrorCategory("y")})  # type: ignore[dict-item]

    def test_category_bar_needs_a_policy_bar_to_inherit(self):
        with pytest.raises(
            ValueError, match=r"categories \['a', 'b'\] set min_confidence but the policy has none"
        ):
            ClassifierRetryPolicy(
                llm_conn_id="test",
                categories={
                    "a": ErrorCategory("A", min_confidence=0.9),
                    "b": ErrorCategory("B", min_confidence=0.8),
                    "c": ErrorCategory("C"),
                },
            )

    def test_policy_bar_is_validated(self):
        with pytest.raises(ValueError, match="min_confidence must be between 0 and 1"):
            ClassifierRetryPolicy(llm_conn_id="test", min_confidence=2)

    @pytest.mark.parametrize(
        "categories",
        [
            pytest.param(None, id="default-table"),
            pytest.param(
                {"a": ErrorCategory("A", delay=timedelta(seconds=1)), "b": ErrorCategory("B")}, id="own"
            ),
        ],
    )
    def test_policy_can_be_deep_copied(self, categories):
        """``TaskGroup(default_args={"retry_policy": ...})`` deep-copies the policy.

        The default table is a mappingproxy, which cannot be pickled, so holding it by
        reference would make the documented configuration the one that fails at Dag parse.
        """
        policy = ClassifierRetryPolicy(llm_conn_id="test", categories=categories)

        assert copy.deepcopy(policy).categories == policy.categories

    def test_chained_policy_can_be_deep_copied(self):
        """The example Dag ships a classifier with an LLMRetryPolicy behind it."""
        policy = ClassifierRetryPolicy(
            llm_conn_id="jev",
            min_confidence=0.8,
            fallback_policy=LLMRetryPolicy(llm_conn_id="text", timeout=7.0),
        )

        copied = copy.deepcopy(policy)

        assert isinstance(copied.fallback_policy, LLMRetryPolicy)
        assert copied.fallback_policy is not policy.fallback_policy
        assert copied.fallback_policy.timeout == 7.0

    @patch(HOOK, autospec=True)
    def test_caller_mapping_is_copied(self, mock_hook_cls):
        """Mutating the caller's mapping after construction must not change the policy."""
        _install(mock_hook_cls, _agent("a"))
        categories = {"a": ErrorCategory("A", delay=timedelta(seconds=7)), "b": ErrorCategory("B")}
        policy = ClassifierRetryPolicy(llm_conn_id="test", categories=categories)
        categories["a"] = ErrorCategory("A", delay=timedelta(seconds=999))

        decision = policy.evaluate(RuntimeError("x"), try_number=1, max_tries=3)

        assert decision.retry_delay == timedelta(seconds=7)

    @pytest.mark.parametrize("max_exception_length", [0, -1])
    def test_non_positive_max_exception_length_raises(self, max_exception_length):
        with pytest.raises(ValueError, match="max_exception_length must be a positive integer"):
            ClassifierRetryPolicy(
                llm_conn_id="test", max_exception_length=max_exception_length, categories=DEFAULT_CATEGORIES
            )

    def test_redact_exception_false_with_explicit_redactor_raises(self):
        with pytest.raises(ValueError, match="redactor must not be set when redact_exception=False"):
            ClassifierRetryPolicy(
                llm_conn_id="test",
                redactor=lambda m: m,
                redact_exception=False,
                categories=DEFAULT_CATEGORIES,
            )


class TestClassification:
    """The model names the category; the table in the worker decides what happens."""

    @pytest.mark.parametrize(
        ("category", "expected_action", "expected_delay"),
        [
            pytest.param("rate_limit", RetryAction.RETRY, timedelta(seconds=60), id="rate_limit"),
            pytest.param("network", RetryAction.RETRY, timedelta(seconds=10), id="network"),
            pytest.param("transient", RetryAction.RETRY, timedelta(seconds=30), id="transient"),
            pytest.param("auth", RetryAction.FAIL, None, id="auth"),
            pytest.param("data", RetryAction.FAIL, None, id="data"),
            pytest.param("resource", RetryAction.FAIL, None, id="resource"),
            pytest.param("permanent", RetryAction.FAIL, None, id="permanent"),
        ],
    )
    @patch(HOOK, autospec=True)
    def test_default_table_covers_every_category(
        self, mock_hook_cls, category, expected_action, expected_delay
    ):
        _install(mock_hook_cls, _agent(category))
        policy = ClassifierRetryPolicy(llm_conn_id="test", categories=DEFAULT_CATEGORIES)

        decision = policy.evaluate(RuntimeError("boom"), try_number=1, max_tries=3)

        assert decision.action == expected_action
        assert decision.retry_delay == expected_delay

    @patch(HOOK, autospec=True)
    def test_default_categories_reach_the_model_with_descriptions(self, mock_hook_cls):
        _install(mock_hook_cls, _agent("auth"))
        ClassifierRetryPolicy(llm_conn_id="test", categories=DEFAULT_CATEGORIES).evaluate(
            RuntimeError("boom"), try_number=1, max_tries=3
        )

        assert _options(_output_type(mock_hook_cls)) == [
            (name, category.description) for name, category in DEFAULT_CATEGORIES.items()
        ]

    @patch(HOOK, autospec=True)
    def test_custom_categories_build_the_schema_in_the_authors_order(self, mock_hook_cls):
        """The author's names and descriptions are the whole taxonomy; the prompt no longer carries one."""
        _install(mock_hook_cls, _agent("warehouse_suspended"))
        policy = ClassifierRetryPolicy(
            llm_conn_id="test",
            categories={
                "warehouse_suspended": ErrorCategory(
                    "The warehouse auto-suspended.", delay=timedelta(seconds=30)
                ),
                "schema_drift": ErrorCategory("A referenced column does not exist.", retry=False),
            },
        )

        decision = policy.evaluate(RuntimeError("Warehouse X is suspended"), try_number=1, max_tries=3)

        assert _options(_output_type(mock_hook_cls)) == [
            ("warehouse_suspended", "The warehouse auto-suspended."),
            ("schema_drift", "A referenced column does not exist."),
        ]
        assert decision.action == RetryAction.RETRY
        assert decision.retry_delay == timedelta(seconds=30)

    @patch(HOOK, autospec=True)
    def test_answer_outside_the_categories_is_rejected_by_the_output_type(self, mock_hook_cls):
        """A near-miss spelling or a sentence never reaches the table: pydantic-ai re-prompts, then gives up."""
        _install(mock_hook_cls, _agent("auth"))
        ClassifierRetryPolicy(llm_conn_id="test", categories=DEFAULT_CATEGORIES).evaluate(
            RuntimeError("boom"), try_number=1, max_tries=3
        )
        adapter = TypeAdapter(_output_type(mock_hook_cls))

        # ``Choices`` validates to the key; the Enum fallback to a member. ``picked_key`` reads both.
        assert picked_key(adapter.validate_python("auth")) == "auth"
        for bad in ("rate-limit", "RATE_LIMIT", "Rate limit exceeded, should retry", ""):
            with pytest.raises(ValidationError):
                adapter.validate_python(bad)

    @patch("airflow.providers.common.ai.utils.decision.Choices", None)
    @patch("airflow.providers.common.ai.utils.decision.Choice", None)
    @patch(HOOK, autospec=True)
    def test_enum_fallback_emits_the_same_schema_and_acts_on_the_member(self, mock_hook_cls):
        """On pydantic-ai without ``Choices`` the Enum fallback carries the same anyOf and the pick unwraps."""
        agent = MagicMock(spec=Agent)

        def capture(**kwargs):
            agent.run_sync.return_value = _run_result(kwargs["output_type"]("rate_limit"))
            return agent

        mock_hook_cls.return_value.create_agent.side_effect = capture
        policy = ClassifierRetryPolicy(llm_conn_id="test", categories=DEFAULT_CATEGORIES)

        decision = policy.evaluate(RuntimeError("429"), try_number=1, max_tries=3)

        assert _options(_output_type(mock_hook_cls))[0] == (
            "rate_limit",
            DEFAULT_CATEGORIES["rate_limit"].description,
        )
        assert decision.action == RetryAction.RETRY
        assert decision.retry_delay == timedelta(seconds=60)
        assert decision.reason.startswith("category=rate_limit ")

    @pytest.mark.parametrize("name", ["_transient_", "mro", "rate limit", "__weird__"])
    @patch("airflow.providers.common.ai.utils.decision.Choices", None)
    @patch("airflow.providers.common.ai.utils.decision.Choice", None)
    @patch(HOOK, autospec=True)
    def test_enum_fallback_survives_names_enum_reserves(self, mock_hook_cls, name):
        """A category name is a value, never an Enum member name: ``_sunder_``, ``mro`` and non-identifiers all work."""
        agent = MagicMock(spec=Agent)

        def capture(**kwargs):
            agent.run_sync.return_value = _run_result(kwargs["output_type"](name))
            return agent

        mock_hook_cls.return_value.create_agent.side_effect = capture
        policy = ClassifierRetryPolicy(
            llm_conn_id="test",
            categories={
                name: ErrorCategory("Odd name.", delay=timedelta(seconds=5)),
                "other": ErrorCategory("O"),
            },
        )

        decision = policy.evaluate(RuntimeError("x"), try_number=1, max_tries=3)

        assert _options(_output_type(mock_hook_cls)) == [(name, "Odd name."), ("other", "O")]
        assert decision.action == RetryAction.RETRY
        assert decision.retry_delay == timedelta(seconds=5)
        assert decision.reason.startswith(f"category={name} ")

    @pytest.mark.parametrize(
        ("category", "confidence", "expected"),
        [
            pytest.param("auth", None, "category=auth confidence=n/a threshold=n/a action=fail", id="fail"),
            pytest.param(
                "rate_limit",
                0.91,
                "category=rate_limit confidence=0.91 threshold=n/a action=retry delay=60s",
                id="retry",
            ),
        ],
    )
    @patch(HOOK, autospec=True)
    def test_reason_is_the_generated_summary(self, mock_hook_cls, category, confidence, expected):
        """No ``reasoning`` field: the reason a person reads is derived from the decision itself."""
        _install(mock_hook_cls, _agent(category, confidence=confidence))
        policy = ClassifierRetryPolicy(llm_conn_id="test", categories=DEFAULT_CATEGORIES)

        decision = policy.evaluate(RuntimeError("boom"), try_number=1, max_tries=3)

        assert decision.reason == expected

    @patch(HOOK, autospec=True)
    def test_none_delay_leaves_the_task_backoff_in_charge(self, mock_hook_cls):
        _install(mock_hook_cls, _agent("flaky"))
        policy = ClassifierRetryPolicy(
            llm_conn_id="test",
            categories={"flaky": ErrorCategory("F"), "broken": ErrorCategory("B", retry=False)},
        )

        decision = policy.evaluate(RuntimeError("glitch"), try_number=1, max_tries=3)

        assert decision.action == RetryAction.RETRY
        assert decision.retry_delay is None
        assert decision.reason.endswith("action=retry delay=task default")

    @patch(HOOK, autospec=True)
    def test_zero_delay_retries_without_waiting(self, mock_hook_cls):
        """timedelta(0) is an override to retry at once, distinct from None."""
        _install(mock_hook_cls, _agent("flaky"))
        policy = ClassifierRetryPolicy(
            llm_conn_id="test",
            categories={
                "flaky": ErrorCategory("F", delay=timedelta(0)),
                "broken": ErrorCategory("B", retry=False),
            },
        )

        decision = policy.evaluate(RuntimeError("glitch"), try_number=1, max_tries=3)

        assert decision.retry_delay == timedelta(0)
        assert decision.reason.endswith("delay=0s")

    @patch(HOOK, autospec=True)
    def test_each_failure_is_classified_afresh(self, mock_hook_cls):
        """Consecutive attempts with different exceptions each go to the model; nothing is replayed."""
        agent = _install(mock_hook_cls, MagicMock(spec=Agent))
        agent.run_sync.side_effect = [_run_result("network"), _run_result("auth")]
        policy = ClassifierRetryPolicy(llm_conn_id="test", categories=DEFAULT_CATEGORIES)

        first = policy.evaluate(ConnectionError("reset"), try_number=1, max_tries=3)
        second = policy.evaluate(PermissionError("expired"), try_number=2, max_tries=3)

        assert (first.action, second.action) == (RetryAction.RETRY, RetryAction.FAIL)
        prompts = [call.args[0] for call in agent.run_sync.call_args_list]
        assert "ConnectionError: reset" in prompts[0]
        assert "attempt 1 of 3" in prompts[0]
        assert "PermissionError: expired" in prompts[1]
        assert "attempt 2 of 3" in prompts[1]

    @patch(HOOK, autospec=True)
    def test_custom_instructions_forwarded_to_agent(self, mock_hook_cls):
        _install(mock_hook_cls, _agent("auth"))
        policy = ClassifierRetryPolicy(
            llm_conn_id="test",
            instructions="Snowflake errors only.",
            categories={"auth": ErrorCategory("A", retry=False), "other": ErrorCategory("O")},
        )

        policy.evaluate(RuntimeError("x"), try_number=1, max_tries=3)

        assert (
            mock_hook_cls.return_value.create_agent.call_args.kwargs["instructions"]
            == "Snowflake errors only."
        )

    @patch(HOOK, autospec=True)
    def test_model_id_and_connection_reach_the_hook(self, mock_hook_cls):
        _install(mock_hook_cls, _agent("auth"))
        policy = ClassifierRetryPolicy(
            llm_conn_id="jev_default", model_id="typesafe:jev-1.13.0", categories=DEFAULT_CATEGORIES
        )

        policy.evaluate(RuntimeError("x"), try_number=1, max_tries=3)

        mock_hook_cls.assert_called_once_with(llm_conn_id="jev_default", model_id="typesafe:jev-1.13.0")

    @patch(HOOK, autospec=True)
    def test_timeout_passed_via_model_settings(self, mock_hook_cls):
        agent = _install(mock_hook_cls, _agent("auth"))
        policy = ClassifierRetryPolicy(llm_conn_id="test", timeout=7.5, categories=DEFAULT_CATEGORIES)

        policy.evaluate(RuntimeError("x"), try_number=1, max_tries=3)

        assert agent.run_sync.call_args.kwargs["model_settings"] == {"timeout": 7.5}


class TestConfidenceGate:
    """Under the bar the answer is discarded and the deterministic fallback path decides."""

    RULES = [RetryRule(exception=RuntimeError, action=RetryAction.FAIL, reason="rule")]

    @pytest.mark.parametrize(
        ("confidence", "expected_action", "expected_reason"),
        [
            pytest.param(
                0.60,
                RetryAction.RETRY,
                "category=network confidence=0.60 threshold=0.60 action=retry delay=10s",
                id="at-bar",
            ),
            pytest.param(
                0.59,
                RetryAction.FAIL,
                "classifier answer not applied (below_threshold); rule",
                id="one-below",
            ),
        ],
    )
    @patch(HOOK, autospec=True)
    def test_bar_boundary(self, mock_hook_cls, confidence, expected_action, expected_reason):
        _install(mock_hook_cls, _agent("network", confidence=confidence))
        policy = ClassifierRetryPolicy(llm_conn_id="test", min_confidence=0.6, fallback_rules=self.RULES)

        decision = policy.evaluate(RuntimeError("reset"), try_number=1, max_tries=3)

        assert decision.action == expected_action
        assert decision.reason == expected_reason

    @patch(HOOK, autospec=True)
    def test_under_the_bar_with_no_rules_keeps_the_task_behaviour(self, mock_hook_cls, caplog):
        """An unsure answer is an ordinary outcome: one INFO line with the distribution, no ERROR traceback."""
        _install(mock_hook_cls, _agent("auth", confidence=0.3, probabilities={"auth": 0.3, "network": 0.28}))
        policy = ClassifierRetryPolicy(llm_conn_id="test", min_confidence=0.6)

        with caplog.at_level(logging.INFO, logger="airflow.providers.common.ai.policies.retry"):
            decision = policy.evaluate(RuntimeError("?"), try_number=1, max_tries=3)

        assert decision.action == RetryAction.DEFAULT
        assert decision.reason == "classifier answer not applied (below_threshold); task retry settings apply"
        [record] = [r for r in caplog.records if r.name == "airflow.providers.common.ai.policies.retry"]
        assert record.levelno == logging.INFO
        assert (
            "not applied (below_threshold): category=auth confidence=0.30 threshold=0.60"
            in record.getMessage()
        )
        assert "probabilities={'auth': 0.3, 'network': 0.28}" in record.getMessage()

    @patch(HOOK, autospec=True)
    def test_under_the_bar_takes_fallback_rules_before_default(self, mock_hook_cls):
        """An unsure ``auth`` does not fail the task on the model's say-so; the matching rule decides."""
        _install(mock_hook_cls, _agent("auth", confidence=0.3))
        policy = ClassifierRetryPolicy(
            llm_conn_id="test",
            min_confidence=0.6,
            fallback_rules=[
                RetryRule(
                    exception=ConnectionError, action=RetryAction.RETRY, retry_delay=timedelta(seconds=5)
                ),
                RetryRule(exception=PermissionError, action=RetryAction.FAIL, reason="rule"),
            ],
        )

        assert policy.evaluate(ConnectionError("x"), try_number=1, max_tries=3).retry_delay == timedelta(
            seconds=5
        )
        assert (
            policy.evaluate(PermissionError("x"), try_number=1, max_tries=3).reason
            == "classifier answer not applied (below_threshold); rule"
        )
        unmatched = policy.evaluate(ValueError("x"), try_number=1, max_tries=3)
        assert unmatched.action == RetryAction.DEFAULT
        assert (
            unmatched.reason == "classifier answer not applied (below_threshold); task retry settings apply"
        )

    @pytest.mark.parametrize(
        ("category", "confidence", "expected_action"),
        [
            pytest.param("permanent", 0.80, RetryAction.DEFAULT, id="permanent-under-its-own-bar"),
            pytest.param("permanent", 0.85, RetryAction.FAIL, id="permanent-at-its-own-bar"),
            pytest.param("transient", 0.80, RetryAction.RETRY, id="transient-inherits-the-policy-bar"),
            pytest.param("network", 0.50, RetryAction.RETRY, id="network-has-a-lower-bar-than-the-policy"),
            pytest.param("transient", 0.50, RetryAction.DEFAULT, id="transient-under-the-policy-bar"),
        ],
    )
    @patch(HOOK, autospec=True)
    def test_category_bar_overrides_the_policy_bar(
        self, mock_hook_cls, category, confidence, expected_action
    ):
        _install(mock_hook_cls, _agent(category, confidence=confidence))
        policy = ClassifierRetryPolicy(
            llm_conn_id="test",
            min_confidence=0.6,
            categories={
                **DEFAULT_CATEGORIES,
                "permanent": ErrorCategory("Will fail identically.", retry=False, min_confidence=0.85),
                "network": ErrorCategory("Cheap to retry.", delay=timedelta(seconds=10), min_confidence=0.4),
            },
        )

        assert policy.evaluate(RuntimeError("x"), try_number=1, max_tries=3).action == expected_action

    @patch(HOOK, autospec=True)
    def test_missing_confidence_with_a_bar_falls_back(self, mock_hook_cls):
        """A text model reports no confidence; a model swap must not switch off a bar the author set."""
        _install(mock_hook_cls, _agent("network"))
        policy = ClassifierRetryPolicy(llm_conn_id="test", min_confidence=0.6, fallback_rules=self.RULES)

        decision = policy.evaluate(RuntimeError("reset"), try_number=1, max_tries=3)

        assert decision.action == RetryAction.FAIL
        assert decision.reason == "classifier answer not applied (missing_confidence); rule"

    @patch(HOOK, autospec=True)
    def test_missing_confidence_without_a_bar_acts_as_before(self, mock_hook_cls):
        _install(mock_hook_cls, _agent("network"))
        policy = ClassifierRetryPolicy(
            llm_conn_id="test", fallback_rules=self.RULES, categories=DEFAULT_CATEGORIES
        )

        decision = policy.evaluate(RuntimeError("reset"), try_number=1, max_tries=3)

        assert decision.action == RetryAction.RETRY
        assert decision.retry_delay == timedelta(seconds=10)

    @patch(HOOK, autospec=True)
    def test_nan_confidence_counts_as_missing(self, mock_hook_cls):
        _install(mock_hook_cls, _agent("network", confidence=math.nan))
        policy = ClassifierRetryPolicy(llm_conn_id="test", min_confidence=0.6)

        assert policy.evaluate(RuntimeError("reset"), try_number=1, max_tries=3).action == RetryAction.DEFAULT

    @patch(HOOK, autospec=True)
    def test_confidence_without_a_bar_is_recorded_but_not_gated(self, mock_hook_cls):
        _install(mock_hook_cls, _agent("auth", confidence=0.05))
        policy = ClassifierRetryPolicy(llm_conn_id="test", categories=DEFAULT_CATEGORIES)

        decision = policy.evaluate(RuntimeError("x"), try_number=1, max_tries=3)

        assert decision.action == RetryAction.FAIL
        assert decision.reason == "category=auth confidence=0.05 threshold=n/a action=fail"


class TestPrompt:
    @patch(HOOK, autospec=True)
    def test_prompt_includes_exception_type_and_message(self, mock_hook_cls):
        agent = _install(mock_hook_cls, _agent("data"))
        policy = ClassifierRetryPolicy(llm_conn_id="test", categories=DEFAULT_CATEGORIES)

        policy.evaluate(ValueError("bad column type"), try_number=2, max_tries=5)

        prompt = agent.run_sync.call_args.args[0]
        assert "ValueError: bad column type" in prompt
        assert "attempt 2 of 5" in prompt

    @pytest.mark.enable_redact
    @patch(HOOK, autospec=True)
    def test_prompt_redacts_known_secrets(self, mock_hook_cls):
        reset_secrets_masker()
        secret_value = "super-secret-conn-password"
        mask_secret(secret_value)
        agent = _install(mock_hook_cls, _agent("auth"))
        policy = ClassifierRetryPolicy(llm_conn_id="test", categories=DEFAULT_CATEGORIES)

        policy.evaluate(
            ConnectionError(f"could not authenticate with password {secret_value}"), try_number=1, max_tries=3
        )

        assert agent.run_sync.call_args.args[0] == (
            "Classify this error from a data pipeline task (attempt 1 of 3):\n\n"
            "ConnectionError: could not authenticate with password ***"
        )

    @pytest.mark.enable_redact
    @patch(HOOK, autospec=True)
    def test_prompt_keeps_raw_message_when_redaction_disabled(self, mock_hook_cls):
        reset_secrets_masker()
        secret_value = "super-secret-conn-password"
        mask_secret(secret_value)
        agent = _install(mock_hook_cls, _agent("auth"))
        policy = ClassifierRetryPolicy(
            llm_conn_id="test", redact_exception=False, categories=DEFAULT_CATEGORIES
        )

        policy.evaluate(ConnectionError(f"password {secret_value}"), try_number=1, max_tries=3)

        assert secret_value in agent.run_sync.call_args.args[0]

    @pytest.mark.enable_redact
    @patch(HOOK, autospec=True)
    def test_explicit_redactor_none_still_applies_default_masking(self, mock_hook_cls):
        reset_secrets_masker()
        secret_value = "super-secret-conn-password"
        mask_secret(secret_value)
        agent = _install(mock_hook_cls, _agent("auth"))
        policy = ClassifierRetryPolicy(llm_conn_id="test", redactor=None, categories=DEFAULT_CATEGORIES)

        policy.evaluate(ConnectionError(f"password {secret_value}"), try_number=1, max_tries=3)

        assert secret_value not in agent.run_sync.call_args.args[0]

    @pytest.mark.enable_redact
    @patch(HOOK, autospec=True)
    def test_custom_redactor_replaces_masker_instead_of_stacking(self, mock_hook_cls):
        reset_secrets_masker()
        secret_value = "super-secret-conn-password"
        mask_secret(secret_value)
        agent = _install(mock_hook_cls, _agent("auth"))
        policy = ClassifierRetryPolicy(
            llm_conn_id="test",
            redactor=lambda m: m.replace("user@example.com", "<email>"),
            categories=DEFAULT_CATEGORIES,
        )

        policy.evaluate(ConnectionError(f"user@example.com {secret_value}"), try_number=1, max_tries=3)

        prompt = agent.run_sync.call_args.args[0]
        assert "<email>" in prompt
        assert secret_value in prompt  # the custom redactor replaced the masker, so this is on the author

    @pytest.mark.parametrize(
        ("length", "expected_tail"),
        [
            pytest.param(50, f"ValueError: {'x' * 10}... (truncated)", id="over"),
            pytest.param(10, f"ValueError: {'x' * 10}", id="exact-fit"),
        ],
    )
    @patch(HOOK, autospec=True)
    def test_message_truncated_only_when_over_max_exception_length(
        self, mock_hook_cls, length, expected_tail
    ):
        agent = _install(mock_hook_cls, _agent("data"))
        policy = ClassifierRetryPolicy(
            llm_conn_id="test", max_exception_length=10, categories=DEFAULT_CATEGORIES
        )

        policy.evaluate(ValueError("x" * length), try_number=1, max_tries=3)

        assert agent.run_sync.call_args.args[0].endswith(expected_tail)

    @pytest.mark.enable_redact
    @patch(HOOK, autospec=True)
    def test_truncation_happens_after_redaction(self, mock_hook_cls):
        """Truncating first could cut a registered secret in half and leak the head of it."""
        reset_secrets_masker()
        secret_value = "super-secret-conn-password"
        mask_secret(secret_value)
        agent = _install(mock_hook_cls, _agent("auth"))
        policy = ClassifierRetryPolicy(
            llm_conn_id="test", max_exception_length=12, categories=DEFAULT_CATEGORIES
        )

        policy.evaluate(ConnectionError(f"pw {secret_value} tail"), try_number=1, max_tries=3)

        assert agent.run_sync.call_args.args[0].endswith("ConnectionError: pw *** tail")


class TestFallbackBehaviour:
    """When the LLM call itself fails the deterministic path decides, unchanged."""

    def test_falls_back_to_rules_when_connection_missing(self):
        policy = ClassifierRetryPolicy(
            llm_conn_id="nonexistent",
            fallback_rules=[
                RetryRule(
                    exception=ConnectionError, action=RetryAction.RETRY, retry_delay=timedelta(seconds=10)
                ),
                RetryRule(exception=PermissionError, action=RetryAction.FAIL, reason="auth fallback"),
            ],
            categories=DEFAULT_CATEGORIES,
        )

        retry = policy.evaluate(ConnectionError("refused"), try_number=1, max_tries=3)
        fail = policy.evaluate(PermissionError("denied"), try_number=1, max_tries=3)

        assert (retry.action, retry.retry_delay) == (RetryAction.RETRY, timedelta(seconds=10))
        assert fail.action == RetryAction.FAIL

    def test_falls_back_to_default_when_no_rules(self):
        policy = ClassifierRetryPolicy(llm_conn_id="nonexistent", categories=DEFAULT_CATEGORIES)

        assert policy.evaluate(ValueError("bad"), try_number=1, max_tries=3).action == RetryAction.DEFAULT

    def test_fallback_rules_no_match_returns_default(self):
        policy = ClassifierRetryPolicy(
            llm_conn_id="nonexistent",
            fallback_rules=[RetryRule(exception=PermissionError, action=RetryAction.FAIL)],
            categories=DEFAULT_CATEGORIES,
        )

        assert policy.evaluate(ValueError("bad"), try_number=1, max_tries=3).action == RetryAction.DEFAULT

    @patch(HOOK, autospec=True)
    def test_agent_run_sync_failure_triggers_fallback(self, mock_hook_cls):
        """A model timeout or API failure surfaces here, as an exception from run_sync."""
        agent = _install(mock_hook_cls, MagicMock(spec=Agent))
        agent.run_sync.side_effect = TimeoutError("model did not answer in time")
        policy = ClassifierRetryPolicy(
            llm_conn_id="test",
            fallback_rules=[RetryRule(exception=ValueError, action=RetryAction.FAIL, reason="fallback")],
            categories=DEFAULT_CATEGORIES,
        )

        decision = policy.evaluate(ValueError("x"), try_number=1, max_tries=3)

        assert decision.action == RetryAction.FAIL
        assert decision.reason == "classifier answer not applied (model_error); fallback"

    @patch(HOOK, autospec=True)
    def test_hook_creation_failure_triggers_fallback(self, mock_hook_cls):
        mock_hook_cls.return_value.create_agent.side_effect = RuntimeError("unexpected")
        policy = ClassifierRetryPolicy(
            llm_conn_id="test",
            fallback_rules=[RetryRule(exception=ValueError, action=RetryAction.FAIL, reason="caught")],
            categories=DEFAULT_CATEGORIES,
        )

        assert (
            policy.evaluate(ValueError("x"), try_number=1, max_tries=3).reason
            == "classifier answer not applied (model_error); caught"
        )

    @patch(HOOK, autospec=True)
    def test_answer_the_type_rejected_triggers_fallback(self, mock_hook_cls):
        """pydantic-ai raises once its re-prompts are exhausted; that is a failed call like any other."""
        agent = _install(mock_hook_cls, MagicMock(spec=Agent))
        agent.run_sync.side_effect = UnexpectedModelBehavior(
            "Exceeded maximum retries (1) for output validation"
        )
        policy = ClassifierRetryPolicy(llm_conn_id="test", categories=DEFAULT_CATEGORIES)

        assert policy.evaluate(ValueError("x"), try_number=1, max_tries=3).action == RetryAction.DEFAULT

    def test_matched_default_rule_keeps_its_delay_and_reason(self):
        """A rule the author wrote with action=DEFAULT is a match like any other; 0.9.0 returned it verbatim."""
        policy = ClassifierRetryPolicy(
            llm_conn_id="nonexistent",
            fallback_rules=[
                RetryRule(
                    exception=ValueError,
                    action=RetryAction.DEFAULT,
                    retry_delay=timedelta(seconds=45),
                    reason="my rule reason",
                )
            ],
            categories=DEFAULT_CATEGORIES,
        )

        decision = policy.evaluate(ValueError("x"), try_number=1, max_tries=3)

        assert decision.action == RetryAction.DEFAULT
        assert decision.retry_delay == timedelta(seconds=45)
        assert decision.reason == "classifier answer not applied (model_error); my rule reason"

    @patch(HOOK, autospec=True)
    def test_answer_missing_from_the_table_falls_back_with_an_error_log(self, mock_hook_cls, caplog):
        """Reachable only if the schema and the table disagree; it must not surface as a KeyError traceback."""
        _install(mock_hook_cls, _agent("not_a_category"))
        policy = ClassifierRetryPolicy(llm_conn_id="test", categories=DEFAULT_CATEGORIES)

        with caplog.at_level(logging.ERROR, logger="airflow.providers.common.ai.policies.retry"):
            decision = policy.evaluate(ValueError("x"), try_number=1, max_tries=3)

        assert decision.action == RetryAction.DEFAULT
        assert decision.reason == "classifier answer not applied (model_error); task retry settings apply"
        assert "answered 'not_a_category', which is not a configured category" in caplog.text
        assert "KeyError" not in caplog.text


class TestOnUncertain:
    """Classifier first; an LLM policy when it is unsure or unreachable; the rules when neither decides."""

    RULES = [
        RetryRule(
            exception=RuntimeError, action=RetryAction.RETRY, retry_delay=timedelta(seconds=5), reason="rule"
        )
    ]

    def _chain(self, mock_hook_cls, classifier_answer, confidence, llm_answer, *, bar=0.8, rules=RULES):
        """Wire two agents behind one patched hook: the classifier by output type, the LLM by ErrorClassification.

        ``None`` for either answer makes that model unreachable.
        """
        if classifier_answer is None:
            classifier = MagicMock(spec=Agent)
            classifier.run_sync.side_effect = TimeoutError("classifier unreachable")
        else:
            classifier = _agent(classifier_answer, confidence=confidence)
        if llm_answer is None:
            llm = MagicMock(spec=Agent)
            llm.run_sync.side_effect = TimeoutError("llm unreachable")
        else:
            llm = _agent(llm_answer)

        def create_agent(**kwargs):
            return llm if kwargs["output_type"] is ErrorClassification else classifier

        mock_hook_cls.return_value.create_agent.side_effect = create_agent
        return ClassifierRetryPolicy(
            llm_conn_id="jev",
            min_confidence=bar,
            fallback_policy=LLMRetryPolicy(llm_conn_id="text"),
            fallback_rules=rules,
        )

    @patch(HOOK, autospec=True)
    def test_without_a_bar_the_llm_is_consulted_only_when_the_classifier_is_unreachable(self, mock_hook_cls):
        llm_answer = ErrorClassification(category="auth", should_retry=False, reasoning="expired key")

        answered = self._chain(mock_hook_cls, "network", None, llm_answer, bar=None)
        decision = answered.evaluate(RuntimeError("reset"), try_number=1, max_tries=3)
        assert decision.action == RetryAction.RETRY
        assert decision.reason == "category=network confidence=n/a threshold=n/a action=retry delay=10s"

        unreachable = self._chain(mock_hook_cls, None, None, llm_answer, bar=None)
        decision = unreachable.evaluate(RuntimeError("reset"), try_number=1, max_tries=3)
        assert decision.action == RetryAction.FAIL
        assert decision.reason == "escalated (model_error); auth: expired key"

    def test_fallback_policy_must_be_a_retry_policy(self):
        with pytest.raises(TypeError, match="fallback_policy must be a RetryPolicy"):
            ClassifierRetryPolicy(llm_conn_id="jev", min_confidence=0.5, fallback_policy="llm")  # type: ignore[arg-type]

    @patch(HOOK, autospec=True)
    def test_confident_classifier_answer_never_consults_the_llm(self, mock_hook_cls):
        policy = self._chain(
            mock_hook_cls,
            "network",
            0.95,
            ErrorClassification(category="x", should_retry=False, reasoning="r"),
        )

        decision = policy.evaluate(RuntimeError("reset"), try_number=1, max_tries=3)

        assert decision.action == RetryAction.RETRY
        assert decision.reason == "category=network confidence=0.95 threshold=0.80 action=retry delay=10s"
        assert mock_hook_cls.return_value.create_agent.call_count == 1

    @patch(HOOK, autospec=True)
    def test_unsure_classifier_escalates_to_the_llm(self, mock_hook_cls):
        llm_answer = ErrorClassification(
            category="rate_limit",
            should_retry=True,
            suggested_delay_seconds=120,
            reasoning="429 with Retry-After",
        )
        policy = self._chain(mock_hook_cls, "auth", 0.45, llm_answer)

        decision = policy.evaluate(RuntimeError("?"), try_number=1, max_tries=3)

        assert decision.action == RetryAction.RETRY
        assert decision.retry_delay == timedelta(seconds=120)
        assert decision.reason == "escalated (below_threshold); rate_limit: 429 with Retry-After"

    @patch(HOOK, autospec=True)
    def test_unreachable_classifier_escalates_to_the_llm(self, mock_hook_cls):
        llm_answer = ErrorClassification(category="auth", should_retry=False, reasoning="expired key")
        policy = self._chain(mock_hook_cls, None, None, llm_answer)

        decision = policy.evaluate(RuntimeError("?"), try_number=1, max_tries=3)

        assert decision.action == RetryAction.FAIL
        assert decision.reason == "escalated (model_error); auth: expired key"

    @patch(HOOK, autospec=True)
    def test_unreachable_llm_falls_to_the_rules(self, mock_hook_cls):
        policy = self._chain(mock_hook_cls, "auth", 0.45, None)

        decision = policy.evaluate(RuntimeError("?"), try_number=1, max_tries=3)

        assert decision.action == RetryAction.RETRY
        assert decision.retry_delay == timedelta(seconds=5)
        assert decision.reason == "classifier answer not applied (below_threshold); rule"

    @patch(HOOK, autospec=True)
    def test_nothing_decides_means_the_task_default(self, mock_hook_cls):
        policy = self._chain(mock_hook_cls, "auth", 0.45, None, rules=None)

        decision = policy.evaluate(ValueError("?"), try_number=1, max_tries=3)

        assert decision.action == RetryAction.DEFAULT
        assert decision.reason == "classifier answer not applied (below_threshold); task retry settings apply"

    def test_inner_policy_with_its_own_rules_is_used_as_is(self):
        """The LLM policy's own fallback_rules count as its decision, so the outer rules are not reached."""
        inner = MagicMock(spec=LLMRetryPolicy)
        inner.evaluate.return_value = RetryDecision.fail(reason="inner rule")
        policy = ClassifierRetryPolicy(
            llm_conn_id="nonexistent", min_confidence=0.8, fallback_policy=inner, fallback_rules=self.RULES
        )

        decision = policy.evaluate(RuntimeError("?"), try_number=1, max_tries=3)

        assert decision.action == RetryAction.FAIL
        assert decision.reason == "escalated (model_error); inner rule"
        inner.evaluate.assert_called_once()

    def test_inner_decision_without_a_reason_is_prefixed_without_a_none_suffix(self):
        """An ExceptionRetryPolicy with default=RETRY returns RETRY and no reason when nothing matches."""
        inner = MagicMock(spec=LLMRetryPolicy)
        inner.evaluate.return_value = RetryDecision.retry(delay=timedelta(seconds=9))
        policy = ClassifierRetryPolicy(llm_conn_id="nonexistent", min_confidence=0.8, fallback_policy=inner)

        decision = policy.evaluate(RuntimeError("?"), try_number=1, max_tries=3)

        assert decision.action == RetryAction.RETRY
        assert decision.retry_delay == timedelta(seconds=9)
        assert decision.reason == "escalated (model_error)"

    @pytest.mark.parametrize(
        "inner_decision",
        [
            pytest.param(RetryDecision.default(), id="bare-default"),
            pytest.param(
                RetryDecision(action=RetryAction.DEFAULT, reason="matched a DEFAULT rule"),
                id="default-with-reason",
            ),
            pytest.param(
                RetryDecision(action=RetryAction.DEFAULT, retry_delay=timedelta(seconds=99), reason="x"),
                id="default-with-delay",
            ),
        ],
    )
    def test_any_default_from_fallback_policy_lets_the_outer_rules_run(self, inner_decision, caplog):
        """Only RETRY or FAIL ends the chain; the reason text on a DEFAULT does not make it a decision."""
        inner = MagicMock(spec=LLMRetryPolicy)
        inner.evaluate.return_value = inner_decision
        policy = ClassifierRetryPolicy(
            llm_conn_id="nonexistent", min_confidence=0.8, fallback_policy=inner, fallback_rules=self.RULES
        )

        with caplog.at_level(logging.INFO, logger="airflow.providers.common.ai.policies.retry"):
            decision = policy.evaluate(RuntimeError("?"), try_number=1, max_tries=3)

        assert decision.action == RetryAction.RETRY
        assert decision.retry_delay == timedelta(seconds=5)
        assert decision.reason == "classifier answer not applied (model_error); rule"
        assert "decided nothing" in caplog.text

    def test_fallback_policy_raising_falls_to_the_outer_rules(self, caplog):
        """A third-party policy that blows up must not take the classifier's rules floor with it."""

        class Boom(RetryPolicy):
            def evaluate(self, exception, try_number, max_tries, context=None):
                raise RuntimeError("policy bug")

        policy = ClassifierRetryPolicy(
            llm_conn_id="nonexistent", min_confidence=0.8, fallback_policy=Boom(), fallback_rules=self.RULES
        )

        with caplog.at_level(logging.ERROR, logger="airflow.providers.common.ai.policies.retry"):
            decision = policy.evaluate(RuntimeError("?"), try_number=1, max_tries=3)

        assert decision.action == RetryAction.RETRY
        assert decision.reason == "classifier answer not applied (model_error); rule"
        assert "fallback_policy failed" in caplog.text

    @patch(HOOK, autospec=True)
    def test_nested_classifiers_both_down_still_reach_the_outer_fail_rule(self, mock_hook_cls):
        """A ClassifierRetryPolicy as fallback_policy falls back with a DEFAULT of its own; the outer rules must still run."""
        mock_hook_cls.return_value.create_agent.return_value.run_sync.side_effect = TimeoutError("down")
        inner = ClassifierRetryPolicy(llm_conn_id="jev_b", min_confidence=0.5)
        policy = ClassifierRetryPolicy(
            llm_conn_id="jev_a",
            min_confidence=0.8,
            fallback_policy=inner,
            fallback_rules=[
                RetryRule(exception=PermissionError, action=RetryAction.FAIL, reason="never retry 403")
            ],
        )

        decision = policy.evaluate(PermissionError("403"), try_number=1, max_tries=3)

        assert decision.action == RetryAction.FAIL
        assert decision.reason == "classifier answer not applied (model_error); never retry 403"
        assert mock_hook_cls.return_value.create_agent.call_count == 2


def _open_agent(category, should_retry, delay=0, reasoning="test"):
    """A mock agent answering the 0.9.0 ``ErrorClassification`` shape, for a policy without categories."""
    return _agent(
        ErrorClassification(
            category=category, should_retry=should_retry, suggested_delay_seconds=delay, reasoning=reasoning
        )
    )


class TestLLMRetryPolicy:
    """The LLM layer is the 0.9.0 policy: the text model decides retry and delay, and nothing classifier-shaped is on it."""

    def test_positional_arguments_keep_the_0_9_0_order(self):
        rules = [RetryRule(exception=ValueError, action=RetryAction.FAIL)]

        policy = LLMRetryPolicy("conn", "openai:gpt-4o", "be brief", rules, 12.0)

        assert (
            policy.llm_conn_id,
            policy.model_id,
            policy.instructions,
            policy.fallback_rules,
            policy.timeout,
        ) == (
            "conn",
            "openai:gpt-4o",
            "be brief",
            rules,
            12.0,
        )

    @pytest.mark.parametrize("kwarg", ["categories", "min_confidence", "fallback_policy"])
    def test_has_no_classifier_arguments(self, kwarg):
        """Those belong to ClassifierRetryPolicy; a text-model user never sees them."""
        with pytest.raises(TypeError, match="unexpected keyword argument"):
            LLMRetryPolicy(llm_conn_id="test", **{kwarg: None})

    @patch(HOOK, autospec=True)
    def test_model_output_type_is_error_classification(self, mock_hook_cls):
        _install(mock_hook_cls, _open_agent("auth", should_retry=False))
        LLMRetryPolicy(llm_conn_id="test").evaluate(PermissionError("403"), try_number=1, max_tries=3)

        kwargs = mock_hook_cls.return_value.create_agent.call_args.kwargs
        assert kwargs["output_type"] is ErrorClassification
        assert kwargs["instructions"] == DEFAULT_INSTRUCTIONS

    @patch(HOOK, autospec=True)
    def test_auth_error_returns_fail_with_the_models_reasoning(self, mock_hook_cls):
        _install(mock_hook_cls, _open_agent("auth", should_retry=False, reasoning="API key expired"))
        policy = LLMRetryPolicy(llm_conn_id="test")

        decision = policy.evaluate(PermissionError("403"), try_number=1, max_tries=3)

        assert decision.action == RetryAction.FAIL
        assert decision.reason == "auth: API key expired"

    @patch(HOOK, autospec=True)
    def test_model_chosen_delay_is_used(self, mock_hook_cls):
        _install(mock_hook_cls, _open_agent("rate_limit", should_retry=True, delay=120, reasoning="429"))
        policy = LLMRetryPolicy(llm_conn_id="test")

        decision = policy.evaluate(RuntimeError("429"), try_number=1, max_tries=3)

        assert decision.action == RetryAction.RETRY
        assert decision.retry_delay == timedelta(seconds=120)
        assert decision.reason == "rate_limit: 429"

    @pytest.mark.parametrize("delay", [0, -5])
    @patch(HOOK, autospec=True)
    def test_zero_or_negative_delay_leaves_the_task_backoff_in_charge(self, mock_hook_cls, delay):
        _install(mock_hook_cls, _open_agent("transient", should_retry=True, delay=delay))
        policy = LLMRetryPolicy(llm_conn_id="test")

        decision = policy.evaluate(RuntimeError("glitch"), try_number=1, max_tries=3)

        assert decision.action == RetryAction.RETRY
        assert decision.retry_delay is None

    @patch(HOOK, autospec=True)
    def test_custom_taxonomy_in_instructions_keeps_working(self, mock_hook_cls):
        """The 0.9.0 guide taught this: a category the prompt invents, with the model choosing the action."""
        _install(
            mock_hook_cls,
            _open_agent("warehouse_suspended", should_retry=True, delay=30, reasoning="auto-resume"),
        )
        policy = LLMRetryPolicy(
            llm_conn_id="test", instructions="'Warehouse suspended' -> warehouse_suspended, retry after 30s"
        )

        decision = policy.evaluate(RuntimeError("Warehouse X is suspended"), try_number=1, max_tries=3)

        assert decision.action == RetryAction.RETRY
        assert decision.retry_delay == timedelta(seconds=30)
        assert decision.reason == "warehouse_suspended: auto-resume"
        assert mock_hook_cls.return_value.create_agent.call_args.kwargs["instructions"].startswith(
            "'Warehouse suspended'"
        )

    def test_fallback_decisions_are_the_rules_verbatim(self):
        """No prefix on this path: the decision is what ExceptionRetryPolicy returns, as in 0.9.0."""
        policy = LLMRetryPolicy(
            llm_conn_id="nonexistent",
            fallback_rules=[
                RetryRule(
                    exception=ConnectionError, action=RetryAction.RETRY, retry_delay=timedelta(seconds=10)
                ),
                RetryRule(exception=PermissionError, action=RetryAction.FAIL, reason="auth fallback"),
            ],
        )

        retry = policy.evaluate(ConnectionError("refused"), try_number=1, max_tries=3)
        fail = policy.evaluate(PermissionError("denied"), try_number=1, max_tries=3)
        unmatched = policy.evaluate(ValueError("x"), try_number=1, max_tries=3)

        assert (retry.action, retry.retry_delay, retry.reason) == (
            RetryAction.RETRY,
            timedelta(seconds=10),
            "Matched rule for ConnectionError",
        )
        assert (fail.action, fail.reason) == (RetryAction.FAIL, "auth fallback")
        assert (unmatched.action, unmatched.reason) == (RetryAction.DEFAULT, None)

    def test_no_rules_falls_back_to_default_with_no_reason(self):
        decision = LLMRetryPolicy(llm_conn_id="nonexistent").evaluate(
            ValueError("x"), try_number=1, max_tries=3
        )

        assert decision == RetryDecision.default()

    @patch(HOOK, autospec=True)
    def test_classifier_refusal_logs_the_categories_hint(self, mock_hook_cls, caplog):
        """A classifier model refuses ErrorClassification's text fields; the log says what to do."""
        agent = _install(mock_hook_cls, MagicMock(spec=Agent))
        agent.run_sync.side_effect = RuntimeError("Output field 'reasoning' is not supported by this model")
        policy = LLMRetryPolicy(llm_conn_id="test", model_id="typesafe:jev-1.13.0")

        with caplog.at_level(logging.ERROR, logger="airflow.providers.common.ai.policies.retry"):
            decision = policy.evaluate(ValueError("x"), try_number=1, max_tries=3)

        assert decision.action == RetryAction.DEFAULT
        assert "use ClassifierRetryPolicy" in caplog.text

    def test_no_warning_for_any_instructions(self):
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            LLMRetryPolicy(
                llm_conn_id="test", instructions="'Statement queued' -> rate_limit, retry after 120s"
            )
            LLMRetryPolicy(llm_conn_id="test", instructions=DEFAULT_INSTRUCTIONS + " hints")
