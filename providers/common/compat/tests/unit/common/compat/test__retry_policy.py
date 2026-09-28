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

import inspect
from datetime import timedelta
from unittest.mock import MagicMock

import pytest

from airflow.providers.common.compat import sdk

from tests_common.test_utils.version_compat import AIRFLOW_V_3_3_PLUS, AIRFLOW_V_3_4_PLUS

pytestmark = pytest.mark.skipif(not AIRFLOW_V_3_3_PLUS, reason="Retry policies arrived in Airflow 3.3")

if AIRFLOW_V_3_3_PLUS:
    import structlog

    from airflow.providers.common.compat._retry_policy import ChainRetryPolicy
    from airflow.sdk import BaseOperator
    from airflow.sdk.api.datamodels._generated import TIRunContext
    from airflow.sdk.definitions import retry_policy as sdk_retry_policy
    from airflow.sdk.definitions.retry_policy import (
        ExceptionRetryPolicy,
        RetryAction,
        RetryDecision,
        RetryPolicy,
        RetryRule,
    )
    from airflow.sdk.execution_time.task_runner import RuntimeTaskInstance, _evaluate_retry_policy

RETRY_POLICY_NAMES = (
    "ChainRetryPolicy",
    "ExceptionRetryPolicy",
    "RetryAction",
    "RetryDecision",
    "RetryPolicy",
    "RetryRule",
)


@pytest.mark.parametrize("name", RETRY_POLICY_NAMES)
def test_retry_policy_names_are_exported(name):
    assert name in sdk.__all__


@pytest.mark.parametrize("name", [n for n in RETRY_POLICY_NAMES if n != "ChainRetryPolicy"])
def test_sdk_retry_policy_types_are_the_sdk_ones(name):
    assert getattr(sdk, name) is getattr(sdk_retry_policy, name)


@pytest.mark.skipif(not AIRFLOW_V_3_4_PLUS, reason="ChainRetryPolicy is in the SDK from Airflow 3.4")
def test_chain_is_the_sdk_class_when_the_sdk_has_it():
    assert sdk.ChainRetryPolicy is sdk_retry_policy.ChainRetryPolicy
    assert sdk.ChainRetryPolicy is not ChainRetryPolicy


def test_chain_falls_back_to_the_copy_when_the_sdk_lacks_it(monkeypatch):
    """
    On 3.3 the SDK module has no ``ChainRetryPolicy`` and removing it is a no-op, so this is the
    real path there. On 3.4 and later the attribute is removed to reach the same path.
    """
    monkeypatch.delattr(sdk_retry_policy, "ChainRetryPolicy", raising=False)

    assert sdk.ChainRetryPolicy is ChainRetryPolicy


@pytest.mark.skipif(not AIRFLOW_V_3_4_PLUS, reason="The SDK class to compare against exists from Airflow 3.4")
@pytest.mark.parametrize("method", ["__init__", "evaluate"])
def test_backport_matches_the_sdk_source(method):
    """The copy must not drift from the SDK class; only the docstrings may differ."""
    assert inspect.getsource(getattr(ChainRetryPolicy, method)) == inspect.getsource(
        getattr(sdk_retry_policy.ChainRetryPolicy, method)
    )


class TestChainRetryPolicyBackport:
    """
    The copy in this provider behaves like the SDK class.

    These tests import the copy directly, so they run on 3.4 as well, where the compat module hands
    out the SDK class and the copy is otherwise never imported. They mirror the SDK's behavioural
    cases in ``TestChainRetryPolicy`` so a divergence between the two shows up here.
    """

    @pytest.fixture(autouse=True)
    def _policies(self):
        self.fail_403 = ExceptionRetryPolicy(
            rules=[RetryRule(exception=PermissionError, action=RetryAction.FAIL, reason="never retry 403")]
        )
        self.floor = ExceptionRetryPolicy(
            rules=[
                RetryRule(
                    exception=ConnectionError,
                    action=RetryAction.RETRY,
                    retry_delay=timedelta(seconds=30),
                    reason="floor",
                )
            ]
        )

    @staticmethod
    def _policy(decision):
        class Fixed(RetryPolicy):
            def evaluate(self, exception, try_number, max_tries, context=None):
                return decision

        return Fixed()

    def test_needs_at_least_one_policy(self):
        with pytest.raises(ValueError, match="at least one policy"):
            ChainRetryPolicy([])

    def test_a_single_policy_must_be_wrapped_in_a_sequence(self):
        with pytest.raises(TypeError, match="wrap the single policy in a list"):
            ChainRetryPolicy(self.floor)  # type: ignore[arg-type]

    def test_members_must_be_retry_policies(self):
        with pytest.raises(TypeError, match=r"policies\[1\] must be a RetryPolicy, got str"):
            ChainRetryPolicy([self.floor, "rules"])  # type: ignore[list-item]

    def test_accepts_any_sequence(self):
        policy = ChainRetryPolicy((self.fail_403, self.floor))

        assert policy.policies == [self.fail_403, self.floor]

    # Action names rather than enum members: the parametrize runs at collection, before the skip applies.
    @pytest.mark.parametrize(
        ("first", "exc", "action", "reason"),
        [
            pytest.param(
                "fail_403", PermissionError("403"), "FAIL", "ExceptionRetryPolicy: never retry 403", id="fail"
            ),
            pytest.param(
                "floor", ConnectionError("refused"), "RETRY", "ExceptionRetryPolicy: floor", id="retry"
            ),
        ],
    )
    def test_first_policy_to_decide_wins_and_later_ones_are_not_consulted(self, first, exc, action, reason):
        consulted = []

        class Recording(RetryPolicy):
            def evaluate(self, exception, try_number, max_tries, context=None):
                consulted.append(True)
                return RetryDecision.fail(reason="should not run")

        policy = ChainRetryPolicy([getattr(self, first), Recording()])

        decision = policy.evaluate(exc, try_number=1, max_tries=3)

        assert decision.action == RetryAction[action]
        assert decision.reason == reason
        assert consulted == []

    def test_default_from_a_policy_moves_to_the_next(self):
        policy = ChainRetryPolicy([self.fail_403, self.floor])

        decision = policy.evaluate(ConnectionError("refused"), try_number=1, max_tries=3)

        assert decision.action == RetryAction.RETRY
        assert decision.retry_delay == timedelta(seconds=30)
        assert decision.reason == "ExceptionRetryPolicy: floor (after ExceptionRetryPolicy: no decision)"

    def test_matched_default_rule_passes_control_on(self):
        """A rule with action=DEFAULT is not a decision inside a chain, whatever its reason or delay."""
        soft = ExceptionRetryPolicy(
            rules=[
                RetryRule(
                    exception=ConnectionError,
                    action=RetryAction.DEFAULT,
                    retry_delay=timedelta(seconds=99),
                    reason="task settings, please",
                )
            ]
        )
        policy = ChainRetryPolicy([soft, self.floor])

        decision = policy.evaluate(ConnectionError("refused"), try_number=1, max_tries=3)

        assert decision.action == RetryAction.RETRY
        assert decision.retry_delay == timedelta(seconds=30)
        assert (
            decision.reason
            == "ExceptionRetryPolicy: floor (after ExceptionRetryPolicy: task settings, please)"
        )

    def test_fail_default_ends_the_chain(self):
        strict = ExceptionRetryPolicy(rules=[], default=RetryAction.FAIL)
        policy = ChainRetryPolicy([strict, self.floor])

        decision = policy.evaluate(ConnectionError("refused"), try_number=1, max_tries=3)

        assert decision.action == RetryAction.FAIL
        assert decision.reason == "ExceptionRetryPolicy: fail"

    def test_every_policy_abstaining_returns_default_with_the_trail_and_no_delay(self):
        soft = ExceptionRetryPolicy(
            rules=[
                RetryRule(exception=ValueError, action=RetryAction.DEFAULT, retry_delay=timedelta(seconds=99))
            ]
        )
        policy = ChainRetryPolicy([self.fail_403, soft])

        decision = policy.evaluate(ValueError("x"), try_number=1, max_tries=3)

        assert decision.action == RetryAction.DEFAULT
        assert decision.retry_delay is None
        assert decision.reason == (
            "no policy decided (ExceptionRetryPolicy: no decision; ExceptionRetryPolicy: Matched rule for ValueError)"
        )

    def test_a_policy_that_raises_is_logged_and_skipped(self, caplog):
        class Broken(RetryPolicy):
            def evaluate(self, exception, try_number, max_tries, context=None):
                raise RuntimeError("policy bug")

        policy = ChainRetryPolicy([Broken(), self.floor])

        with caplog.at_level("ERROR", logger="airflow.providers.common.compat._retry_policy"):
            decision = policy.evaluate(ConnectionError("refused"), try_number=1, max_tries=3)

        assert decision.action == RetryAction.RETRY
        assert decision.reason == "ExceptionRetryPolicy: floor (after Broken: raised RuntimeError)"
        assert "Broken raised while evaluating the retry policy" in caplog.text
        assert "policy bug" in caplog.text

    @pytest.mark.parametrize("returned", [None, "retry", {"action": "retry"}])
    def test_a_policy_returning_something_else_is_logged_and_skipped(self, returned, caplog):
        policy = ChainRetryPolicy([self._policy(returned), self.floor])

        with caplog.at_level("ERROR", logger="airflow.providers.common.compat._retry_policy"):
            decision = policy.evaluate(ConnectionError("refused"), try_number=1, max_tries=3)

        assert decision.action == RetryAction.RETRY
        assert decision.reason == "ExceptionRetryPolicy: floor (after Fixed: invalid decision)"
        assert "instead of a RetryDecision" in caplog.text

    def test_members_are_called_by_keyword_like_the_worker_does(self):
        class KeywordOnly(RetryPolicy):
            def evaluate(self, *, exception, try_number, max_tries, context=None):
                return RetryDecision.fail(reason="kw-only")

        decision = ChainRetryPolicy([KeywordOnly()]).evaluate(ValueError("x"), try_number=1, max_tries=3)

        assert decision.action == RetryAction.FAIL
        assert decision.reason == "KeywordOnly: kw-only"

    @pytest.mark.parametrize("exc", [KeyboardInterrupt, SystemExit])
    def test_base_exceptions_propagate(self, exc):
        class Cancelled(RetryPolicy):
            def evaluate(self, exception, try_number, max_tries, context=None):
                raise exc()

        policy = ChainRetryPolicy([Cancelled(), self.floor])

        with pytest.raises(exc):
            policy.evaluate(ConnectionError("refused"), try_number=1, max_tries=3)

    def test_every_policy_sees_the_original_exception_and_arguments(self):
        seen = []

        class Recording(RetryPolicy):
            def evaluate(self, exception, try_number, max_tries, context=None):
                seen.append((exception, try_number, max_tries, context))
                return RetryDecision.default()

        exc = ConnectionError("refused")
        ctx = {"params": {}}
        ChainRetryPolicy([Recording(), Recording()]).evaluate(exc, try_number=2, max_tries=5, context=ctx)

        assert seen == [(exc, 2, 5, ctx), (exc, 2, 5, ctx)]

    def test_decision_without_a_reason_names_the_action(self):
        policy = ChainRetryPolicy([self._policy(RetryDecision.retry(delay=timedelta(seconds=7)))])

        decision = policy.evaluate(ValueError("x"), try_number=1, max_tries=3)

        assert decision.retry_delay == timedelta(seconds=7)
        assert decision.reason == "Fixed: retry"

    def test_nested_chains_compose(self):
        inner = ChainRetryPolicy([self.fail_403])
        policy = ChainRetryPolicy([inner, self.floor])

        decision = policy.evaluate(ConnectionError("refused"), try_number=1, max_tries=3)

        assert decision.action == RetryAction.RETRY
        assert decision.reason == (
            "ExceptionRetryPolicy: floor (after ChainRetryPolicy: no policy decided (ExceptionRetryPolicy: no decision))"
        )

    def test_runs_through_the_task_runner(self):
        """The worker's own helper drives the copy the way it drives any policy."""
        ti = MagicMock(spec=RuntimeTaskInstance)
        ti.task = MagicMock(spec=BaseOperator)
        ti.task.retry_policy = ChainRetryPolicy([self.fail_403, self.floor])
        ti.try_number = 1
        ti._ti_context_from_server = MagicMock(spec=TIRunContext)
        ti._ti_context_from_server.max_tries = 3

        result = _evaluate_retry_policy(ti, ConnectionError("refused"), structlog.get_logger("test"))

        assert result.action == RetryAction.RETRY
        assert result.retry_delay == timedelta(seconds=30)
        assert result.reason == "ExceptionRetryPolicy: floor (after ExceptionRetryPolicy: no decision)"
