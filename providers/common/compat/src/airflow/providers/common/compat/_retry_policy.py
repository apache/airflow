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
"""
Backport of ``airflow.sdk.definitions.retry_policy.ChainRetryPolicy`` for Airflow 3.3.

Retry policies arrived in Airflow 3.3 and ``ChainRetryPolicy`` in 3.4. This copy lets a Dag that
imports it from :mod:`airflow.providers.common.compat.sdk` run on 3.3 with the same behaviour. That
module tries the SDK first, so wherever the SDK has the class (3.4 and later) it hands out the SDK
class and this one is never imported.

The worker only calls ``evaluate`` on whatever ``retry_policy`` holds, so a policy built from this
class behaves the same as one built from the SDK class. Keep the logic here in step with the SDK;
the compat tests compare the source of both classes and fail on drift.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from airflow.sdk.definitions.retry_policy import RetryAction, RetryDecision, RetryPolicy

if TYPE_CHECKING:
    from collections.abc import Sequence

    from airflow.sdk.definitions.context import Context

log = logging.getLogger(__name__)

__all__ = ["ChainRetryPolicy"]


class ChainRetryPolicy(RetryPolicy):
    """
    Consult policies in order; the first RETRY or FAIL wins.

    Every policy sees the original task exception. A policy that returns DEFAULT has nothing
    to add, whatever reason it attached, and the next one is asked. When every policy returns
    DEFAULT the chain does too, and the task's own ``retries`` and ``retry_delay`` apply.

    This makes the order of a fallback explicit and lets any policies compose, including ones
    from different packages::

        retry_policy = ChainRetryPolicy(
            [
                # Known failures first, so no model call is spent on them.
                ExceptionRetryPolicy(rules=[RetryRule(exception=PermissionError, action=RetryAction.FAIL)]),
                # A policy that consults a model (airflow.providers.common.ai.policies.retry).
                LLMRetryPolicy(llm_conn_id="pydanticai_default"),
                # The floor when the model is unreachable.
                ExceptionRetryPolicy(
                    rules=[RetryRule(exception=ConnectionError, retry_delay=timedelta(seconds=30))]
                ),
            ]
        )

    A policy that raises an ordinary exception, or returns something other than a
    :class:`~airflow.sdk.definitions.retry_policy.RetryDecision`, is logged and treated as DEFAULT.
    ``BaseException`` propagates.

    The winning decision's reason names the policy that decided, then what every earlier policy
    said: ``HTTPStatusRetryPolicy: HTTP 404 (after ExceptionRetryPolicy: no decision)``. The
    worker stores it as ``retry_reason`` on a RETRY and logs it otherwise.

    :param policies: The policies to consult, in order. At least one.
    """

    def __init__(self, policies: Sequence[RetryPolicy]) -> None:
        if isinstance(policies, RetryPolicy):
            raise TypeError(
                "ChainRetryPolicy takes a sequence of policies; wrap the single policy in a list."
            )
        policies = list(policies)
        if not policies:
            raise ValueError("ChainRetryPolicy needs at least one policy.")
        for position, policy in enumerate(policies):
            if not isinstance(policy, RetryPolicy):
                raise TypeError(
                    f"ChainRetryPolicy policies[{position}] must be a RetryPolicy, got {type(policy).__name__}."
                )
        self.policies = policies

    def evaluate(
        self,
        exception: BaseException,
        try_number: int,
        max_tries: int,
        context: Context | None = None,
    ) -> RetryDecision:
        trail: list[str] = []
        for policy in self.policies:
            name = type(policy).__name__
            try:
                decision = policy.evaluate(
                    exception=exception, try_number=try_number, max_tries=max_tries, context=context
                )
            except Exception as exc:
                log.exception("%s raised while evaluating the retry policy; treated as no decision", name)
                trail.append(f"{name}: raised {type(exc).__name__}")
                continue
            if not isinstance(decision, RetryDecision):
                log.error("%s returned %r instead of a RetryDecision; treated as no decision", name, decision)
                trail.append(f"{name}: invalid decision")
                continue
            if decision.action is RetryAction.DEFAULT:
                trail.append(f"{name}: {decision.reason or 'no decision'}")
                continue
            reason = f"{name}: {decision.reason or decision.action.value}"
            if trail:
                reason = f"{reason} (after {'; '.join(trail)})"
            return RetryDecision(action=decision.action, retry_delay=decision.retry_delay, reason=reason)
        # The worker logs this reason as the policy decision; it is not stored, since nothing is retried by it.
        return RetryDecision(action=RetryAction.DEFAULT, reason=f"no policy decided ({'; '.join(trail)})")
