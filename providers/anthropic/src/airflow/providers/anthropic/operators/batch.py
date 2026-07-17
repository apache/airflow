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

import time
from collections.abc import Sequence
from datetime import timedelta
from functools import cached_property
from typing import TYPE_CHECKING, Any

from airflow.providers.anthropic.exceptions import AnthropicBatchJobError, AnthropicBatchTimeout
from airflow.providers.anthropic.hooks.anthropic import (
    AnthropicHook,
    evaluate_batch_counts,
    validate_execute_complete_event,
)
from airflow.providers.anthropic.triggers.batch import AnthropicBatchTrigger
from airflow.providers.common.compat.sdk import BaseOperator, conf

if TYPE_CHECKING:
    from airflow.providers.common.compat.sdk import Context


class AnthropicBatchOperator(BaseOperator):
    """
    Submit an Anthropic Message Batch and wait for it to complete.

    Message Batches process many ``messages.create`` requests asynchronously at 50% of
    standard cost; most complete within an hour (24h SLA). This operator submits the
    batch and, in deferrable mode, releases the worker slot while a trigger polls for
    completion.

    The operator returns the **batch ID only** — never the results. Pull results with
    :meth:`~airflow.providers.anthropic.hooks.anthropic.AnthropicHook.stream_batch_results`
    and persist them to object storage; results can be very large and must not be pushed
    to XCom. Results are retained for 29 days after the batch is created.

    .. note::
        A retry re-submits a brand-new batch. Prefer ``retries=0`` on this task (the
        submitted ``batch_id`` is pushed to XCom under key ``batch_id`` immediately, so
        a crashed run never loses track of an in-flight batch).

    .. seealso::
        For more information, take a look at the guide:
        :ref:`howto/operator:AnthropicBatchOperator`

    :param requests: A list of ``{"custom_id": str, "params": {...}}`` dicts, where
        ``params`` is a ``messages.create`` payload (``model``, ``max_tokens``, ``messages``, ...).
        A request that omits ``model`` inherits ``model`` below, or the connection's
        ``default_model`` (``extra['model']``) when that is unset too.
    :param model: Default model id applied to requests that don't set their own. Lets you
        pick the batch's model once instead of repeating it in every request.
    :param conn_id: The Anthropic connection ID to use.
    :param deferrable: Run the operator in deferrable mode.
    :param poll_interval: Seconds between status checks, in both the synchronous and
        deferrable paths.
    :param timeout: Seconds to wait for the batch to reach a terminal status. Defaults to
        24 hours (the Message Batches SLA). In deferrable mode this also bounds the
        deferral; set ``execution_timeout`` only if you want a shorter hard cap (note a
        shorter ``execution_timeout`` preempts the graceful cancel-on-timeout path).
    :param wait_for_completion: Whether to wait for the batch to complete. If ``False``,
        the operator returns the batch ID immediately after submission.
    :param fail_on_partial_error: If ``True``, fail the task when any request errored or
        expired. Defaults to ``False`` (succeed and log a warning so the successful
        results are not discarded).
    """

    template_fields: Sequence[str] = ("requests", "model")

    def __init__(
        self,
        requests: list[dict[str, Any]],
        model: str | None = None,
        conn_id: str = AnthropicHook.default_conn_name,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        poll_interval: float = 60,
        timeout: float = 24 * 60 * 60,
        wait_for_completion: bool = True,
        fail_on_partial_error: bool = False,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.requests = requests
        self.model = model
        self.conn_id = conn_id
        self.deferrable = deferrable
        self.poll_interval = poll_interval
        self.timeout = timeout
        self.wait_for_completion = wait_for_completion
        self.fail_on_partial_error = fail_on_partial_error
        self.batch_id: str | None = None

    @cached_property
    def hook(self) -> AnthropicHook:
        """Return an instance of the AnthropicHook."""
        return AnthropicHook(conn_id=self.conn_id)

    def execute(self, context: Context) -> str | None:
        if not self.requests:
            raise ValueError("AnthropicBatchOperator requires at least one request; got an empty list.")
        batch = self.hook.create_batch(self.requests, model=self.model)
        self.batch_id = batch.id
        # Push immediately so a crash between submit and completion never loses the batch.
        context["ti"].xcom_push(key="batch_id", value=batch.id)
        self.log.info("Submitted Anthropic Message Batch %s (%d requests)", batch.id, len(self.requests))

        if not self.wait_for_completion:
            return self.batch_id

        if self.deferrable:
            self.defer(
                # Backstop the deferral slightly beyond the trigger's own end_time so the
                # trigger's clean "timeout" event (which cancels the batch) wins over a
                # generic AirflowTaskTimeout. A user-set execution_timeout still applies
                # as a shorter hard cap.
                timeout=self.execution_timeout or timedelta(seconds=self.timeout + self.poll_interval + 60),
                trigger=AnthropicBatchTrigger(
                    conn_id=self.conn_id,
                    batch_id=self.batch_id,
                    poll_interval=self.poll_interval,
                    end_time=time.time() + self.timeout,
                ),
                method_name="execute_complete",
            )

        self.log.info("Waiting for batch %s to complete", self.batch_id)
        try:
            batch = self.hook.wait_for_batch(
                self.batch_id, wait_seconds=self.poll_interval, timeout=self.timeout
            )
        except Exception:
            # Any failure after submission (timeout, SDK 5xx, auth expiry) leaves the batch
            # running and billing; cancel it best-effort before the task fails.
            self.log.warning("Batch %s failed while waiting; requesting cancellation.", self.batch_id)
            self._cancel_batch_quietly()
            raise
        counts = batch.request_counts
        self._apply_policy(counts.canceled, counts.errored, counts.expired, counts.succeeded)
        return self.batch_id

    def execute_complete(self, context: Context, event: Any = None) -> str:
        """
        Resume after the trigger fires.

        The deferred task is a fresh instance, so the batch ID is read from the event,
        not ``self.batch_id``.
        """
        event = validate_execute_complete_event(event)
        self.batch_id = event["batch_id"]
        status = event["status"]
        if status == "timeout":
            self.log.warning("Batch %s timed out; requesting cancellation.", self.batch_id)
            self._cancel_batch_quietly()
            raise AnthropicBatchTimeout(event["message"])
        if status == "error":
            # The trigger yields "error" when polling gives up (transient failures
            # exhausted or the deadline passed mid-poll) while the batch may still be
            # running; cancel it best-effort so it does not keep billing.
            self.log.warning("Batch %s errored while polling; requesting cancellation.", self.batch_id)
            self._cancel_batch_quietly()
            raise AnthropicBatchJobError(event["message"])

        counts = event.get("request_counts") or {}
        self._apply_policy(
            counts.get("canceled", 0),
            counts.get("errored", 0),
            counts.get("expired", 0),
            counts.get("succeeded", 0),
        )
        self.log.info("%s completed successfully.", self.task_id)
        return self.batch_id

    def _apply_policy(self, canceled: int, errored: int, expired: int, succeeded: int) -> None:
        evaluate_batch_counts(
            batch_id=self.batch_id,
            canceled=canceled,
            errored=errored,
            expired=expired,
            succeeded=succeeded,
            fail_on_partial_error=self.fail_on_partial_error,
        )

    def on_kill(self) -> None:
        """
        Cancel the batch if the (non-deferred) task is killed.

        This only fires while the worker process is alive, i.e. the synchronous path
        (``deferrable=False``). On Airflow 3.3+ a killed deferred task is cancelled by the
        trigger's ``on_kill``. On older Airflow the batch of a killed deferred task is not
        cancelled automatically; cancel it manually via the hook.
        """
        if self.batch_id:
            self.log.info("on_kill: cancelling Anthropic batch %s", self.batch_id)
            self._cancel_batch_quietly()

    def _cancel_batch_quietly(self) -> None:
        """Best-effort batch cancellation for the timeout and kill paths."""
        if not self.batch_id:
            return
        try:
            self.hook.cancel_batch(self.batch_id)
        except Exception as e:
            self.log.warning("Failed to cancel batch %s: %s", self.batch_id, e)
