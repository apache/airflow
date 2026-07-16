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
    BatchStatus,
    evaluate_batch_counts,
    validate_execute_complete_event,
)
from airflow.providers.anthropic.triggers.batch import AnthropicBatchTrigger
from airflow.providers.common.compat.sdk import BaseSensorOperator, conf

if TYPE_CHECKING:
    from airflow.providers.common.compat.sdk import Context


class AnthropicBatchSensor(BaseSensorOperator):
    """
    Wait for an already-submitted Anthropic Message Batch to reach a terminal status.

    Pairs with ``AnthropicBatchOperator(wait_for_completion=False)`` (or any out-of-band
    submission) for a fire-and-forget submit + re-entrant await. Because the sensor only
    polls an existing ``batch_id``, it is naturally idempotent across retries — unlike a
    submit step, retrying it never creates a new batch.

    On a terminal batch it applies the same policy as the operator: a fully-cancelled
    batch skips the task, and ``fail_on_partial_error`` controls whether errored/expired
    requests fail it.

    .. seealso::
        For more information, take a look at the guide:
        :ref:`howto/sensor:AnthropicBatchSensor`

    :param batch_id: The ID of the batch to wait for.
    :param conn_id: The Anthropic connection ID to use.
    :param deferrable: Run the sensor in deferrable mode (polls via a trigger).
    :param fail_on_partial_error: If ``True``, fail when any request errored or expired.
        Defaults to ``False`` (succeed and log a warning).
    """

    template_fields: Sequence[str] = ("batch_id",)

    def __init__(
        self,
        *,
        batch_id: str,
        conn_id: str = AnthropicHook.default_conn_name,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        fail_on_partial_error: bool = False,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.batch_id = batch_id
        self.conn_id = conn_id
        self.deferrable = deferrable
        self.fail_on_partial_error = fail_on_partial_error

    @cached_property
    def hook(self) -> AnthropicHook:
        """Return an instance of the AnthropicHook."""
        return AnthropicHook(conn_id=self.conn_id)

    def poke(self, context: Context) -> bool:
        batch = self.hook.get_batch(self.batch_id)
        if BatchStatus.is_in_progress(batch.processing_status):
            return False
        counts = batch.request_counts
        evaluate_batch_counts(
            batch_id=self.batch_id,
            canceled=counts.canceled,
            errored=counts.errored,
            expired=counts.expired,
            succeeded=counts.succeeded,
            fail_on_partial_error=self.fail_on_partial_error,
        )
        return True

    def execute(self, context: Context) -> None:
        if self.deferrable:
            self.defer(
                timeout=timedelta(seconds=self.timeout),
                trigger=AnthropicBatchTrigger(
                    conn_id=self.conn_id,
                    batch_id=self.batch_id,
                    poll_interval=self.poke_interval,
                    end_time=time.time() + self.timeout,
                ),
                method_name="execute_complete",
            )
        super().execute(context)

    def execute_complete(self, context: Context, event: Any = None) -> None:
        event = validate_execute_complete_event(event)
        status = event["status"]
        if status == "timeout":
            raise AnthropicBatchTimeout(event["message"])
        if status == "error":
            raise AnthropicBatchJobError(event["message"])
        counts = event.get("request_counts") or {}
        evaluate_batch_counts(
            batch_id=event["batch_id"],
            canceled=counts.get("canceled", 0),
            errored=counts.get("errored", 0),
            expired=counts.get("expired", 0),
            succeeded=counts.get("succeeded", 0),
            fail_on_partial_error=self.fail_on_partial_error,
        )
        self.log.info("Batch %s reached a terminal status.", event["batch_id"])
