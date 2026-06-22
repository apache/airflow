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
Shared-stream SQS trigger and producer for event-driven scheduling.

Triggers that declare the same ``sqs_queue`` + ``aws_conn_id`` share a single
SQS consumer in the triggerer (one receive loop broadcast to every subscriber)
instead of polling the queue once each. The :class:`SqsSharedStreamProducer`
owns that consumer and deletes a message only after the derived
:class:`~airflow.triggers.base.TriggerEvent` instances have been persisted, via
the shared-stream ack channel.

SQS is explicit-delete: ``ReceiveMessage`` only makes a message invisible for
the visibility timeout, never removing it, so an unconfirmed message is
redelivered rather than lost. Deleting only after the event is persisted --
never on receipt -- is what keeps a triggerer crash from dropping a message the
queue still considers in-flight.

Requires an Airflow version whose ``airflow.triggers.shared_stream`` module
provides the producer-side ack channel.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator, Hashable, Iterator, Sequence
from contextlib import AsyncExitStack, suppress
from typing import Any, NamedTuple

from airflow.providers.amazon.aws.hooks.sqs import SqsHook
from airflow.triggers.base import BaseEventTrigger, TriggerEvent
from airflow.triggers.shared_stream import AdvanceItem, SharedStreamProducer

log = logging.getLogger(__name__)

# SQS caps batch entries (delete / change-visibility) at 10 per request.
_SQS_BATCH_LIMIT = 10


def _in_batches(items: Sequence[Any], size: int) -> Iterator[Sequence[Any]]:
    for start in range(0, len(items), size):
        yield items[start : start + size]


class SqsBrokerPayload(NamedTuple):
    """
    The ``broker_payload`` carried alongside each raw event from ``open_stream``.

    ``receipt_handle`` is what :meth:`SqsSharedStreamProducer.advance` deletes
    with and what the background renewer extends visibility with; it is bound at
    receive time and is not changed by visibility extension. ``message_id`` is
    the message's stable identity (unchanged across redelivery) kept for logging
    and tracing. ``group_id`` is the FIFO ``MessageGroupId`` used as the advance
    lane key; it is ``None`` for standard queues.
    """

    receipt_handle: str
    message_id: str
    group_id: str | None


class SqsSharedStreamProducer(SharedStreamProducer):
    """
    Broker-side half of a shared SQS stream running in ack mode.

    Drives one SQS consumer for a shared-stream group and deletes a message
    only after every subscriber that derived a ``TriggerEvent`` from it has had
    that event persisted -- the ack channel gates the delete, so a triggerer
    crash cannot drop a message the queue already considers in-flight.

    ``ReceiveMessage`` only makes a message invisible for the visibility timeout
    and never deletes it, so SQS is at-least-once by default. This producer keeps
    it that way by deleting exclusively from :meth:`advance`, never on receipt.

    While a message is outstanding (received, not yet resolved) a background
    task extends its visibility timeout before it lapses, so a slow subscriber
    does not cause SQS to redeliver a message that is still being processed.
    The visibility timeout therefore only bounds redelivery latency after a
    triggerer crash (when renewal stops), not how long a subscriber may take.

    Events a subscriber **rejects** (via ``reject_shared_stream_event``) are
    terminal and dropped (deleted without redelivery). Involuntary failures
    (ack timeout / overflow) reset the message's visibility to zero so SQS
    redelivers it immediately instead of waiting out the visibility timeout. A
    broadcast no subscriber was online for is left to lapse and be redelivered
    once a subscriber returns. Configure a redrive policy on the queue to
    dead-letter messages that fail repeatedly.

    :param sqs_queue: URL of the SQS queue the shared consumer receives from.
    :param aws_conn_id: AWS connection id, defaults to ``aws_default``.
    :param max_messages: Maximum messages to fetch per ``ReceiveMessage`` (1-10).
    :param wait_time_seconds: Long-poll wait per ``ReceiveMessage`` call.
    :param visibility_timeout: Visibility timeout applied at receive time and on
        every renewal. Renewal keeps the message invisible while it is
        outstanding, so this bounds redelivery latency after a crash, not
        subscriber processing time.
    :param region_name: AWS region of the queue.
    :param verify: ``botocore`` TLS verification flag passed to the hook.
    :param botocore_config: Optional ``botocore`` client config dict.
    """

    def __init__(
        self,
        *,
        sqs_queue: str,
        aws_conn_id: str | None = "aws_default",
        max_messages: int = 10,
        wait_time_seconds: int = 20,
        visibility_timeout: int = 120,
        region_name: str | None = None,
        verify: bool | str | None = None,
        botocore_config: dict | None = None,
    ) -> None:
        self.sqs_queue = sqs_queue
        self.aws_conn_id = aws_conn_id
        self.max_messages = max_messages
        self.wait_time_seconds = wait_time_seconds
        self.visibility_timeout = visibility_timeout
        self.region_name = region_name
        self.verify = verify
        self.botocore_config = botocore_config
        self._exit_stack: AsyncExitStack | None = None
        self._client: Any = None
        self._renew_task: asyncio.Task | None = None
        # Outstanding messages keyed by receipt handle: added on yield, removed
        # when advance resolves them. The renewer extends visibility for these.
        self._outstanding: dict[str, SqsBrokerPayload] = {}

    @property
    def hook(self) -> SqsHook:
        return SqsHook(
            aws_conn_id=self.aws_conn_id,
            region_name=self.region_name,
            verify=self.verify,
            config=self.botocore_config,
        )

    async def open_stream(self) -> AsyncIterator[tuple[Any, SqsBrokerPayload]]:
        """Open the consumer lazily and yield (body, SqsBrokerPayload) per message."""
        self._exit_stack = AsyncExitStack()
        self._client = await self._exit_stack.enter_async_context(await self.hook.get_async_conn())
        self._renew_task = asyncio.create_task(self._renew_visibility_loop())
        while True:
            response = await self._client.receive_message(
                QueueUrl=self.sqs_queue,
                MaxNumberOfMessages=self.max_messages,
                WaitTimeSeconds=self.wait_time_seconds,
                VisibilityTimeout=self.visibility_timeout,
                # Standard queues have no MessageGroupId; SQS simply omits it from
                # the response rather than erroring, so this is safe to request
                # unconditionally and lets one code path serve standard and FIFO.
                MessageSystemAttributeNames=["MessageGroupId"],
            )
            for message in response.get("Messages", []):
                payload = SqsBrokerPayload(
                    receipt_handle=message["ReceiptHandle"],
                    message_id=message["MessageId"],
                    group_id=message.get("Attributes", {}).get("MessageGroupId"),
                )
                self._outstanding[payload.receipt_handle] = payload
                yield message.get("Body"), payload

    def get_advance_lane(self, broker_payload: SqsBrokerPayload) -> Hashable:
        """
        Order advances per message group, falling back to per-message.

        A FIFO queue guarantees order within a ``MessageGroupId``, so messages
        sharing one must advance in fan-out order -- the group is the lane. A
        standard queue has no ordering and its deletes are independent, so each
        message gets its own lane (its receipt handle) for maximum parallelism
        with no head-of-line blocking.
        """
        return broker_payload.group_id or broker_payload.receipt_handle

    async def advance(self, batch: Sequence[AdvanceItem]) -> None:
        """
        Resolve each message in the batch independently.

        Deletes do not accumulate, so each item is handled on its own:

        * ``acked`` -- accepted and persisted; delete it.
        * ``rejected`` -- terminally refused; delete it (dropped, not redelivered).
        * ``failed`` -- an involuntary failure (ack timeout / overflow); reset its
          visibility to zero so SQS redelivers it immediately, rather than waiting
          out the remaining visibility timeout.
        * all-zero (a broadcast no subscriber was online for) -- leave it to lapse
          and be redelivered once a subscriber returns. Unlike a failure it is not
          reset to zero: an immediate redelivery would busy-loop while no
          subscriber is online, so letting it lapse gives one time to return.

        Every item is removed from the outstanding set first so the renewer stops
        extending its visibility. A delete or visibility reset the broker reports
        as failed is logged, not raised: the message simply lapses and is
        redelivered, preserving at-least-once.
        """
        to_delete: list[SqsBrokerPayload] = []
        to_redeliver: list[SqsBrokerPayload] = []
        for item in batch:
            payload = item.broker_payload
            outcome = item.outcome
            self._outstanding.pop(payload.receipt_handle, None)
            if outcome.failed:
                to_redeliver.append(payload)
            elif outcome.acked == 0 and outcome.rejected == 0:
                continue
            else:
                to_delete.append(payload)
        if to_delete:
            await self._delete(to_delete)
        if to_redeliver:
            await self._redeliver(to_redeliver)

    async def _delete(self, payloads: list[SqsBrokerPayload]) -> None:
        for chunk in _in_batches(payloads, _SQS_BATCH_LIMIT):
            entries = [
                {"Id": str(index), "ReceiptHandle": payload.receipt_handle}
                for index, payload in enumerate(chunk)
            ]
            response = await self._client.delete_message_batch(QueueUrl=self.sqs_queue, Entries=entries)
            failed = response.get("Failed", [])
            if failed:
                log.warning(
                    "Failed to delete %d SQS message(s); they will be redelivered: %s", len(failed), failed
                )

    async def _redeliver(self, payloads: list[SqsBrokerPayload]) -> None:
        """Reset visibility to zero so SQS redelivers the failed messages immediately."""
        for chunk in _in_batches(payloads, _SQS_BATCH_LIMIT):
            entries = [
                {"Id": str(index), "ReceiptHandle": payload.receipt_handle, "VisibilityTimeout": 0}
                for index, payload in enumerate(chunk)
            ]
            try:
                await self._client.change_message_visibility_batch(QueueUrl=self.sqs_queue, Entries=entries)
            except Exception:
                log.warning(
                    "Failed to reset SQS visibility for %d message(s); they will lapse instead",
                    len(entries),
                    exc_info=True,
                )

    async def _renew_visibility_loop(self) -> None:
        interval = max(1, self.visibility_timeout // 2)
        while True:
            await asyncio.sleep(interval)
            await self._renew_visibility()

    async def _renew_visibility(self) -> None:
        payloads = list(self._outstanding.values())
        for chunk in _in_batches(payloads, _SQS_BATCH_LIMIT):
            entries = [
                {
                    "Id": str(index),
                    "ReceiptHandle": payload.receipt_handle,
                    "VisibilityTimeout": self.visibility_timeout,
                }
                for index, payload in enumerate(chunk)
            ]
            try:
                await self._client.change_message_visibility_batch(QueueUrl=self.sqs_queue, Entries=entries)
            except Exception:
                log.warning("Failed to extend SQS visibility for %d message(s)", len(entries), exc_info=True)

    async def aclose(self) -> None:
        """Cancel the renewer and close the SQS client when the poll ends; best-effort."""
        task = self._renew_task
        self._renew_task = None
        if task is not None:
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task
        stack = self._exit_stack
        self._exit_stack = None
        if stack is not None:
            try:
                await stack.aclose()
            except Exception:
                log.warning("Failed to close SQS client", exc_info=True)
        self._client = None
        self._outstanding.clear()


class SqsSharedStreamTrigger(BaseEventTrigger):
    """
    Event-driven trigger that watches an SQS queue through a shared consumer.

    Triggers that declare the same ``sqs_queue`` + ``aws_conn_id`` share one
    underlying SQS consumer in the triggerer (a single receive loop broadcast
    to every subscriber). Each subscriber fires a ``TriggerEvent`` per message;
    override :meth:`filter_shared_stream` to fire only for the messages this
    trigger cares about.

    Designed to back an :class:`~airflow.sdk.AssetWatcher` for event-driven
    scheduling. A message is deleted only after the derived ``TriggerEvent`` is
    persisted -- never on receipt; see :class:`SqsSharedStreamProducer` for the
    at-least-once delete semantics.

    :param sqs_queue: URL of the SQS queue to watch.
    :param aws_conn_id: AWS connection id, defaults to ``aws_default``.
    :param max_messages: Maximum messages to fetch per ``ReceiveMessage`` (1-10).
    :param wait_time_seconds: Long-poll wait per ``ReceiveMessage`` call.
    :param visibility_timeout: Visibility timeout applied at receive time and on
        every renewal; see :class:`SqsSharedStreamProducer`.
    :param region_name: AWS region of the queue.
    :param verify: ``botocore`` TLS verification flag passed to the hook.
    :param botocore_config: Optional ``botocore`` client config dict.
    """

    def __init__(
        self,
        *,
        sqs_queue: str,
        aws_conn_id: str | None = "aws_default",
        max_messages: int = 10,
        wait_time_seconds: int = 20,
        visibility_timeout: int = 120,
        region_name: str | None = None,
        verify: bool | str | None = None,
        botocore_config: dict | None = None,
    ) -> None:
        super().__init__()
        self.sqs_queue = sqs_queue
        self.aws_conn_id = aws_conn_id
        self.max_messages = max_messages
        self.wait_time_seconds = wait_time_seconds
        self.visibility_timeout = visibility_timeout
        self.region_name = region_name
        self.verify = verify
        self.botocore_config = botocore_config

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            "airflow.providers.amazon.aws.triggers.shared_stream.SqsSharedStreamTrigger",
            {
                "sqs_queue": self.sqs_queue,
                "aws_conn_id": self.aws_conn_id,
                "max_messages": self.max_messages,
                "wait_time_seconds": self.wait_time_seconds,
                "visibility_timeout": self.visibility_timeout,
                "region_name": self.region_name,
                "verify": self.verify,
                "botocore_config": self.botocore_config,
            },
        )

    def shared_stream_key(self) -> Hashable:
        """Triggers on the same queue + connection share one consumer."""
        return ("sqs-shared-stream", self.sqs_queue, self.aws_conn_id, self.region_name)

    @classmethod
    def create_shared_stream_producer(cls, kwargs: dict[str, Any]) -> SqsSharedStreamProducer:
        return SqsSharedStreamProducer(
            sqs_queue=kwargs["sqs_queue"],
            aws_conn_id=kwargs["aws_conn_id"],
            max_messages=kwargs["max_messages"],
            wait_time_seconds=kwargs["wait_time_seconds"],
            visibility_timeout=kwargs["visibility_timeout"],
            region_name=kwargs.get("region_name"),
            verify=kwargs.get("verify"),
            botocore_config=kwargs.get("botocore_config"),
        )

    async def filter_shared_stream(self, shared_stream: AsyncIterator[Any]) -> AsyncIterator[TriggerEvent]:
        """Fire one ``TriggerEvent`` per message body. Override to filter or transform."""
        async for body in shared_stream:
            yield TriggerEvent(body)

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """
        Not supported -- this trigger runs only through the shared-stream manager.

        ``shared_stream_key`` always returns non-``None``, so the triggerer
        drives this trigger through :meth:`filter_shared_stream`; the shared
        group owns the SQS consumer and message deletes. There is no standalone
        path: deleting safely needs the ack channel to gate it on trigger-event
        persistence, which only the manager provides.
        """
        raise NotImplementedError(
            "SqsSharedStreamTrigger runs only through the triggerer's shared-stream "
            "manager (via filter_shared_stream); it has no standalone run() path."
        )
        yield  # pragma: no cover - marks this as an async generator
