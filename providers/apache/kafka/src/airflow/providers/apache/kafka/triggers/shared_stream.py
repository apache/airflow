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
Shared-stream Kafka trigger and producer for event-driven scheduling.

Triggers that declare the same ``topics`` + ``kafka_config_id`` share a
single Kafka consumer in the triggerer (one poll loop broadcast to every
subscriber) instead of opening one consumer each.
The :class:`KafkaSharedStreamProducer` owns that consumer and commits
offsets only after the derived :class:`~airflow.triggers.base.TriggerEvent`
instances have been persisted, via the shared-stream ack channel.

Requires an Airflow version whose ``airflow.triggers.shared_stream`` module
provides the producer-side ack channel.
"""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator, Hashable, Sequence
from typing import TYPE_CHECKING, Any, NamedTuple, cast

from asgiref.sync import sync_to_async
from confluent_kafka import TopicPartition

from airflow.providers.apache.kafka.hooks.consume import KafkaConsumerHook
from airflow.providers.apache.kafka.hooks.produce import KafkaProducerHook
from airflow.triggers.base import BaseEventTrigger, TriggerEvent
from airflow.triggers.shared_stream import AdvanceItem, SharedStreamProducer

if TYPE_CHECKING:
    from confluent_kafka import Consumer, Producer

log = logging.getLogger(__name__)


class KafkaBrokerPayload(NamedTuple):
    """
    The ``broker_payload`` carried alongside each raw event from ``open_stream``.

    ``(topic, partition, offset)`` let :meth:`KafkaSharedStreamProducer.advance`
    commit the right ``TopicPartition`` and
    :meth:`KafkaSharedStreamProducer.get_advance_lane` key the advance by
    partition. ``value`` / ``key`` are populated only when a ``dlq_topic`` is
    configured -- they are what a dead-letter send re-publishes, so retaining
    them through the whole outstanding window is pointless without a DLQ.
    """

    topic: str
    partition: int
    offset: int
    value: Any = None
    key: Any = None


class KafkaSharedStreamProducer(SharedStreamProducer):
    """
    Broker-side half of a shared Kafka stream running in ack mode.

    Drives one confluent-kafka ``Consumer`` for a shared-stream group and
    commits offsets only after every subscriber that derived a
    ``TriggerEvent`` from a message has had it persisted -- the ack channel
    gates the commit, so a triggerer crash cannot drop a message the broker
    already considers delivered.

    .. warning::

        The Kafka connection used by ``kafka_config_id`` **must** set
        ``enable.auto.commit=false``. With auto-commit on, the consumer
        commits offsets on its own schedule regardless of the ack channel,
        which defeats the persistence gate and loses messages on a triggerer
        crash.

    Events that a subscriber **rejects** (via ``reject_shared_stream_event``) are
    terminal: by default they are dropped (committed past). Set ``dlq_topic``
    to instead re-publish each rejected message to a dead-letter topic before
    committing past it. Involuntary failures (ack timeout / overflow) and
    broadcasts no subscriber was online for are never dropped -- the offset floor
    is held and the consumer is sought back so the message is redelivered this
    session: a failure to the remaining healthy subscribers, a zero-subscriber
    broadcast to whoever subscribes next.

    :param topics: Topics the shared consumer subscribes to.
    :param kafka_config_id: Kafka connection id, defaults to ``kafka_default``.
    :param poll_timeout: Seconds the consumer waits on each ``poll`` call.
    :param dlq_topic: Optional dead-letter topic for rejected messages, produced
        through ``kafka_config_id``. When unset, rejected messages are dropped.
    """

    def __init__(
        self,
        *,
        topics: Sequence[str],
        kafka_config_id: str = "kafka_default",
        poll_timeout: float = 1.0,
        dlq_topic: str | None = None,
    ) -> None:
        self.topics = list(topics)
        self.kafka_config_id = kafka_config_id
        self.poll_timeout = poll_timeout
        self.dlq_topic = dlq_topic
        self._consumer: Consumer | None = None
        self._dlq_producer: Producer | None = None
        # lane (topic, partition) -> earliest offset held for redelivery. An
        # involuntary failure / zero-subscriber broadcast must not be committed
        # past until it is reprocessed; the floor is carried across advance()
        # calls so a later batch's cumulative commit cannot overtake it.
        self._floor: dict[Hashable, int] = {}

    async def open_stream(self) -> AsyncIterator[tuple[Any, KafkaBrokerPayload]]:
        """Open the consumer lazily and yield (value, KafkaBrokerPayload) per message."""
        hook = KafkaConsumerHook(topics=self.topics, kafka_config_id=self.kafka_config_id)
        await self._ensure_manual_commit(hook)
        consumer = await sync_to_async(hook.get_consumer)()
        self._consumer = consumer
        poll = sync_to_async(consumer.poll)
        keep_for_dlq = self.dlq_topic is not None
        while True:
            message = await poll(self.poll_timeout)
            if message is None:
                continue
            if message.error():
                raise RuntimeError(f"Kafka consumer error: {message.error()}")
            value = message.value()
            payload = KafkaBrokerPayload(
                topic=cast("str", message.topic()),
                partition=cast("int", message.partition()),
                offset=cast("int", message.offset()),
                value=value if keep_for_dlq else None,
                key=message.key() if keep_for_dlq else None,
            )
            yield value, payload

    async def _ensure_manual_commit(self, hook: KafkaConsumerHook) -> None:
        """
        Refuse to start unless the connection disables Kafka auto-commit.

        The shared-stream ack channel owns offset commits -- it commits only
        after the derived trigger events are persisted. ``enable.auto.commit``
        defaults to ``true`` in Kafka; left on, the consumer commits on its own
        schedule regardless of the ack channel and silently drops messages on a
        triggerer crash. Fail fast rather than run in a lossy configuration.
        """
        connection = await hook.aget_connection(self.kafka_config_id)
        auto_commit = connection.extra_dejson.get("enable.auto.commit", True)
        if str(auto_commit).strip().lower() != "false":
            raise ValueError(
                f"KafkaSharedStreamProducer requires enable.auto.commit=false in the "
                f"{self.kafka_config_id!r} Kafka connection; the shared-stream ack channel "
                f"commits offsets only after trigger events are persisted "
                f"(got enable.auto.commit={auto_commit!r})."
            )

    def get_advance_lane(self, broker_payload: KafkaBrokerPayload) -> Hashable:
        """Order commits per (topic, partition)."""
        return broker_payload.topic, broker_payload.partition

    async def advance(self, batch: Sequence[AdvanceItem]) -> None:
        """
        Commit one partition's resolved prefix; hold and seek back the first held offset.

        Every item shares one ``(topic, partition)`` (the lane). A Kafka commit
        is cumulative, so committing offset ``N + 1`` marks everything up to
        ``N`` as consumed. Within a batch we commit only through the last item
        that was *terminally handled* and stop at the first that should come back:

        * ``acked`` -- accepted; safe to commit past.
        * ``rejected`` -- terminally refused. If ``dlq_topic`` is set the message
          is re-published there (and flushed) before being committed past;
          otherwise it is dropped. Either way it is not redelivered.
        * ``failed`` (ack timeout / overflow) or all-zero (a broadcast no
          subscriber was online for) -- hold the floor here and ``seek`` the
          consumer back to it, so it and everything after are redelivered this
          session. A failure lands on the healthy subscribers the manager left
          online; a zero-subscriber broadcast reaches whoever subscribes next,
          instead of waiting for a full consumer rebuild. The floor lifts once a
          re-read batch acks through the held offset.

        The floor is per-lane and carried across calls (``self._floor``): once a
        lane holds at offset ``H``, no later batch may commit past ``H`` until a
        re-read batch starting at or before ``H`` acks through it.

        The batch's rejected messages are produced to the DLQ together and
        flushed once, before the offset is committed, so a crash cannot commit
        past a message that never reached the DLQ. If the commit later fails the
        batch is redelivered, which may re-send an already-dead-lettered message
        -- DLQ consumers should tolerate duplicates.
        """
        first = batch[0].broker_payload
        lane = self.get_advance_lane(first)
        commit_through: int | None = None
        held_offset: int | None = None
        to_dlq: list[KafkaBrokerPayload] = []
        for item in batch:
            payload = item.broker_payload
            outcome = item.outcome
            if outcome.failed > 0 or (outcome.acked == 0 and outcome.rejected == 0):
                # Held: an involuntary failure, or a broadcast no subscriber took.
                # Seek back so it -- and everything after -- is redelivered this
                # session, to the subscribers the manager left online or to
                # whoever subscribes next.
                held_offset = payload.offset
                break
            if outcome.rejected and self.dlq_topic is not None:
                to_dlq.append(payload)
            commit_through = payload.offset

        floor = self._resolve_floor(lane, first.offset, commit_through, held_offset)

        if to_dlq and self.dlq_topic is not None:
            await sync_to_async(self._dead_letter)(self.dlq_topic, to_dlq)
        if commit_through is not None:
            target = commit_through + 1
            if floor is not None:
                # floor may be a previous batch's held offset, so commit_through
                # can exceed it. Use minimal value to avoid it.
                target = min(target, floor)
            await sync_to_async(self._commit)(first.topic, first.partition, target)
        if held_offset is not None:
            # Seek to the lane's floor (the earliest unresolved held offset), not
            # this batch's break point: an earlier batch may hold a smaller offset,
            # and seeking forward to a later one would skip and lose it.
            seek_target = floor if floor is not None else held_offset
            await sync_to_async(self._seek)(first.topic, first.partition, seek_target)

    def _resolve_floor(
        self, lane: Hashable, batch_start: int, commit_through: int | None, held_offset: int | None
    ) -> int | None:
        """
        Update and return this lane's redelivery floor.

        The floor is the earliest unresolved held offset on the lane: the minimum
        of any still-unresolved previous floor and this batch's held offset.
        A previous floor lifts only when this batch starts at or before it and acks
        consecutively through it.
        A later arrived ack before the seek operation, or a failure at a later offset can
        neither clear nor advance a floor whose offset was never reprocessed.
        """
        previous = self._floor.get(lane)
        cleared = (
            previous is not None
            and batch_start <= previous
            and commit_through is not None
            and commit_through >= previous
        )
        candidates: list[int] = []
        if previous is not None and not cleared:
            candidates.append(previous)
        if held_offset is not None:
            candidates.append(held_offset)
        floor: int | None = min(candidates) if candidates else None
        if floor is None:
            self._floor.pop(lane, None)
        else:
            self._floor[lane] = floor
        return floor

    def _commit(self, topic: str, partition: int, offset: int) -> None:
        if self._consumer is None:
            log.warning("Cannot commit %s[%d]@%d: no open consumer", topic, partition, offset)
            return
        self._consumer.commit(offsets=[TopicPartition(topic, partition, offset)], asynchronous=False)

    def _seek(self, topic: str, partition: int, offset: int) -> None:
        if self._consumer is None:
            log.warning("Cannot seek %s[%d] to %d: no open consumer", topic, partition, offset)
            return
        self._consumer.seek(TopicPartition(topic, partition, offset))

    def _dead_letter(self, topic: str, payloads: list[KafkaBrokerPayload]) -> None:
        if self._dlq_producer is None:
            self._dlq_producer = KafkaProducerHook(kafka_config_id=self.kafka_config_id).get_producer()
        producer = self._dlq_producer
        for payload in payloads:
            producer.produce(topic, value=payload.value, key=payload.key)
        # Flush once so the whole dead-letter batch is durable before we commit past it.
        producer.flush()

    async def aclose(self) -> None:
        """Flush the DLQ producer and close the consumer when the poll ends; best-effort."""
        producer = self._dlq_producer
        if producer is not None:
            self._dlq_producer = None
            try:
                await sync_to_async(producer.flush)()
            except Exception:
                log.warning("Failed to flush Kafka DLQ producer", exc_info=True)
        consumer = self._consumer
        if consumer is not None:
            self._consumer = None
            try:
                await sync_to_async(consumer.close)()
            except Exception:
                log.warning("Failed to close Kafka consumer", exc_info=True)


class KafkaSharedStreamTrigger(BaseEventTrigger):
    """
    Event-driven trigger that watches Kafka topics through a shared consumer.

    Triggers that declare the same ``topics`` + ``kafka_config_id`` share one
    underlying Kafka consumer in the triggerer (a single poll loop broadcast
    to every subscriber). Each subscriber fires a ``TriggerEvent`` per
    message; override :meth:`filter_shared_stream` to fire only for the
    messages this trigger cares about.

    Designed to back an :class:`~airflow.sdk.AssetWatcher` for event-driven
    scheduling. The offset is committed only after the derived
    ``TriggerEvent`` is persisted -- see :class:`KafkaSharedStreamProducer`
    for the ``enable.auto.commit=false`` requirement.

    :param topics: Topics to watch.
    :param kafka_config_id: Kafka connection id, defaults to ``kafka_default``.
    :param poll_timeout: Seconds the consumer waits on each ``poll`` call.
    :param dlq_topic: Optional dead-letter topic for messages a subscriber
        rejects; see :class:`KafkaSharedStreamProducer`. When unset, rejected
        messages are dropped.
    """

    def __init__(
        self,
        *,
        topics: Sequence[str],
        kafka_config_id: str = "kafka_default",
        poll_timeout: float = 1.0,
        dlq_topic: str | None = None,
    ) -> None:
        super().__init__()
        # Sort once here so shared_stream_key() is order-independent without
        # re-sorting on every call: triggers on the same topics in any order
        # share one consumer.
        self.topics = sorted(topics)
        self.kafka_config_id = kafka_config_id
        self.poll_timeout = poll_timeout
        self.dlq_topic = dlq_topic

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            "airflow.providers.apache.kafka.triggers.shared_stream.KafkaSharedStreamTrigger",
            {
                "topics": self.topics,
                "kafka_config_id": self.kafka_config_id,
                "poll_timeout": self.poll_timeout,
                "dlq_topic": self.dlq_topic,
            },
        )

    def shared_stream_key(self) -> Hashable:
        """Triggers on the same topics + connection share one consumer."""
        return "kafka-shared-stream", tuple(self.topics), self.kafka_config_id

    @classmethod
    def create_shared_stream_producer(cls, kwargs: dict[str, Any]) -> KafkaSharedStreamProducer:
        return KafkaSharedStreamProducer(
            topics=kwargs["topics"],
            kafka_config_id=kwargs["kafka_config_id"],
            poll_timeout=kwargs["poll_timeout"],
            dlq_topic=kwargs.get("dlq_topic"),
        )

    async def filter_shared_stream(self, shared_stream: AsyncIterator[Any]) -> AsyncIterator[TriggerEvent]:
        """Fire one ``TriggerEvent`` per message. Override to filter or transform."""
        async for value in shared_stream:
            yield TriggerEvent(self._decode(value))

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """
        Not supported -- this trigger runs only through the shared-stream manager.

        ``shared_stream_key`` always returns non-``None``, so the triggerer drives
        this trigger through :meth:`filter_shared_stream`; the ``_SharedStreamGroup``
        owns the Kafka consumer and offset commits. There is no standalone path:
        committing offsets safely needs the ack channel to gate them on
        trigger-event persistence, which only the manager provides.
        """
        raise NotImplementedError(
            "KafkaSharedStreamTrigger runs only through the triggerer's shared-stream "
            "manager (via filter_shared_stream); it has no standalone run() path."
        )
        yield  # pragma: no cover - marks this as an async generator

    @staticmethod
    def _decode(value: Any) -> Any:
        return value.decode("utf-8") if isinstance(value, bytes) else value
