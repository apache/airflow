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

import asyncio
import json
import time
import uuid

import pytest
from confluent_kafka import Consumer, Producer, TopicPartition

pytest.importorskip("airflow.triggers.shared_stream")

from airflow.models import Connection
from airflow.providers.apache.kafka.triggers.shared_stream import KafkaSharedStreamProducer
from airflow.triggers.shared_stream import AdvanceItem, AdvanceOutcome

BROKER = "broker:29092"


def _produce(topic: str, values: list[bytes]) -> None:
    """Produce ``values`` to partition 0 of ``topic`` so offsets are deterministic (0, 1, ...)."""
    producer = Producer({"bootstrap.servers": BROKER})
    for value in values:
        producer.produce(topic, value=value, partition=0)
    assert producer.flush(10) == 0


def _poll_one(consumer: Consumer, timeout: float):
    """Poll ``consumer`` until a non-error message arrives or ``timeout`` elapses."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        message = consumer.poll(1.0)
        if message is not None and message.error() is None:
            return message
    return None


async def _take(stream, n: int) -> list:
    out = []
    async for item in stream:
        out.append(item)
        if len(out) >= n:
            break
    return out


@pytest.mark.integration("kafka")
class TestKafkaSharedStreamProducerIntegration:
    @pytest.fixture(autouse=True)
    def setup_connections(self, create_connection_without_db):
        self.group = f"shared-stream-it-{uuid.uuid4().hex[:8]}"
        self.conn_id = "kafka_shared_stream_it"
        self.bad_conn_id = "kafka_shared_stream_it_autocommit"
        for conn_id, extra in (
            (
                self.conn_id,
                {
                    "bootstrap.servers": BROKER,
                    "group.id": self.group,
                    "enable.auto.commit": False,
                    "auto.offset.reset": "beginning",
                    "socket.timeout.ms": 10000,
                },
            ),
            (
                self.bad_conn_id,
                {
                    "bootstrap.servers": BROKER,
                    "group.id": f"{self.group}-bad",
                    "enable.auto.commit": True,
                    "auto.offset.reset": "beginning",
                },
            ),
        ):
            create_connection_without_db(
                Connection(
                    conn_id=conn_id,
                    conn_type="kafka",
                    extra=json.dumps(extra),
                )
            )

    @pytest.mark.asyncio
    async def test_open_stream_consumes_and_advance_commits_offsets(self):
        topic = f"shared-stream-it-commit-{uuid.uuid4().hex[:8]}"
        _produce(topic, [b"m0", b"m1", b"m2"])

        producer = KafkaSharedStreamProducer(topics=[topic], kafka_config_id=self.conn_id, poll_timeout=1)
        try:
            stream = producer.open_stream()
            items = await asyncio.wait_for(_take(stream, 3), timeout=30)
            values = [value for value, _ in items]
            payloads = [payload for _, payload in items]
            assert values == [b"m0", b"m1", b"m2"]
            assert [p.offset for p in payloads] == [0, 1, 2]

            await producer.advance(
                [AdvanceItem(payload, AdvanceOutcome(acked=1, failed=0)) for payload in payloads]
            )
            await stream.aclose()
        finally:
            await producer.aclose()

        # The committed offset for the group must have advanced past the three messages.
        verifier = Consumer(
            {"bootstrap.servers": BROKER, "group.id": self.group, "enable.auto.commit": False}
        )
        try:
            committed = verifier.committed([TopicPartition(topic, 0)], timeout=10)
            assert committed[0].offset == 3
        finally:
            verifier.close()

    @pytest.mark.asyncio
    async def test_advance_dead_letters_rejected_message(self):
        topic = f"shared-stream-it-dlq-{uuid.uuid4().hex[:8]}"
        dlq_topic = f"{topic}.dlq"
        _produce(topic, [b"to-dlq"])

        producer = KafkaSharedStreamProducer(
            topics=[topic], kafka_config_id=self.conn_id, poll_timeout=1, dlq_topic=dlq_topic
        )
        try:
            stream = producer.open_stream()
            ((value, payload),) = await asyncio.wait_for(_take(stream, 1), timeout=30)
            assert value == b"to-dlq"
            await producer.advance([AdvanceItem(payload, AdvanceOutcome(acked=0, failed=0, rejected=1))])
            await stream.aclose()
        finally:
            await producer.aclose()

        # The rejected message must be readable from the dead-letter topic.
        dlq_consumer = Consumer(
            {
                "bootstrap.servers": BROKER,
                "group.id": f"{self.group}-dlq-verify",
                "enable.auto.commit": False,
                "auto.offset.reset": "beginning",
            }
        )
        try:
            dlq_consumer.subscribe([dlq_topic])
            message = _poll_one(dlq_consumer, timeout=30)
            assert message is not None, "expected the rejected message on the DLQ topic"
            assert message.value() == b"to-dlq"
        finally:
            dlq_consumer.close()

    @pytest.mark.asyncio
    async def test_open_stream_rejects_connection_with_auto_commit_enabled(self):
        topic = f"shared-stream-it-autocommit-{uuid.uuid4().hex[:8]}"
        producer = KafkaSharedStreamProducer(topics=[topic], kafka_config_id=self.bad_conn_id, poll_timeout=1)
        with pytest.raises(ValueError, match="enable.auto.commit=false"):
            await anext(producer.open_stream())

    @pytest.mark.asyncio
    async def test_advance_seeks_back_so_failed_message_is_redelivered(self):
        topic = f"shared-stream-it-seek-{uuid.uuid4().hex[:8]}"
        _produce(topic, [b"m0", b"m1", b"m2"])

        producer = KafkaSharedStreamProducer(topics=[topic], kafka_config_id=self.conn_id, poll_timeout=1)
        try:
            stream = producer.open_stream()
            items = await asyncio.wait_for(_take(stream, 3), timeout=30)
            payloads = [payload for _, payload in items]
            assert [p.offset for p in payloads] == [0, 1, 2]

            # offset 0 acked, offset 1 failed: commit stops at 1 and the consumer
            # is sought back to 1 so the failed message is redelivered in-session.
            await producer.advance(
                [
                    AdvanceItem(payloads[0], AdvanceOutcome(acked=1, failed=0)),
                    AdvanceItem(payloads[1], AdvanceOutcome(acked=0, failed=1)),
                ]
            )

            # The next poll re-reads offset 1 (m1), proving the seek redelivered it.
            ((value, payload),) = await asyncio.wait_for(_take(stream, 1), timeout=30)
            assert payload.offset == 1
            assert value == b"m1"
            await stream.aclose()
        finally:
            await producer.aclose()

        # The committed offset advanced only to 1 (past the acked 0); it never
        # moved past the failed offset 1.
        verifier = Consumer(
            {"bootstrap.servers": BROKER, "group.id": self.group, "enable.auto.commit": False}
        )
        try:
            committed = verifier.committed([TopicPartition(topic, 0)], timeout=10)
            assert committed[0].offset == 1
        finally:
            verifier.close()

    @pytest.mark.asyncio
    async def test_advance_seeks_to_earliest_held_offset_across_batches(self):
        topic = f"shared-stream-it-floor-{uuid.uuid4().hex[:8]}"
        _produce(topic, [b"m0", b"m1", b"m2", b"m3"])

        producer = KafkaSharedStreamProducer(topics=[topic], kafka_config_id=self.conn_id, poll_timeout=1)
        try:
            stream = producer.open_stream()
            items = await asyncio.wait_for(_take(stream, 4), timeout=30)
            payloads = [payload for _, payload in items]
            assert [p.offset for p in payloads] == [0, 1, 2, 3]

            # First batch: 0 acked, 1 failed -> floor 1. Second batch: 2 acked, 3
            # failed -> the floor must stay at the earliest held 1, not move to 3.
            await producer.advance(
                [
                    AdvanceItem(payloads[0], AdvanceOutcome(acked=1, failed=0)),
                    AdvanceItem(payloads[1], AdvanceOutcome(acked=0, failed=1)),
                ]
            )
            await producer.advance(
                [
                    AdvanceItem(payloads[2], AdvanceOutcome(acked=1, failed=0)),
                    AdvanceItem(payloads[3], AdvanceOutcome(acked=0, failed=1)),
                ]
            )

            # The next poll re-reads offset 1 (the earliest held), not 3 -- seeking
            # forward to 3 would have lost 1 and 2.
            ((value, payload),) = await asyncio.wait_for(_take(stream, 1), timeout=30)
            assert payload.offset == 1
            assert value == b"m1"
            await stream.aclose()
        finally:
            await producer.aclose()

        # The committed offset never moved past the earliest held offset 1.
        verifier = Consumer(
            {"bootstrap.servers": BROKER, "group.id": self.group, "enable.auto.commit": False}
        )
        try:
            committed = verifier.committed([TopicPartition(topic, 0)], timeout=10)
            assert committed[0].offset == 1
        finally:
            verifier.close()
