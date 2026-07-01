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

import pytest

pytest.importorskip("airflow.triggers.shared_stream")

from airflow.providers.apache.kafka.hooks.consume import KafkaConsumerHook
from airflow.providers.apache.kafka.hooks.produce import KafkaProducerHook
from airflow.providers.apache.kafka.triggers.shared_stream import (
    KafkaBrokerPayload,
    KafkaSharedStreamProducer,
    KafkaSharedStreamTrigger,
)
from airflow.triggers.base import TriggerEvent
from airflow.triggers.shared_stream import AdvanceItem, AdvanceOutcome


class _FakeMessage:
    def __init__(self, value, topic="orders", partition=0, offset=0, error=None, key=None):
        self._value = value
        self._topic = topic
        self._partition = partition
        self._offset = offset
        self._error = error
        self._key = key

    def value(self):
        return self._value

    def key(self):
        return self._key

    def topic(self):
        return self._topic

    def partition(self):
        return self._partition

    def offset(self):
        return self._offset

    def error(self):
        return self._error


class _FakeConsumer:
    """A minimal stand-in for confluent_kafka.Consumer (all calls are synchronous)."""

    def __init__(self, messages=()):
        self._messages = list(messages)
        self.committed: list = []
        self.seeks: list = []
        self.closed = False

    def poll(self, timeout):
        return self._messages.pop(0) if self._messages else None

    def commit(self, offsets, asynchronous):
        self.committed.append(offsets)

    def seek(self, partition):
        self.seeks.append(partition)

    def close(self):
        self.closed = True


class _FakeProducer:
    """A minimal stand-in for confluent_kafka.Producer."""

    def __init__(self):
        self.produced: list = []
        self.flushed = 0

    def produce(self, topic, value=None, key=None):
        self.produced.append((topic, value, key))

    def flush(self, *args, **kwargs):
        self.flushed += 1


def _item(topic, partition, offset, outcome, value=b"v", key=None):
    return AdvanceItem(KafkaBrokerPayload(topic, partition, offset, value, key), outcome)


def _acked(topic, partition, offset):
    return _item(topic, partition, offset, AdvanceOutcome(acked=1, failed=0))


def _failed(topic, partition, offset):
    return _item(topic, partition, offset, AdvanceOutcome(acked=0, failed=1))


def _rejected(topic, partition, offset, value=b"v", key=None):
    return _item(
        topic, partition, offset, AdvanceOutcome(acked=0, failed=0, rejected=1), value=value, key=key
    )


def _zero_subscriber(topic, partition, offset):
    return _item(topic, partition, offset, AdvanceOutcome(acked=0, failed=0))


def _patch_connection(mocker, auto_commit="false"):
    """Patch the connection so the manual-commit check reads ``enable.auto.commit``."""
    conn = mocker.MagicMock(extra_dejson={"enable.auto.commit": auto_commit})
    mocker.patch.object(
        KafkaConsumerHook, "aget_connection", new_callable=mocker.AsyncMock, return_value=conn
    )
    return conn


class TestKafkaSharedStreamProducer:
    @pytest.mark.asyncio
    async def test_open_stream_yields_value_and_broker_payload(self, mocker):
        consumer = _FakeConsumer([_FakeMessage(b"hello", topic="orders", partition=2, offset=42)])
        mocker.patch.object(KafkaConsumerHook, "get_consumer", return_value=consumer)
        _patch_connection(mocker)

        producer = KafkaSharedStreamProducer(topics=["orders"])
        stream = producer.open_stream()
        value, broker_payload = await anext(stream)
        await stream.aclose()

        assert value == b"hello"
        # No dlq_topic -> value/key are not retained in the broker payload.
        assert broker_payload == KafkaBrokerPayload("orders", 2, 42, None, None)
        assert producer._consumer is consumer

    @pytest.mark.asyncio
    async def test_open_stream_retains_value_and_key_when_dlq_configured(self, mocker):
        consumer = _FakeConsumer([_FakeMessage(b"hello", partition=2, offset=42, key=b"k")])
        mocker.patch.object(KafkaConsumerHook, "get_consumer", return_value=consumer)
        _patch_connection(mocker)

        producer = KafkaSharedStreamProducer(topics=["orders"], dlq_topic="orders.dlq")
        stream = producer.open_stream()
        _value, broker_payload = await anext(stream)
        await stream.aclose()

        assert broker_payload == KafkaBrokerPayload("orders", 2, 42, b"hello", b"k")

    @pytest.mark.asyncio
    async def test_open_stream_skips_empty_polls(self, mocker):
        consumer = _FakeConsumer([None, None, _FakeMessage(b"x", offset=1)])
        mocker.patch.object(KafkaConsumerHook, "get_consumer", return_value=consumer)
        _patch_connection(mocker)

        producer = KafkaSharedStreamProducer(topics=["orders"])
        stream = producer.open_stream()
        _value, broker_payload = await anext(stream)
        await stream.aclose()

        assert broker_payload == KafkaBrokerPayload("orders", 0, 1, None, None)

    @pytest.mark.asyncio
    async def test_open_stream_raises_on_message_error(self, mocker):
        consumer = _FakeConsumer([_FakeMessage(b"x", error="boom")])
        mocker.patch.object(KafkaConsumerHook, "get_consumer", return_value=consumer)
        _patch_connection(mocker)

        producer = KafkaSharedStreamProducer(topics=["orders"])
        stream = producer.open_stream()
        with pytest.raises(RuntimeError, match="Kafka consumer error: boom"):
            await anext(stream)

    @pytest.mark.asyncio
    @pytest.mark.parametrize("auto_commit", ["true", "True", True, 1, "1", "yes", 0, "0"])
    async def test_open_stream_rejects_enabled_auto_commit(self, mocker, auto_commit):
        # The consumer must never be built when auto-commit is on.
        get_consumer = mocker.patch.object(KafkaConsumerHook, "get_consumer")
        _patch_connection(mocker, auto_commit=auto_commit)

        producer = KafkaSharedStreamProducer(topics=["orders"], kafka_config_id="my_kafka")
        with pytest.raises(ValueError, match="enable.auto.commit=false"):
            await anext(producer.open_stream())
        get_consumer.assert_not_called()

    @pytest.mark.asyncio
    async def test_open_stream_rejects_unset_auto_commit(self, mocker):
        # Absent key defaults to Kafka's enable.auto.commit=true, which must be rejected.
        get_consumer = mocker.patch.object(KafkaConsumerHook, "get_consumer")
        conn = mocker.MagicMock(extra_dejson={})
        mocker.patch.object(
            KafkaConsumerHook, "aget_connection", new_callable=mocker.AsyncMock, return_value=conn
        )

        producer = KafkaSharedStreamProducer(topics=["orders"])
        with pytest.raises(ValueError, match="enable.auto.commit=false"):
            await anext(producer.open_stream())
        get_consumer.assert_not_called()

    def test_get_advance_lane_returns_topic_partition(self):
        producer = KafkaSharedStreamProducer(topics=["orders"])
        assert producer.get_advance_lane(KafkaBrokerPayload("orders", 3, 99)) == ("orders", 3)

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("batch", "expected_offset"),
        [
            pytest.param(
                [_acked("orders", 0, 10), _acked("orders", 0, 11), _acked("orders", 0, 12)],
                13,
                id="all-acked-commits-last-plus-one",
            ),
            pytest.param(
                [_acked("orders", 0, 10), _rejected("orders", 0, 11)],
                12,
                id="rejected-is-committed-past",
            ),
            pytest.param(
                [_acked("orders", 0, 10), _failed("orders", 0, 11), _acked("orders", 0, 12)],
                11,
                id="stops-before-failed",
            ),
            pytest.param(
                [_acked("orders", 0, 10), _zero_subscriber("orders", 0, 11)],
                11,
                id="stops-before-zero-subscriber",
            ),
        ],
    )
    async def test_advance_commits_terminally_resolved_prefix(self, batch, expected_offset):
        producer = KafkaSharedStreamProducer(topics=["orders"])
        producer._consumer = _FakeConsumer()

        await producer.advance(batch)

        committed = producer._consumer.committed
        assert len(committed) == 1
        tp = committed[0][0]
        assert (tp.topic, tp.partition, tp.offset) == ("orders", 0, expected_offset)

    @pytest.mark.asyncio
    async def test_advance_does_not_commit_when_head_is_failed(self):
        producer = KafkaSharedStreamProducer(topics=["orders"])
        producer._consumer = _FakeConsumer()

        await producer.advance([_failed("orders", 0, 10), _acked("orders", 0, 11)])

        assert producer._consumer.committed == []
        assert producer._consumer.seeks[0].offset == 10

    @pytest.mark.asyncio
    async def test_advance_dead_letters_rejected_in_a_single_flush(self, mocker):
        dlq = _FakeProducer()
        mocker.patch.object(KafkaProducerHook, "get_producer", return_value=dlq)
        producer = KafkaSharedStreamProducer(topics=["orders"], dlq_topic="orders.dlq")
        producer._consumer = _FakeConsumer()

        await producer.advance(
            [
                _acked("orders", 0, 10),
                _rejected("orders", 0, 11, value=b"v11", key=b"k11"),
                _rejected("orders", 0, 12, value=b"v12", key=b"k12"),
            ]
        )

        # Both rejected messages are produced, then flushed exactly once for the batch.
        assert dlq.produced == [("orders.dlq", b"v11", b"k11"), ("orders.dlq", b"v12", b"k12")]
        assert dlq.flushed == 1
        # The acked and both dead-lettered offsets are committed past.
        tp = producer._consumer.committed[0][0]
        assert (tp.topic, tp.partition, tp.offset) == ("orders", 0, 13)

    @pytest.mark.asyncio
    async def test_advance_drops_rejected_without_dlq(self, mocker):
        get_producer = mocker.patch.object(KafkaProducerHook, "get_producer")
        producer = KafkaSharedStreamProducer(topics=["orders"])  # no dlq_topic
        producer._consumer = _FakeConsumer()

        await producer.advance([_rejected("orders", 0, 10)])

        # No DLQ producer is created; the rejected message is dropped (committed past).
        get_producer.assert_not_called()
        tp = producer._consumer.committed[0][0]
        assert (tp.topic, tp.partition, tp.offset) == ("orders", 0, 11)

    @pytest.mark.asyncio
    async def test_advance_with_dlq_does_not_commit_when_head_is_failed(self, mocker):
        get_producer = mocker.patch.object(KafkaProducerHook, "get_producer")
        producer = KafkaSharedStreamProducer(topics=["orders"], dlq_topic="orders.dlq")
        producer._consumer = _FakeConsumer()

        await producer.advance([_failed("orders", 0, 10)])

        get_producer.assert_not_called()
        assert producer._consumer.committed == []
        assert producer._consumer.seeks[0].offset == 10

    @pytest.mark.asyncio
    async def test_advance_seeks_back_to_failed_offset(self):
        producer = KafkaSharedStreamProducer(topics=["orders"])
        producer._consumer = _FakeConsumer()

        await producer.advance([_acked("orders", 0, 10), _failed("orders", 0, 11)])

        # 10 acked is committed past; 11 failed holds the floor and the consumer
        # is sought back to 11 so it -- and everything after -- is redelivered.
        committed = producer._consumer.committed[0][0]
        assert (committed.topic, committed.partition, committed.offset) == ("orders", 0, 11)
        sought = producer._consumer.seeks[0]
        assert (sought.topic, sought.partition, sought.offset) == ("orders", 0, 11)

    @pytest.mark.asyncio
    async def test_advance_seeks_back_on_zero_subscriber(self):
        producer = KafkaSharedStreamProducer(topics=["orders"])
        producer._consumer = _FakeConsumer()

        await producer.advance([_acked("orders", 0, 10), _zero_subscriber("orders", 0, 11)])

        # A broadcast no subscriber saw is held (commit stops at 11) and sought
        # back too, so a subscriber that joins later still receives it without
        # rebuilding the whole consumer.
        committed = producer._consumer.committed[0][0]
        assert (committed.topic, committed.partition, committed.offset) == ("orders", 0, 11)
        sought = producer._consumer.seeks[0]
        assert (sought.topic, sought.partition, sought.offset) == ("orders", 0, 11)

    @pytest.mark.asyncio
    async def test_advance_floor_blocks_later_batch_from_committing_past_failure(self):
        # The at-least-once guarantee: an offset held failed in one batch must not
        # be committed past by a later batch's cumulative commit.
        producer = KafkaSharedStreamProducer(topics=["orders"])
        producer._consumer = _FakeConsumer()

        await producer.advance([_failed("orders", 0, 11)])
        # A later batch (e.g. an in-flight ack that arrived after the failure)
        # acks offset 14 -- it must NOT commit 15 and skip the held 11.
        await producer.advance([_acked("orders", 0, 14)])

        # First batch held 11 (no commit); the second is clamped to the floor 11.
        assert [c[0].offset for c in producer._consumer.committed] == [11]

    @pytest.mark.asyncio
    async def test_advance_releases_floor_after_held_offset_is_reacked(self):
        producer = KafkaSharedStreamProducer(topics=["orders"])
        producer._consumer = _FakeConsumer()

        await producer.advance([_failed("orders", 0, 11)])
        # Re-read after the seek: the redelivered 11-13 are acked by the remaining
        # healthy subscribers, so the floor lifts and commit advances past them.
        await producer.advance([_acked("orders", 0, 11), _acked("orders", 0, 12), _acked("orders", 0, 13)])
        assert producer._consumer.committed[-1][0].offset == 14

        # Floor is gone: a subsequent ack commits normally past 14.
        await producer.advance([_acked("orders", 0, 14)])
        assert producer._consumer.committed[-1][0].offset == 15

    @pytest.mark.asyncio
    async def test_advance_seeks_to_earliest_held_offset_across_batches(self):
        # A later batch failing at a higher offset must not move the floor (nor the
        # seek) forward past an earlier batch's still-unresolved held offset.
        producer = KafkaSharedStreamProducer(topics=["orders"])
        producer._consumer = _FakeConsumer()

        await producer.advance([_failed("orders", 0, 11)])
        await producer.advance([_acked("orders", 0, 12), _failed("orders", 0, 13)])

        # Both seeks target the earliest held offset 11, never the later 13 --
        # otherwise 11 and 12 would be skipped and lost.
        assert [s.offset for s in producer._consumer.seeks] == [11, 11]
        # Commit never advances past the held 11.
        assert all(c[0].offset <= 11 for c in producer._consumer.committed)

    @pytest.mark.asyncio
    async def test_aclose_closes_consumer_once(self):
        producer = KafkaSharedStreamProducer(topics=["orders"])
        consumer = _FakeConsumer()
        producer._consumer = consumer

        await producer.aclose()
        assert consumer.closed is True
        assert producer._consumer is None
        # Second close is a no-op.
        await producer.aclose()


class TestKafkaSharedStreamTrigger:
    def test_serialize_roundtrips_classpath_and_kwargs(self):
        trigger = KafkaSharedStreamTrigger(
            topics=["b", "a"], kafka_config_id="c", poll_timeout=2.0, dlq_topic="a.dlq"
        )
        classpath, kwargs = trigger.serialize()
        assert classpath == ("airflow.providers.apache.kafka.triggers.shared_stream.KafkaSharedStreamTrigger")
        # topics are sorted at construction, so serialize round-trips them sorted.
        assert kwargs == {
            "topics": ["a", "b"],
            "kafka_config_id": "c",
            "poll_timeout": 2.0,
            "dlq_topic": "a.dlq",
        }

    def test_shared_stream_key_is_order_independent_and_hashable(self):
        a = KafkaSharedStreamTrigger(topics=["t2", "t1"], kafka_config_id="c")
        b = KafkaSharedStreamTrigger(topics=["t1", "t2"], kafka_config_id="c")
        assert a.shared_stream_key() == b.shared_stream_key()
        # Must be usable as a dict key.
        assert {a.shared_stream_key(): 1}

    def test_poll_timeout_does_not_affect_key(self):
        # poll_timeout only tunes the poll wait, not which messages -- still share.
        a = KafkaSharedStreamTrigger(topics=["t1"], poll_timeout=1.0)
        b = KafkaSharedStreamTrigger(topics=["t1"], poll_timeout=5.0)
        assert a.shared_stream_key() == b.shared_stream_key()

    def test_create_shared_stream_producer_builds_producer_from_kwargs(self):
        trigger = KafkaSharedStreamTrigger(
            topics=["orders"], kafka_config_id="c", poll_timeout=3.0, dlq_topic="orders.dlq"
        )
        producer = trigger.create_shared_stream_producer(trigger.serialize()[1])
        assert isinstance(producer, KafkaSharedStreamProducer)
        assert producer.topics == ["orders"]
        assert producer.kafka_config_id == "c"
        assert producer.poll_timeout == 3.0
        assert producer.dlq_topic == "orders.dlq"

    @pytest.mark.asyncio
    async def test_filter_shared_stream_decodes_and_yields_one_event_per_message(self):
        trigger = KafkaSharedStreamTrigger(topics=["orders"])

        async def raw_stream():
            yield b"first"
            yield b"second"

        events = [event async for event in trigger.filter_shared_stream(raw_stream())]
        assert [type(e) for e in events] == [TriggerEvent, TriggerEvent]
        assert [e.payload for e in events] == ["first", "second"]

    @pytest.mark.asyncio
    async def test_run_is_not_supported_standalone(self):
        trigger = KafkaSharedStreamTrigger(topics=["orders"])
        with pytest.raises(NotImplementedError, match="shared-stream manager"):
            await anext(trigger.run())
