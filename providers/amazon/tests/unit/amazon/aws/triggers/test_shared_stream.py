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
from contextlib import AsyncExitStack
from unittest import mock
from unittest.mock import AsyncMock

import pytest

pytest.importorskip("airflow.triggers.shared_stream")

from airflow.providers.amazon.aws.hooks.sqs import SqsHook
from airflow.providers.amazon.aws.triggers.shared_stream import (
    SqsBrokerPayload,
    SqsSharedStreamProducer,
    SqsSharedStreamTrigger,
)
from airflow.triggers.base import TriggerEvent
from airflow.triggers.shared_stream import AdvanceItem, AdvanceOutcome


class _FakeSqsClient:
    """A minimal stand-in for an aiobotocore SQS client (also its own async CM)."""

    def __init__(self, message_batches=(), delete_failed=()):
        self._batches = list(message_batches)
        self._delete_failed = list(delete_failed)
        self.receive_kwargs: list[dict] = []
        self.deleted_entries: list[list[dict]] = []
        self.visibility_entries: list[list[dict]] = []
        self.closed = False

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        self.closed = True
        return False

    async def receive_message(self, **kwargs):
        self.receive_kwargs.append(kwargs)
        if self._batches:
            return {"Messages": self._batches.pop(0)}
        return {"Messages": []}

    async def delete_message_batch(self, **kwargs):
        self.deleted_entries.append(list(kwargs["Entries"]))
        return {"Successful": list(kwargs["Entries"]), "Failed": list(self._delete_failed)}

    async def change_message_visibility_batch(self, **kwargs):
        self.visibility_entries.append(list(kwargs["Entries"]))
        return {"Successful": list(kwargs["Entries"])}


def _msg(message_id, receipt_handle=None, body="body", group_id=None):
    message = {
        "MessageId": message_id,
        "ReceiptHandle": receipt_handle or f"rh-{message_id}",
        "Body": body,
    }
    if group_id is not None:
        message["Attributes"] = {"MessageGroupId": group_id}
    return message


def _payload(message_id, receipt_handle=None, group_id=None):
    return SqsBrokerPayload(
        receipt_handle=receipt_handle or f"rh-{message_id}",
        message_id=message_id,
        group_id=group_id,
    )


def _item(message_id, outcome):
    return AdvanceItem(_payload(message_id), outcome)


def _acked(message_id):
    return _item(message_id, AdvanceOutcome(acked=1, failed=0))


def _rejected(message_id):
    return _item(message_id, AdvanceOutcome(acked=0, failed=0, rejected=1))


def _failed(message_id):
    return _item(message_id, AdvanceOutcome(acked=0, failed=1))


def _zero_subscriber(message_id):
    return _item(message_id, AdvanceOutcome(acked=0, failed=0))


def _outstanding_from(items):
    return {item.broker_payload.receipt_handle: item.broker_payload for item in items}


class TestSqsSharedStreamProducer:
    @pytest.mark.asyncio
    @mock.patch.object(SqsHook, "get_async_conn", new_callable=AsyncMock)
    async def test_open_stream_yields_body_and_payload_and_tracks_outstanding(self, mock_get_async_conn):
        client = _FakeSqsClient([[_msg("m1", "rh1", "hello")]])
        mock_get_async_conn.return_value = client

        producer = SqsSharedStreamProducer(sqs_queue="q", visibility_timeout=30)
        stream = producer.open_stream()
        body, payload = await anext(stream)

        assert body == "hello"
        assert payload == SqsBrokerPayload(receipt_handle="rh1", message_id="m1", group_id=None)
        assert producer._outstanding == {"rh1": payload}
        # The receive call carries the configured visibility timeout so the message
        # stays invisible from the first moment, and requests MessageGroupId so a
        # FIFO queue can be ordered by group.
        assert client.receive_kwargs[0]["VisibilityTimeout"] == 30
        assert client.receive_kwargs[0]["MessageSystemAttributeNames"] == ["MessageGroupId"]
        # Opening the stream starts the background visibility renewer.
        assert producer._renew_task is not None

        await producer.aclose()
        await stream.aclose()

    @pytest.mark.asyncio
    @mock.patch.object(SqsHook, "get_async_conn", new_callable=AsyncMock)
    async def test_open_stream_captures_group_id_for_fifo(self, mock_get_async_conn):
        client = _FakeSqsClient([[_msg("m1", "rh1", "hello", group_id="orders")]])
        mock_get_async_conn.return_value = client

        producer = SqsSharedStreamProducer(sqs_queue="q.fifo")
        stream = producer.open_stream()
        _body, payload = await anext(stream)

        # FIFO messages carry MessageGroupId, which becomes the advance lane.
        assert payload.group_id == "orders"
        assert producer.get_advance_lane(payload) == "orders"

        await producer.aclose()
        await stream.aclose()

    @pytest.mark.parametrize(
        ("group_id", "expected_lane"),
        [
            pytest.param(None, "rh-m1", id="standard-falls-back-to-receipt-handle"),
            pytest.param("orders", "orders", id="fifo-uses-message-group-id"),
        ],
    )
    def test_get_advance_lane_prefers_group_then_receipt_handle(self, group_id, expected_lane):
        producer = SqsSharedStreamProducer(sqs_queue="q")
        assert producer.get_advance_lane(_payload("m1", group_id=group_id)) == expected_lane

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("item_factory", "should_delete", "should_redeliver"),
        [
            pytest.param(_acked, True, False, id="acked-deleted"),
            pytest.param(_rejected, True, False, id="rejected-dropped-by-delete"),
            pytest.param(_failed, False, True, id="failed-reset-to-zero-for-immediate-redelivery"),
            pytest.param(_zero_subscriber, False, False, id="zero-subscriber-left-to-lapse"),
        ],
    )
    async def test_advance_resolves_each_outcome_and_always_clears_outstanding(
        self, item_factory, should_delete, should_redeliver
    ):
        client = _FakeSqsClient()
        producer = SqsSharedStreamProducer(sqs_queue="q")
        producer._client = client
        item = item_factory("m1")
        producer._outstanding = _outstanding_from([item])

        await producer.advance([item])

        deleted = [entry["ReceiptHandle"] for batch in client.deleted_entries for entry in batch]
        assert (deleted == ["rh-m1"]) is should_delete
        # A failed message is reset to visibility 0 for immediate redelivery; a
        # zero-subscriber message is left to lapse instead (no visibility change).
        reset = [
            entry["ReceiptHandle"]
            for batch in client.visibility_entries
            for entry in batch
            if entry["VisibilityTimeout"] == 0
        ]
        assert (reset == ["rh-m1"]) is should_redeliver
        # Every item is removed so the renewer stops extending it.
        assert producer._outstanding == {}

    @pytest.mark.asyncio
    async def test_advance_mixed_batch_deletes_accepted_and_redelivers_failed(self):
        client = _FakeSqsClient()
        producer = SqsSharedStreamProducer(sqs_queue="q")
        producer._client = client
        items = [_acked("a"), _failed("b"), _rejected("c"), _zero_subscriber("d")]
        producer._outstanding = _outstanding_from(items)

        await producer.advance(items)

        deleted = {entry["ReceiptHandle"] for batch in client.deleted_entries for entry in batch}
        assert deleted == {"rh-a", "rh-c"}
        reset = {
            entry["ReceiptHandle"]
            for batch in client.visibility_entries
            for entry in batch
            if entry["VisibilityTimeout"] == 0
        }
        # Only the failed message is reset for immediate redelivery; the
        # zero-subscriber one (d) is left to lapse.
        assert reset == {"rh-b"}
        assert producer._outstanding == {}

    @pytest.mark.asyncio
    async def test_advance_batches_deletes_in_groups_of_ten(self):
        client = _FakeSqsClient()
        producer = SqsSharedStreamProducer(sqs_queue="q")
        producer._client = client
        items = [_acked(f"m{i}") for i in range(21)]
        producer._outstanding = _outstanding_from(items)

        await producer.advance(items)

        assert [len(batch) for batch in client.deleted_entries] == [10, 10, 1]

    @pytest.mark.asyncio
    async def test_advance_delete_failure_is_not_raised(self):
        client = _FakeSqsClient(delete_failed=[{"Id": "0", "SenderFault": True, "Code": "x"}])
        producer = SqsSharedStreamProducer(sqs_queue="q")
        producer._client = client
        item = _acked("a")
        producer._outstanding = _outstanding_from([item])

        await producer.advance([item])

        assert client.deleted_entries  # delete was attempted; reported failure is swallowed

    @pytest.mark.asyncio
    async def test_advance_batches_redeliveries_in_groups_of_ten(self):
        client = _FakeSqsClient()
        producer = SqsSharedStreamProducer(sqs_queue="q")
        producer._client = client
        items = [_failed(f"m{i}") for i in range(21)]
        producer._outstanding = _outstanding_from(items)

        await producer.advance(items)

        assert [len(batch) for batch in client.visibility_entries] == [10, 10, 1]
        assert all(entry["VisibilityTimeout"] == 0 for batch in client.visibility_entries for entry in batch)

    @pytest.mark.asyncio
    async def test_advance_redeliver_failure_is_not_raised(self):
        client = _FakeSqsClient()
        client.change_message_visibility_batch = AsyncMock(side_effect=RuntimeError("boom"))
        producer = SqsSharedStreamProducer(sqs_queue="q")
        producer._client = client
        item = _failed("a")
        producer._outstanding = _outstanding_from([item])

        await producer.advance([item])  # no raise; the message lapses instead

        assert producer._outstanding == {}

    @pytest.mark.asyncio
    async def test_renew_visibility_extends_all_outstanding(self):
        client = _FakeSqsClient()
        producer = SqsSharedStreamProducer(sqs_queue="q", visibility_timeout=100)
        producer._client = client
        producer._outstanding = _outstanding_from([_acked("m1"), _acked("m2")])

        await producer._renew_visibility()

        entries = client.visibility_entries[0]
        assert {entry["ReceiptHandle"] for entry in entries} == {"rh-m1", "rh-m2"}
        assert all(entry["VisibilityTimeout"] == 100 for entry in entries)

    @pytest.mark.asyncio
    async def test_renew_visibility_failure_is_swallowed(self):
        client = _FakeSqsClient()
        client.change_message_visibility_batch = AsyncMock(side_effect=RuntimeError("boom"))
        producer = SqsSharedStreamProducer(sqs_queue="q")
        producer._client = client
        producer._outstanding = {"rh1": _payload("m1")}

        await producer._renew_visibility()  # no raise

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.amazon.aws.triggers.shared_stream.asyncio.sleep", new_callable=AsyncMock)
    async def test_renew_visibility_loop_renews_each_interval(self, mock_sleep):
        mock_sleep.side_effect = [None, asyncio.CancelledError]
        client = _FakeSqsClient()
        producer = SqsSharedStreamProducer(sqs_queue="q", visibility_timeout=10)
        producer._client = client
        producer._outstanding = {"rh1": _payload("m1")}

        with pytest.raises(asyncio.CancelledError):
            await producer._renew_visibility_loop()

        # interval is half the visibility timeout, and one renewal ran before cancel.
        mock_sleep.assert_awaited_with(5)
        assert len(client.visibility_entries) == 1

    @pytest.mark.asyncio
    async def test_aclose_cancels_renewer_and_closes_client_once(self):
        client = _FakeSqsClient()
        producer = SqsSharedStreamProducer(sqs_queue="q")
        stack = AsyncExitStack()
        producer._exit_stack = stack
        producer._client = await stack.enter_async_context(client)
        producer._renew_task = asyncio.create_task(asyncio.sleep(3600))
        producer._outstanding = {"rh1": _payload("m1")}

        await producer.aclose()

        assert client.closed is True
        assert producer._client is None
        assert producer._renew_task is None
        assert producer._outstanding == {}
        # Second close is a no-op.
        await producer.aclose()


class TestSqsSharedStreamTrigger:
    def test_serialize_roundtrips_classpath_and_kwargs(self):
        trigger = SqsSharedStreamTrigger(
            sqs_queue="q",
            aws_conn_id="c",
            max_messages=5,
            wait_time_seconds=10,
            visibility_timeout=30,
            region_name="us-east-1",
        )
        classpath, kwargs = trigger.serialize()
        assert classpath == "airflow.providers.amazon.aws.triggers.shared_stream.SqsSharedStreamTrigger"
        assert kwargs == {
            "sqs_queue": "q",
            "aws_conn_id": "c",
            "max_messages": 5,
            "wait_time_seconds": 10,
            "visibility_timeout": 30,
            "region_name": "us-east-1",
            "verify": None,
            "botocore_config": None,
        }

    def test_shared_stream_key_shares_on_queue_and_connection(self):
        # Poll-tuning fields (max_messages, ...) do not change which queue is watched.
        a = SqsSharedStreamTrigger(sqs_queue="q", aws_conn_id="c", max_messages=1)
        b = SqsSharedStreamTrigger(sqs_queue="q", aws_conn_id="c", max_messages=99)
        assert a.shared_stream_key() == b.shared_stream_key()
        # Must be usable as a dict key.
        assert {a.shared_stream_key(): 1}

    def test_shared_stream_key_differs_by_queue(self):
        a = SqsSharedStreamTrigger(sqs_queue="q1")
        b = SqsSharedStreamTrigger(sqs_queue="q2")
        assert a.shared_stream_key() != b.shared_stream_key()

    def test_create_shared_stream_producer_builds_producer_from_kwargs(self):
        trigger = SqsSharedStreamTrigger(
            sqs_queue="q", aws_conn_id="c", max_messages=5, wait_time_seconds=10, visibility_timeout=30
        )
        producer = trigger.create_shared_stream_producer(trigger.serialize()[1])
        assert isinstance(producer, SqsSharedStreamProducer)
        assert producer.sqs_queue == "q"
        assert producer.aws_conn_id == "c"
        assert producer.max_messages == 5
        assert producer.wait_time_seconds == 10
        assert producer.visibility_timeout == 30

    @pytest.mark.asyncio
    async def test_filter_shared_stream_yields_one_event_per_body(self):
        trigger = SqsSharedStreamTrigger(sqs_queue="q")

        async def raw_stream():
            yield "first"
            yield "second"

        events = [event async for event in trigger.filter_shared_stream(raw_stream())]
        assert [type(e) for e in events] == [TriggerEvent, TriggerEvent]
        assert [e.payload for e in events] == ["first", "second"]

    @pytest.mark.asyncio
    async def test_run_is_not_supported_standalone(self):
        trigger = SqsSharedStreamTrigger(sqs_queue="q")
        with pytest.raises(NotImplementedError, match="shared-stream manager"):
            await anext(trigger.run())
