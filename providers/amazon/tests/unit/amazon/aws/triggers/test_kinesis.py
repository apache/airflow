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

import datetime
from types import SimpleNamespace
from unittest import mock
from unittest.mock import AsyncMock

import pytest

from airflow.providers.amazon.aws.triggers.kinesis import KinesisTrigger
from airflow.triggers.base import TriggerEvent

MODULE = "airflow.providers.amazon.aws.triggers.kinesis"
STREAM_NAME = "test-stream"
AWS_CONN_ID = "test-aws-conn"
REGION_NAME = "us-east-1"
SHARD_ID = "shardId-000000000000"
CHILD_SHARD_ID = "shardId-000000000001"


class ExpiredIteratorException(Exception):
    pass


class InvalidArgumentException(Exception):
    pass


class ProvisionedThroughputExceededException(Exception):
    pass


class StopPolling(Exception):
    pass


class AsyncPages:
    def __init__(self, pages):
        self._pages = iter(pages)

    def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            return next(self._pages)
        except StopIteration:
            raise StopAsyncIteration


class AsyncClientContext:
    def __init__(self, client):
        self.client = client

    async def __aenter__(self):
        return self.client

    async def __aexit__(self, exc_type, exc_value, traceback):
        return None


@pytest.fixture
def trigger():
    return KinesisTrigger(
        stream_name=STREAM_NAME,
        aws_conn_id=AWS_CONN_ID,
        shard_iterator_type="LATEST",
        batch_size=100,
        waiter_delay=10,
        region_name=REGION_NAME,
        verify=True,
        botocore_config={"retries": {"max_attempts": 3}},
    )


def create_client(pages):
    client = mock.MagicMock(spec=["exceptions", "get_paginator", "get_records", "get_shard_iterator"])
    client.exceptions = SimpleNamespace(
        ExpiredIteratorException=ExpiredIteratorException,
        InvalidArgumentException=InvalidArgumentException,
        ProvisionedThroughputExceededException=ProvisionedThroughputExceededException,
    )
    client.get_records = AsyncMock()
    client.get_shard_iterator = AsyncMock()
    paginator = mock.MagicMock(spec=["paginate"])
    paginator.paginate.return_value = AsyncPages(pages)
    client.get_paginator.return_value = paginator
    return client, paginator


def configure_hook(hook_property, client):
    hook = mock.MagicMock(spec=["get_async_conn"])
    hook.get_async_conn = AsyncMock(return_value=AsyncClientContext(client))
    hook_property.return_value = hook
    return hook


def configure_checkpoint_store(trigger, checkpoint):
    store = mock.MagicMock(spec=["get", "set"])
    store.get.return_value = checkpoint
    trigger.asset_state_store = store
    return store


def create_record(sequence_number="1", data=b"message", *, include_timestamp=True):
    record = {
        "SequenceNumber": sequence_number,
        "PartitionKey": "partition-key",
        "Data": data,
    }
    if include_timestamp:
        record["ApproximateArrivalTimestamp"] = datetime.datetime(
            2026, 8, 4, 12, 30, tzinfo=datetime.timezone.utc
        )
    return record


def test_serialize(trigger):
    assert trigger.serialize() == (
        f"{MODULE}.KinesisTrigger",
        {
            "stream_name": STREAM_NAME,
            "aws_conn_id": AWS_CONN_ID,
            "shard_iterator_type": "LATEST",
            "batch_size": 100,
            "waiter_delay": 10,
            "region_name": REGION_NAME,
            "verify": True,
            "botocore_config": {"retries": {"max_attempts": 3}},
        },
    )


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"shard_iterator_type": "AT_TIMESTAMP"}, "shard_iterator_type must be one of"),
        ({"batch_size": 0}, "batch_size must be between"),
        ({"batch_size": 10_001}, "batch_size must be between"),
        ({"waiter_delay": 0}, "waiter_delay must be at least"),
    ],
)
def test_invalid_arguments_raise(kwargs, message):
    with pytest.raises(ValueError, match=message):
        KinesisTrigger(stream_name=STREAM_NAME, **kwargs)


@mock.patch(f"{MODULE}.KinesisHook", autospec=True)
def test_hook(mock_hook, trigger):
    assert trigger.hook == mock_hook.return_value
    mock_hook.assert_called_once_with(
        aws_conn_id=AWS_CONN_ID,
        region_name=REGION_NAME,
        verify=True,
        config={"retries": {"max_attempts": 3}},
    )


@pytest.mark.parametrize("field", ["stream_name", "aws_conn_id", "region_name"])
def test_checkpoint_key_uses_stream_identity(trigger, field):
    kwargs = trigger.serialize()[1]
    kwargs[field] = f"different-{field}"
    other = KinesisTrigger(**kwargs)

    assert trigger._build_checkpoint_key() != other._build_checkpoint_key()


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("shard_iterator_type", "TRIM_HORIZON"),
        ("batch_size", 200),
        ("waiter_delay", 5),
        ("verify", False),
        ("botocore_config", {"retries": {"max_attempts": 5}}),
    ],
)
def test_checkpoint_key_ignores_polling_and_client_options(trigger, field, value):
    kwargs = trigger.serialize()[1]
    kwargs[field] = value
    other = KinesisTrigger(**kwargs)

    assert trigger._build_checkpoint_key() == other._build_checkpoint_key()


def test_load_and_save_checkpoint(trigger):
    checkpoint = {SHARD_ID: "123"}
    store = configure_checkpoint_store(trigger, checkpoint)

    assert trigger._load_checkpoint() == checkpoint
    trigger._save_checkpoint(checkpoint)

    store.get.assert_called_once_with(trigger._build_checkpoint_key(), default={})
    store.set.assert_called_once_with(trigger._build_checkpoint_key(), checkpoint)


@pytest.mark.parametrize(
    "checkpoint",
    [
        ["not-a-dict"],
        {1: "123"},
        {SHARD_ID: 123},
    ],
)
@mock.patch.object(KinesisTrigger, "log", new_callable=mock.PropertyMock)
def test_invalid_checkpoint_falls_back_to_initial_position(mock_log, trigger, checkpoint):
    configure_checkpoint_store(trigger, checkpoint)

    assert trigger._load_checkpoint() == {}
    mock_log.return_value.warning.assert_called_once()


@pytest.mark.parametrize("missing_attribute", [True, False])
@mock.patch.object(KinesisTrigger, "log", new_callable=mock.PropertyMock)
def test_checkpoint_falls_back_to_memory_without_store(mock_log, trigger, missing_attribute):
    if missing_attribute:
        trigger.__dict__.pop("asset_state_store", None)
    else:
        trigger.asset_state_store = None

    assert trigger._load_checkpoint() == {}
    trigger._save_checkpoint({SHARD_ID: "123"})

    mock_log.return_value.warning.assert_called_once()


@mock.patch.object(KinesisTrigger, "log", new_callable=mock.PropertyMock)
def test_checkpoint_falls_back_to_memory_for_multiple_assets(mock_log, trigger):
    store = configure_checkpoint_store(trigger, {})
    store.get.side_effect = ValueError

    assert trigger._load_checkpoint() == {}

    store.get.side_effect = None
    store.set.side_effect = ValueError
    trigger._save_checkpoint({SHARD_ID: "123"})

    mock_log.return_value.warning.assert_called_once()


@pytest.mark.asyncio
async def test_find_shards_follows_pagination(trigger):
    client, paginator = create_client(
        [
            {"Shards": [{"ShardId": SHARD_ID}]},
            {"Shards": [{"ShardId": CHILD_SHARD_ID}]},
        ]
    )

    assert await trigger._find_shard_ids(client) == [SHARD_ID, CHILD_SHARD_ID]
    paginator.paginate.assert_called_once_with(StreamName=STREAM_NAME)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("sequence_number", "fallback_type", "expected_request"),
    [
        (
            "123",
            "LATEST",
            {
                "StreamName": STREAM_NAME,
                "ShardId": SHARD_ID,
                "ShardIteratorType": "AFTER_SEQUENCE_NUMBER",
                "StartingSequenceNumber": "123",
            },
        ),
        (
            None,
            "TRIM_HORIZON",
            {
                "StreamName": STREAM_NAME,
                "ShardId": SHARD_ID,
                "ShardIteratorType": "TRIM_HORIZON",
            },
        ),
    ],
)
async def test_get_shard_iterator(trigger, sequence_number, fallback_type, expected_request):
    client, _ = create_client([])
    client.get_shard_iterator.return_value = {"ShardIterator": "iterator"}

    assert await trigger._get_shard_iterator(client, SHARD_ID, sequence_number, fallback_type) == "iterator"
    client.get_shard_iterator.assert_awaited_once_with(**expected_request)


@pytest.mark.asyncio
@mock.patch.object(KinesisTrigger, "log", new_callable=mock.PropertyMock)
async def test_stale_checkpoint_falls_back_to_initial_position(mock_log, trigger):
    client, _ = create_client([])
    client.get_shard_iterator.side_effect = [
        InvalidArgumentException,
        {"ShardIterator": "fallback-iterator"},
    ]

    assert await trigger._get_shard_iterator(client, SHARD_ID, "123", "LATEST") == "fallback-iterator"
    assert client.get_shard_iterator.await_args_list == [
        mock.call(
            StreamName=STREAM_NAME,
            ShardId=SHARD_ID,
            ShardIteratorType="AFTER_SEQUENCE_NUMBER",
            StartingSequenceNumber="123",
        ),
        mock.call(
            StreamName=STREAM_NAME,
            ShardId=SHARD_ID,
            ShardIteratorType="LATEST",
        ),
    ]
    mock_log.return_value.warning.assert_called_once()


@pytest.mark.asyncio
async def test_invalid_initial_position_is_not_suppressed(trigger):
    client, _ = create_client([])
    client.get_shard_iterator.side_effect = InvalidArgumentException

    with pytest.raises(InvalidArgumentException):
        await trigger._get_shard_iterator(client, SHARD_ID, None, "LATEST")


def test_build_event_records():
    assert KinesisTrigger._build_event_records(
        SHARD_ID,
        [create_record(), create_record("2", b"second", include_timestamp=False)],
    ) == [
        {
            "ShardId": SHARD_ID,
            "SequenceNumber": "1",
            "PartitionKey": "partition-key",
            "ApproximateArrivalTimestamp": "2026-08-04T12:30:00+00:00",
            "Data": "bWVzc2FnZQ==",
        },
        {
            "ShardId": SHARD_ID,
            "SequenceNumber": "2",
            "PartitionKey": "partition-key",
            "ApproximateArrivalTimestamp": None,
            "Data": "c2Vjb25k",
        },
    ]


@pytest.mark.asyncio
@mock.patch.object(KinesisTrigger, "hook", new_callable=mock.PropertyMock)
@mock.patch(f"{MODULE}.asyncio.sleep", new_callable=AsyncMock)
async def test_run_yields_events_and_keeps_running(mock_sleep, hook_property, trigger):
    client, _ = create_client([{"Shards": [{"ShardId": SHARD_ID}]}])
    client.get_shard_iterator.return_value = {"ShardIterator": "iterator-1"}
    client.get_records.side_effect = [
        {"Records": [create_record("1")], "NextShardIterator": "iterator-2"},
        {"Records": [create_record("2")], "NextShardIterator": "iterator-3"},
    ]
    configure_hook(hook_property, client)
    store = configure_checkpoint_store(trigger, {})

    generator = trigger.run()
    first_event = await anext(generator)
    second_event = await anext(generator)
    await generator.aclose()

    assert first_event.payload["message_batch"][0]["SequenceNumber"] == "1"
    assert second_event.payload["message_batch"][0]["SequenceNumber"] == "2"
    store.set.assert_called_once_with(trigger._build_checkpoint_key(), {SHARD_ID: "1"})
    mock_sleep.assert_awaited_once_with(10)


@pytest.mark.asyncio
@mock.patch.object(KinesisTrigger, "hook", new_callable=mock.PropertyMock)
@mock.patch(f"{MODULE}.asyncio.sleep", new_callable=AsyncMock)
async def test_run_checkpoints_once_after_sweep(mock_sleep, hook_property, trigger):
    client, _ = create_client([{"Shards": [{"ShardId": SHARD_ID}, {"ShardId": CHILD_SHARD_ID}]}])
    client.get_shard_iterator.side_effect = [
        {"ShardIterator": "iterator-1"},
        {"ShardIterator": "iterator-2"},
    ]
    client.get_records.side_effect = [
        {"Records": [create_record("1")], "NextShardIterator": "iterator-1-next"},
        {"Records": [create_record("2")], "NextShardIterator": "iterator-2-next"},
    ]
    mock_sleep.side_effect = StopPolling
    configure_hook(hook_property, client)
    store = configure_checkpoint_store(trigger, {})

    generator = trigger.run()
    assert await anext(generator) == TriggerEvent(
        {
            "status": "success",
            "message_batch": KinesisTrigger._build_event_records(SHARD_ID, [create_record("1")]),
        }
    )
    assert await anext(generator) == TriggerEvent(
        {
            "status": "success",
            "message_batch": KinesisTrigger._build_event_records(CHILD_SHARD_ID, [create_record("2")]),
        }
    )
    with pytest.raises(StopPolling):
        await anext(generator)

    store.set.assert_called_once_with(
        trigger._build_checkpoint_key(),
        {SHARD_ID: "1", CHILD_SHARD_ID: "2"},
    )


@pytest.mark.asyncio
@mock.patch.object(KinesisTrigger, "hook", new_callable=mock.PropertyMock)
@mock.patch(f"{MODULE}.asyncio.sleep", new_callable=AsyncMock)
async def test_checkpoint_prunes_expired_shards_on_startup(mock_sleep, hook_property, trigger):
    client, _ = create_client([{"Shards": [{"ShardId": SHARD_ID}]}])
    client.get_shard_iterator.return_value = {"ShardIterator": "iterator"}
    client.get_records.return_value = {"Records": [], "NextShardIterator": "next-iterator"}
    mock_sleep.side_effect = StopPolling
    configure_hook(hook_property, client)
    store = configure_checkpoint_store(trigger, {SHARD_ID: "1", "expired-shard": "2"})

    with pytest.raises(StopPolling):
        await anext(trigger.run())

    store.set.assert_called_once_with(trigger._build_checkpoint_key(), {SHARD_ID: "1"})


@pytest.mark.asyncio
@mock.patch.object(KinesisTrigger, "hook", new_callable=mock.PropertyMock)
@mock.patch(f"{MODULE}.asyncio.sleep", new_callable=AsyncMock)
async def test_checkpoint_retains_closed_parent(mock_sleep, hook_property, trigger):
    client, _ = create_client([{"Shards": [{"ShardId": SHARD_ID}]}])
    client.get_shard_iterator.side_effect = [
        {"ShardIterator": "parent-iterator"},
        {"ShardIterator": "child-iterator"},
    ]
    client.get_records.side_effect = [
        {
            "Records": [],
            "ChildShards": [{"ShardId": CHILD_SHARD_ID}],
        },
        {
            "Records": [create_record("child-sequence")],
            "NextShardIterator": "child-next-iterator",
        },
    ]
    mock_sleep.side_effect = [None, StopPolling]
    configure_hook(hook_property, client)
    store = configure_checkpoint_store(trigger, {SHARD_ID: "parent-sequence"})

    generator = trigger.run()
    event = await anext(generator)
    with pytest.raises(StopPolling):
        await anext(generator)

    assert event.payload["message_batch"][0]["SequenceNumber"] == "child-sequence"
    store.set.assert_called_once_with(
        trigger._build_checkpoint_key(),
        {SHARD_ID: "parent-sequence", CHILD_SHARD_ID: "child-sequence"},
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("iterator_type", ["LATEST", "TRIM_HORIZON"])
@mock.patch.object(KinesisTrigger, "hook", new_callable=mock.PropertyMock)
@mock.patch(f"{MODULE}.asyncio.sleep", new_callable=AsyncMock)
async def test_startup_without_checkpoint_honours_configured_position(
    mock_sleep, hook_property, iterator_type
):
    trigger = KinesisTrigger(
        stream_name=STREAM_NAME,
        shard_iterator_type=iterator_type,
    )
    client, _ = create_client([{"Shards": [{"ShardId": SHARD_ID}]}])
    client.get_shard_iterator.return_value = {"ShardIterator": "iterator"}
    client.get_records.return_value = {"Records": [], "NextShardIterator": "next-iterator"}
    mock_sleep.side_effect = StopPolling
    configure_hook(hook_property, client)
    configure_checkpoint_store(trigger, {})

    with pytest.raises(StopPolling):
        await anext(trigger.run())

    client.get_shard_iterator.assert_awaited_once_with(
        StreamName=STREAM_NAME,
        ShardId=SHARD_ID,
        ShardIteratorType=iterator_type,
    )


@pytest.mark.asyncio
@mock.patch.object(KinesisTrigger, "hook", new_callable=mock.PropertyMock)
@mock.patch(f"{MODULE}.asyncio.sleep", new_callable=AsyncMock)
async def test_expired_iterator_is_recovered_from_checkpoint(mock_sleep, hook_property, trigger):
    client, _ = create_client([{"Shards": [{"ShardId": SHARD_ID}]}])
    client.get_shard_iterator.side_effect = [
        {"ShardIterator": "initial-iterator"},
        {"ShardIterator": "recovered-iterator"},
    ]
    client.get_records.side_effect = [
        ExpiredIteratorException,
        {"Records": [create_record("124")], "NextShardIterator": "next-iterator"},
    ]
    configure_hook(hook_property, client)
    configure_checkpoint_store(trigger, {SHARD_ID: "123"})

    event = await anext(trigger.run())

    assert event.payload["message_batch"][0]["SequenceNumber"] == "124"
    assert client.get_shard_iterator.await_args_list == [
        mock.call(
            StreamName=STREAM_NAME,
            ShardId=SHARD_ID,
            ShardIteratorType="AFTER_SEQUENCE_NUMBER",
            StartingSequenceNumber="123",
        ),
        mock.call(
            StreamName=STREAM_NAME,
            ShardId=SHARD_ID,
            ShardIteratorType="AFTER_SEQUENCE_NUMBER",
            StartingSequenceNumber="123",
        ),
    ]
    mock_sleep.assert_awaited_once_with(10)


@pytest.mark.asyncio
@mock.patch.object(KinesisTrigger, "hook", new_callable=mock.PropertyMock)
@mock.patch(f"{MODULE}.asyncio.sleep", new_callable=AsyncMock)
async def test_child_iterator_recovery_preserves_trim_horizon(mock_sleep, hook_property, trigger):
    client, _ = create_client([{"Shards": [{"ShardId": SHARD_ID}]}])
    client.get_shard_iterator.side_effect = [
        {"ShardIterator": "parent-iterator"},
        {"ShardIterator": "child-iterator"},
        {"ShardIterator": "recovered-child-iterator"},
    ]
    client.get_records.side_effect = [
        {
            "Records": [],
            "ChildShards": [{"ShardId": CHILD_SHARD_ID}],
        },
        ExpiredIteratorException,
        {
            "Records": [create_record("1")],
            "NextShardIterator": "child-next-iterator",
        },
    ]
    configure_hook(hook_property, client)
    configure_checkpoint_store(trigger, {})

    event = await anext(trigger.run())

    assert event.payload["message_batch"][0]["SequenceNumber"] == "1"
    assert client.get_shard_iterator.await_args_list[-2:] == [
        mock.call(
            StreamName=STREAM_NAME,
            ShardId=CHILD_SHARD_ID,
            ShardIteratorType="TRIM_HORIZON",
        ),
        mock.call(
            StreamName=STREAM_NAME,
            ShardId=CHILD_SHARD_ID,
            ShardIteratorType="TRIM_HORIZON",
        ),
    ]
    assert mock_sleep.await_count == 2


@pytest.mark.asyncio
@mock.patch.object(KinesisTrigger, "hook", new_callable=mock.PropertyMock)
@mock.patch(f"{MODULE}.asyncio.sleep", new_callable=AsyncMock)
@mock.patch.object(KinesisTrigger, "log", new_callable=mock.PropertyMock)
async def test_throttling_is_logged_and_paced(mock_log, mock_sleep, hook_property, trigger):
    client, _ = create_client([{"Shards": [{"ShardId": SHARD_ID}]}])
    client.get_shard_iterator.return_value = {"ShardIterator": "iterator"}
    client.get_records.side_effect = ProvisionedThroughputExceededException
    mock_sleep.side_effect = StopPolling
    configure_hook(hook_property, client)
    configure_checkpoint_store(trigger, {})

    with pytest.raises(StopPolling):
        await anext(trigger.run())

    mock_log.return_value.warning.assert_called_once()
    mock_sleep.assert_awaited_once_with(10)


@pytest.mark.asyncio
@mock.patch.object(KinesisTrigger, "hook", new_callable=mock.PropertyMock)
@mock.patch(f"{MODULE}.asyncio.sleep", new_callable=AsyncMock)
async def test_closed_shard_adds_each_child_once(mock_sleep, hook_property, trigger):
    client, _ = create_client([{"Shards": [{"ShardId": SHARD_ID}]}])
    client.get_shard_iterator.side_effect = [
        {"ShardIterator": "parent-iterator"},
        {"ShardIterator": "child-iterator"},
    ]
    client.get_records.return_value = {
        "Records": [],
        "ChildShards": [
            {"ShardId": CHILD_SHARD_ID},
            {"ShardId": CHILD_SHARD_ID},
        ],
    }
    mock_sleep.side_effect = StopPolling
    configure_hook(hook_property, client)
    configure_checkpoint_store(trigger, {})

    with pytest.raises(StopPolling):
        await anext(trigger.run())

    assert client.get_shard_iterator.await_args_list == [
        mock.call(
            StreamName=STREAM_NAME,
            ShardId=SHARD_ID,
            ShardIteratorType="LATEST",
        ),
        mock.call(
            StreamName=STREAM_NAME,
            ShardId=CHILD_SHARD_ID,
            ShardIteratorType="TRIM_HORIZON",
        ),
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize("records", [[], [create_record()]])
@mock.patch.object(KinesisTrigger, "hook", new_callable=mock.PropertyMock)
@mock.patch(f"{MODULE}.asyncio.sleep", new_callable=AsyncMock)
async def test_run_sleeps_after_empty_and_busy_sweeps(mock_sleep, hook_property, records, trigger):
    client, _ = create_client([{"Shards": [{"ShardId": SHARD_ID}]}])
    client.get_shard_iterator.return_value = {"ShardIterator": "iterator"}
    client.get_records.return_value = {
        "Records": records,
        "NextShardIterator": "next-iterator",
    }
    mock_sleep.side_effect = StopPolling
    configure_hook(hook_property, client)
    configure_checkpoint_store(trigger, {})

    generator = trigger.run()
    if records:
        await anext(generator)
    with pytest.raises(StopPolling):
        await anext(generator)

    mock_sleep.assert_awaited_once_with(10)
