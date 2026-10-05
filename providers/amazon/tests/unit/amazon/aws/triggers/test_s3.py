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
from datetime import datetime
from unittest import mock as async_mock

import pytest

from airflow.providers.amazon.aws.triggers.s3 import (
    S3KeysUnchangedTrigger,
    S3KeyTrigger,
    S3KeyUpdateTrigger,
)
from airflow.triggers.base import TriggerEvent


class TestS3KeyTrigger:
    def test_serialization(self):
        """
        Asserts that the TaskStateTrigger correctly serializes its arguments
        and classpath.
        """
        trigger = S3KeyTrigger(
            bucket_key="s3://test_bucket/file",
            bucket_name="test_bucket",
            wildcard_match=True,
            metadata_keys=["Size", "LastModified"],
        )
        classpath, kwargs = trigger.serialize()
        assert classpath == "airflow.providers.amazon.aws.triggers.s3.S3KeyTrigger"
        assert kwargs == {
            "bucket_name": "test_bucket",
            "bucket_key": "s3://test_bucket/file",
            "wildcard_match": True,
            "aws_conn_id": "aws_default",
            "hook_params": {},
            "poke_interval": 5.0,
            "should_check_fn": False,
            "use_regex": False,
            "verify": None,
            "region_name": None,
            "botocore_config": None,
            "metadata_keys": ["Size", "LastModified"],
        }

    @pytest.mark.asyncio
    @async_mock.patch("airflow.providers.amazon.aws.triggers.s3.S3Hook.get_async_conn")
    async def test_run_success(self, mock_client):
        """
        Test if the task is run is in triggerr successfully.
        """
        mock_client.return_value.return_value.check_key.return_value = True
        trigger = S3KeyTrigger(bucket_key="test_bucket/file", bucket_name="test_bucket")
        task = asyncio.create_task(trigger.run().__anext__())
        await asyncio.sleep(0.5)

        assert task.done() is True
        result = await task
        assert result == TriggerEvent({"status": "success"})
        asyncio.get_event_loop().stop()

    @pytest.mark.asyncio
    @async_mock.patch("airflow.providers.amazon.aws.triggers.s3.S3Hook.check_key_async")
    @async_mock.patch("airflow.providers.amazon.aws.triggers.s3.S3Hook.get_async_conn")
    async def test_run_pending(self, mock_client, mock_check_key_async):
        """
        Test if the task is run is in trigger successfully and set check_key to return false.
        """
        mock_check_key_async.return_value = False
        trigger = S3KeyTrigger(bucket_key="test_bucket/file", bucket_name="test_bucket")
        task = asyncio.create_task(trigger.run().__anext__())
        await asyncio.sleep(0.5)

        assert task.done() is False
        asyncio.get_event_loop().stop()

    @pytest.mark.asyncio
    @async_mock.patch("airflow.providers.amazon.aws.triggers.s3.S3Hook.get_files_async")
    @async_mock.patch("airflow.providers.amazon.aws.triggers.s3.S3Hook.get_head_object_async")
    @async_mock.patch("airflow.providers.amazon.aws.triggers.s3.S3Hook.check_key_async")
    @async_mock.patch("airflow.providers.amazon.aws.triggers.s3.S3Hook.get_async_conn")
    async def test_run_with_metadata(
        self,
        mock_get_async_conn,
        mock_check_key_async,
        mock_get_head_object_async,
        mock_get_files_async,
    ):
        """Test if the task retrieves metadata correctly when should_check_fn is True."""
        mock_check_key_async.return_value = True
        mock_get_files_async.return_value = ["file1.txt", "file2.txt"]

        async def fake_get_head_object_async(*args, **kwargs):
            key = kwargs.get("key")
            if key == "file1.txt":
                return {"ContentLength": 1024, "LastModified": "2023-10-01T12:00:00Z"}
            if key == "file2.txt":
                return {"ContentLength": 2048, "LastModified": "2023-10-02T12:00:00Z"}

        mock_get_head_object_async.side_effect = fake_get_head_object_async
        mock_get_async_conn.return_value.__aenter__.return_value = async_mock.AsyncMock()
        trigger = S3KeyTrigger(
            bucket_key="test_bucket/file",
            bucket_name="test_bucket",
            should_check_fn=True,
            metadata_keys=["Size", "LastModified"],
            poke_interval=0.1,  # reduce waiting time
        )
        result = await asyncio.wait_for(trigger.run().__anext__(), timeout=2)
        expected = TriggerEvent(
            {
                "status": "running",
                "files": [
                    {"Size": 1024, "LastModified": "2023-10-01T12:00:00Z", "Key": "file1.txt"},
                    {"Size": 2048, "LastModified": "2023-10-02T12:00:00Z", "Key": "file2.txt"},
                ],
            }
        )

        assert result == expected

    @pytest.mark.asyncio
    @async_mock.patch("airflow.providers.amazon.aws.triggers.s3.S3Hook.get_files_async")
    @async_mock.patch("airflow.providers.amazon.aws.triggers.s3.S3Hook.get_head_object_async")
    @async_mock.patch("airflow.providers.amazon.aws.triggers.s3.S3Hook.check_key_async")
    @async_mock.patch("airflow.providers.amazon.aws.triggers.s3.S3Hook.get_async_conn")
    async def test_run_with_all_metadata(
        self, mock_get_async_conn, mock_check_key_async, mock_get_head_object_async, mock_get_files_async
    ):
        """
        Test if the task retrieves all metadata when metadata_keys contains '*'.
        """
        mock_check_key_async.return_value = True
        mock_get_files_async.return_value = ["file1.txt"]

        async def fake_get_head_object_async(*args, **kwargs):
            return {
                "ContentLength": 1024,
                "LastModified": "2023-10-01T12:00:00Z",
                "ETag": "abc123",
            }

        mock_get_head_object_async.side_effect = fake_get_head_object_async
        mock_get_async_conn.return_value.__aenter__.return_value = async_mock.AsyncMock()
        trigger = S3KeyTrigger(
            bucket_key="test_bucket/file",
            bucket_name="test_bucket",
            should_check_fn=True,
            metadata_keys=["*"],
            poke_interval=0.1,
        )
        result = await asyncio.wait_for(trigger.run().__anext__(), timeout=2)
        expected = TriggerEvent(
            {
                "status": "running",
                "files": [
                    {
                        "ContentLength": 1024,
                        "LastModified": "2023-10-01T12:00:00Z",
                        "ETag": "abc123",
                        "Key": "file1.txt",
                    }
                ],
            }
        )
        assert result == expected


class TestS3KeysUnchangedTrigger:
    def test_serialization(self):
        """
        Asserts that the S3KeysUnchangedTrigger correctly serializes its arguments
        and classpath.
        """
        trigger = S3KeysUnchangedTrigger(
            bucket_name="test_bucket",
            prefix="test",
            inactivity_period=1,
            min_objects=1,
            inactivity_seconds=0,
            previous_objects=None,
        )
        classpath, kwargs = trigger.serialize()
        assert classpath == "airflow.providers.amazon.aws.triggers.s3.S3KeysUnchangedTrigger"
        assert kwargs == {
            "bucket_name": "test_bucket",
            "prefix": "test",
            "inactivity_period": 1,
            "min_objects": 1,
            "inactivity_seconds": 0,
            "previous_objects": set(),
            "allow_delete": True,
            "aws_conn_id": "aws_default",
            "last_activity_time": None,
            "hook_params": {},
            "verify": None,
            "region_name": None,
            "botocore_config": None,
            "polling_period_seconds": 0,
        }

    @pytest.mark.asyncio
    @async_mock.patch("airflow.providers.amazon.aws.triggers.s3.S3Hook.get_async_conn")
    async def test_run_wait(self, mock_client):
        """Test if the task is run in trigger successfully."""
        mock_client.return_value.return_value.check_key.return_value = True
        trigger = S3KeysUnchangedTrigger(bucket_name="test_bucket", prefix="test")
        with mock_client:
            task = asyncio.create_task(trigger.run().__anext__())
            await asyncio.sleep(0.5)

            assert task.done() is True
            asyncio.get_event_loop().stop()

    def test_run_raise_value_error(self):
        """
        Test if the S3KeysUnchangedTrigger raises Value error for negative inactivity_period.
        """
        with pytest.raises(ValueError, match="inactivity_period must be non-negative"):
            S3KeysUnchangedTrigger(bucket_name="test_bucket", prefix="test", inactivity_period=-100)

    @pytest.mark.asyncio
    @async_mock.patch("airflow.providers.amazon.aws.triggers.s3.S3Hook.get_async_conn")
    @async_mock.patch("airflow.providers.amazon.aws.triggers.s3.S3Hook.is_keys_unchanged_async")
    async def test_run_success(self, mock_is_keys_unchanged, mock_client):
        """
        Test if the task is run in triggerer successfully.
        """
        mock_is_keys_unchanged.return_value = {"status": "success"}
        trigger = S3KeysUnchangedTrigger(bucket_name="test_bucket", prefix="test")
        generator = trigger.run()
        actual = await generator.asend(None)
        assert TriggerEvent({"status": "success"}) == actual

    @pytest.mark.asyncio
    @async_mock.patch("airflow.providers.amazon.aws.triggers.s3.S3Hook.get_async_conn")
    @async_mock.patch("airflow.providers.amazon.aws.triggers.s3.S3Hook.is_keys_unchanged_async")
    async def test_run_pending(self, mock_is_keys_unchanged, mock_client):
        """Test if the task is run in triggerer successfully."""
        mock_is_keys_unchanged.return_value = {"status": "pending", "last_activity_time": datetime.now()}
        trigger = S3KeysUnchangedTrigger(bucket_name="test_bucket", prefix="test")
        task = asyncio.create_task(trigger.run().__anext__())
        await asyncio.sleep(0.5)
        # TriggerEvent was not returned
        assert task.done() is False
        asyncio.get_event_loop().stop()


def _head(etag, last_modified=None, size=None):
    """Build a head_object response pointing at ``etag``. ``None`` means the key is absent."""
    if etag is None:
        return None
    return {"ETag": etag, "LastModified": last_modified, "ContentLength": size}


async def _collect(trigger, count, timeout=10.0):
    """Pull up to ``count`` payloads off the trigger, giving up after ``timeout``."""
    from contextlib import aclosing, suppress

    payloads = []
    generator = trigger.run()
    async with aclosing(generator):

        async def pump():
            async for event in generator:
                payloads.append(event.payload)
                if len(payloads) >= count:
                    return

        with suppress(asyncio.TimeoutError):
            await asyncio.wait_for(pump(), timeout=timeout)
    return payloads


@async_mock.patch("airflow.providers.amazon.aws.triggers.s3.S3Hook.get_async_conn")
class TestS3KeyUpdateTrigger:
    @staticmethod
    def _conn(mock_get_async_conn):
        mock_get_async_conn.return_value.__aenter__.return_value = async_mock.AsyncMock()

    def test_serialize_round_trip(self, mock_get_async_conn):
        trigger = S3KeyUpdateTrigger(
            bucket_name="my-bucket",
            bucket_key="reports/daily.csv",
            aws_conn_id="my_conn",
            poke_interval=5,
            region_name="eu-west-1",
        )
        classpath, kwargs = trigger.serialize()

        assert classpath == "airflow.providers.amazon.aws.triggers.s3.S3KeyUpdateTrigger"
        assert kwargs == {
            "bucket_name": "my-bucket",
            "bucket_key": "reports/daily.csv",
            "aws_conn_id": "my_conn",
            "poke_interval": 5,
            "region_name": "eu-west-1",
            "verify": None,
            "botocore_config": None,
            "last_seen_etag": None,
            "hook_params": {},
        }
        assert S3KeyUpdateTrigger(**kwargs).serialize() == (classpath, kwargs)

    @pytest.mark.asyncio
    @async_mock.patch(
        "airflow.providers.amazon.aws.triggers.s3.S3Hook.get_head_object_async",
        new_callable=async_mock.AsyncMock,
    )
    async def test_emits_current_object_on_first_poll(self, mock_head, mock_get_async_conn):
        """With no watermark the current object is itself the first event."""
        self._conn(mock_get_async_conn)
        mock_head.side_effect = [_head('"aaa"')]
        trigger = S3KeyUpdateTrigger(bucket_name="b", bucket_key="k", poke_interval=0.01)

        payloads = await _collect(trigger, 1)

        assert [(p["etag"], p["previous_etag"]) for p in payloads] == [('"aaa"', None)]

    @pytest.mark.asyncio
    @async_mock.patch(
        "airflow.providers.amazon.aws.triggers.s3.S3Hook.get_head_object_async",
        new_callable=async_mock.AsyncMock,
    )
    async def test_silent_while_etag_unchanged(self, mock_head, mock_get_async_conn):
        """An object that has not changed must not schedule anything."""
        self._conn(mock_get_async_conn)
        mock_head.return_value = _head('"aaa"')
        trigger = S3KeyUpdateTrigger(
            bucket_name="b", bucket_key="k", poke_interval=0.01, last_seen_etag='"aaa"'
        )

        payloads = await _collect(trigger, 1, timeout=0.2)

        assert payloads == []

    @pytest.mark.asyncio
    @async_mock.patch(
        "airflow.providers.amazon.aws.triggers.s3.S3Hook.get_head_object_async",
        new_callable=async_mock.AsyncMock,
    )
    async def test_emits_once_per_update(self, mock_head, mock_get_async_conn):
        """Each new ETag produces exactly one event carrying the ETag it replaced."""
        self._conn(mock_get_async_conn)
        mock_head.side_effect = [_head('"aaa"'), _head('"bbb"'), _head('"bbb"'), _head('"ccc"')]
        trigger = S3KeyUpdateTrigger(
            bucket_name="b", bucket_key="k", poke_interval=0.01, last_seen_etag='"aaa"'
        )

        payloads = await _collect(trigger, 2)

        assert [(p["previous_etag"], p["etag"]) for p in payloads] == [('"aaa"', '"bbb"'), ('"bbb"', '"ccc"')]

    @pytest.mark.asyncio
    @async_mock.patch(
        "airflow.providers.amazon.aws.triggers.s3.S3Hook.get_head_object_async",
        new_callable=async_mock.AsyncMock,
    )
    async def test_silent_while_key_absent(self, mock_head, mock_get_async_conn):
        """Watching a key that does not exist yet waits for it rather than failing."""
        self._conn(mock_get_async_conn)
        mock_head.return_value = _head(None)
        trigger = S3KeyUpdateTrigger(bucket_name="b", bucket_key="k", poke_interval=0.01)

        payloads = await _collect(trigger, 1, timeout=0.2)

        assert payloads == []

    @pytest.mark.asyncio
    @async_mock.patch(
        "airflow.providers.amazon.aws.triggers.s3.S3Hook.get_head_object_async",
        new_callable=async_mock.AsyncMock,
    )
    async def test_fires_once_the_key_appears(self, mock_head, mock_get_async_conn):
        self._conn(mock_get_async_conn)
        mock_head.side_effect = [_head(None), _head(None), _head('"aaa"')]
        trigger = S3KeyUpdateTrigger(bucket_name="b", bucket_key="k", poke_interval=0.01)

        payloads = await _collect(trigger, 1)

        assert [p["etag"] for p in payloads] == ['"aaa"']

    @pytest.mark.asyncio
    @async_mock.patch(
        "airflow.providers.amazon.aws.triggers.s3.S3Hook.get_head_object_async",
        new_callable=async_mock.AsyncMock,
    )
    async def test_event_payload_fields(self, mock_head, mock_get_async_conn):
        """The event reports bucket, key, size and an ISO-8601 last_modified."""
        self._conn(mock_get_async_conn)
        mock_head.side_effect = [_head('"aaa"', last_modified=datetime(2026, 10, 5, 12, 0, 0), size=42)]
        trigger = S3KeyUpdateTrigger(bucket_name="b", bucket_key="k", poke_interval=0.01)

        payloads = await _collect(trigger, 1)

        assert payloads == [
            {
                "bucket_name": "b",
                "bucket_key": "k",
                "etag": '"aaa"',
                "previous_etag": None,
                "last_modified": "2026-10-05T12:00:00",
                "size": 42,
            }
        ]

    @pytest.mark.asyncio
    @async_mock.patch(
        "airflow.providers.amazon.aws.triggers.s3.S3Hook.get_head_object_async",
        new_callable=async_mock.AsyncMock,
    )
    async def test_resumes_from_the_stored_watermark(self, mock_head, mock_get_async_conn):
        """A restarted triggerer must not re-emit an object it already reported."""
        self._conn(mock_get_async_conn)
        store = async_mock.MagicMock()
        store.get.return_value = '"bbb"'
        mock_head.return_value = _head('"bbb"')
        trigger = S3KeyUpdateTrigger(
            bucket_name="b", bucket_key="k", poke_interval=0.01, last_seen_etag='"aaa"'
        )
        trigger.asset_state_store = store

        payloads = await _collect(trigger, 1, timeout=0.2)

        assert payloads == []
        store.get.assert_called_once_with("etag")

    @pytest.mark.asyncio
    @async_mock.patch(
        "airflow.providers.amazon.aws.triggers.s3.S3Hook.get_head_object_async",
        new_callable=async_mock.AsyncMock,
    )
    async def test_persists_the_watermark_on_each_event(self, mock_head, mock_get_async_conn):
        self._conn(mock_get_async_conn)
        store = async_mock.MagicMock()
        store.get.return_value = None
        mock_head.side_effect = [_head('"aaa"'), _head('"bbb"'), _head('"bbb"')]
        trigger = S3KeyUpdateTrigger(bucket_name="b", bucket_key="k", poke_interval=0.01)
        trigger.asset_state_store = store

        payloads = await _collect(trigger, 2)

        assert [p["etag"] for p in payloads] == ['"aaa"', '"bbb"']
        assert [c.args for c in store.set.call_args_list] == [("etag", '"aaa"'), ("etag", '"bbb"')]
