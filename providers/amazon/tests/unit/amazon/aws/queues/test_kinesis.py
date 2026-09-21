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

import importlib
from unittest import mock

import pytest

from tests_common.test_utils.common_msg_queue import mark_common_msg_queue_test

pytest.importorskip("airflow.providers.common.messaging.providers.base_provider")


def test_message_kinesis_queue_create():
    from airflow.providers.amazon.aws.queues.kinesis import KinesisMessageQueueProvider
    from airflow.providers.common.messaging.providers.base_provider import BaseMessageQueueProvider

    provider = KinesisMessageQueueProvider()
    assert isinstance(provider, BaseMessageQueueProvider)
    assert provider.scheme == "kinesis"


@pytest.mark.parametrize(
    ("scheme", "expected_result"),
    [
        pytest.param("kinesis", True, id="kinesis_scheme"),
        pytest.param("sqs", False, id="sqs_scheme"),
        pytest.param("kafka", False, id="kafka_scheme"),
        pytest.param("redis+pubsub", False, id="redis_scheme"),
        pytest.param("unknown", False, id="unknown_scheme"),
    ],
)
def test_message_kinesis_scheme_matches(scheme, expected_result):
    from airflow.providers.amazon.aws.queues.kinesis import KinesisMessageQueueProvider

    provider = KinesisMessageQueueProvider()
    assert provider.scheme_matches(scheme) == expected_result


@pytest.mark.parametrize(
    "queue",
    [
        pytest.param("kinesis://my-stream", id="kinesis_uri"),
        pytest.param("arn:aws:kinesis:us-east-1:123456789012:stream/my-stream", id="kinesis_arn"),
        pytest.param("my-stream", id="stream_name_only"),
    ],
)
def test_message_kinesis_queue_matches(queue):
    from airflow.providers.amazon.aws.queues.kinesis import KinesisMessageQueueProvider

    provider = KinesisMessageQueueProvider()
    assert provider.queue_matches(queue) is False


def test_message_kinesis_queue_trigger_class():
    from airflow.providers.amazon.aws.queues.kinesis import KinesisMessageQueueProvider
    from airflow.providers.amazon.aws.triggers.kinesis import KinesisTrigger

    provider = KinesisMessageQueueProvider()
    assert provider.trigger_class() == KinesisTrigger


def test_message_kinesis_queue_trigger_kwargs():
    from airflow.providers.amazon.aws.queues.kinesis import KinesisMessageQueueProvider

    provider = KinesisMessageQueueProvider()
    assert provider.trigger_kwargs("kinesis://my-stream", stream_name="my-stream") == {}


def test_message_kinesis_missing_common_messaging_dependency():
    import airflow.providers.amazon.aws.queues.kinesis as kinesis_mod
    from airflow.providers.common.compat.sdk import AirflowOptionalProviderFeatureException

    with mock.patch.dict("sys.modules", {"airflow.providers.common.messaging.providers.base_provider": None}):
        with pytest.raises(
            AirflowOptionalProviderFeatureException,
            match=r"This feature requires the 'common\.messaging' provider to be installed in version >= 2\.0\.0\.",
        ):
            importlib.reload(kinesis_mod)

    importlib.reload(kinesis_mod)


@mark_common_msg_queue_test
class TestKinesisMessageQueueTriggerIntegration:
    """Integration tests for KinesisMessageQueueProvider with MessageQueueTrigger and ProvidersManager."""

    @pytest.mark.usefixtures("cleanup_providers_manager")
    def test_provider_discovery(self):
        from airflow.providers_manager import ProvidersManager

        manager = ProvidersManager()
        manager.initialize_providers_queues()
        assert (
            "airflow.providers.amazon.aws.queues.kinesis.KinesisMessageQueueProvider"
            in manager.queue_class_names
        )

    @pytest.mark.usefixtures("cleanup_providers_manager")
    def test_message_queue_trigger_dispatch_to_kinesis(self):
        from airflow.providers.amazon.aws.triggers.kinesis import KinesisTrigger
        from airflow.providers.common.messaging.triggers.msg_queue import MessageQueueTrigger

        trigger = MessageQueueTrigger(
            scheme="kinesis",
            stream_name="test-stream",
            aws_conn_id="aws_test",
            shard_iterator_type="TRIM_HORIZON",
        )
        assert isinstance(trigger.trigger, KinesisTrigger)
        assert trigger.trigger.stream_name == "test-stream"
        assert trigger.trigger.aws_conn_id == "aws_test"
        assert trigger.trigger.shard_iterator_type == "TRIM_HORIZON"

    @pytest.mark.usefixtures("cleanup_providers_manager")
    def test_message_queue_trigger_serialize(self):
        from airflow.providers.common.messaging.triggers.msg_queue import MessageQueueTrigger

        trigger = MessageQueueTrigger(
            scheme="kinesis",
            stream_name="test-stream",
            aws_conn_id="aws_test",
        )
        classpath, kwargs = trigger.serialize()
        assert classpath == "airflow.providers.amazon.aws.triggers.kinesis.KinesisTrigger"
        assert kwargs["stream_name"] == "test-stream"
        assert kwargs["aws_conn_id"] == "aws_test"

    @pytest.mark.asyncio
    @pytest.mark.usefixtures("cleanup_providers_manager")
    async def test_message_queue_trigger_run_yields_events(self):
        from airflow.providers.amazon.aws.triggers.kinesis import KinesisTrigger
        from airflow.providers.common.messaging.triggers.msg_queue import MessageQueueTrigger
        from airflow.triggers.base import TriggerEvent

        trigger = MessageQueueTrigger(
            scheme="kinesis",
            stream_name="test-stream",
            aws_conn_id="aws_test",
        )

        sample_event = TriggerEvent(
            {
                "status": "success",
                "message_batch": [
                    {
                        "ShardId": "shardId-000000000000",
                        "SequenceNumber": "1",
                        "PartitionKey": "partition_key_1",
                        "ApproximateArrivalTimestamp": None,
                        "Data": "dGVzdF9kYXRh",
                    }
                ],
            }
        )

        async def mock_run():
            yield sample_event

        with mock.patch.object(KinesisTrigger, "run", return_value=mock_run()):
            events = []
            async for event in trigger.run():
                events.append(event)

            assert len(events) == 1
            assert events[0].payload == sample_event.payload
