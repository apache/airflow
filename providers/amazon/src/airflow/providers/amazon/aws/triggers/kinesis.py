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
import base64
import hashlib
import json
from collections.abc import AsyncIterator
from typing import TYPE_CHECKING, Any

from airflow.providers.amazon.aws.hooks.kinesis import KinesisHook
from airflow.providers.amazon.version_compat import AIRFLOW_V_3_0_PLUS

if AIRFLOW_V_3_0_PLUS:
    from airflow.triggers.base import BaseEventTrigger, TriggerEvent
else:
    from airflow.triggers.base import (  # type: ignore
        BaseTrigger as BaseEventTrigger,
        TriggerEvent,
    )

if TYPE_CHECKING:
    from airflow.providers.amazon.aws.hooks.base_aws import BaseAwsConnection

_CHECKPOINT_KEY_PREFIX = "kinesis_shard_sequence_numbers"
_ITERATOR_TYPES_WITHOUT_EXTRA_ARGS = frozenset({"LATEST", "TRIM_HORIZON"})


class KinesisTrigger(BaseEventTrigger):
    """
    Wait asynchronously for records on an Amazon Kinesis Data Stream.

    The trigger is long-running and emits one event for each non-empty shard response. Record data is
    base64-encoded in the event payload and must be decoded by the consumer. Delivery is best-effort:
    a triggerer failure can cause records to be repeated or missed around the failure window.

    When Airflow provides an asset state store for a single watched asset, the trigger checkpoints the
    last sequence number read from each shard. The same asset and stream identity share one logical cursor;
    do not configure multiple watchers that require independent progress for the same stream on one asset.

    :param stream_name: Name of the Kinesis Data Stream to watch.
    :param aws_conn_id: AWS connection id.
    :param shard_iterator_type: Position used when a shard has no checkpoint. ``LATEST`` only sees records
        that arrive after the watcher starts; ``TRIM_HORIZON`` starts from the oldest retained record.
    :param batch_size: Maximum records per ``GetRecords`` call and trigger event. Must be between 1 and
        10,000. Record data is base64-encoded before it is stored in the metadata database, so use a
        conservative value for large records.
    :param waiter_delay: Seconds between complete polling sweeps. Kinesis permits at most five
        ``GetRecords`` calls per second per shard. When reading a backlog with ``TRIM_HORIZON``, draining
        ``N`` records from one shard takes roughly ``ceil(N / batch_size) * waiter_delay`` seconds when
        calls return full batches; with the defaults, 10,000 records take about 1,000 seconds.
    :param region_name: AWS region for the Kinesis client.
    :param verify: Whether to verify SSL certificates, or the path to a CA bundle.
    :param botocore_config: Botocore configuration passed to the Kinesis client.
    """

    def __init__(
        self,
        stream_name: str,
        aws_conn_id: str | None = "aws_default",
        shard_iterator_type: str = "LATEST",
        batch_size: int = 100,
        waiter_delay: int = 10,
        region_name: str | None = None,
        verify: bool | str | None = None,
        botocore_config: dict | None = None,
    ) -> None:
        super().__init__()
        if shard_iterator_type not in _ITERATOR_TYPES_WITHOUT_EXTRA_ARGS:
            raise ValueError(
                "shard_iterator_type must be one of "
                f"{sorted(_ITERATOR_TYPES_WITHOUT_EXTRA_ARGS)}; got {shard_iterator_type!r}"
            )
        if not 1 <= batch_size <= 10_000:
            raise ValueError("batch_size must be between 1 and 10000")
        if waiter_delay < 1:
            raise ValueError("waiter_delay must be at least 1 second")

        self.stream_name = stream_name
        self.aws_conn_id = aws_conn_id
        self.shard_iterator_type = shard_iterator_type
        self.batch_size = batch_size
        self.waiter_delay = waiter_delay
        self.region_name = region_name
        self.verify = verify
        self.botocore_config = botocore_config
        self._checkpoint_warning_logged = False

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            self.__class__.__module__ + "." + self.__class__.__qualname__,
            {
                "stream_name": self.stream_name,
                "aws_conn_id": self.aws_conn_id,
                "shard_iterator_type": self.shard_iterator_type,
                "batch_size": self.batch_size,
                "waiter_delay": self.waiter_delay,
                "region_name": self.region_name,
                "verify": self.verify,
                "botocore_config": self.botocore_config,
            },
        )

    @property
    def hook(self) -> KinesisHook:
        return KinesisHook(
            aws_conn_id=self.aws_conn_id,
            region_name=self.region_name,
            verify=self.verify,
            config=self.botocore_config,
        )

    def _build_checkpoint_key(self) -> str:
        identity = json.dumps(
            {
                "stream_name": self.stream_name,
                "aws_conn_id": self.aws_conn_id,
                "region_name": self.region_name,
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
        return f"{_CHECKPOINT_KEY_PREFIX}:{hashlib.sha256(identity).hexdigest()}"

    def _log_checkpoint_warning_once(self, message: str) -> None:
        if self._checkpoint_warning_logged:
            return
        self.log.warning(message)
        self._checkpoint_warning_logged = True

    def _load_checkpoint(self) -> dict[str, str]:
        store = getattr(self, "asset_state_store", None)
        if store is None:
            self._log_checkpoint_warning_once(
                "Kinesis checkpointing is unavailable; using an in-memory cursor"
            )
            return {}

        try:
            checkpoint = store.get(self._build_checkpoint_key(), default={}) or {}
        except ValueError:
            self._log_checkpoint_warning_once(
                "Kinesis checkpointing requires a single watched asset; using an in-memory cursor"
            )
            return {}

        if not isinstance(checkpoint, dict) or not all(
            isinstance(shard_id, str) and isinstance(sequence_number, str)
            for shard_id, sequence_number in checkpoint.items()
        ):
            self._log_checkpoint_warning_once(
                "Kinesis checkpoint data is invalid; using the configured initial position"
            )
            return {}
        return dict(checkpoint)

    def _save_checkpoint(self, sequence_numbers: dict[str, str]) -> None:
        store = getattr(self, "asset_state_store", None)
        if store is None:
            self._log_checkpoint_warning_once(
                "Kinesis checkpointing is unavailable; using an in-memory cursor"
            )
            return

        try:
            store.set(self._build_checkpoint_key(), dict(sequence_numbers))
        except ValueError:
            self._log_checkpoint_warning_once(
                "Kinesis checkpointing requires a single watched asset; using an in-memory cursor"
            )

    async def _find_shard_ids(self, client: BaseAwsConnection) -> list[str]:
        paginator = client.get_paginator("list_shards")
        shard_ids: list[str] = []
        async for page in paginator.paginate(StreamName=self.stream_name):
            shard_ids.extend(shard["ShardId"] for shard in page["Shards"])
        return shard_ids

    async def _get_shard_iterator(
        self,
        client: BaseAwsConnection,
        shard_id: str,
        after_sequence_number: str | None,
        fallback_iterator_type: str,
    ) -> str:
        request: dict[str, Any] = {"StreamName": self.stream_name, "ShardId": shard_id}
        if after_sequence_number:
            request.update(
                ShardIteratorType="AFTER_SEQUENCE_NUMBER",
                StartingSequenceNumber=after_sequence_number,
            )
        else:
            request["ShardIteratorType"] = fallback_iterator_type

        try:
            response = await client.get_shard_iterator(**request)
        except client.exceptions.InvalidArgumentException:
            if not after_sequence_number:
                raise
            self.log.warning(
                "Stored Kinesis checkpoint for shard %s is no longer valid; using the configured initial position",
                shard_id,
            )
            response = await client.get_shard_iterator(
                StreamName=self.stream_name,
                ShardId=shard_id,
                ShardIteratorType=fallback_iterator_type,
            )
        return response["ShardIterator"]

    @staticmethod
    def _build_event_records(shard_id: str, records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        event_records = []
        for record in records:
            timestamp = record.get("ApproximateArrivalTimestamp")
            event_record = {
                "ShardId": shard_id,
                "SequenceNumber": record["SequenceNumber"],
                "PartitionKey": record["PartitionKey"],
                "ApproximateArrivalTimestamp": timestamp.isoformat() if timestamp else None,
                "Data": base64.b64encode(record["Data"]).decode("ascii"),
            }
            event_records.append(event_record)
        return event_records

    async def run(self) -> AsyncIterator[TriggerEvent]:
        loaded_sequence_numbers = self._load_checkpoint()

        async with await self.hook.get_async_conn() as client:
            shard_ids = await self._find_shard_ids(client)
            known_shard_ids = set(shard_ids)
            sequence_numbers = {
                shard_id: sequence_number
                for shard_id, sequence_number in loaded_sequence_numbers.items()
                if shard_id in known_shard_ids
            }
            checkpoint_dirty = sequence_numbers != loaded_sequence_numbers
            iterators: dict[str, str] = {}
            fallback_iterator_types: dict[str, str] = {}

            for shard_id in shard_ids:
                iterators[shard_id] = await self._get_shard_iterator(
                    client,
                    shard_id,
                    sequence_numbers.get(shard_id),
                    self.shard_iterator_type,
                )

            while True:
                for shard_id, shard_iterator in list(iterators.items()):
                    try:
                        response = await client.get_records(
                            ShardIterator=shard_iterator,
                            Limit=self.batch_size,
                        )
                    except client.exceptions.ExpiredIteratorException:
                        iterators[shard_id] = await self._get_shard_iterator(
                            client,
                            shard_id,
                            sequence_numbers.get(shard_id),
                            fallback_iterator_types.get(shard_id, self.shard_iterator_type),
                        )
                        continue
                    except client.exceptions.ProvisionedThroughputExceededException:
                        self.log.warning("Kinesis read throughput exceeded for shard %s", shard_id)
                        continue

                    next_shard_iterator = response.get("NextShardIterator")
                    records = response.get("Records", [])
                    if records:
                        sequence_numbers[shard_id] = records[-1]["SequenceNumber"]
                        checkpoint_dirty = True
                        yield TriggerEvent(
                            {
                                "status": "success",
                                "message_batch": self._build_event_records(shard_id, records),
                            }
                        )

                    if next_shard_iterator is None:
                        for child in response.get("ChildShards", []):
                            child_id = child["ShardId"]
                            if child_id not in iterators:
                                fallback_iterator_types[child_id] = "TRIM_HORIZON"
                                iterators[child_id] = await self._get_shard_iterator(
                                    client,
                                    child_id,
                                    None,
                                    fallback_iterator_types[child_id],
                                )
                        iterators.pop(shard_id, None)
                        fallback_iterator_types.pop(shard_id, None)
                    else:
                        iterators[shard_id] = next_shard_iterator

                if checkpoint_dirty:
                    self._save_checkpoint(sequence_numbers)
                    checkpoint_dirty = False

                await asyncio.sleep(self.waiter_delay)
