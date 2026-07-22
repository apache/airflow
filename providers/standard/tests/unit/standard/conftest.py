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
Shared fixtures for the database-backed asset sensor/trigger tests.

The :class:`AssetEventSensor` / :class:`AssetEventTrigger` always fetch through the Task SDK
:class:`~airflow.sdk.execution_time.context.InletEventsAccessor`, which talks to
``SUPERVISOR_COMMS`` (normally a socket to the supervisor, which calls the execution API, which
queries the DB). There is no supervisor in a unit test, so :class:`_DBBackedComms` replaces only
that transport seam: it runs the *real* execution-API SQL against the test metadata DB, so the
partition-key / extra / time-range / limit filters execute for real. ``process_result`` and the
count check then run unmocked on the returned events.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import pytest

ASSET_NAME = "my_asset"
ASSET_URI = "s3://bucket/key"
ALIAS_NAME = "my_alias"


def event_timestamp(day: int) -> datetime:
    return datetime(2024, 1, day, tzinfo=timezone.utc)


class _DBBackedComms:
    """A ``SUPERVISOR_COMMS`` stand-in that answers asset-event queries from the metadata DB.

    It mirrors what :class:`~airflow.sdk.execution_time.supervisor.ActivitySubprocess` does for
    ``GetAssetEventByAsset`` / ``GetAssetEventByAssetAlias``, but instead of an HTTP round-trip it
    calls the execution-API route function directly against a fresh session (so it is safe to use
    from the triggerer's worker thread as well).
    """

    def send(self, msg: Any) -> Any:
        from airflow.api_fastapi.common.parameters import FilterParam, _RegexParam
        from airflow.api_fastapi.execution_api.routes.asset_events import (
            get_asset_event_by_asset_alias,
            get_asset_event_by_asset_name_uri,
        )
        from airflow.models.asset import AssetEvent
        from airflow.sdk.execution_time.comms import (
            AssetEventsResult,
            GetAssetEventByAsset,
            GetAssetEventByAssetAlias,
        )
        from airflow.utils.session import create_session

        def _extra_list(extra: dict[str, str] | None) -> list[str] | None:
            return [f"{k}={v}" for k, v in extra.items()] if extra else None

        with create_session() as session:
            partition_key = FilterParam(AssetEvent.partition_key, msg.partition_key)
            partition_key_regexp = _RegexParam(AssetEvent.partition_key, msg.partition_key_regexp_pattern)
            if isinstance(msg, GetAssetEventByAsset):
                response = get_asset_event_by_asset_name_uri(
                    name=msg.name,
                    uri=msg.uri,
                    session=session,
                    partition_key=partition_key,
                    partition_key_regexp_pattern=partition_key_regexp,
                    after=msg.after,
                    before=msg.before,
                    ascending=msg.ascending,
                    limit=msg.limit,
                    extra=_extra_list(msg.extra),
                )
            elif isinstance(msg, GetAssetEventByAssetAlias):
                response = get_asset_event_by_asset_alias(
                    name=msg.alias_name,
                    session=session,
                    partition_key=partition_key,
                    partition_key_regexp_pattern=partition_key_regexp,
                    after=msg.after,
                    before=msg.before,
                    ascending=msg.ascending,
                    limit=msg.limit,
                    extra=_extra_list(msg.extra),
                )
            else:  # pragma: no cover - defensive
                raise AssertionError(f"unexpected supervisor message: {msg!r}")
            return AssetEventsResult.from_asset_events_response(response)


@pytest.fixture
def db_supervisor_comms(monkeypatch):
    """Route the Task SDK ``SUPERVISOR_COMMS`` to the metadata DB for the duration of a test."""
    from airflow.sdk.execution_time import task_runner

    comms = _DBBackedComms()
    monkeypatch.setattr(task_runner, "SUPERVISOR_COMMS", comms, raising=False)
    return comms


@pytest.fixture
def asset_event_rows(session):
    """Create one active asset with five events (varied partition keys / extras / timestamps).

    Layout (ascending by timestamp):
      id=1  us|2024-01-01  {"region": "us"}
      id=2  us|2024-01-02  {"region": "us"}
      id=3  eu|2024-01-01  {"region": "eu"}
      id=4  us|2024-01-01  {"region": "us"}   # duplicate partition key of id=1 (exercises dedup)
      id=5  None           {}
    Also links every event to an alias named ``my_alias``.
    """
    from airflow.models.asset import AssetActive, AssetAliasModel, AssetEvent, AssetModel

    from tests_common.test_utils.db import clear_db_assets

    clear_db_assets()

    asset = AssetModel(id=1, name=ASSET_NAME, uri=ASSET_URI, group="asset", extra={})
    session.add_all([asset, AssetActive.for_asset(asset)])
    session.commit()

    common = {
        "asset_id": 1,
        "source_dag_id": "producer",
        "source_task_id": "emit",
        "source_run_id": "run_1",
        "source_map_index": -1,
    }
    events = [
        AssetEvent(
            id=1,
            timestamp=event_timestamp(1),
            partition_key="us|2024-01-01",
            extra={"region": "us"},
            **common,
        ),
        AssetEvent(
            id=2,
            timestamp=event_timestamp(2),
            partition_key="us|2024-01-02",
            extra={"region": "us"},
            **common,
        ),
        AssetEvent(
            id=3,
            timestamp=event_timestamp(3),
            partition_key="eu|2024-01-01",
            extra={"region": "eu"},
            **common,
        ),
        AssetEvent(
            id=4,
            timestamp=event_timestamp(4),
            partition_key="us|2024-01-01",
            extra={"region": "us"},
            **common,
        ),
        AssetEvent(id=5, timestamp=event_timestamp(5), partition_key=None, extra={}, **common),
    ]
    session.add_all(events)
    session.commit()

    alias = AssetAliasModel(id=1, name=ALIAS_NAME)
    alias.asset_events = events
    alias.assets.append(asset)
    session.add(alias)
    session.commit()

    yield events

    clear_db_assets()


@pytest.fixture
def add_asset_event(session):
    """Return a helper that inserts (and commits) an extra asset event for ``asset_id=1``.

    Useful for exercising the trigger's polling loop: insert a new matching event "between" two
    fetches (e.g. from a patched ``asyncio.sleep``) and watch the trigger fire.
    """
    from airflow.models.asset import AssetEvent
    from airflow.utils.session import create_session

    def _add(*, id: int, day: int, partition_key: str | None, extra: dict[str, str] | None = None) -> None:
        with create_session() as new_session:
            new_session.add(
                AssetEvent(
                    id=id,
                    timestamp=event_timestamp(day),
                    partition_key=partition_key,
                    extra=extra or {},
                    asset_id=1,
                    source_dag_id="producer",
                    source_task_id="emit",
                    source_run_id="run_1",
                    source_map_index=-1,
                )
            )
            new_session.commit()

    return _add
