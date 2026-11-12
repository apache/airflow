#
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

from typing import TYPE_CHECKING

import attrs

if TYPE_CHECKING:
    from datetime import datetime

    from pydantic import JsonValue

    from airflow.serialization.definitions.assets import SerializedAsset, SerializedAssetAlias


@attrs.define
class AssetEvent:
    """
    Asset event representation for asset listener hooks.

    ``id``, ``timestamp`` and ``asset_id`` are those of the persisted asset event. The
    ``source_watcher_name`` and ``source_trigger_classpath`` are only set when the event was emitted by an asset watcher.
    """

    asset: SerializedAsset
    extra: dict[str, JsonValue]
    source_dag_id: str | None
    source_task_id: str | None
    source_run_id: str | None
    source_map_index: int | None
    source_aliases: list[SerializedAssetAlias]
    partition_key: str | None
    partition_date: datetime | None = None
    # Keyword-only so required fields can follow the defaulted ones without reordering the class.
    id: int = attrs.field(kw_only=True)
    timestamp: datetime = attrs.field(kw_only=True)
    asset_id: int = attrs.field(kw_only=True)
    source_watcher_name: str | None = None
    source_trigger_classpath: str | None = None
