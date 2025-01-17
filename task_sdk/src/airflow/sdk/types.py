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

from typing import TYPE_CHECKING, Any, Protocol

if TYPE_CHECKING:
    from collections.abc import Iterator
    from datetime import datetime

    from airflow.sdk.definitions.asset import Asset, AssetAlias, AssetAliasEvent, BaseAssetUniqueKey
    from airflow.sdk.definitions.baseoperator import BaseOperator


class DagRunProtocol(Protocol):
    """Minimal interface for a DAG run available during the execution."""

    dag_id: str
    run_id: str
    logical_date: datetime
    data_interval_start: datetime | None
    data_interval_end: datetime | None
    start_date: datetime
    end_date: datetime | None
    run_type: Any
    conf: dict[str, Any] | None


class RuntimeTaskInstanceProtocol(Protocol):
    """Minimal interface for a task instance available during the execution."""

    task: BaseOperator
    task_id: str
    dag_id: str
    run_id: str
    try_number: int
    map_index: int
    max_tries: int
    hostname: str | None = None

    def xcom_pull(
        self,
        task_ids: str | list[str] | None = None,
        dag_id: str | None = None,
        key: str = "return_value",
        # TODO: `include_prior_dates` isn't yet supported in the SDK
        # include_prior_dates: bool = False,
        *,
        map_indexes: int | list[int] | None = None,
        default: Any = None,
        run_id: str | None = None,
    ) -> Any: ...

    def xcom_push(self, key: str, value: Any) -> None: ...


class OutletEventAccessorProtocol(Protocol):
    """Protocol for managing access to a specific outlet event accessor."""

    key: BaseAssetUniqueKey
    extra: dict[str, Any]
    asset_alias_events: list[AssetAliasEvent]

    def __init__(
        self,
        *,
        key: BaseAssetUniqueKey,
        extra: dict[str, Any],
        asset_alias_events: list[AssetAliasEvent],
    ) -> None: ...
    def add(self, asset: Asset, extra: dict[str, Any] | None = None) -> None: ...


class OutletEventAccessorsProtocol(Protocol):
    """Protocol for managing access to outlet event accessors."""

    def __iter__(self) -> Iterator[Asset | AssetAlias]: ...
    def __len__(self) -> int: ...
    def __getitem__(self, key: Asset | AssetAlias) -> OutletEventAccessorProtocol: ...
