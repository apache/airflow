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

import uuid
from collections.abc import Iterable
from typing import TYPE_CHECKING, Any, Protocol, TypeAlias

from airflow.sdk.bases.xcom import BaseXCom
from airflow.sdk.definitions._internal.types import NOTSET, ArgNotSet

if TYPE_CHECKING:
    from collections.abc import Iterator

    from pydantic import AwareDatetime

    from airflow.sdk._shared.logging.types import Logger as Logger
    from airflow.sdk.bases.operator import BaseOperator
    from airflow.sdk.definitions.asset import Asset, AssetAlias, AssetAliasEvent, AssetRef, BaseAssetUniqueKey
    from airflow.sdk.definitions.context import Context
    from airflow.sdk.definitions.mappedoperator import MappedOperator

    Operator: TypeAlias = BaseOperator | MappedOperator


class DagRunProtocol(Protocol):
    """Minimal interface for a Dag run available during the execution."""

    dag_id: str
    run_id: str
    logical_date: AwareDatetime | None
    data_interval_start: AwareDatetime | None
    data_interval_end: AwareDatetime | None
    start_date: AwareDatetime
    end_date: AwareDatetime | None
    run_type: Any
    run_after: AwareDatetime
    conf: dict[str, Any] | None


class RuntimeTaskInstanceProtocol(Protocol):
    """Minimal interface for a task instance available during the execution."""

    id: uuid.UUID
    dag_version_id: uuid.UUID
    task: BaseOperator
    task_id: str
    dag_id: str
    run_id: str
    try_number: int
    map_index: int | None
    max_tries: int
    hostname: str | None = None
    start_date: AwareDatetime
    end_date: AwareDatetime | None = None

    def xcom_pull(
        self,
        task_ids: str | list[str] | None = None,
        dag_id: str | None = None,
        key: str = BaseXCom.XCOM_RETURN_KEY,
        include_prior_dates: bool = False,
        *,
        map_indexes: int | Iterable[int] | None | ArgNotSet = NOTSET,
        default: Any = None,
        run_id: str | None = None,
    ) -> Any: ...

    def xcom_push(self, key: str, value: Any) -> None: ...

    def get_template_context(self) -> Context: ...

    def get_first_reschedule_date(self, first_try_number) -> AwareDatetime | None: ...

    def get_previous_dagrun(self, state: str | None = None) -> DagRunProtocol | None: ...

    @staticmethod
    def get_ti_count(
        dag_id: str,
        map_index: int | None = None,
        task_ids: list[str] | None = None,
        task_group_id: str | None = None,
        logical_dates: list[AwareDatetime] | None = None,
        run_ids: list[str] | None = None,
        states: list[str] | None = None,
    ) -> int: ...

    @staticmethod
    def get_task_states(
        dag_id: str,
        map_index: int | None = None,
        task_ids: list[str] | None = None,
        task_group_id: str | None = None,
        logical_dates: list[AwareDatetime] | None = None,
        run_ids: list[str] | None = None,
    ) -> dict[str, Any]: ...

    @staticmethod
    def get_dr_count(
        dag_id: str,
        logical_dates: list[AwareDatetime] | None = None,
        run_ids: list[str] | None = None,
        states: list[str] | None = None,
    ) -> int: ...

    @staticmethod
    def get_dagrun_state(dag_id: str, run_id: str) -> str: ...


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
    def __getitem__(self, key: Asset | AssetAlias | AssetRef) -> OutletEventAccessorProtocol: ...
