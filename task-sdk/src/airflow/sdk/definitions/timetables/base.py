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

from collections.abc import Iterator
from typing import TYPE_CHECKING, Any

from airflow.sdk.definitions.asset import AssetUniqueKey, BaseAsset

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from airflow.sdk.definitions.asset import Asset, AssetAlias, AssetRef
    from airflow.serialization.dag_dependency import DagDependency


class NullAsset(BaseAsset):
    """
    Sentinel type that represents "no assets".

    This is only implemented to make typing easier in timetables, and not
    expected to be used anywhere else.

    :meta private:
    """

    def __bool__(self) -> bool:
        return False

    def __or__(self, other: BaseAsset) -> BaseAsset:
        return NotImplemented

    def __and__(self, other: BaseAsset) -> BaseAsset:
        return NotImplemented

    def as_expression(self) -> Any:
        return None

    def evaluate(self, statuses: dict[AssetUniqueKey, bool], *, session: Session | None = None) -> bool:
        return False

    def iter_assets(self) -> Iterator[tuple[AssetUniqueKey, Asset]]:
        return iter(())

    def iter_asset_aliases(self) -> Iterator[tuple[str, AssetAlias]]:
        return iter(())

    def iter_asset_refs(self) -> Iterator[AssetRef]:
        return iter(())

    def iter_dag_dependencies(self, source, target) -> Iterator[DagDependency]:
        return iter(())


class BaseTimetable:
    """Base class inherited by all user-facing timetables."""

    can_be_scheduled: bool = True
    """
    Whether this timetable can actually schedule runs in an automated manner.

    This defaults to and should generally be *True* (including non periodic
    execution types like *@once* and data triggered tables), but
    ``NullTimetable`` sets this to *False*.
    """

    active_runs_limit: int | None = None
    """
    Maximum active runs that can be active at one time for a DAG.

    This is called during DAG initialization, and the return value is used as
    the DAG's default ``max_active_runs`` if not set on the DAG explicitly. This
    should generally return *None* (no limit), but some timetables may limit
    parallelism, such as :class:`~airflow.timetable.simple.ContinuousTimetable`.
    """

    assets: BaseAsset = NullAsset()
