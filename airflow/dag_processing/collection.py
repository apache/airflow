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

"""
Utility code that write DAGs in bulk into the database.

This should generally only be called by internal methods such as
``DagBag._sync_to_db``, ``DAG.bulk_write_to_db``.

:meta private:
"""

from __future__ import annotations

import itertools
import logging
from typing import TYPE_CHECKING, NamedTuple

from sqlalchemy import func, select, tuple_
from sqlalchemy.orm import joinedload, load_only

from airflow.assets import Asset, AssetAlias
from airflow.assets.manager import asset_manager
from airflow.models.asset import (
    AssetActive,
    AssetAliasModel,
    AssetModel,
    DagScheduleAssetAliasReference,
    DagScheduleAssetReference,
    TaskOutletAssetReference,
)
from airflow.models.dag import DAG, DagModel, DagOwnerAttributes, DagTag
from airflow.models.dagrun import DagRun
from airflow.utils.sqlalchemy import with_row_locks
from airflow.utils.timezone import utcnow
from airflow.utils.types import DagRunType

if TYPE_CHECKING:
    from collections.abc import Collection, Iterable, Iterator

    from sqlalchemy.orm import Session
    from sqlalchemy.sql import Select

    from airflow.typing_compat import Self

log = logging.getLogger(__name__)


def _find_orm_dags(dag_ids: Iterable[str], *, session: Session) -> dict[str, DagModel]:
    """Find existing DagModel objects from DAG objects."""
    stmt = (
        select(DagModel)
        .options(joinedload(DagModel.tags, innerjoin=False))
        .where(DagModel.dag_id.in_(dag_ids))
        .options(joinedload(DagModel.schedule_dataset_references))
        .options(joinedload(DagModel.schedule_dataset_alias_references))
        .options(joinedload(DagModel.task_outlet_dataset_references))
    )
    stmt = with_row_locks(stmt, of=DagModel, session=session)
    return {dm.dag_id: dm for dm in session.scalars(stmt).unique()}


def _create_orm_dags(dags: Iterable[DAG], *, session: Session) -> Iterator[DagModel]:
    for dag in dags:
        orm_dag = DagModel(dag_id=dag.dag_id)
        if dag.is_paused_upon_creation is not None:
            orm_dag.is_paused = dag.is_paused_upon_creation
        log.info("Creating ORM DAG for %s", dag.dag_id)
        session.add(orm_dag)
        yield orm_dag


def _get_latest_runs_stmt(dag_ids: Collection[str]) -> Select:
    """Build a select statement to retrieve the last automated run for each dag."""
    if len(dag_ids) == 1:  # Index optimized fast path to avoid more complicated & slower groupby queryplan.
        (dag_id,) = dag_ids
        last_automated_runs_subq = (
            select(func.max(DagRun.execution_date).label("max_execution_date"))
            .where(
                DagRun.dag_id == dag_id,
                DagRun.run_type.in_((DagRunType.BACKFILL_JOB, DagRunType.SCHEDULED)),
            )
            .scalar_subquery()
        )
        query = select(DagRun).where(
            DagRun.dag_id == dag_id,
            DagRun.execution_date == last_automated_runs_subq,
        )
    else:
        last_automated_runs_subq = (
            select(DagRun.dag_id, func.max(DagRun.execution_date).label("max_execution_date"))
            .where(
                DagRun.dag_id.in_(dag_ids),
                DagRun.run_type.in_((DagRunType.BACKFILL_JOB, DagRunType.SCHEDULED)),
            )
            .group_by(DagRun.dag_id)
            .subquery()
        )
        query = select(DagRun).where(
            DagRun.dag_id == last_automated_runs_subq.c.dag_id,
            DagRun.execution_date == last_automated_runs_subq.c.max_execution_date,
        )
    return query.options(
        load_only(
            DagRun.dag_id,
            DagRun.execution_date,
            DagRun.data_interval_start,
            DagRun.data_interval_end,
        )
    )


class _RunInfo(NamedTuple):
    latest_runs: dict[str, DagRun]
    num_active_runs: dict[str, int]

    @classmethod
    def calculate(cls, dags: dict[str, DAG], *, session: Session) -> Self:
        """
        Query the the run counts from the db.

        :param dags: dict of dags to query
        """
        # Skip these queries entirely if no DAGs can be scheduled to save time.
        if not any(dag.timetable.can_be_scheduled for dag in dags.values()):
            return cls({}, {})

        latest_runs = {run.dag_id: run for run in session.scalars(_get_latest_runs_stmt(dag_ids=dags.keys()))}
        active_run_counts = DagRun.active_runs_of_dags(
            dag_ids=dags.keys(),
            exclude_backfill=True,
            session=session,
        )

        return cls(latest_runs, active_run_counts)


def _update_dag_tags(tag_names: set[str], dm: DagModel, *, session: Session) -> None:
    orm_tags = {t.name: t for t in dm.tags}
    for name, orm_tag in orm_tags.items():
        if name not in tag_names:
            session.delete(orm_tag)
    dm.tags.extend(DagTag(name=name, dag_id=dm.dag_id) for name in tag_names.difference(orm_tags))


def _update_dag_owner_links(dag_owner_links: dict[str, str], dm: DagModel, *, session: Session) -> None:
    orm_dag_owner_attributes = {obj.owner: obj for obj in dm.dag_owner_links}
    for owner, obj in orm_dag_owner_attributes.items():
        try:
            link = dag_owner_links[owner]
        except KeyError:
            session.delete(obj)
        else:
            if obj.link != link:
                obj.link = link
    dm.dag_owner_links.extend(
        DagOwnerAttributes(dag_id=dm.dag_id, owner=owner, link=link)
        for owner, link in dag_owner_links.items()
        if owner not in orm_dag_owner_attributes
    )


class DagModelOperation(NamedTuple):
    """Collect DAG objects and perform database operations for them."""

    dags: dict[str, DAG]

    def add_dags(self, *, session: Session) -> dict[str, DagModel]:
        orm_dags = _find_orm_dags(self.dags, session=session)
        orm_dags.update(
            (model.dag_id, model)
            for model in _create_orm_dags(
                (dag for dag_id, dag in self.dags.items() if dag_id not in orm_dags),
                session=session,
            )
        )
        return orm_dags

    def update_dags(
        self,
        orm_dags: dict[str, DagModel],
        *,
        processor_subdir: str | None = None,
        session: Session,
    ) -> None:
        # we exclude backfill from active run counts since their concurrency is separate
        run_info = _RunInfo.calculate(
            dags=self.dags,
            session=session,
        )

        for dag_id, dm in sorted(orm_dags.items()):
            dag = self.dags[dag_id]
            dm.fileloc = dag.fileloc
            dm.owners = dag.owner
            dm.is_active = True
            dm.has_import_errors = False
            dm.last_parsed_time = utcnow()
            dm.default_view = dag.default_view
            dm._dag_display_property_value = dag._dag_display_property_value
            dm.description = dag.description
            dm.max_active_tasks = dag.max_active_tasks
            dm.max_active_runs = dag.max_active_runs
            dm.max_consecutive_failed_dag_runs = dag.max_consecutive_failed_dag_runs
            dm.has_task_concurrency_limits = any(
                t.max_active_tis_per_dag is not None or t.max_active_tis_per_dagrun is not None
                for t in dag.tasks
            )
            dm.timetable_summary = dag.timetable.summary
            dm.timetable_description = dag.timetable.description
            dm.dataset_expression = dag.timetable.asset_condition.as_expression()
            dm.processor_subdir = processor_subdir

            last_automated_run: DagRun | None = run_info.latest_runs.get(dag.dag_id)
            if last_automated_run is None:
                last_automated_data_interval = None
            else:
                last_automated_data_interval = dag.get_run_data_interval(last_automated_run)
            if run_info.num_active_runs.get(dag.dag_id, 0) >= dm.max_active_runs:
                dm.next_dagrun_create_after = None
            else:
                dm.calculate_dagrun_date_fields(dag, last_automated_data_interval)

            if not dag.timetable.asset_condition:
                dm.schedule_dataset_references = []
                dm.schedule_dataset_alias_references = []
            # FIXME: STORE NEW REFERENCES.

            if dag.tags:
                _update_dag_tags(set(dag.tags), dm, session=session)
            else:  # Optimization: no references at all, just clear everything.
                dm.tags = []
            if dag.owner_links:
                _update_dag_owner_links(dag.owner_links, dm, session=session)
            else:  # Optimization: no references at all, just clear everything.
                dm.dag_owner_links = []


def _find_all_assets(dags: Iterable[DAG]) -> Iterator[Asset]:
    for dag in dags:
        for _, asset in dag.timetable.asset_condition.iter_assets():
            yield asset
        for task in dag.task_dict.values():
            for obj in itertools.chain(task.inlets, task.outlets):
                if isinstance(obj, Asset):
                    yield obj


def _find_all_asset_aliases(dags: Iterable[DAG]) -> Iterator[AssetAlias]:
    for dag in dags:
        for _, alias in dag.timetable.asset_condition.iter_asset_aliases():
            yield alias
        for task in dag.task_dict.values():
            for obj in itertools.chain(task.inlets, task.outlets):
                if isinstance(obj, AssetAlias):
                    yield obj


class AssetModelOperation(NamedTuple):
    """Collect asset/alias objects from DAGs and perform database operations for them."""

    schedule_asset_references: dict[str, list[Asset]]
    schedule_asset_alias_references: dict[str, list[AssetAlias]]
    outlet_references: dict[str, list[tuple[str, Asset]]]
    assets: dict[str, Asset]
    asset_aliases: dict[str, AssetAlias]

    @classmethod
    def collect(cls, dags: dict[str, DAG]) -> Self:
        coll = cls(
            schedule_asset_references={
                dag_id: [asset for _, asset in dag.timetable.asset_condition.iter_assets()]
                for dag_id, dag in dags.items()
            },
            schedule_asset_alias_references={
                dag_id: [alias for _, alias in dag.timetable.asset_condition.iter_asset_aliases()]
                for dag_id, dag in dags.items()
            },
            outlet_references={
                dag_id: [
                    (task_id, outlet)
                    for task_id, task in dag.task_dict.items()
                    for outlet in task.outlets
                    if isinstance(outlet, Asset)
                ]
                for dag_id, dag in dags.items()
            },
            assets={asset.uri: asset for asset in _find_all_assets(dags.values())},
            asset_aliases={alias.name: alias for alias in _find_all_asset_aliases(dags.values())},
        )
        return coll

    def add_assets(self, *, session: Session) -> dict[str, AssetModel]:
        # Optimization: skip all database calls if no assets were collected.
        if not self.assets:
            return {}
        orm_assets: dict[str, AssetModel] = {
            am.uri: am for am in session.scalars(select(AssetModel).where(AssetModel.uri.in_(self.assets)))
        }
        orm_assets.update(
            (model.uri, model)
            for model in asset_manager.create_assets(
                [asset for uri, asset in self.assets.items() if uri not in orm_assets],
                session=session,
            )
        )
        return orm_assets

    def add_asset_aliases(self, *, session: Session) -> dict[str, AssetAliasModel]:
        # Optimization: skip all database calls if no asset aliases were collected.
        if not self.asset_aliases:
            return {}
        orm_aliases: dict[str, AssetAliasModel] = {
            da.name: da
            for da in session.scalars(
                select(AssetAliasModel).where(AssetAliasModel.name.in_(self.asset_aliases))
            )
        }
        orm_aliases.update(
            (model.name, model)
            for model in asset_manager.create_asset_aliases(
                [alias for name, alias in self.asset_aliases.items() if name not in orm_aliases],
                session=session,
            )
        )
        return orm_aliases

    def add_asset_active_references(self, assets: Collection[AssetModel], *, session: Session) -> None:
        existing_entries = set(
            session.execute(
                select(AssetActive.name, AssetActive.uri).where(
                    tuple_(AssetActive.name, AssetActive.uri).in_((asset.name, asset.uri) for asset in assets)
                )
            )
        )
        session.add_all(
            AssetActive.for_asset(asset)
            for asset in assets
            if (asset.name, asset.uri) not in existing_entries
        )

    def add_dag_asset_references(
        self,
        dags: dict[str, DagModel],
        assets: dict[str, AssetModel],
        *,
        session: Session,
    ) -> None:
        # Optimization: No assets means there are no references to update.
        if not assets:
            return
        for dag_id, references in self.schedule_asset_references.items():
            # Optimization: no references at all; this is faster than repeated delete().
            if not references:
                dags[dag_id].schedule_dataset_references = []
                continue
            referenced_asset_ids = {asset.id for asset in (assets[r.uri] for r in references)}
            orm_refs = {r.dataset_id: r for r in dags[dag_id].schedule_dataset_references}
            for asset_id, ref in orm_refs.items():
                if asset_id not in referenced_asset_ids:
                    session.delete(ref)
            session.bulk_save_objects(
                DagScheduleAssetReference(dataset_id=asset_id, dag_id=dag_id)
                for asset_id in referenced_asset_ids
                if asset_id not in orm_refs
            )

    def add_dag_asset_alias_references(
        self,
        dags: dict[str, DagModel],
        aliases: dict[str, AssetAliasModel],
        *,
        session: Session,
    ) -> None:
        # Optimization: No aliases means there are no references to update.
        if not aliases:
            return
        for dag_id, references in self.schedule_asset_alias_references.items():
            # Optimization: no references at all; this is faster than repeated delete().
            if not references:
                dags[dag_id].schedule_dataset_alias_references = []
                continue
            referenced_alias_ids = {alias.id for alias in (aliases[r.name] for r in references)}
            orm_refs = {a.alias_id: a for a in dags[dag_id].schedule_dataset_alias_references}
            for alias_id, ref in orm_refs.items():
                if alias_id not in referenced_alias_ids:
                    session.delete(ref)
            session.bulk_save_objects(
                DagScheduleAssetAliasReference(alias_id=alias_id, dag_id=dag_id)
                for alias_id in referenced_alias_ids
                if alias_id not in orm_refs
            )

    def add_task_asset_references(
        self,
        dags: dict[str, DagModel],
        assets: dict[str, AssetModel],
        *,
        session: Session,
    ) -> None:
        # Optimization: No assets means there are no references to update.
        if not assets:
            return
        for dag_id, references in self.outlet_references.items():
            # Optimization: no references at all; this is faster than repeated delete().
            if not references:
                dags[dag_id].task_outlet_dataset_references = []
                continue
            referenced_outlets = {
                (task_id, asset.id)
                for task_id, asset in ((task_id, assets[d.uri]) for task_id, d in references)
            }
            orm_refs = {(r.task_id, r.dataset_id): r for r in dags[dag_id].task_outlet_dataset_references}
            for key, ref in orm_refs.items():
                if key not in referenced_outlets:
                    session.delete(ref)
            session.bulk_save_objects(
                TaskOutletAssetReference(dataset_id=asset_id, dag_id=dag_id, task_id=task_id)
                for task_id, asset_id in referenced_outlets
                if (task_id, asset_id) not in orm_refs
            )
