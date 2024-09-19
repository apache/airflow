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

from sqlalchemy import func, select
from sqlalchemy.orm import joinedload, load_only
from sqlalchemy.sql import expression

from airflow.datasets import Dataset, DatasetAlias
from airflow.datasets.manager import dataset_manager
from airflow.models.dag import DAG, DagModel, DagOwnerAttributes, DagTag
from airflow.models.dagrun import DagRun
from airflow.models.dataset import (
    DagScheduleDatasetAliasReference,
    DagScheduleDatasetReference,
    DatasetAliasModel,
    DatasetModel,
    TaskOutletDatasetReference,
)
from airflow.utils.sqlalchemy import with_row_locks
from airflow.utils.timezone import utcnow
from airflow.utils.types import DagRunType

if TYPE_CHECKING:
    from collections.abc import Collection, Iterable, Iterator

    from sqlalchemy.orm import Session
    from sqlalchemy.sql import Select

    from airflow.typing_compat import Self

log = logging.getLogger(__name__)


def collect_orm_dags(dags: dict[str, DAG], *, session: Session) -> dict[str, DagModel]:
    """
    Collect DagModel objects from DAG objects.

    An existing DagModel is fetched if there's a matching ID in the database.
    Otherwise, a new DagModel is created and added to the session.
    """
    stmt = (
        select(DagModel)
        .options(joinedload(DagModel.tags, innerjoin=False))
        .where(DagModel.dag_id.in_(dags))
        .options(joinedload(DagModel.schedule_dataset_references))
        .options(joinedload(DagModel.schedule_dataset_alias_references))
        .options(joinedload(DagModel.task_outlet_dataset_references))
    )
    stmt = with_row_locks(stmt, of=DagModel, session=session)
    existing_orm_dags = {dm.dag_id: dm for dm in session.scalars(stmt).unique()}

    for dag_id, dag in dags.items():
        if dag_id in existing_orm_dags:
            continue
        orm_dag = DagModel(dag_id=dag_id)
        if dag.is_paused_upon_creation is not None:
            orm_dag.is_paused = dag.is_paused_upon_creation
        orm_dag.tags = []
        log.info("Creating ORM DAG for %s", dag_id)
        session.add(orm_dag)
        existing_orm_dags[dag_id] = orm_dag

    return existing_orm_dags


def create_orm_dag(dag: DAG, session: Session) -> DagModel:
    orm_dag = DagModel(dag_id=dag.dag_id)
    if dag.is_paused_upon_creation is not None:
        orm_dag.is_paused = dag.is_paused_upon_creation
    orm_dag.tags = []
    log.info("Creating ORM DAG for %s", dag.dag_id)
    session.add(orm_dag)
    return orm_dag


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
        # Skip these queries entirely if no DAGs can be scheduled to save time.
        if not any(dag.timetable.can_be_scheduled for dag in dags.values()):
            return cls({}, {})
        return cls(
            {run.dag_id: run for run in session.scalars(_get_latest_runs_stmt(dag_ids=dags))},
            DagRun.active_runs_of_dags(dag_ids=dags, session=session),
        )


def update_orm_dags(
    source_dags: dict[str, DAG],
    target_dags: dict[str, DagModel],
    *,
    processor_subdir: str | None = None,
    session: Session,
) -> None:
    """
    Apply DAG attributes to DagModel objects.

    Objects in ``target_dags`` are modified in-place.
    """
    run_info = _RunInfo.calculate(source_dags, session=session)

    for dag_id, dm in sorted(target_dags.items()):
        dag = source_dags[dag_id]
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
            t.max_active_tis_per_dag is not None or t.max_active_tis_per_dagrun is not None for t in dag.tasks
        )
        dm.timetable_summary = dag.timetable.summary
        dm.timetable_description = dag.timetable.description
        dm.dataset_expression = dag.timetable.dataset_condition.as_expression()
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

        if not dag.timetable.dataset_condition:
            dm.schedule_dataset_references = []
            dm.schedule_dataset_alias_references = []
        # FIXME: STORE NEW REFERENCES.

        dag_tags = set(dag.tags or ())
        for orm_tag in (dm_tags := list(dm.tags or [])):
            if orm_tag.name not in dag_tags:
                session.delete(orm_tag)
                dm.tags.remove(orm_tag)
        orm_tag_names = {t.name for t in dm_tags}
        for dag_tag in dag_tags:
            if dag_tag not in orm_tag_names:
                dag_tag_orm = DagTag(name=dag_tag, dag_id=dag.dag_id)
                dm.tags.append(dag_tag_orm)
                session.add(dag_tag_orm)

        dm_links = dm.dag_owner_links or []
        for dm_link in dm_links:
            if dm_link not in dag.owner_links:
                session.delete(dm_link)
        for owner_name, owner_link in dag.owner_links.items():
            dag_owner_orm = DagOwnerAttributes(dag_id=dag.dag_id, owner=owner_name, link=owner_link)
            session.add(dag_owner_orm)


def _find_all_datasets(dags: Iterable[DAG]) -> Iterator[Dataset]:
    for dag in dags:
        for _, dataset in dag.timetable.dataset_condition.iter_datasets():
            yield dataset
        for task in dag.task_dict.values():
            for obj in itertools.chain(task.inlets, task.outlets):
                if isinstance(obj, Dataset):
                    yield obj


def _find_all_dataset_aliases(dags: Iterable[DAG]) -> Iterator[DatasetAlias]:
    for dag in dags:
        for _, alias in dag.timetable.dataset_condition.iter_dataset_aliases():
            yield alias
        for task in dag.task_dict.values():
            for obj in itertools.chain(task.inlets, task.outlets):
                if isinstance(obj, DatasetAlias):
                    yield obj


class DatasetModelOperation(NamedTuple):
    """Collect dataset/alias objects from DAGs and perform database operations for them."""

    schedule_dataset_references: dict[str, list[Dataset]]
    schedule_dataset_alias_references: dict[str, list[DatasetAlias]]
    outlet_references: dict[str, list[tuple[str, Dataset]]]
    datasets: dict[str, Dataset]
    dataset_aliases: dict[str, DatasetAlias]

    @classmethod
    def collect(cls, dags: dict[str, DAG]) -> Self:
        coll = cls(
            schedule_dataset_references={
                dag_id: [dataset for _, dataset in dag.timetable.dataset_condition.iter_datasets()]
                for dag_id, dag in dags.items()
            },
            schedule_dataset_alias_references={
                dag_id: [alias for _, alias in dag.timetable.dataset_condition.iter_dataset_aliases()]
                for dag_id, dag in dags.items()
            },
            outlet_references={
                dag_id: [
                    (task_id, outlet)
                    for task_id, task in dag.task_dict.items()
                    for outlet in task.outlets
                    if isinstance(outlet, Dataset)
                ]
                for dag_id, dag in dags.items()
            },
            datasets={dataset.uri: dataset for dataset in _find_all_datasets(dags.values())},
            dataset_aliases={alias.name: alias for alias in _find_all_dataset_aliases(dags.values())},
        )
        return coll

    def add_datasets(self, *, session: Session) -> dict[str, DatasetModel]:
        # Optimization: skip all database calls if no datasets were collected.
        if not self.datasets:
            return {}
        orm_datasets: dict[str, DatasetModel] = {
            dm.uri: dm
            for dm in session.scalars(select(DatasetModel).where(DatasetModel.uri.in_(self.datasets)))
        }
        for model in orm_datasets.values():
            model.is_orphaned = expression.false()
        orm_datasets.update(
            (model.uri, model)
            for model in dataset_manager.create_datasets(
                [dataset for uri, dataset in self.datasets.items() if uri not in orm_datasets],
                session=session,
            )
        )
        return orm_datasets

    def add_dataset_aliases(self, *, session: Session) -> dict[str, DatasetAliasModel]:
        # Optimization: skip all database calls if no dataset aliases were collected.
        if not self.dataset_aliases:
            return {}
        orm_aliases: dict[str, DatasetAliasModel] = {
            da.name: da
            for da in session.scalars(
                select(DatasetAliasModel).where(DatasetAliasModel.name.in_(self.dataset_aliases))
            )
        }
        orm_aliases.update(
            (model.name, model)
            for model in dataset_manager.create_dataset_aliases(
                [alias for name, alias in self.dataset_aliases.items() if name not in orm_aliases],
                session=session,
            )
        )
        return orm_aliases

    def add_dag_dataset_references(
        self,
        dags: dict[str, DagModel],
        datasets: dict[str, DatasetModel],
        *,
        session: Session,
    ) -> None:
        # Optimization: No datasets means there are no references to update.
        if not datasets:
            return
        for dag_id, references in self.schedule_dataset_references.items():
            # Optimization: no references at all; this is faster than repeated delete().
            if not references:
                dags[dag_id].schedule_dataset_references = []
                continue
            referenced_dataset_ids = {dataset.id for dataset in (datasets[r.uri] for r in references)}
            orm_refs = {r.dataset_id: r for r in dags[dag_id].schedule_dataset_references}
            for dataset_id, ref in orm_refs.items():
                if dataset_id not in referenced_dataset_ids:
                    session.delete(ref)
            session.bulk_save_objects(
                DagScheduleDatasetReference(dataset_id=dataset_id, dag_id=dag_id)
                for dataset_id in referenced_dataset_ids
                if dataset_id not in orm_refs
            )

    def add_dag_dataset_alias_references(
        self,
        dags: dict[str, DagModel],
        aliases: dict[str, DatasetAliasModel],
        *,
        session: Session,
    ) -> None:
        # Optimization: No aliases means there are no references to update.
        if not aliases:
            return
        for dag_id, references in self.schedule_dataset_alias_references.items():
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
                DagScheduleDatasetAliasReference(alias_id=alias_id, dag_id=dag_id)
                for alias_id in referenced_alias_ids
                if alias_id not in orm_refs
            )

    def add_task_dataset_references(
        self,
        dags: dict[str, DagModel],
        datasets: dict[str, DatasetModel],
        *,
        session: Session,
    ) -> None:
        # Optimization: No datasets means there are no references to update.
        if not datasets:
            return
        for dag_id, references in self.outlet_references.items():
            # Optimization: no references at all; this is faster than repeated delete().
            if not references:
                dags[dag_id].task_outlet_dataset_references = []
                continue
            referenced_outlets = {
                (task_id, dataset.id)
                for task_id, dataset in ((task_id, datasets[d.uri]) for task_id, d in references)
            }
            orm_refs = {(r.task_id, r.dataset_id): r for r in dags[dag_id].task_outlet_dataset_references}
            for key, ref in orm_refs.items():
                if key not in referenced_outlets:
                    session.delete(ref)
            session.bulk_save_objects(
                TaskOutletDatasetReference(dataset_id=dataset_id, dag_id=dag_id, task_id=task_id)
                for task_id, dataset_id in referenced_outlets
                if (task_id, dataset_id) not in orm_refs
            )
