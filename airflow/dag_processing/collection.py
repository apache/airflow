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

import json
import logging
import traceback
from typing import TYPE_CHECKING, Any, NamedTuple

from sqlalchemy import and_, delete, exists, func, insert, select, tuple_
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import joinedload, load_only

from airflow.assets.manager import asset_manager
from airflow.models.asset import (
    AssetActive,
    AssetAliasModel,
    AssetModel,
    DagScheduleAssetAliasReference,
    DagScheduleAssetNameReference,
    DagScheduleAssetReference,
    DagScheduleAssetUriReference,
    TaskOutletAssetReference,
)
from airflow.models.dag import DAG, DagModel, DagOwnerAttributes, DagTag
from airflow.models.dagrun import DagRun
from airflow.models.dagwarning import DagWarningType
from airflow.models.errors import ParseImportError
from airflow.models.trigger import Trigger
from airflow.sdk.definitions.asset import Asset, AssetAlias, AssetNameRef, AssetUriRef
from airflow.serialization.serialized_objects import BaseSerialization
from airflow.triggers.base import BaseTrigger
from airflow.utils.retries import MAX_DB_RETRIES, run_with_db_retries
from airflow.utils.sqlalchemy import with_row_locks
from airflow.utils.timezone import utcnow
from airflow.utils.types import DagRunType

if TYPE_CHECKING:
    from collections.abc import Collection, Iterable, Iterator

    from sqlalchemy.orm import Session
    from sqlalchemy.sql import Select

    from airflow.models.dagwarning import DagWarning
    from airflow.serialization.serialized_objects import MaybeSerializedDAG
    from airflow.triggers.base import BaseTrigger
    from airflow.typing_compat import Self

log = logging.getLogger(__name__)


def _create_orm_dags(dags: Iterable[MaybeSerializedDAG], *, session: Session) -> Iterator[DagModel]:
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
            select(func.max(DagRun.logical_date).label("max_logical_date"))
            .where(
                DagRun.dag_id == dag_id,
                DagRun.run_type.in_((DagRunType.BACKFILL_JOB, DagRunType.SCHEDULED)),
            )
            .scalar_subquery()
        )
        query = select(DagRun).where(
            DagRun.dag_id == dag_id,
            DagRun.logical_date == last_automated_runs_subq,
        )
    else:
        last_automated_runs_subq = (
            select(DagRun.dag_id, func.max(DagRun.logical_date).label("max_logical_date"))
            .where(
                DagRun.dag_id.in_(dag_ids),
                DagRun.run_type.in_((DagRunType.BACKFILL_JOB, DagRunType.SCHEDULED)),
            )
            .group_by(DagRun.dag_id)
            .subquery()
        )
        query = select(DagRun).where(
            DagRun.dag_id == last_automated_runs_subq.c.dag_id,
            DagRun.logical_date == last_automated_runs_subq.c.max_logical_date,
        )
    return query.options(
        load_only(
            DagRun.dag_id,
            DagRun.logical_date,
            DagRun.data_interval_start,
            DagRun.data_interval_end,
        )
    )


class _RunInfo(NamedTuple):
    latest_runs: dict[str, DagRun]
    num_active_runs: dict[str, int]

    @classmethod
    def calculate(cls, dags: dict[str, MaybeSerializedDAG], *, session: Session) -> Self:
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


def _serialize_dag_capturing_errors(dag: MaybeSerializedDAG, session: Session):
    """
    Try to serialize the dag to the DB, but make a note of any errors.

    We can't place them directly in import_errors, as this may be retried, and work the next time
    """
    from airflow import settings
    from airflow.configuration import conf
    from airflow.models.dagcode import DagCode
    from airflow.models.serialized_dag import SerializedDagModel

    try:
        # We can't use bulk_write_to_db as we want to capture each error individually
        dag_was_updated = SerializedDagModel.write_dag(
            dag,
            min_update_interval=settings.MIN_SERIALIZED_DAG_UPDATE_INTERVAL,
            session=session,
        )
        if dag_was_updated:
            _sync_dag_perms(dag, session=session)
        else:
            # Check and update DagCode
            DagCode.update_source_code(dag.dag_id, dag.fileloc)
        return []
    except OperationalError:
        raise
    except Exception:
        log.exception("Failed to write serialized DAG dag_id=%s fileloc=%s", dag.dag_id, dag.fileloc)
        dagbag_import_error_traceback_depth = conf.getint("core", "dagbag_import_error_traceback_depth")
        return [(dag.fileloc, traceback.format_exc(limit=-dagbag_import_error_traceback_depth))]


def _sync_dag_perms(dag: MaybeSerializedDAG, session: Session):
    """Sync DAG specific permissions."""
    dag_id = dag.dag_id

    log.debug("Syncing DAG permissions: %s to the DB", dag_id)
    from airflow.www.security_appless import ApplessAirflowSecurityManager

    security_manager = ApplessAirflowSecurityManager(session=session)
    security_manager.sync_perm_for_dag(dag_id, dag.access_control)


def _update_dag_warnings(
    dag_ids: list[str], warnings: set[DagWarning], warning_types: tuple[DagWarningType], session: Session
):
    from airflow.models.dagwarning import DagWarning

    stored_warnings = set(
        session.scalars(
            select(DagWarning).where(
                DagWarning.dag_id.in_(dag_ids),
                DagWarning.warning_type.in_(warning_types),
            )
        )
    )

    for warning_to_delete in stored_warnings - warnings:
        session.delete(warning_to_delete)

    for warning_to_add in warnings:
        session.merge(warning_to_add)


def _update_import_errors(files_parsed: set[str], import_errors: dict[str, str], session: Session):
    from airflow.listeners.listener import get_listener_manager

    # We can remove anything from files parsed in this batch that doesn't have an error. We need to remove old
    # errors (i.e. from files that are removed) separately

    session.execute(delete(ParseImportError).where(ParseImportError.filename.in_(list(files_parsed))))

    existing_import_error_files = set(session.scalars(select(ParseImportError.filename)))

    # Add the errors of the processed files
    for filename, stacktrace in import_errors.items():
        if filename in existing_import_error_files:
            session.query(ParseImportError).where(ParseImportError.filename == filename).update(
                {"filename": filename, "timestamp": utcnow(), "stacktrace": stacktrace},
            )
            # sending notification when an existing dag import error occurs
            get_listener_manager().hook.on_existing_dag_import_error(filename=filename, stacktrace=stacktrace)
        else:
            session.add(
                ParseImportError(
                    filename=filename,
                    timestamp=utcnow(),
                    stacktrace=stacktrace,
                )
            )
            # sending notification when a new dag import error occurs
            get_listener_manager().hook.on_new_dag_import_error(filename=filename, stacktrace=stacktrace)
        session.query(DagModel).filter(DagModel.fileloc == filename).update({"has_import_errors": True})


def update_dag_parsing_results_in_db(
    dags: Collection[MaybeSerializedDAG],
    import_errors: dict[str, str],
    warnings: set[DagWarning],
    session: Session,
    *,
    warning_types: tuple[DagWarningType] = (DagWarningType.NONEXISTENT_POOL,),
):
    """
    Update everything to do with DAG parsing in the DB.

    This function will create or update rows in the following tables:

    - DagModel (`dag` table), DagTag, DagCode and DagVersion
    - SerializedDagModel (`serialized_dag` table)
    - ParseImportError (including with any errors as a result of serialization, not just parsing)
    - DagWarning
    - DAG Permissions

    This function will not remove any rows for dags not passed in. It will remove parse errors and warnings
    from dags/dag files that are passed in. In order words, if a DAG is passed in with a fileloc of `a.py`
    then all warnings and errors related to this file will be removed.

    ``import_errors`` will be updated in place with an new errors
    """
    # Retry 'DAG.bulk_write_to_db' & 'SerializedDagModel.bulk_sync_to_db' in case
    # of any Operational Errors
    # In case of failures, provide_session handles rollback
    for attempt in run_with_db_retries(logger=log):
        with attempt:
            serialize_errors = []
            log.debug(
                "Running dagbag.bulk_write_to_db with retries. Try %d of %d",
                attempt.retry_state.attempt_number,
                MAX_DB_RETRIES,
            )
            log.debug("Calling the DAG.bulk_sync_to_db method")
            try:
                DAG.bulk_write_to_db(dags, session=session)
                # Write Serialized DAGs to DB, capturing errors
                # Write Serialized DAGs to DB, capturing errors
                for dag in dags:
                    serialize_errors.extend(_serialize_dag_capturing_errors(dag, session))
            except OperationalError:
                session.rollback()
                raise
            # Only now we are "complete" do we update import_errors - don't want to record errors from
            # previous failed attempts
            import_errors.update(dict(serialize_errors))

    # Record import errors into the ORM - we don't retry on this one as it's not as critical that it works
    try:
        # TODO: This won't clear errors for files that exist that no longer contain DAGs. Do we need to pass
        # in the list of file parsed?

        good_dag_filelocs = {dag.fileloc for dag in dags if dag.fileloc not in import_errors}
        _update_import_errors(
            files_parsed=good_dag_filelocs,
            import_errors=import_errors,
            session=session,
        )
    except Exception:
        log.exception("Error logging import errors!")

    # Record DAG warnings in the metadatabase.
    try:
        _update_dag_warnings([dag.dag_id for dag in dags], warnings, warning_types, session)
    except Exception:
        log.exception("Error logging DAG warnings.")

    session.flush()


class DagModelOperation(NamedTuple):
    """Collect DAG objects and perform database operations for them."""

    dags: dict[str, MaybeSerializedDAG]

    def find_orm_dags(self, *, session: Session) -> dict[str, DagModel]:
        """Find existing DagModel objects from DAG objects."""
        stmt = (
            select(DagModel)
            .options(joinedload(DagModel.tags, innerjoin=False))
            .where(DagModel.dag_id.in_(self.dags))
            .options(joinedload(DagModel.schedule_asset_references))
            .options(joinedload(DagModel.schedule_asset_alias_references))
            .options(joinedload(DagModel.task_outlet_asset_references))
        )
        stmt = with_row_locks(stmt, of=DagModel, session=session)
        return {dm.dag_id: dm for dm in session.scalars(stmt).unique()}

    def add_dags(self, *, session: Session) -> dict[str, DagModel]:
        orm_dags = self.find_orm_dags(session=session)
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
        session: Session,
    ) -> None:
        from airflow.configuration import conf

        # we exclude backfill from active run counts since their concurrency is separate
        run_info = _RunInfo.calculate(
            dags=self.dags,
            session=session,
        )

        for dag_id, dm in sorted(orm_dags.items()):
            dag = self.dags[dag_id]
            dm.fileloc = dag.fileloc
            dm.owners = dag.owner or conf.get("operators", "default_owner")
            dm.is_active = True
            dm.has_import_errors = False
            dm.last_parsed_time = utcnow()
            dm.default_view = dag.default_view or conf.get("webserver", "dag_default_view").lower()
            if hasattr(dag, "_dag_display_property_value"):
                dm._dag_display_property_value = dag._dag_display_property_value
            elif dag.dag_display_name != dag.dag_id:
                dm._dag_display_property_value = dag.dag_display_name
            dm.description = dag.description

            # These "is not None" checks are because with a LazySerializedDag object where the user hasn't
            # specified an explicit value, we don't get the default values from the config in the lazy
            # serialized ver
            # we just
            if dag.max_active_tasks is not None:
                dm.max_active_tasks = dag.max_active_tasks
            elif dag.max_active_tasks is None and dm.max_active_tasks is None:
                dm.max_active_tasks = conf.getint("core", "max_active_tasks_per_dag")

            if dag.max_active_runs is not None:
                dm.max_active_runs = dag.max_active_runs
            elif dag.max_active_runs is None and dm.max_active_runs is None:
                dm.max_active_runs = conf.getint("core", "max_active_runs_per_dag")

            if dag.max_consecutive_failed_dag_runs is not None:
                dm.max_consecutive_failed_dag_runs = dag.max_consecutive_failed_dag_runs
            elif dag.max_consecutive_failed_dag_runs is None and dm.max_consecutive_failed_dag_runs is None:
                dm.max_consecutive_failed_dag_runs = conf.getint(
                    "core", "max_consecutive_failed_dag_runs_per_dag"
                )

            if hasattr(dag, "has_task_concurrency_limits"):
                dm.has_task_concurrency_limits = dag.has_task_concurrency_limits
            else:
                dm.has_task_concurrency_limits = any(
                    t.max_active_tis_per_dag is not None or t.max_active_tis_per_dagrun is not None
                    for t in dag.tasks
                )
            dm.timetable_summary = dag.timetable.summary
            dm.timetable_description = dag.timetable.description
            dm.asset_expression = dag.timetable.asset_condition.as_expression()

            last_automated_run: DagRun | None = run_info.latest_runs.get(dag.dag_id)
            if last_automated_run is None:
                last_automated_data_interval = None
            else:
                last_automated_data_interval = dag.get_run_data_interval(last_automated_run)
            if run_info.num_active_runs.get(dag.dag_id, 0) >= dm.max_active_runs:
                dm.next_dagrun_create_after = None
            else:
                dm.calculate_dagrun_date_fields(dag, last_automated_data_interval)  # type: ignore[arg-type]

            if not dag.timetable.asset_condition:
                dm.schedule_asset_references = []
                dm.schedule_asset_alias_references = []
            # FIXME: STORE NEW REFERENCES.

            if dag.tags:
                _update_dag_tags(set(dag.tags), dm, session=session)
            else:  # Optimization: no references at all, just clear everything.
                dm.tags = []
            if dag.owner_links:
                _update_dag_owner_links(dag.owner_links, dm, session=session)
            else:  # Optimization: no references at all, just clear everything.
                dm.dag_owner_links = []


def _find_all_assets(dags: Iterable[MaybeSerializedDAG]) -> Iterator[Asset]:
    for dag in dags:
        for _, asset in dag.timetable.asset_condition.iter_assets():
            yield asset
        for _, alias in dag.get_task_assets(of_type=Asset):
            yield alias


def _find_all_asset_aliases(dags: Iterable[MaybeSerializedDAG]) -> Iterator[AssetAlias]:
    for dag in dags:
        for _, alias in dag.timetable.asset_condition.iter_asset_aliases():
            yield alias
        for _, alias in dag.get_task_assets(of_type=AssetAlias):
            yield alias


def _find_active_assets(name_uri_assets, session: Session):
    active_dags = {
        dm.dag_id
        for dm in session.scalars(select(DagModel).where(DagModel.is_active).where(~DagModel.is_paused))
    }

    return set(
        session.execute(
            select(
                AssetModel.name,
                AssetModel.uri,
            ).where(
                tuple_(AssetModel.name, AssetModel.uri).in_(name_uri_assets),
                exists(
                    select(1).where(
                        and_(
                            AssetActive.name == AssetModel.name,
                            AssetActive.uri == AssetModel.uri,
                        ),
                    )
                ),
                exists(
                    select(1).where(
                        and_(
                            DagScheduleAssetReference.asset_id == AssetModel.id,
                            DagScheduleAssetReference.dag_id.in_(active_dags),
                        )
                    )
                ),
            )
        )
    )


class AssetModelOperation(NamedTuple):
    """Collect asset/alias objects from DAGs and perform database operations for them."""

    schedule_asset_references: dict[str, list[Asset]]
    schedule_asset_alias_references: dict[str, list[AssetAlias]]
    schedule_asset_name_references: set[tuple[str, str]]  # dag_id, ref_name.
    schedule_asset_uri_references: set[tuple[str, str]]  # dag_id, ref_uri.
    outlet_references: dict[str, list[tuple[str, Asset]]]
    assets: dict[tuple[str, str], Asset]
    asset_aliases: dict[str, AssetAlias]

    @classmethod
    def collect(cls, dags: dict[str, MaybeSerializedDAG]) -> Self:
        coll = cls(
            schedule_asset_references={
                dag_id: [asset for _, asset in dag.timetable.asset_condition.iter_assets()]
                for dag_id, dag in dags.items()
            },
            schedule_asset_alias_references={
                dag_id: [alias for _, alias in dag.timetable.asset_condition.iter_asset_aliases()]
                for dag_id, dag in dags.items()
            },
            schedule_asset_name_references={
                (dag_id, ref.name)
                for dag_id, dag in dags.items()
                for ref in dag.timetable.asset_condition.iter_asset_refs()
                if isinstance(ref, AssetNameRef)
            },
            schedule_asset_uri_references={
                (dag_id, ref.uri)
                for dag_id, dag in dags.items()
                for ref in dag.timetable.asset_condition.iter_asset_refs()
                if isinstance(ref, AssetUriRef)
            },
            outlet_references={
                dag_id: list(dag.get_task_assets(inlets=False, outlets=True)) for dag_id, dag in dags.items()
            },
            assets={(asset.name, asset.uri): asset for asset in _find_all_assets(dags.values())},
            asset_aliases={alias.name: alias for alias in _find_all_asset_aliases(dags.values())},
        )
        return coll

    def add_assets(self, *, session: Session) -> dict[tuple[str, str], AssetModel]:
        # Optimization: skip all database calls if no assets were collected.
        if not self.assets:
            return {}
        orm_assets: dict[tuple[str, str], AssetModel] = {
            (am.name, am.uri): am
            for am in session.scalars(
                select(AssetModel).where(tuple_(AssetModel.name, AssetModel.uri).in_(self.assets))
            )
        }
        orm_assets.update(
            ((model.name, model.uri), model)
            for model in asset_manager.create_assets(
                [asset for name_uri, asset in self.assets.items() if name_uri not in orm_assets],
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

    def add_dag_asset_references(
        self,
        dags: dict[str, DagModel],
        assets: dict[tuple[str, str], AssetModel],
        *,
        session: Session,
    ) -> None:
        # Optimization: No assets means there are no references to update.
        if not assets:
            return
        for dag_id, references in self.schedule_asset_references.items():
            # Optimization: no references at all; this is faster than repeated delete().
            if not references:
                dags[dag_id].schedule_asset_references = []
                continue
            referenced_asset_ids = {asset.id for asset in (assets[r.name, r.uri] for r in references)}
            orm_refs = {r.asset_id: r for r in dags[dag_id].schedule_asset_references}
            for asset_id, ref in orm_refs.items():
                if asset_id not in referenced_asset_ids:
                    session.delete(ref)
            session.bulk_save_objects(
                DagScheduleAssetReference(asset_id=asset_id, dag_id=dag_id)
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
                dags[dag_id].schedule_asset_alias_references = []
                continue
            referenced_alias_ids = {alias.id for alias in (aliases[r.name] for r in references)}
            orm_refs = {a.alias_id: a for a in dags[dag_id].schedule_asset_alias_references}
            for alias_id, ref in orm_refs.items():
                if alias_id not in referenced_alias_ids:
                    session.delete(ref)
            session.bulk_save_objects(
                DagScheduleAssetAliasReference(alias_id=alias_id, dag_id=dag_id)
                for alias_id in referenced_alias_ids
                if alias_id not in orm_refs
            )

    @staticmethod
    def _add_dag_asset_references(
        references: set[tuple[str, str]],
        model: type[DagScheduleAssetNameReference] | type[DagScheduleAssetUriReference],
        attr: str,
        *,
        session: Session,
    ) -> None:
        if not references:
            return
        orm_refs = set(
            session.scalars(
                select(model.dag_id, getattr(model, attr)).where(
                    model.dag_id.in_(dag_id for dag_id, _ in references)
                )
            )
        )
        new_refs = references - orm_refs
        old_refs = orm_refs - references
        if old_refs:
            session.execute(delete(model).where(tuple_(model.dag_id, getattr(model, attr)).in_(old_refs)))
        if new_refs:
            session.execute(insert(model), [{"dag_id": d, attr: r} for d, r in new_refs])

    def add_dag_asset_name_uri_references(self, *, session: Session) -> None:
        self._add_dag_asset_references(
            self.schedule_asset_name_references,
            DagScheduleAssetNameReference,
            "name",
            session=session,
        )
        self._add_dag_asset_references(
            self.schedule_asset_uri_references,
            DagScheduleAssetUriReference,
            "uri",
            session=session,
        )

    def add_task_asset_references(
        self,
        dags: dict[str, DagModel],
        assets: dict[tuple[str, str], AssetModel],
        *,
        session: Session,
    ) -> None:
        # Optimization: No assets means there are no references to update.
        if not assets:
            return
        for dag_id, references in self.outlet_references.items():
            # Optimization: no references at all; this is faster than repeated delete().
            if not references:
                dags[dag_id].task_outlet_asset_references = []
                continue
            referenced_outlets = {
                (task_id, asset.id)
                for task_id, asset in ((task_id, assets[d.name, d.uri]) for task_id, d in references)
            }
            orm_refs = {(r.task_id, r.asset_id): r for r in dags[dag_id].task_outlet_asset_references}
            for key, ref in orm_refs.items():
                if key not in referenced_outlets:
                    session.delete(ref)
            session.bulk_save_objects(
                TaskOutletAssetReference(asset_id=asset_id, dag_id=dag_id, task_id=task_id)
                for task_id, asset_id in referenced_outlets
                if (task_id, asset_id) not in orm_refs
            )

    def add_asset_trigger_references(
        self, assets: dict[tuple[str, str], AssetModel], *, session: Session
    ) -> None:
        # Update references from assets being used
        refs_to_add: dict[tuple[str, str], set[int]] = {}
        refs_to_remove: dict[tuple[str, str], set[int]] = {}
        triggers: dict[int, BaseTrigger] = {}

        # Optimization: if no asset collected, skip fetching active assets
        active_assets = _find_active_assets(self.assets.keys(), session=session) if self.assets else {}

        for name_uri, asset in self.assets.items():
            # If the asset belong to a DAG not active or paused, consider there is no watcher associated to it
            asset_watchers = asset.watchers if name_uri in active_assets else []
            trigger_hash_to_trigger_dict: dict[int, BaseTrigger] = {
                self._get_base_trigger_hash(trigger): trigger for trigger in asset_watchers
            }
            triggers.update(trigger_hash_to_trigger_dict)
            trigger_hash_from_asset: set[int] = set(trigger_hash_to_trigger_dict.keys())

            asset_model = assets[name_uri]
            trigger_hash_from_asset_model: set[int] = {
                self._get_trigger_hash(trigger.classpath, trigger.kwargs) for trigger in asset_model.triggers
            }

            # Optimization: no diff between the DB and DAG definitions, no update needed
            if trigger_hash_from_asset == trigger_hash_from_asset_model:
                continue

            diff_to_add = trigger_hash_from_asset - trigger_hash_from_asset_model
            diff_to_remove = trigger_hash_from_asset_model - trigger_hash_from_asset
            if diff_to_add:
                refs_to_add[name_uri] = diff_to_add
            if diff_to_remove:
                refs_to_remove[name_uri] = diff_to_remove

        if refs_to_add:
            all_trigger_hashes: set[int] = {
                trigger_hash for trigger_hashes in refs_to_add.values() for trigger_hash in trigger_hashes
            }

            all_trigger_keys: set[tuple[str, str]] = {
                self._encrypt_trigger_kwargs(triggers[trigger_hash])
                for trigger_hashes in refs_to_add.values()
                for trigger_hash in trigger_hashes
            }
            orm_triggers: dict[int, Trigger] = {
                self._get_trigger_hash(trigger.classpath, trigger.kwargs): trigger
                for trigger in session.scalars(
                    select(Trigger).where(
                        tuple_(Trigger.classpath, Trigger.encrypted_kwargs).in_(all_trigger_keys)
                    )
                )
            }

            # Create new triggers
            new_trigger_models = [
                trigger
                for trigger in [
                    Trigger.from_object(triggers[trigger_hash])
                    for trigger_hash in all_trigger_hashes
                    if trigger_hash not in orm_triggers
                ]
            ]
            session.add_all(new_trigger_models)
            orm_triggers.update(
                (self._get_trigger_hash(trigger.classpath, trigger.kwargs), trigger)
                for trigger in new_trigger_models
            )

            # Add new references
            for name_uri, trigger_hashes in refs_to_add.items():
                asset_model = assets[name_uri]
                asset_model.triggers.extend(
                    [orm_triggers.get(trigger_hash) for trigger_hash in trigger_hashes]
                )

        if refs_to_remove:
            # Remove old references
            for name_uri, trigger_hashes in refs_to_remove.items():
                asset_model = assets[name_uri]
                asset_model.triggers = [
                    trigger
                    for trigger in asset_model.triggers
                    if self._get_trigger_hash(trigger.classpath, trigger.kwargs) not in trigger_hashes
                ]

        # Remove references from assets no longer used
        orphan_assets = session.scalars(
            select(AssetModel).filter(~AssetModel.consuming_dags.any()).filter(AssetModel.triggers.any())
        )
        for asset_model in orphan_assets:
            if (asset_model.name, asset_model.uri) not in self.assets:
                asset_model.triggers = []

    @staticmethod
    def _encrypt_trigger_kwargs(trigger: BaseTrigger) -> tuple[str, str]:
        classpath, kwargs = trigger.serialize()
        return classpath, Trigger.encrypt_kwargs(kwargs)

    @staticmethod
    def _get_trigger_hash(classpath: str, kwargs: dict[str, Any]) -> int:
        """
        Return the hash of the trigger classpath and kwargs. This is used to uniquely identify a trigger.

        We do not want to move this logic in a `__hash__` method in `BaseTrigger` because we do not want to
        make the triggers hashable. The reason being, when the triggerer retrieve the list of triggers, we do
        not want it dedupe them. When used to defer tasks, 2 triggers can have the same classpath and kwargs.
        This is not true for event driven scheduling.
        """
        return hash((classpath, json.dumps(BaseSerialization.serialize(kwargs)).encode("utf-8")))

    def _get_base_trigger_hash(self, trigger: BaseTrigger) -> int:
        classpath, kwargs = trigger.serialize()
        return self._get_trigger_hash(classpath, kwargs)
