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
Utility code that writes DAGs in bulk into the database.

This should generally only be called by internal methods such as
``DagBag._sync_to_db``, ``DAG.bulk_write_to_db``.

:meta private:
"""

from __future__ import annotations

import traceback
from typing import TYPE_CHECKING, Any, NamedTuple, TypeVar, cast

import structlog
from sqlalchemy import delete, func, insert, select, tuple_, update
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import joinedload, load_only

from airflow._shared.timezones.timezone import utcnow
from airflow.assets.manager import asset_manager
from airflow.configuration import conf
from airflow.models.asset import (
    AssetActive,
    AssetAliasModel,
    AssetModel,
    DagScheduleAssetAliasReference,
    DagScheduleAssetNameReference,
    DagScheduleAssetReference,
    DagScheduleAssetUriReference,
    TaskInletAssetReference,
    TaskOutletAssetReference,
)
from airflow.models.dag import DagModel, DagOwnerAttributes, DagTag, get_run_data_interval
from airflow.models.dagrun import DagRun
from airflow.models.dagwarning import DagWarningType
from airflow.models.errors import ParseImportError
from airflow.models.trigger import Trigger
from airflow.sdk import Asset, AssetAlias
from airflow.sdk.definitions.asset import AssetNameRef, AssetUriRef, BaseAsset
from airflow.serialization.enums import Encoding
from airflow.serialization.serialized_objects import BaseSerialization, LazyDeserializedDAG, SerializedDAG
from airflow.triggers.base import BaseEventTrigger
from airflow.utils.retries import MAX_DB_RETRIES, run_with_db_retries
from airflow.utils.sqlalchemy import get_dialect_name, with_row_locks
from airflow.utils.types import DagRunType

if TYPE_CHECKING:
    from collections.abc import Collection, Iterable, Iterator

    from sqlalchemy.orm import Session
    from sqlalchemy.sql import Select, Subquery

    from airflow.models.dagwarning import DagWarning
    from airflow.typing_compat import Self

AssetT = TypeVar("AssetT", bound=BaseAsset)

log = structlog.get_logger(__name__)


def _create_orm_dags(
    bundle_name: str,
    dags: Iterable[LazyDeserializedDAG],
    *,
    session: Session,
) -> Iterator[DagModel]:
    for dag in dags:
        orm_dag = DagModel(dag_id=dag.dag_id, bundle_name=bundle_name)
        if dag.is_paused_upon_creation is not None:
            orm_dag.is_paused = dag.is_paused_upon_creation
        log.info("Creating ORM DAG for %s", dag.dag_id)
        session.add(orm_dag)
        yield orm_dag


def _get_latest_runs_stmt(dag_ids: Collection[str]) -> Select:
    """Build a select statement to retrieve the last automated run for each dag."""
    if len(dag_ids) == 1:  # Index optimized fast path to avoid more complicated & slower groupby queryplan.
        (dag_id,) = dag_ids
        last_automated_runs_subq_scalar: Any = (
            select(func.max(DagRun.logical_date).label("max_logical_date"))
            .where(
                DagRun.dag_id == dag_id,
                DagRun.run_type.in_((DagRunType.BACKFILL_JOB, DagRunType.SCHEDULED)),
            )
            .scalar_subquery()
        )
        query = select(DagRun).where(
            DagRun.dag_id == dag_id,
            DagRun.logical_date == last_automated_runs_subq_scalar,
        )
    else:
        last_automated_runs_subq_table: Subquery = (
            select(DagRun.dag_id, func.max(DagRun.logical_date).label("max_logical_date"))
            .where(
                DagRun.dag_id.in_(dag_ids),
                DagRun.run_type.in_((DagRunType.BACKFILL_JOB, DagRunType.SCHEDULED)),
            )
            .group_by(DagRun.dag_id)
            .subquery()
        )
        query = select(DagRun).where(
            DagRun.dag_id == last_automated_runs_subq_table.c.dag_id,
            DagRun.logical_date == last_automated_runs_subq_table.c.max_logical_date,
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
    def calculate(cls, dags: dict[str, LazyDeserializedDAG], *, session: Session) -> Self:
        """
        Query the run counts from the db.

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
    tags_to_delete = []
    for name, orm_tag in orm_tags.items():
        if name not in tag_names:
            session.delete(orm_tag)
            tags_to_delete.append(orm_tag)

    tags_to_add = tag_names.difference(orm_tags)
    if tags_to_delete:
        # Remove deleted tags from the collection to keep it in sync
        for tag in tags_to_delete:
            dm.tags.remove(tag)

        # Check if there's a potential case-only rename on MySQL (e.g., 'tag' -> 'TAG').
        # MySQL uses case-insensitive collation for the (name, dag_id) primary key by default,
        # which can cause duplicate key errors when renaming tags with only case changes.
        if get_dialect_name(session) == "mysql":
            orm_tags_lower = {name.lower(): name for name in orm_tags}
            has_case_only_change = any(tag.lower() in orm_tags_lower for tag in tags_to_add)

            if has_case_only_change:
                # Force DELETE operations to execute before INSERT operations.
                session.flush()
                # Refresh the tags relationship from the database to reflect the deletions.
                session.expire(dm, ["tags"])

    dm.tags.extend(DagTag(name=name, dag_id=dm.dag_id) for name in tags_to_add)


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


def _serialize_dag_capturing_errors(
    dag: LazyDeserializedDAG, bundle_name, session: Session, bundle_version: str | None
):
    """
    Try to serialize the dag to the DB, but make a note of any errors.

    We can't place them directly in import_errors, as this may be retried, and work the next time
    """
    from airflow import settings
    from airflow.models.dagcode import DagCode
    from airflow.models.serialized_dag import SerializedDagModel

    try:
        # We can't use bulk_write_to_db as we want to capture each error individually
        dag_was_updated = SerializedDagModel.write_dag(
            dag,
            bundle_name=bundle_name,
            bundle_version=bundle_version,
            min_update_interval=settings.MIN_SERIALIZED_DAG_UPDATE_INTERVAL,
            session=session,
        )
        if not dag_was_updated:
            # Check and update DagCode
            DagCode.update_source_code(dag.dag_id, dag.fileloc)
        if "FabAuthManager" in conf.get("core", "auth_manager"):
            _sync_dag_perms(dag, session=session)

        return []
    except OperationalError:
        raise
    except Exception:
        log.exception("Failed to write serialized DAG dag_id=%s fileloc=%s", dag.dag_id, dag.fileloc)
        dagbag_import_error_traceback_depth = conf.getint("core", "dagbag_import_error_traceback_depth")
        return [
            (
                (bundle_name, dag.relative_fileloc),
                traceback.format_exc(limit=-dagbag_import_error_traceback_depth),
            )
        ]


def _sync_dag_perms(dag: LazyDeserializedDAG, session: Session):
    """Sync DAG specific permissions."""
    dag_id = dag.dag_id

    log.debug("Syncing DAG permissions: %s to the DB", dag_id)
    from airflow.providers.fab.www.security_appless import ApplessAirflowSecurityManager

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


def _update_import_errors(
    files_parsed: set[tuple[str, str]],
    bundle_name: str,
    import_errors: dict[tuple[str, str], str],
    session: Session,
):
    from airflow.listeners.listener import get_listener_manager

    # Check existing import errors BEFORE deleting, so we can determine if we should update or create
    existing_import_error_files = set(
        session.execute(select(ParseImportError.bundle_name, ParseImportError.filename))
    )

    # Delete errors for files that were parsed but don't have errors in import_errors
    # (i.e., files that were successfully parsed without errors)
    files_to_clear = files_parsed.difference(import_errors)
    if files_to_clear:
        session.execute(
            delete(ParseImportError).where(
                tuple_(ParseImportError.bundle_name, ParseImportError.filename).in_(files_to_clear)
            )
        )

    # Add or update the errors of the processed files
    for key, stacktrace in import_errors.items():
        bundle_name_, relative_fileloc = key

        if key in existing_import_error_files:
            session.execute(
                update(ParseImportError)
                .where(
                    ParseImportError.filename == relative_fileloc,
                    ParseImportError.bundle_name == bundle_name_,
                )
                .values(
                    filename=relative_fileloc,
                    bundle_name=bundle_name_,
                    timestamp=utcnow(),
                    stacktrace=stacktrace,
                ),
            )
            # sending notification when an existing dag import error occurs
            try:
                # todo: make listener accept bundle_name and relative_filename
                import_error = session.scalar(
                    select(ParseImportError).where(
                        ParseImportError.bundle_name == bundle_name_,
                        ParseImportError.filename == relative_fileloc,
                    )
                )
                if import_error is not None:
                    get_listener_manager().hook.on_existing_dag_import_error(
                        filename=import_error.full_file_path(), stacktrace=stacktrace
                    )
            except Exception:
                log.exception("error calling listener")
        else:
            import_error = ParseImportError(
                filename=relative_fileloc,
                bundle_name=bundle_name,
                timestamp=utcnow(),
                stacktrace=stacktrace,
            )
            session.add(import_error)
            # sending notification when a new dag import error occurs
            try:
                get_listener_manager().hook.on_new_dag_import_error(
                    filename=import_error.full_file_path(), stacktrace=stacktrace
                )
            except Exception:
                log.exception("error calling listener")
        session.execute(
            update(DagModel)
            .where(
                DagModel.relative_fileloc == relative_fileloc,
            )
            .values(
                has_import_errors=True,
                bundle_name=bundle_name,
                is_stale=True,
            )
            .execution_options(synchronize_session="fetch")
        )


def update_dag_parsing_results_in_db(
    bundle_name: str,
    bundle_version: str | None,
    dags: Collection[LazyDeserializedDAG],
    import_errors: dict[tuple[str, str], str],
    parse_duration: float | None,
    warnings: set[DagWarning],
    session: Session,
    *,
    warning_types: tuple[DagWarningType] = (DagWarningType.NONEXISTENT_POOL,),
    files_parsed: set[tuple[str, str]] | None = None,
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

    :param files_parsed: Set of (bundle_name, relative_fileloc) tuples for all files that were parsed.
        If None, will be inferred from dags and import_errors. Passing this explicitly ensures that
        import errors are cleared for files that were parsed but no longer contain DAGs.
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
                SerializedDAG.bulk_write_to_db(
                    bundle_name, bundle_version, dags, parse_duration, session=session
                )
                # Write Serialized DAGs to DB, capturing errors
                for dag in dags:
                    serialize_errors.extend(
                        _serialize_dag_capturing_errors(
                            dag=dag,
                            bundle_name=bundle_name,
                            bundle_version=bundle_version,
                            session=session,
                        )
                    )
            except OperationalError:
                session.rollback()
                raise
            # Only now we are "complete" do we update import_errors - don't want to record errors from
            # previous failed attempts
            import_errors.update(serialize_errors)
    # Record import errors into the ORM - we don't retry on this one as it's not as critical that it works
    try:
        _update_import_errors(
            files_parsed=files_parsed if files_parsed is not None else set(),
            bundle_name=bundle_name,
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

    dags: dict[str, LazyDeserializedDAG]
    bundle_name: str
    bundle_version: str | None

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
        stmt = cast("Select[tuple[DagModel]]", with_row_locks(stmt, of=DagModel, session=session))
        return {dm.dag_id: dm for dm in session.scalars(stmt).unique()}

    def add_dags(self, *, session: Session) -> dict[str, DagModel]:
        orm_dags = self.find_orm_dags(session=session)
        orm_dags.update(
            (model.dag_id, model)
            for model in _create_orm_dags(
                bundle_name=self.bundle_name,
                dags=(dag for dag_id, dag in self.dags.items() if dag_id not in orm_dags),
                session=session,
            )
        )
        return orm_dags

    def update_dags(
        self,
        orm_dags: dict[str, DagModel],
        parse_duration: float | None,
        *,
        session: Session,
    ) -> None:
        # we exclude backfill from active run counts since their concurrency is separate
        run_info = _RunInfo.calculate(dags=self.dags, session=session)
        for dag_id, dm in sorted(orm_dags.items()):
            dag = self.dags[dag_id]
            dm.fileloc = dag.fileloc
            dm.relative_fileloc = dag.relative_fileloc
            dm.owners = dag.owner or conf.get("operators", "default_owner")
            dm.is_stale = False
            dm.has_import_errors = False
            dm.last_parsed_time = utcnow()
            dm.last_parse_duration = parse_duration
            if hasattr(dag, "_dag_display_property_value"):
                dm._dag_display_property_value = dag._dag_display_property_value
            elif dag.dag_display_name != dag.dag_id:
                dm._dag_display_property_value = dag.dag_display_name
            dm.description = dag.description

            # These "is not None" checks are because a LazySerializedDag object does not
            # provide the default value if the user doesn't provide an explicit value.

            # if dag.max_active_tasks come as None then default max_active_tasks should be updated
            # similar for max_consecutive_failed_dag_runs, max_active_runs

            if dag.max_active_tasks is None:
                dm.max_active_tasks = conf.getint("core", "max_active_tasks_per_dag")
            else:
                dm.max_active_tasks = dag.max_active_tasks

            if dag.max_active_runs is None:
                dm.max_active_runs = conf.getint("core", "max_active_runs_per_dag")
            else:
                dm.max_active_runs = dag.max_active_runs

            if dag.max_consecutive_failed_dag_runs is None:
                dm.max_consecutive_failed_dag_runs = conf.getint(
                    "core", "max_consecutive_failed_dag_runs_per_dag"
                )
            else:
                dm.max_consecutive_failed_dag_runs = dag.max_consecutive_failed_dag_runs

            if dag.deadline is not None:
                dm.deadline = dag.deadline

            if hasattr(dag, "has_task_concurrency_limits"):
                dm.has_task_concurrency_limits = dag.has_task_concurrency_limits
            else:
                dm.has_task_concurrency_limits = any(
                    t.max_active_tis_per_dag is not None or t.max_active_tis_per_dagrun is not None
                    for t in dag.tasks
                )
            dm.timetable_summary = dag.timetable.summary
            dm.timetable_description = dag.timetable.description
            dm.fail_fast = dag.fail_fast if dag.fail_fast is not None else False

            dm.bundle_name = self.bundle_name
            dm.bundle_version = self.bundle_version

            last_automated_run: DagRun | None = run_info.latest_runs.get(dag.dag_id)
            if last_automated_run is None:
                last_automated_data_interval = None
            else:
                last_automated_data_interval = get_run_data_interval(dag.timetable, last_automated_run)
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

    def update_dag_asset_expression(
        self,
        *,
        orm_dags: dict[str, DagModel],
        orm_assets: dict[tuple[str, str], AssetModel],
    ) -> None:
        def expand_asset_obj_expr(asset_expr: dict) -> dict[str, dict[str, str]]:
            asset_name = asset_expr["name"]
            asset_uri = asset_expr["uri"]

            asset_expr["id"] = orm_assets[(asset_name, asset_uri)].id
            return asset_expr

        def expand_composite_asset_expr(asset_exprs: list) -> list:
            for expr_index, expr in enumerate(asset_exprs):
                key = expr.keys()
                expaned_expr: dict
                if key == {"all"}:
                    # e.g., {"all": [{"asset": {"name": ..., "uri": ...}}]}
                    expaned_expr = {"all": expand_composite_asset_expr(expr["all"])}
                elif key == {"any"}:
                    # e.g., {"any": [{"asset": {"name": ..., "uri": ...}}]}
                    expaned_expr = {"any": expand_composite_asset_expr(expr["any"])}
                elif key == {"asset"}:
                    # e.g., {"asset": {"name": ..., "uri": ...}}
                    expaned_expr = {"asset": expand_asset_obj_expr(expr["asset"])}
                else:
                    # we don't need to handle asset aliases, asset refs
                    continue

                asset_exprs[expr_index] = expaned_expr

            return asset_exprs

        def expand_asset_expr(asset_expr: dict[str, list | dict]) -> dict[str, list | dict]:
            for key, value in asset_expr.items():
                # these isisntance checks are only used to make Mypy happy
                if key == "asset" and isinstance(value, dict):
                    # e.g., {"asset": {"name": ..., "uri": ...}}
                    asset_expr[key] = expand_asset_obj_expr(value)
                elif key in {"all", "any"} and isinstance(value, list):
                    # e.g., {"all": [{"asset": {"name": ..., "uri": ...}}]}
                    asset_expr[key] = expand_composite_asset_expr(value)

                # we don't need to handle asset aliases and asset refs
            return asset_expr

        for dag_id, dm in sorted(orm_dags.items()):
            asset_expression = self.dags[dag_id].timetable.asset_condition.as_expression()
            if asset_expression is not None:
                asset_expression = expand_asset_expr(asset_expression)
            dm.asset_expression = asset_expression


def _get_task_ports(data: dict, inlets: bool, outlets: bool) -> Iterable[str]:
    if inlets:
        yield from data.get("inlets") or ()
    if outlets:
        yield from data.get("outlets") or ()


def _get_dag_assets(
    dag: LazyDeserializedDAG,
    of: type[AssetT],
    *,
    inlets: bool = True,
    outlets: bool = True,
) -> Iterable[tuple[str, AssetT]]:
    for raw_task in dag.data["dag"]["tasks"]:
        task = raw_task[Encoding.VAR]
        ports = _get_task_ports(task["partial_kwargs"] if task.get("_is_mapped") else task, inlets, outlets)
        for port in ports:
            if isinstance(obj := BaseSerialization.deserialize(port), of):
                yield task["task_id"], obj


def _find_all_assets(dags: Iterable[LazyDeserializedDAG]) -> Iterator[Asset]:
    for dag in dags:
        for _, asset in dag.timetable.asset_condition.iter_assets():
            yield asset
        for _, asset in _get_dag_assets(dag, of=Asset):
            yield asset


def _find_all_asset_aliases(dags: Iterable[LazyDeserializedDAG]) -> Iterator[AssetAlias]:
    for dag in dags:
        for _, alias in dag.timetable.asset_condition.iter_asset_aliases():
            yield alias
        for _, alias in _get_dag_assets(dag, of=AssetAlias):
            yield alias


def _find_active_assets(name_uri_assets: Iterable[tuple[str, str]], session: Session) -> set[tuple[str, str]]:
    return {
        tuple(row)
        for row in session.execute(
            select(AssetModel.name, AssetModel.uri).where(
                tuple_(AssetModel.name, AssetModel.uri).in_(name_uri_assets),
                AssetModel.active.has(),
                AssetModel.scheduled_dags.any(
                    DagScheduleAssetReference.dag.has(~DagModel.is_stale & ~DagModel.is_paused)
                ),
            )
        )
    }


class AssetModelOperation(NamedTuple):
    """Collect asset/alias objects from DAGs and perform database operations for them."""

    schedule_asset_references: dict[str, list[Asset]]
    schedule_asset_alias_references: dict[str, list[AssetAlias]]
    schedule_asset_name_references: set[tuple[str, str]]  # dag_id, ref_name.
    schedule_asset_uri_references: set[tuple[str, str]]  # dag_id, ref_uri.
    inlet_references: dict[str, list[tuple[str, Asset]]]
    outlet_references: dict[str, list[tuple[str, Asset]]]
    assets: dict[tuple[str, str], Asset]
    asset_aliases: dict[str, AssetAlias]

    @classmethod
    def collect(cls, dags: dict[str, LazyDeserializedDAG]) -> Self:
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
            inlet_references={
                dag_id: list(_get_dag_assets(dag, Asset, inlets=True, outlets=False))
                for dag_id, dag in dags.items()
            },
            outlet_references={
                dag_id: list(_get_dag_assets(dag, Asset, inlets=False, outlets=True))
                for dag_id, dag in dags.items()
            },
            assets={(asset.name, asset.uri): asset for asset in _find_all_assets(dags.values())},
            asset_aliases={alias.name: alias for alias in _find_all_asset_aliases(dags.values())},
        )
        return coll

    def sync_assets(self, *, session: Session) -> dict[tuple[str, str], AssetModel]:
        # Optimization: skip all database calls if no assets were collected.
        if not self.assets:
            return {}
        orm_assets: dict[tuple[str, str], AssetModel] = {
            (am.name, am.uri): am
            for am in session.scalars(
                select(AssetModel).where(tuple_(AssetModel.name, AssetModel.uri).in_(self.assets))
            )
        }
        for key, model in orm_assets.items():
            asset = self.assets[key]
            model.group = asset.group
            model.extra = asset.extra
        orm_assets.update(
            ((model.name, model.uri), model)
            for model in asset_manager.create_assets(
                [asset for name_uri, asset in self.assets.items() if name_uri not in orm_assets],
                session=session,
            )
        )
        return orm_assets

    def sync_asset_aliases(self, *, session: Session) -> dict[str, AssetAliasModel]:
        # Optimization: skip all database calls if no asset aliases were collected.
        if not self.asset_aliases:
            return {}
        orm_aliases: dict[str, AssetAliasModel] = {
            da.name: da
            for da in session.scalars(
                select(AssetAliasModel).where(AssetAliasModel.name.in_(self.asset_aliases))
            )
        }
        for name, model in orm_aliases.items():
            model.group = self.asset_aliases[name].group
        orm_aliases.update(
            (model.name, model)
            for model in asset_manager.create_asset_aliases(
                [alias for name, alias in self.asset_aliases.items() if name not in orm_aliases],
                session=session,
            )
        )
        return orm_aliases

    def activate_assets_if_possible(self, models: Iterable[AssetModel], *, session: Session) -> None:
        """
        Try to activate assets eagerly.

        This inserts a record to AssetActive for an asset so it is activated
        on creation if its ``name`` and ``uri`` values do not conflict with
        anything else. This is a best-effort operation; we simply give up if
        there's a conflict. The scheduler makes a more comprehensive pass
        through all assets in ``_update_asset_orphanage``.
        """
        if (dialect_name := get_dialect_name(session)) == "postgresql":
            from sqlalchemy.dialects.postgresql import insert as postgresql_insert

            stmt: Any = postgresql_insert(AssetActive).on_conflict_do_nothing()
        elif session.bind is not None and dialect_name == "mysql":
            from sqlalchemy.dialects.mysql import insert as mysql_insert

            # MySQL does not support "do nothing"; this updates the row in
            # conflict with its own value to achieve the same idea.
            stmt = mysql_insert(AssetActive).on_duplicate_key_update(name=AssetActive.name)
        else:
            from sqlalchemy.dialects.sqlite import insert as sqlite_insert

            stmt = sqlite_insert(AssetActive).on_conflict_do_nothing()
        if values := [{"name": m.name, "uri": m.uri} for m in models]:
            session.execute(stmt, values)

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
        orm_refs = {
            tuple(row)
            for row in session.execute(
                select(model.dag_id, getattr(model, attr)).where(
                    model.dag_id.in_(dag_id for dag_id, _ in references)
                )
            )
        }
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
        for dag_id, references in self.inlet_references.items():
            # Optimization: no references at all; this is faster than repeated delete().
            if not references:
                dags[dag_id].task_inlet_asset_references = []
                continue
            referenced_inlets = {
                (task_id, asset.id)
                for task_id, asset in ((task_id, assets[d.name, d.uri]) for task_id, d in references)
            }
            orm_refs = {(r.task_id, r.asset_id): r for r in dags[dag_id].task_inlet_asset_references}
            for key, ref in orm_refs.items():
                if key not in referenced_inlets:
                    session.delete(ref)
            session.bulk_save_objects(
                TaskInletAssetReference(asset_id=asset_id, dag_id=dag_id, task_id=task_id)
                for task_id, asset_id in referenced_inlets
                if (task_id, asset_id) not in orm_refs
            )
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
        from airflow.serialization.serialized_objects import _encode_trigger

        # Update references from assets being used
        refs_to_add: dict[tuple[str, str], set[int]] = {}
        refs_to_remove: dict[tuple[str, str], set[int]] = {}
        triggers: dict[int, dict] = {}

        # Optimization: if no asset collected, skip fetching active assets
        active_assets = _find_active_assets(self.assets, session=session) if self.assets else {}

        for name_uri, asset in self.assets.items():
            # If the asset belong to a DAG not active or paused, consider there is no watcher associated to it
            asset_watcher_triggers = (
                [
                    {**_encode_trigger(watcher.trigger), "watcher_name": watcher.name}
                    for watcher in asset.watchers
                ]
                if name_uri in active_assets
                else []
            )
            trigger_hash_to_trigger_dict: dict[int, dict] = {
                BaseEventTrigger.hash(trigger["classpath"], trigger["kwargs"]): trigger
                for trigger in asset_watcher_triggers
            }
            triggers.update(trigger_hash_to_trigger_dict)
            trigger_hash_from_asset: set[int] = set(trigger_hash_to_trigger_dict.keys())

            asset_model = assets[name_uri]
            trigger_hash_from_asset_model: set[int] = {
                BaseEventTrigger.hash(trigger.classpath, trigger.kwargs) for trigger in asset_model.triggers
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
                (
                    triggers[trigger_hash]["classpath"],
                    Trigger.encrypt_kwargs(triggers[trigger_hash]["kwargs"]),
                )
                for trigger_hashes in refs_to_add.values()
                for trigger_hash in trigger_hashes
            }
            orm_triggers: dict[int, Trigger] = {
                BaseEventTrigger.hash(trigger.classpath, trigger.kwargs): trigger
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
                    Trigger(
                        classpath=triggers[trigger_hash]["classpath"], kwargs=triggers[trigger_hash]["kwargs"]
                    )
                    for trigger_hash in all_trigger_hashes
                    if trigger_hash not in orm_triggers
                ]
            ]
            session.add_all(new_trigger_models)
            session.flush()  # Flush to get the IDs assigned
            orm_triggers.update(
                (BaseEventTrigger.hash(trigger.classpath, trigger.kwargs), trigger)
                for trigger in new_trigger_models
            )

            # Add new references
            for name_uri, trigger_hashes in refs_to_add.items():
                asset_model = assets[name_uri]

                for trigger_hash in trigger_hashes:
                    trigger = triggers.get(trigger_hash)
                    orm_trigger = orm_triggers.get(trigger_hash)
                    if orm_trigger and trigger:
                        asset_model.add_trigger(orm_trigger, trigger["watcher_name"])

        if refs_to_remove:
            # Remove old references
            for name_uri, trigger_hashes in refs_to_remove.items():
                asset_model = assets[name_uri]
                asset_model.watchers = [
                    watcher
                    for watcher in asset_model.watchers
                    if BaseEventTrigger.hash(watcher.trigger.classpath, watcher.trigger.kwargs)
                    not in trigger_hashes
                ]

        # Remove references from assets no longer used
        orphan_assets = session.scalars(
            select(AssetModel).filter(~AssetModel.scheduled_dags.any()).filter(AssetModel.triggers.any())
        )
        for asset_model in orphan_assets:
            if (asset_model.name, asset_model.uri) not in self.assets:
                # Delete all watchers for this orphaned asset
                asset_model.watchers = []
