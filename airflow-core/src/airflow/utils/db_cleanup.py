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
This module took inspiration from the community maintenance dag.

See:
(https://github.com/teamclairvoyant/airflow-maintenance-dags/blob/4e5c7682a808082561d60cbc9cafaa477b0d8c65/db-cleanup/airflow-db-cleanup.py).
"""

from __future__ import annotations

import csv
import logging
import os
from contextlib import contextmanager
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from sqlalchemy import and_, column, func, inspect, select, table, text
from sqlalchemy.exc import OperationalError, ProgrammingError
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.orm import aliased
from sqlalchemy.sql.expression import ClauseElement, Executable, tuple_

from airflow._shared.timezones import timezone
from airflow.cli.simple_table import AirflowConsole
from airflow.configuration import conf
from airflow.exceptions import AirflowException
from airflow.utils.db import reflect_tables
from airflow.utils.helpers import ask_yesno
from airflow.utils.session import NEW_SESSION, provide_session
from airflow.utils.types import DagRunType

if TYPE_CHECKING:
    from pendulum import DateTime
    from sqlalchemy.orm import Query, Session

    from airflow.models import Base

logger = logging.getLogger(__name__)

ARCHIVE_TABLE_PREFIX = "_airflow_deleted__"
# Archived tables created by DB migrations
ARCHIVED_TABLES_FROM_DB_MIGRATIONS = [
    "_xcom_archive"  # Table created by the AF 2 -> 3.0.0 migration when the XComs had pickled values
]


@dataclass
class _TableConfig:
    """
    Config class for performing cleanup on a table.

    :param table_name: the table
    :param extra_columns: any columns besides recency_column_name that we'll need in queries
    :param recency_column_name: date column to filter by
    :param keep_last: whether the last record should be kept even if it's older than clean_before_timestamp
    :param keep_last_filters: the "keep last" functionality will preserve the most recent record
        in the table.  to ignore certain records even if they are the latest in the table, you can
        supply additional filters here (e.g. externally triggered dag runs)
    :param keep_last_group_by: if keeping the last record, can keep the last record for each group
    :param dependent_tables: list of tables which have FK relationship with this table
    :param skip_if_referenced_by: dict mapping referencing table name to the FK column name.
    """

    table_name: str
    recency_column_name: str
    extra_columns: list[str] | None = None
    keep_last: bool = False
    keep_last_filters: Any | None = None
    keep_last_group_by: Any | None = None
    # We explicitly list these tables instead of detecting foreign keys automatically,
    # because the relationships are unlikely to change and the number of tables is small.
    # Relying on automation here would increase complexity and reduce maintainability.
    dependent_tables: list[str] | None = None
    skip_if_referenced_by: dict[str, str] | None = None

    def __post_init__(self):
        self.recency_column = column(self.recency_column_name)
        self.orm_model: Base = table(
            self.table_name, *[column(x) for x in self.extra_columns or []], self.recency_column
        )

    def __lt__(self, other):
        return self.table_name < other.table_name

    @property
    def readable_config(self):
        return {
            "table": self.orm_model.name,
            "recency_column": str(self.recency_column),
            "keep_last": self.keep_last,
            "keep_last_filters": [str(x) for x in self.keep_last_filters] if self.keep_last_filters else None,
            "keep_last_group_by": str(self.keep_last_group_by),
        }


config_list: list[_TableConfig] = [
    _TableConfig(table_name="job", recency_column_name="latest_heartbeat"),
    _TableConfig(
        table_name="dag",
        recency_column_name="last_parsed_time",
        dependent_tables=["dag_version", "deadline"],
    ),
    _TableConfig(
        table_name="dag_run",
        recency_column_name="start_date",
        extra_columns=["dag_id", "run_type"],
        keep_last=True,
        keep_last_filters=[column("run_type") != DagRunType.MANUAL],
        keep_last_group_by=["dag_id"],
        dependent_tables=["task_instance", "deadline"],
    ),
    _TableConfig(table_name="asset_event", recency_column_name="timestamp"),
    _TableConfig(table_name="import_error", recency_column_name="timestamp"),
    _TableConfig(table_name="log", recency_column_name="dttm"),
    _TableConfig(table_name="sla_miss", recency_column_name="timestamp"),
    _TableConfig(
        table_name="task_instance",
        recency_column_name="start_date",
        dependent_tables=["task_instance_history", "xcom"],
    ),
    _TableConfig(table_name="task_instance_history", recency_column_name="start_date"),
    _TableConfig(table_name="task_reschedule", recency_column_name="start_date"),
    _TableConfig(table_name="xcom", recency_column_name="timestamp"),
    _TableConfig(table_name="_xcom_archive", recency_column_name="timestamp"),
    _TableConfig(table_name="callback_request", recency_column_name="created_at"),
    _TableConfig(table_name="celery_taskmeta", recency_column_name="date_done"),
    _TableConfig(table_name="celery_tasksetmeta", recency_column_name="date_done"),
    _TableConfig(
        table_name="trigger",
        recency_column_name="created_date",
        dependent_tables=["task_instance"],
    ),
    _TableConfig(
        table_name="dag_version",
        recency_column_name="created_at",
        dependent_tables=["task_instance", "dag_run"],
        skip_if_referenced_by={"task_instance": "dag_version_id", "dag_run": "created_dag_version_id"},
    ),
    _TableConfig(table_name="deadline", recency_column_name="deadline_time"),
]

# We need to have `fallback="database"` because this is executed at top level code and provider configuration
# might not be loaded
if (
    "FabAuthManager" in conf.get("core", "auth_manager")
    and conf.get("fab", "session_backend", fallback="database") == "database"
):
    config_list.append(_TableConfig(table_name="session", recency_column_name="expiry"))

config_dict: dict[str, _TableConfig] = {x.orm_model.name: x for x in sorted(config_list)}


def _check_for_rows(*, query: Query, print_rows: bool = False) -> int:
    num_entities = query.count()
    print(f"Found {num_entities} rows meeting deletion criteria.")
    if not print_rows or num_entities == 0:
        return num_entities

    max_rows_to_print = 100
    print(f"Printing first {max_rows_to_print} rows.")
    logger.debug("print entities query: %s", query)
    for entry in query.limit(max_rows_to_print):
        print(entry.__dict__)
    return num_entities


def _dump_table_to_file(*, target_table: str, file_path: str, export_format: str, session: Session) -> None:
    if export_format == "csv":
        with open(file_path, "w") as f:
            csv_writer = csv.writer(f)
            cursor = session.execute(text(f"SELECT * FROM {target_table}"))
            csv_writer.writerow(cursor.keys())
            BATCH_SIZE = 500
            rows = cursor.fetchmany(BATCH_SIZE)
            while rows:
                csv_writer.writerows(rows)
                rows = cursor.fetchmany(BATCH_SIZE)
    else:
        raise AirflowException(f"Export format {export_format} is not supported.")


def _do_delete(
    *, query: Query, orm_model: Base, skip_archive: bool, session: Session, batch_size: int | None
) -> None:
    import itertools
    import re

    bind = session.get_bind()
    dialect_name = bind.dialect.name
    batch_counter = itertools.count(1)

    while True:
        limited_query = query.limit(batch_size) if batch_size else query
        if limited_query.count() == 0:  # nothing left to delete
            break

        batch_no = next(batch_counter)
        suffix = f"__b{batch_no}" if batch_size else ""

        if batch_size:
            print(f"Performing Delete (batch {batch_no}, max {batch_size} rows)...")
        else:
            print("Performing Delete...")

        # using bulk delete
        # create a new table and copy the rows there
        timestamp_str = re.sub(r"[^\d]", "", timezone.utcnow().isoformat())[:14]
        target_table_name = f"{ARCHIVE_TABLE_PREFIX}{orm_model.name}__{timestamp_str}{suffix}"
        print(f"Moving data to table {target_table_name}")
        target_table = None

        try:
            if dialect_name == "mysql":
                # MySQL with replication needs this split into two queries, so just do it for all MySQL
                # ERROR 1786 (HY000): Statement violates GTID consistency: CREATE TABLE ... SELECT.
                session.execute(text(f"CREATE TABLE {target_table_name} LIKE {orm_model.name}"))
                metadata = reflect_tables([target_table_name], session)
                target_table = metadata.tables[target_table_name]
                insert_stm = target_table.insert().from_select(target_table.c, limited_query)
                logger.debug("insert statement:\n%s", insert_stm.compile())
                session.execute(insert_stm)
            else:
                stmt = CreateTableAs(target_table_name, limited_query.selectable)
                logger.debug("ctas query:\n%s", stmt.compile())
                session.execute(stmt)
            session.commit()

            # delete the rows from the old table
            metadata = reflect_tables([orm_model.name, target_table_name], session)
            source_table = metadata.tables[orm_model.name]
            target_table = metadata.tables[target_table_name]
            logger.debug("rows moved; purging from %s", source_table.name)
            if dialect_name == "sqlite":
                pk_cols = source_table.primary_key.columns
                delete = source_table.delete().where(
                    tuple_(*pk_cols).in_(
                        select(*[target_table.c[x.name] for x in source_table.primary_key.columns])
                    )
                )
            else:
                delete = source_table.delete().where(
                    and_(*[col == target_table.c[col.name] for col in source_table.primary_key.columns])
                )
            logger.debug("delete statement:\n%s", delete.compile())
            session.execute(delete)
            session.commit()

        except BaseException as e:
            raise e
        finally:
            if target_table is not None and skip_archive:
                bind = session.get_bind()
                target_table.drop(bind=bind)
                session.commit()

    print("Finished Performing Delete")


def _subquery_keep_last(
    *, recency_column, keep_last_filters, group_by_columns, max_date_colname, session: Session
):
    subquery = select(*group_by_columns, func.max(recency_column).label(max_date_colname))

    if keep_last_filters is not None:
        for entry in keep_last_filters:
            subquery = subquery.filter(entry)

    if group_by_columns is not None:
        subquery = subquery.group_by(*group_by_columns)

    return subquery.subquery(name="latest")


class CreateTableAs(Executable, ClauseElement):
    """Custom sqlalchemy clause element for CTAS operations."""

    inherit_cache = False

    def __init__(self, name, query):
        self.name = name
        self.query = query


@compiles(CreateTableAs)
def _compile_create_table_as__other(element, compiler, **kw):
    return f"CREATE TABLE {element.name} AS {compiler.process(element.query)}"


def _build_skip_if_referenced_filter(
    *,
    skip_if_referenced_by: dict[str, str],
) -> list:
    """
    Build filter conditions to exclude rows that are still referenced by other tables.

    :param skip_if_referenced_by: Dict mapping referencing table name to FK column name.
    :return: List of filter conditions to exclude referenced rows
    """
    conditions = []

    for ref_table_name, ref_column_name in skip_if_referenced_by.items():
        ref_table = table(ref_table_name, column(ref_column_name))

        # Build a subquery to find all IDs that are still referenced
        referenced_ids_subquery = (
            select(column(ref_column_name))
            .select_from(ref_table)
            .where(column(ref_column_name).isnot(None))
            .distinct()
            .scalar_subquery()
        )

        # Exclude rows where the primary key is in the set of referenced IDs
        conditions.append(column("id").notin_(referenced_ids_subquery))

    return conditions


def _build_query(
    *,
    orm_model,
    recency_column,
    keep_last,
    keep_last_filters,
    keep_last_group_by,
    clean_before_timestamp: DateTime,
    skip_if_referenced_by: dict[str, str] | None = None,
    session: Session,
    **kwargs,
) -> Query:
    base_table_alias = "base"
    base_table = aliased(orm_model, name=base_table_alias)
    query = session.query(base_table).with_entities(text(f"{base_table_alias}.*"))
    base_table_recency_col = base_table.c[recency_column.name]
    conditions = [base_table_recency_col < clean_before_timestamp]
    if keep_last:
        max_date_col_name = "max_date_per_group"
        group_by_columns: list[Any] = [column(x) for x in keep_last_group_by]
        subquery = _subquery_keep_last(
            recency_column=recency_column,
            keep_last_filters=keep_last_filters,
            group_by_columns=group_by_columns,
            max_date_colname=max_date_col_name,
            session=session,
        )
        query = query.select_from(base_table).outerjoin(
            subquery,
            and_(
                *[base_table.c[x] == subquery.c[x] for x in keep_last_group_by],  # type: ignore[attr-defined]
                base_table_recency_col == column(max_date_col_name),
            ),
        )
        conditions.append(column(max_date_col_name).is_(None))

    # Add conditions to skip rows that are still referenced by other tables
    if skip_if_referenced_by:
        skip_conditions = _build_skip_if_referenced_filter(
            skip_if_referenced_by=skip_if_referenced_by,
        )
        conditions.extend(skip_conditions)

    query = query.filter(and_(*conditions))
    return query


def _cleanup_table(
    *,
    orm_model,
    recency_column,
    keep_last,
    keep_last_filters,
    keep_last_group_by,
    clean_before_timestamp: DateTime,
    skip_if_referenced_by: dict[str, str] | None = None,
    dry_run: bool = True,
    verbose: bool = False,
    skip_archive: bool = False,
    session: Session,
    batch_size: int | None = None,
    **kwargs,
) -> None:
    print()
    if dry_run:
        print(f"Performing dry run for table {orm_model.name}")
    query = _build_query(
        orm_model=orm_model,
        recency_column=recency_column,
        keep_last=keep_last,
        keep_last_filters=keep_last_filters,
        keep_last_group_by=keep_last_group_by,
        clean_before_timestamp=clean_before_timestamp,
        skip_if_referenced_by=skip_if_referenced_by,
        session=session,
    )
    logger.debug("old rows query:\n%s", query.selectable.compile())
    print(f"Checking table {orm_model.name}")
    num_rows = _check_for_rows(query=query, print_rows=False)

    if num_rows and not dry_run:
        _do_delete(
            query=query,
            orm_model=orm_model,
            skip_archive=skip_archive,
            session=session,
            batch_size=batch_size,
        )

    session.commit()


def _confirm_delete(*, date: DateTime, tables: list[str]) -> None:
    for_tables = f" for tables {tables!r}" if tables else ""
    question = (
        f"You have requested that we purge all data prior to {date}{for_tables}.\n"
        f"This is irreversible.  Consider backing up the tables first and / or doing a dry run "
        f"with option --dry-run.\n"
        f"Enter 'delete rows' (without quotes) to proceed."
    )
    print(question)
    answer = input().strip()
    if answer != "delete rows":
        raise SystemExit("User did not confirm; exiting.")


def _confirm_drop_archives(*, tables: list[str]) -> None:
    # if length of tables is greater than 3, show the total count
    if len(tables) > 3:
        text_ = f"{len(tables)} archived tables prefixed with {ARCHIVE_TABLE_PREFIX}"
    else:
        text_ = f"the following archived tables: {', '.join(tables)}"
    question = (
        f"You have requested that we drop {text_}.\n"
        f"This is irreversible. Consider backing up the tables first.\n"
    )
    print(question)
    if len(tables) > 3:
        show_tables = ask_yesno("Show tables that will be dropped? (y/n): ")
        if show_tables:
            for table in tables:
                print(f"  {table}")
            print("\n")
    answer = input("Enter 'drop archived tables' (without quotes) to proceed.\n").strip()
    if answer != "drop archived tables":
        raise SystemExit("User did not confirm; exiting.")


def _print_config(*, configs: dict[str, _TableConfig]) -> None:
    data = [x.readable_config for x in configs.values()]
    AirflowConsole().print_as_table(data=data)


@contextmanager
def _suppress_with_logging(table: str, session: Session):
    """Suppresses errors but logs them."""
    try:
        yield
    except (OperationalError, ProgrammingError):
        logger.warning("Encountered error when attempting to clean table '%s'. ", table)
        logger.debug("Traceback for table '%s'", table, exc_info=True)
        if session.is_active:
            logger.debug("Rolling back transaction")
            session.rollback()


def _effective_table_names(*, table_names: list[str] | None) -> tuple[list[str], dict[str, _TableConfig]]:
    desired_table_names = set(table_names or config_dict)

    outliers = desired_table_names - set(config_dict.keys())
    if outliers:
        logger.warning(
            "The following table(s) are not valid choices and will be skipped: %s",
            sorted(outliers),
        )
    desired_table_names = desired_table_names - outliers

    visited: set[str] = set()
    effective_table_names: list[str] = []

    def collect_deps(table: str):
        if table in visited:
            return
        visited.add(table)
        config = config_dict[table]
        for dep in config.dependent_tables or []:
            collect_deps(dep)
        effective_table_names.append(table)

    for table_name in desired_table_names:
        collect_deps(table_name)

    effective_config_dict = {n: config_dict[n] for n in effective_table_names}

    if not effective_config_dict:
        raise SystemExit("No tables selected for db cleanup. Please choose valid table names.")

    return effective_table_names, effective_config_dict


def _get_archived_table_names(table_names: list[str] | None, session: Session) -> list[str]:
    inspector = inspect(session.bind)
    db_table_names = [
        x
        for x in (inspector.get_table_names() if inspector else [])
        if x.startswith(ARCHIVE_TABLE_PREFIX) or x in ARCHIVED_TABLES_FROM_DB_MIGRATIONS
    ]
    effective_table_names, _ = _effective_table_names(table_names=table_names)
    # Filter out tables that don't start with the archive prefix
    archived_table_names = [
        table_name
        for table_name in db_table_names
        if (
            any("__" + x + "__" in table_name for x in effective_table_names)
            or table_name in ARCHIVED_TABLES_FROM_DB_MIGRATIONS
        )
    ]
    return archived_table_names


@provide_session
def run_cleanup(
    *,
    clean_before_timestamp: DateTime,
    table_names: list[str] | None = None,
    dry_run: bool = False,
    verbose: bool = False,
    confirm: bool = True,
    skip_archive: bool = False,
    session: Session = NEW_SESSION,
    batch_size: int | None = None,
) -> None:
    """
    Purges old records in airflow metadata database.

    The last non-externally-triggered dag run will always be kept in order to ensure
    continuity of scheduled dag runs.

    Where there are foreign key relationships, deletes will cascade, so that for
    example if you clean up old dag runs, the associated task instances will
    be deleted.

    :param clean_before_timestamp: The timestamp before which data should be purged
    :param table_names: Optional. List of table names to perform maintenance on.  If list not provided,
        will perform maintenance on all tables.
    :param dry_run: If true, print rows meeting deletion criteria
    :param verbose: If true, may provide more detailed output.
    :param confirm: Require user input to confirm before processing deletions.
    :param skip_archive: Set to True if you don't want the purged rows preserved in an archive table.
    :param session: Session representing connection to the metadata database.
    :param batch_size: Maximum number of rows to delete or archive in a single transaction.
    """
    clean_before_timestamp = timezone.coerce_datetime(clean_before_timestamp)

    # Get all tables to clean (root + dependents)
    effective_table_names, effective_config_dict = _effective_table_names(table_names=table_names)
    if dry_run:
        print("Performing dry run for db cleanup.")
        print(
            f"Data prior to {clean_before_timestamp} would be purged "
            f"from tables {effective_table_names} with the following config:\n"
        )
        _print_config(configs=effective_config_dict)
    if not dry_run and confirm:
        _confirm_delete(date=clean_before_timestamp, tables=sorted(effective_table_names))
    existing_tables = reflect_tables(tables=None, session=session).tables

    for table_name, table_config in effective_config_dict.items():
        if table_name in existing_tables:
            with _suppress_with_logging(table_name, session):
                _cleanup_table(
                    clean_before_timestamp=clean_before_timestamp,
                    dry_run=dry_run,
                    verbose=verbose,
                    **table_config.__dict__,
                    skip_archive=skip_archive,
                    session=session,
                    batch_size=batch_size,
                )
                session.commit()
        else:
            logger.warning("Table %s not found.  Skipping.", table_name)


@provide_session
def export_archived_records(
    export_format: str,
    output_path: str,
    table_names: list[str] | None = None,
    drop_archives: bool = False,
    needs_confirm: bool = True,
    session: Session = NEW_SESSION,
) -> None:
    """Export archived data to the given output path in the given format."""
    archived_table_names = _get_archived_table_names(table_names, session)
    # If user chose to drop archives, check there are archive tables that exists
    # before asking for confirmation
    if drop_archives and archived_table_names and needs_confirm:
        _confirm_drop_archives(tables=sorted(archived_table_names))
    export_count = 0
    dropped_count = 0
    for table_name in archived_table_names:
        logger.info("Exporting table %s", table_name)
        _dump_table_to_file(
            target_table=table_name,
            file_path=os.path.join(output_path, f"{table_name}.{export_format}"),
            export_format=export_format,
            session=session,
        )
        export_count += 1
        if drop_archives:
            logger.info("Dropping archived table %s", table_name)
            session.execute(text(f"DROP TABLE {table_name}"))
            dropped_count += 1
    logger.info("Total exported tables: %s, Total dropped tables: %s", export_count, dropped_count)


@provide_session
def drop_archived_tables(
    table_names: list[str] | None, needs_confirm: bool, session: Session = NEW_SESSION
) -> None:
    """Drop archived tables."""
    archived_table_names = _get_archived_table_names(table_names, session)
    if needs_confirm and archived_table_names:
        _confirm_drop_archives(tables=sorted(archived_table_names))
    dropped_count = 0
    for table_name in archived_table_names:
        logger.info("Dropping archived table %s", table_name)
        session.execute(text(f"DROP TABLE {table_name}"))
        dropped_count += 1
    logger.info("Total dropped tables: %s", dropped_count)
