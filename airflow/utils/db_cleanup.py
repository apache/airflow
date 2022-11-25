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
This module took inspiration from the community maintenance dag
(https://github.com/teamclairvoyant/airflow-maintenance-dags/blob/4e5c7682a808082561d60cbc9cafaa477b0d8c65/db-cleanup/airflow-db-cleanup.py).
"""
from __future__ import annotations

import logging
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any

from pendulum import DateTime
from sqlalchemy import and_, column, false, func, table, text
from sqlalchemy.exc import OperationalError, ProgrammingError
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.orm import Query, Session, aliased
from sqlalchemy.sql.expression import ClauseElement, Executable, tuple_

from airflow.cli.simple_table import AirflowConsole
from airflow.models import Base
from airflow.utils import timezone
from airflow.utils.db import reflect_tables
from airflow.utils.session import NEW_SESSION, provide_session

logger = logging.getLogger(__file__)


@dataclass
class _TableConfig:
    """
    Config class for performing cleanup on a table

    :param table_name: the table
    :param extra_columns: any columns besides recency_column_name that we'll need in queries
    :param recency_column_name: date column to filter by
    :param keep_last: whether the last record should be kept even if it's older than clean_before_timestamp
    :param keep_last_filters: the "keep last" functionality will preserve the most recent record
        in the table.  to ignore certain records even if they are the latest in the table, you can
        supply additional filters here (e.g. externally triggered dag runs)
    :param keep_last_group_by: if keeping the last record, can keep the last record for each group
    """

    table_name: str
    recency_column_name: str
    extra_columns: list[str] | None = None
    keep_last: bool = False
    keep_last_filters: Any | None = None
    keep_last_group_by: Any | None = None

    def __post_init__(self):
        self.recency_column = column(self.recency_column_name)
        self.orm_model: Base = table(
            self.table_name, *[column(x) for x in self.extra_columns or []], self.recency_column
        )

    def __lt__(self, other):
        return self.table_name < other.table_name

    @property
    def readable_config(self):
        return dict(
            table=self.orm_model.name,
            recency_column=str(self.recency_column),
            keep_last=self.keep_last,
            keep_last_filters=[str(x) for x in self.keep_last_filters] if self.keep_last_filters else None,
            keep_last_group_by=str(self.keep_last_group_by),
        )


config_list: list[_TableConfig] = [
    _TableConfig(table_name="job", recency_column_name="latest_heartbeat"),
    _TableConfig(table_name="dag", recency_column_name="last_parsed_time"),
    _TableConfig(
        table_name="dag_run",
        recency_column_name="start_date",
        extra_columns=["dag_id", "external_trigger"],
        keep_last=True,
        keep_last_filters=[column("external_trigger") == false()],
        keep_last_group_by=["dag_id"],
    ),
    _TableConfig(table_name="dataset_event", recency_column_name="created_at"),
    _TableConfig(table_name="import_error", recency_column_name="timestamp"),
    _TableConfig(table_name="log", recency_column_name="dttm"),
    _TableConfig(table_name="rendered_task_instance_fields", recency_column_name="execution_date"),
    _TableConfig(table_name="sla_miss", recency_column_name="timestamp"),
    _TableConfig(table_name="task_fail", recency_column_name="start_date"),
    _TableConfig(table_name="task_instance", recency_column_name="start_date"),
    _TableConfig(table_name="task_reschedule", recency_column_name="start_date"),
    _TableConfig(table_name="xcom", recency_column_name="timestamp"),
    _TableConfig(table_name="callback_request", recency_column_name="created_at"),
    _TableConfig(table_name="celery_taskmeta", recency_column_name="date_done"),
    _TableConfig(table_name="celery_tasksetmeta", recency_column_name="date_done"),
]

config_dict: dict[str, _TableConfig] = {x.orm_model.name: x for x in sorted(config_list)}


def _check_for_rows(*, query: Query, print_rows=False):
    num_entities = query.count()
    print(f"Found {num_entities} rows meeting deletion criteria.")
    if print_rows:
        max_rows_to_print = 100
        if num_entities > 0:
            print(f"Printing first {max_rows_to_print} rows.")
        logger.debug("print entities query: %s", query)
        for entry in query.limit(max_rows_to_print):
            print(entry.__dict__)
    return num_entities


def _do_delete(*, query, orm_model, skip_archive, session):
    import re
    from datetime import datetime

    print("Performing Delete...")
    # using bulk delete
    # create a new table and copy the rows there
    timestamp_str = re.sub(r"[^\d]", "", datetime.utcnow().isoformat())[:14]
    target_table_name = f"_airflow_deleted__{orm_model.name}__{timestamp_str}"
    print(f"Moving data to table {target_table_name}")
    stmt = CreateTableAs(target_table_name, query.selectable)
    logger.debug("ctas query:\n%s", stmt.compile())
    session.execute(stmt)
    session.commit()

    # delete the rows from the old table
    metadata = reflect_tables([orm_model.name, target_table_name], session)
    source_table = metadata.tables[orm_model.name]
    target_table = metadata.tables[target_table_name]
    logger.debug("rows moved; purging from %s", source_table.name)
    bind = session.get_bind()
    dialect_name = bind.dialect.name
    if dialect_name == "sqlite":
        pk_cols = source_table.primary_key.columns
        delete = source_table.delete().where(
            tuple_(*pk_cols).in_(
                session.query(*[target_table.c[x.name] for x in source_table.primary_key.columns]).subquery()
            )
        )
    else:
        delete = source_table.delete().where(
            and_(col == target_table.c[col.name] for col in source_table.primary_key.columns)
        )
    logger.debug("delete statement:\n%s", delete.compile())
    session.execute(delete)
    session.commit()
    if skip_archive:
        target_table.drop()
    session.commit()
    print("Finished Performing Delete")


def _subquery_keep_last(*, recency_column, keep_last_filters, group_by_columns, max_date_colname, session):
    subquery = session.query(*group_by_columns, func.max(recency_column).label(max_date_colname))

    if keep_last_filters is not None:
        for entry in keep_last_filters:
            subquery = subquery.filter(entry)

    if group_by_columns is not None:
        subquery = subquery.group_by(*group_by_columns)

    return subquery.subquery(name="latest")


class CreateTableAs(Executable, ClauseElement):
    """Custom sqlalchemy clause element for CTAS operations."""

    def __init__(self, name, query):
        self.name = name
        self.query = query


@compiles(CreateTableAs)
def _compile_create_table_as__other(element, compiler, **kw):
    return f"CREATE TABLE {element.name} AS {compiler.process(element.query)}"


@compiles(CreateTableAs, "mssql")
def _compile_create_table_as__mssql(element, compiler, **kw):
    return f"WITH cte AS ( {compiler.process(element.query)} ) SELECT * INTO {element.name} FROM cte"


def _build_query(
    *,
    orm_model,
    recency_column,
    keep_last,
    keep_last_filters,
    keep_last_group_by,
    clean_before_timestamp,
    session,
    **kwargs,
):
    base_table_alias = "base"
    base_table = aliased(orm_model, name=base_table_alias)
    query = session.query(base_table).with_entities(text(f"{base_table_alias}.*"))
    base_table_recency_col = base_table.c[recency_column.name]
    conditions = [base_table_recency_col < clean_before_timestamp]
    if keep_last:
        max_date_col_name = "max_date_per_group"
        group_by_columns = [column(x) for x in keep_last_group_by]
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
                *[base_table.c[x] == subquery.c[x] for x in keep_last_group_by],
                base_table_recency_col == column(max_date_col_name),
            ),
        )
        conditions.append(column(max_date_col_name).is_(None))
    query = query.filter(and_(*conditions))
    return query


def _cleanup_table(
    *,
    orm_model,
    recency_column,
    keep_last,
    keep_last_filters,
    keep_last_group_by,
    clean_before_timestamp,
    dry_run=True,
    verbose=False,
    skip_archive=False,
    session=None,
    **kwargs,
):
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
        session=session,
    )
    logger.debug("old rows query:\n%s", query.selectable.compile())
    print(f"Checking table {orm_model.name}")
    num_rows = _check_for_rows(query=query, print_rows=False)

    if num_rows and not dry_run:
        _do_delete(query=query, orm_model=orm_model, skip_archive=skip_archive, session=session)

    session.commit()


def _confirm_delete(*, date: DateTime, tables: list[str]):
    for_tables = f" for tables {tables!r}" if tables else ""
    question = (
        f"You have requested that we purge all data prior to {date}{for_tables}.\n"
        f"This is irreversible.  Consider backing up the tables first and / or doing a dry run "
        f"with option --dry-run.\n"
        f"Enter 'delete rows' (without quotes) to proceed."
    )
    print(question)
    answer = input().strip()
    if not answer == "delete rows":
        raise SystemExit("User did not confirm; exiting.")


def _print_config(*, configs: dict[str, _TableConfig]):
    data = [x.readable_config for x in configs.values()]
    AirflowConsole().print_as_table(data=data)


@contextmanager
def _suppress_with_logging(table, session):
    """
    Suppresses errors but logs them.
    Also stores the exception instance so it can be referred to after exiting context.
    """
    try:
        yield
    except (OperationalError, ProgrammingError):
        logger.warning("Encountered error when attempting to clean table '%s'. ", table)
        logger.debug("Traceback for table '%s'", table, exc_info=True)
        if session.is_active:
            logger.debug("Rolling back transaction")
            session.rollback()


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
):
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
    :param skip_archive: Set to True if you don't want the purged rows preservied in an archive table.
    :param session: Session representing connection to the metadata database.
    """
    clean_before_timestamp = timezone.coerce_datetime(clean_before_timestamp)
    effective_table_names = table_names if table_names else list(config_dict.keys())
    effective_config_dict = {k: v for k, v in config_dict.items() if k in effective_table_names}
    if dry_run:
        print("Performing dry run for db cleanup.")
        print(
            f"Data prior to {clean_before_timestamp} would be purged "
            f"from tables {effective_table_names} with the following config:\n"
        )
        _print_config(configs=effective_config_dict)
    if not dry_run and confirm:
        _confirm_delete(date=clean_before_timestamp, tables=list(effective_config_dict.keys()))
    existing_tables = reflect_tables(tables=None, session=session).tables
    for table_name, table_config in effective_config_dict.items():
        if table_name not in existing_tables:
            logger.warning("Table %s not found.  Skipping.", table_name)
            continue
        with _suppress_with_logging(table_name, session):
            _cleanup_table(
                clean_before_timestamp=clean_before_timestamp,
                dry_run=dry_run,
                verbose=verbose,
                **table_config.__dict__,
                skip_archive=skip_archive,
                session=session,
            )
            session.commit()
