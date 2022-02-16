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


import logging
from contextlib import AbstractContextManager
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union

from pendulum import DateTime
from sqlalchemy import and_, false, func
from sqlalchemy.exc import OperationalError

from airflow.cli.simple_table import AirflowConsole
from airflow.jobs.base_job import BaseJob
from airflow.models import (
    Base,
    DagModel,
    DagRun,
    ImportError,
    Log,
    RenderedTaskInstanceFields,
    SlaMiss,
    TaskFail,
    TaskInstance,
    TaskReschedule,
    XCom,
)
from airflow.utils import timezone
from airflow.utils.session import NEW_SESSION, provide_session

if TYPE_CHECKING:
    from sqlalchemy.orm import Query, Session
    from sqlalchemy.orm.attributes import InstrumentedAttribute
    from sqlalchemy.sql.schema import Column


@dataclass
class _TableConfig:
    """
    Config class for performing cleanup on a table

    :param orm_model: the table
    :param recency_column: date column to filter by
    :param keep_last: whether the last record should be kept even if it's older than clean_before_timestamp
    :param keep_last_filters:
    :param keep_last_group_by: if keeping the last record, can keep the last record for each group
    :param warn_if_missing: If True, then we'll suppress "table missing" exception and log a warning.
        If False then the exception will go uncaught.
    """

    orm_model: Base
    recency_column: Union["Column", "InstrumentedAttribute"]
    keep_last: bool = False
    keep_last_filters: Optional[Any] = None
    keep_last_group_by: Optional[Any] = None
    warn_if_missing: bool = False

    def __lt__(self, other):
        return self.orm_model.__tablename__ < self.orm_model.__tablename__

    @property
    def readable_config(self):
        return dict(
            table=self.orm_model.__tablename__,
            recency_column=str(self.recency_column),
            keep_last=self.keep_last,
            keep_last_filters=[str(x) for x in self.keep_last_filters] if self.keep_last_filters else None,
            keep_last_group_by=str(self.keep_last_group_by),
            warn_if_missing=str(self.warn_if_missing),
        )


config_list: List[_TableConfig] = [
    _TableConfig(orm_model=BaseJob, recency_column=BaseJob.latest_heartbeat),
    _TableConfig(orm_model=DagModel, recency_column=DagModel.last_parsed_time),
    _TableConfig(
        orm_model=DagRun,
        recency_column=DagRun.start_date,
        keep_last=True,
        keep_last_filters=[DagRun.external_trigger == false()],
        keep_last_group_by=DagRun.dag_id,
    ),
    _TableConfig(orm_model=ImportError, recency_column=ImportError.timestamp),
    _TableConfig(orm_model=Log, recency_column=Log.dttm),
    _TableConfig(
        orm_model=RenderedTaskInstanceFields, recency_column=RenderedTaskInstanceFields.execution_date
    ),
    _TableConfig(orm_model=SlaMiss, recency_column=SlaMiss.timestamp),
    _TableConfig(orm_model=TaskFail, recency_column=TaskFail.start_date),
    _TableConfig(orm_model=TaskInstance, recency_column=TaskInstance.start_date),
    _TableConfig(orm_model=TaskReschedule, recency_column=TaskReschedule.start_date),
    _TableConfig(orm_model=XCom, recency_column=XCom.timestamp),
]
try:
    from celery.backends.database.models import Task, TaskSet

    config_list.extend(
        [
            _TableConfig(orm_model=Task, recency_column=Task.date_done, warn_if_missing=True),
            _TableConfig(orm_model=TaskSet, recency_column=TaskSet.date_done, warn_if_missing=True),
        ]
    )
except ImportError:
    pass

config_dict: Dict[str, _TableConfig] = {x.orm_model.__tablename__: x for x in sorted(config_list)}


def _print_entities(*, query: "Query", print_rows=False):
    num_entities = query.count()
    print(f"Found {num_entities} rows meeting deletion criteria.")
    if not print_rows:
        return
    max_rows_to_print = 100
    if num_entities > 0:
        print(f"Printing first {max_rows_to_print} rows.")
    logger.debug("print entities query: " + str(query))
    for entry in query.limit(max_rows_to_print):
        print(entry.__dict__)


def _do_delete(*, query, session):
    print("Performing Delete...")
    # using bulk delete
    query.delete(synchronize_session=False)
    session.commit()
    print("Finished Performing Delete")


def _subquery_keep_last(*, recency_column, keep_last_filters, keep_last_group_by, session):
    # workaround for MySQL "table specified twice" issue
    # https://github.com/teamclairvoyant/airflow-maintenance-dags/issues/41
    subquery = session.query(func.max(recency_column))

    if keep_last_filters is not None:
        for entry in keep_last_filters:
            subquery = subquery.filter(entry)

    if keep_last_group_by is not None:
        subquery = subquery.group_by(keep_last_group_by)

    subquery = subquery.from_self()
    return subquery


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
    query = session.query(orm_model)
    conditions = [recency_column < clean_before_timestamp]
    if keep_last:
        subquery = _subquery_keep_last(
            recency_column=recency_column,
            keep_last_filters=keep_last_filters,
            keep_last_group_by=keep_last_group_by,
            session=session,
        )
        conditions.append(recency_column.notin_(subquery))
    query = query.filter(and_(*conditions))
    return query


logger = logging.getLogger(__file__)


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
    session=None,
    **kwargs,
):
    print()
    if dry_run:
        print(f"Performing dry run for table {orm_model.__tablename__!r}")
    query = _build_query(
        orm_model=orm_model,
        recency_column=recency_column,
        keep_last=keep_last,
        keep_last_filters=keep_last_filters,
        keep_last_group_by=keep_last_group_by,
        clean_before_timestamp=clean_before_timestamp,
        session=session,
    )

    _print_entities(query=query, print_rows=False)

    if not dry_run:
        _do_delete(query=query, session=session)
        session.commit()


def _confirm_delete(*, date: DateTime, tables: List[str]):
    for_tables = f" for tables {tables!r}" if tables else ''
    question = (
        f"You have requested that we purge all data prior to {date}{for_tables}.\n"
        f"This is irreversible.  Consider backing up the tables first and / or doing a dry run "
        f"with option --dry-run.\n"
        f"Enter 'delete rows' (without quotes) to proceed."
    )
    print(question)
    answer = input().strip()
    if not answer == 'delete rows':
        raise SystemExit("User did not confirm; exiting.")


def _print_config(*, configs: Dict[str, _TableConfig]):
    data = [x.readable_config for x in configs.values()]
    AirflowConsole().print_as_table(data=data)


class _warn_if_missing(AbstractContextManager):
    def __init__(self, table, suppress):
        self.table = table
        self.suppress = suppress

    def __enter__(self):
        return self

    def __exit__(self, exctype, excinst, exctb):
        caught_error = exctype is not None and issubclass(exctype, OperationalError)
        if caught_error:
            logger.warning("Table %r not found.  Skipping.", self.table)
        return caught_error


@provide_session
def run_cleanup(
    *,
    clean_before_timestamp: DateTime,
    table_names: Optional[List[str]] = None,
    dry_run: bool = False,
    verbose: bool = False,
    confirm: bool = True,
    session: 'Session' = NEW_SESSION,
):
    """
    Purges old records in airflow metadata database.

    The last non-externally-triggered dag run will always be kept in order to ensure
    continuity of scheduled dag runs.

    Where there are foreign key relationships, deletes will cascade, so that for
    example if you clean up old dag runs, the associated task instances will
    be deleted.

    :param clean_before_timestamp: The timestamp before which data should be purged
    :type clean_before_timestamp: DateTime
    :param table_names: Optional. List of table names to perform maintenance on.  If list not provided,
        will perform maintenance on all tables.
    :type table_names: Optional[List[str]]
    :param dry_run: If true, print rows meeting deletion criteria
    :type dry_run: bool
    :param verbose: If true, may provide more detailed output.
    :type verbose: bool
    :param confirm: Require user input to confirm before processing deletions.
    :type confirm: bool
    :param session: Session representing connection to the metadata database.
    :type session: Session
    """
    clean_before_timestamp = timezone.coerce_datetime(clean_before_timestamp)
    effective_table_names = table_names if table_names else list(config_dict.keys())
    effective_config_dict = {k: v for k, v in config_dict.items() if k in effective_table_names}
    if dry_run:
        print('Performing dry run for db cleanup.')
        print(
            f"Data prior to {clean_before_timestamp} would be purged "
            f"from tables {effective_table_names} with the following config:\n"
        )
        _print_config(configs=effective_config_dict)
    if not dry_run and confirm:
        _confirm_delete(date=clean_before_timestamp, tables=list(effective_config_dict.keys()))
    for table_name, table_config in effective_config_dict.items():
        with _warn_if_missing(table_name, table_config.warn_if_missing):
            _cleanup_table(
                clean_before_timestamp=clean_before_timestamp,
                dry_run=dry_run,
                verbose=verbose,
                **table_config.__dict__,
                session=session,
            )
