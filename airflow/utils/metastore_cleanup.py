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
orm_model: the table
recency_column: date column to filter by
keep_last: whether the last record should be kept even if it's older than clean_before_timestamp
keep_last_filters:
keep_last_group_by: if keeping the last record, can keep the last record for each group
"""


import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union

from pendulum import DateTime
from sqlalchemy import and_, func

from airflow.cli.simple_table import AirflowConsole
from airflow.configuration import conf
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
from airflow.settings import Session
from airflow.utils import timezone
from airflow.utils.session import NEW_SESSION, provide_session

if TYPE_CHECKING:
    from sqlalchemy.orm import Query
    from sqlalchemy.orm.attributes import InstrumentedAttribute
    from sqlalchemy.sql.schema import Column


@dataclass
class _Config:
    orm_model: Base
    recency_column: Union["Column", "InstrumentedAttribute"]
    keep_last: bool = False
    keep_last_filters: Optional[Any] = None
    keep_last_group_by: Optional[Any] = None

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
        )


objects_list: List[_Config] = [
    _Config(orm_model=BaseJob, recency_column=BaseJob.latest_heartbeat),
    _Config(orm_model=DagModel, recency_column=DagModel.last_parsed_time),
    _Config(
        orm_model=DagRun,
        recency_column=DagRun.start_date,
        keep_last=True,
        keep_last_filters=[DagRun.external_trigger.is_(False)],
        keep_last_group_by=DagRun.dag_id,
    ),
    _Config(orm_model=ImportError, recency_column=ImportError.timestamp),
    _Config(orm_model=Log, recency_column=Log.dttm),
    _Config(orm_model=RenderedTaskInstanceFields, recency_column=RenderedTaskInstanceFields.execution_date),
    _Config(orm_model=SlaMiss, recency_column=SlaMiss.timestamp),
    _Config(orm_model=TaskFail, recency_column=TaskFail.start_date),
    _Config(orm_model=TaskInstance, recency_column=TaskInstance.start_date),
    _Config(orm_model=TaskReschedule, recency_column=TaskReschedule.start_date),
    _Config(orm_model=XCom, recency_column=XCom.timestamp),
]

if str(conf.get("core", "executor")) == "CeleryExecutor":
    try:
        from celery.backends.database.models import Task, TaskSet

        objects_list.extend(
            [
                _Config(orm_model=Task, recency_column=Task.date_done),
                _Config(orm_model=TaskSet, recency_column=TaskSet.date_done),
            ]
        )
    except Exception as e:
        logging.error(e)

objects_dict: Dict[str, _Config] = {x.orm_model.__tablename__: x for x in sorted(objects_list)}


def _print_entities(*, query: "Query", print_rows=False):
    num_entities = query.count()
    print(f"Found {num_entities} rows meeting deletion criteria.")
    if not print_rows:
        return
    max_rows_to_print = 100
    if num_entities > 0:
        print(f"Printing first {max_rows_to_print} rows.")
    entries_to_delete = query.limit(max_rows_to_print).all()
    logging.debug("print entities query: " + str(query))
    for entry in entries_to_delete:  # type: Log
        print(entry.__dict__)


def _do_delete(*, query, session):
    print("Performing Delete...")
    # using bulk delete
    query.delete(synchronize_session=False)
    session.commit()
    print("Finished Performing Delete")


def _subquery_keep_last(*, keep_last_filters, keep_last_group_by, session):
    # workaround for MySQL "table specified twice" issue
    # https://github.com/teamclairvoyant/airflow-maintenance-dags/issues/41
    subquery = session.query(func.max(DagRun.execution_date))

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
):
    query = session.query(orm_model)
    conditions = [recency_column < clean_before_timestamp]
    if keep_last:
        subquery = _subquery_keep_last(
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


def _confirm_delete(*, date, tables):
    for_tables = f" for tables {tables!r}" if tables else ''
    question = '\n'.join(
        [
            f"You have requested that we purge all data prior to {date}{for_tables}.",
            "This is irreversible.  Consider backing up the tables first and / or doing a dry run with option --dry-run.",
            "Enter 'delete rows' to proceed.",
        ]
    )
    print(question)
    answer = input().strip()
    if not answer == 'delete rows':
        raise SystemExit("User did not confirm; exiting.")


def _print_config(*, table_subset):
    data = [x.readable_config for x in table_subset.values()]
    AirflowConsole().print_as_table(data=data)


@provide_session
def run_cleanup(
    *,
    clean_before_timestamp: DateTime,
    table_names: Optional[List[str]] = None,
    dry_run: bool = False,
    verbose: bool = False,
    confirm: bool = True,
    session: Session = NEW_SESSION,
):
    """
    Purges old records in airflow metastore database.

    :param clean_before_timestamp: The timestamp before which data should be purged
    :type clean_before_timestamp: DateTime
    :param table_names: Optional. List of table names to perform maintenance on.  If list not provided, will perform maintenance on all tables.
    :type table_names: Optional[List[str]]
    :param dry_run: If true, print rows meeting deletion criteria
    :type dry_run: bool
    :param verbose: If true, may provide more detailed output.
    :type verbose: bool
    :param confirm: Require user input to confirm before processing deletions.
    :type confirm: bool
    :param session: Session representing connection to the metastore database.
    :type session: Session
    """
    clean_before_timestamp = timezone.coerce_datetime(clean_before_timestamp)
    table_subset = {k: v for k, v in objects_dict.items() if (k in table_names if table_names else True)}
    if dry_run:
        print('Performing dry run for metastore table cleanup.')
        print(
            f"Data prior to {clean_before_timestamp} would be purged "
            f"from tables {list(table_subset.keys())} with the following config:\n"
        )
        _print_config(table_subset=table_subset)
    if not dry_run and confirm:
        _confirm_delete(date=clean_before_timestamp, tables=list(table_subset.keys()))
    for table_name, table_config in table_subset.items():
        _cleanup_table(
            clean_before_timestamp=clean_before_timestamp,
            dry_run=dry_run,
            verbose=verbose,
            **table_config.__dict__,
            session=session,
        )
