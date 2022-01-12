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


from pendulum import DateTime
from sqlalchemy.orm import Query

from airflow import settings
from airflow.configuration import conf
from airflow.models import (
    DagModel,
    DagRun,
    ImportError,
    Log,
    RenderedTaskInstanceFields,
    SlaMiss,
    TaskFail,
    TaskInstance,
    XCom,
)

try:
    from airflow.jobs import BaseJob
except Exception as e:
    from airflow.jobs.base_job import BaseJob

import logging

from sqlalchemy import and_, func

from airflow.models import TaskReschedule
from airflow.utils import timezone

now = timezone.utcnow


objects = {
    'job': {
        'keep_last': False,
        'keep_last_filters': None,
        'keep_last_group_by': None,
        'orm_model': BaseJob,
        'recency_column': BaseJob.latest_heartbeat,
    },
    'dag': {
        'keep_last': False,
        'keep_last_filters': None,
        'keep_last_group_by': None,
        'orm_model': DagModel,
        'recency_column': DagModel.last_parsed_time,
    },
    'dag_run': {
        'keep_last': True,
        "keep_last_filters": [DagRun.external_trigger.is_(False)],
        "keep_last_group_by": DagRun.dag_id,
        'orm_model': DagRun,
        'recency_column': DagRun.execution_date,
    },
    'import_error': {
        'keep_last': False,
        'keep_last_filters': None,
        'keep_last_group_by': None,
        'orm_model': ImportError,
        'recency_column': ImportError.timestamp,
    },
    'log': {
        'keep_last': False,
        'keep_last_filters': None,
        'keep_last_group_by': None,
        'orm_model': Log,
        'recency_column': Log.dttm,
    },
    'rendered_task_instance_fields': {
        'keep_last': False,
        'keep_last_filters': None,
        'keep_last_group_by': None,
        'orm_model': RenderedTaskInstanceFields,
        'recency_column': RenderedTaskInstanceFields.execution_date,
    },
    'sla_miss': {
        'keep_last': False,
        'keep_last_filters': None,
        'keep_last_group_by': None,
        'orm_model': SlaMiss,
        'recency_column': SlaMiss.execution_date,
    },
    'task_fail': {
        'keep_last': False,
        'keep_last_filters': None,
        'keep_last_group_by': None,
        'orm_model': TaskFail,
        'recency_column': TaskFail.execution_date,
    },
    'task_instance': {
        'keep_last': False,
        'keep_last_filters': None,
        'keep_last_group_by': None,
        'orm_model': TaskInstance,
        'recency_column': TaskInstance.execution_date,
    },
    'task_reschedule': {
        'keep_last': False,
        'keep_last_filters': None,
        'keep_last_group_by': None,
        'orm_model': TaskReschedule,
        'recency_column': TaskReschedule.execution_date,
    },
    'xcom': {
        'keep_last': False,
        'keep_last_filters': None,
        'keep_last_group_by': None,
        'orm_model': XCom,
        'recency_column': XCom.execution_date,
    },
}


airflow_executor = str(conf.get("core", "executor"))
if airflow_executor == "CeleryExecutor":
    from celery.backends.database.models import Task, TaskSet

    print("Including Celery Modules")
    try:
        objects.update(
            **{
                'task': {
                    'keep_last': False,
                    'keep_last_filters': None,
                    'keep_last_group_by': None,
                    'orm_model': Task,
                    'recency_column': Task.date_done,
                },
                'task_set': {
                    'keep_last': False,
                    'keep_last_filters': None,
                    'keep_last_group_by': None,
                    'orm_model': TaskSet,
                    'recency_column': TaskSet.date_done,
                },
            }
        )
    except Exception as e:
        logging.error(e)


session = settings.Session()


def print_entities(query: Query, print_rows=False):
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


def do_delete(query):
    print("Performing Delete...")
    # using bulk delete
    query.delete(synchronize_session=False)
    session.commit()
    print("Finished Performing Delete")


def subquery_keep_last(keep_last_filters, keep_last_group_by):
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


def build_query(
    orm_model, recency_column, keep_last, keep_last_filters, keep_last_group_by, clean_before_timestamp
):
    query = session.query(orm_model)
    conditions = [recency_column < clean_before_timestamp]
    if keep_last:
        subquery = subquery_keep_last(
            keep_last_filters=keep_last_filters, keep_last_group_by=keep_last_group_by
        )
        conditions.append(recency_column.notin_(subquery))
    query = query.filter(and_(*conditions))
    return query


logger = logging.getLogger()


def _cleanup_table(
    orm_model,
    recency_column,
    keep_last,
    keep_last_filters,
    keep_last_group_by,
    clean_before_timestamp,
    dry_run=True,
    verbose=False,
):
    print()
    if dry_run:
        print(f"Performing dry run for table {orm_model.__tablename__!r}")
    query = build_query(
        orm_model=orm_model,
        recency_column=recency_column,
        keep_last=keep_last,
        keep_last_filters=keep_last_filters,
        keep_last_group_by=keep_last_group_by,
        clean_before_timestamp=clean_before_timestamp,
    )

    print_entities(query, print_rows=False)

    if not dry_run:
        do_delete(query)


def confirm(date, tables):
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
        SystemExit("User did not confirm; exiting.")


def run_cleanup(clean_before_timestamp: DateTime, table_names=None, dry_run=False, verbose=False):
    clean_before_timestamp = timezone.coerce_datetime(clean_before_timestamp)
    if not dry_run:
        confirm(clean_before_timestamp, table_names)
    if table_names:
        for table_name in table_names:
            table_config = objects[table_name]
            _cleanup_table(
                **table_config,
                clean_before_timestamp=clean_before_timestamp,
                dry_run=dry_run,
                verbose=verbose,
            )
    else:
        for table_name, table_config in objects.items():
            _cleanup_table(
                **table_config,
                clean_before_timestamp=clean_before_timestamp,
                dry_run=dry_run,
                verbose=verbose,
            )
