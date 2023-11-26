#!/usr/bin/env python
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
"""Migration script to port an Apache Airflow 2.7.3 DB to another DB engine.

Call it with `python migrate_script.py --extract` on the source environment
to dump the configured airflow metadata database to a SQLite file called
`migration.db`. Then you can copy this file to the target environment
(or re-configure your database backend) and run `python migrate_script.py --restore`.

Note that it is common sense probably that this script is assuming that schedulers,
workers, triggerer and webserver are stopped while running this script. It is also
advised to halt all running jobs and DAG runs.

Note that this script is made for Airflow 2.7.3 and is provided without any warranty.
"""

# TODO test 2.7.3
# TODO docs
from __future__ import annotations

import argparse
import logging
import os
import subprocess

from sqlalchemy import create_engine, delete, select, text
from sqlalchemy.orm import Session

from airflow.auth.managers.fab.models import Action, Permission, RegisterUser, Resource, Role, User
from airflow.jobs.job import Job
from airflow.models.connection import Connection
from airflow.models.dag import DagModel, DagOwnerAttributes, DagTag
from airflow.models.dagcode import DagCode
from airflow.models.dagpickle import DagPickle
from airflow.models.dagrun import DagRun, DagRunNote
from airflow.models.dagwarning import DagWarning
from airflow.models.dataset import (
    DagScheduleDatasetReference,
    DatasetDagRunQueue,
    DatasetEvent,
    DatasetModel,
    TaskOutletDatasetReference,
)
from airflow.models.db_callback_request import DbCallbackRequest
from airflow.models.errors import ImportError
from airflow.models.log import Log
from airflow.models.pool import Pool
from airflow.models.renderedtifields import RenderedTaskInstanceFields
from airflow.models.serialized_dag import SerializedDagModel
from airflow.models.slamiss import SlaMiss
from airflow.models.taskfail import TaskFail
from airflow.models.taskinstance import TaskInstance, TaskInstanceNote
from airflow.models.tasklog import LogTemplate
from airflow.models.taskmap import TaskMap
from airflow.models.taskreschedule import TaskReschedule
from airflow.models.trigger import Trigger
from airflow.models.variable import Variable
from airflow.models.xcom import BaseXCom, XCom
from airflow.settings import engine

# configuration variables
airflow_db_url = engine.url
temp_db_url = "sqlite:///migration.db"
supported_db_versions = [
    # see https://airflow.apache.org/docs/apache-airflow/stable/migrations-ref.html
    "788397e78828"  # Airflow 2.7.3
]

# initialise logging
logging.basicConfig(filename="migration.log", level=logging.DEBUG)


def copy_airflow_tables(source_engine, target_engine):
    objects_to_migrate = [
        Action,
        Resource,
        Role,
        User,
        Permission,
        RegisterUser,
        Connection,
        DagModel,
        DagCode,
        DagOwnerAttributes,
        DagPickle,
        DagTag,
        Job,
        LogTemplate,
        DagRun,
        DagRunNote,
        DagWarning,
        DatasetModel,
        DagScheduleDatasetReference,
        TaskOutletDatasetReference,
        DatasetDagRunQueue,
        DatasetEvent,
        DbCallbackRequest,
        ImportError,
        Log,
        Pool,
        Trigger,
        RenderedTaskInstanceFields,
        SerializedDagModel,
        SlaMiss,
        TaskInstance,
        TaskInstanceNote,
        TaskFail,
        TaskMap,
        TaskReschedule,
        Variable,
        BaseXCom,
        "ab_user_role",
        "ab_permission_view",
        "ab_permission_view_role",
    ]
    source_session = Session(bind=source_engine)
    target_session = Session(bind=target_engine)
    target_session.autoflush = False
    dialect_name = target_session.bind.dialect.name
    quote = "`" if dialect_name == "mysql" else '"'

    # check that source DB is a supported version
    db_version = target_session.scalar(f"SELECT * FROM {quote}alembic_version{quote}")
    if db_version not in supported_db_versions:
        raise ValueError(f"Unsupported Airflow Schema version {db_version}")

    # Deserialization fails, but we want to transfer the blob as original anyway, mock serialization away
    def deserialize_mock(self: XCom):
        return self.value

    BaseXCom.orm_deserialize_value = deserialize_mock

    for clz in reversed(objects_to_migrate):
        if isinstance(clz, str):
            logging.info("Cleaning table %s", clz)
            target_session.execute(f"DELETE FROM {quote}{clz}{quote}")
        else:
            logging.info("Cleaning table %s", clz.__tablename__)
            if clz == User:
                # The user has a self-constraint, need to delete in batches not to violate cross-dependencies
                continue_delete = True
                while continue_delete:
                    filter_uids = set()
                    for uid in target_session.execute(
                        text("SELECT changed_by_fk FROM ab_user WHERE changed_by_fk IS NOT NULL")
                    ).fetchall():
                        filter_uids.add(uid)
                    for uid in target_session.execute(
                        text("SELECT created_by_fk FROM ab_user WHERE created_by_fk IS NOT NULL")
                    ).fetchall():
                        filter_uids.add(uid)
                    uid_list = ",".join(str(uid[0]) for uid in filter_uids)
                    if uid_list:
                        continue_delete = (
                            target_session.execute(
                                f"DELETE FROM ab_user WHERE id NOT IN ({uid_list})"
                            ).rowcount
                            > 0
                        )
                    else:
                        continue_delete = target_session.execute(delete(clz)).rowcount > 0
            else:
                target_session.execute(delete(clz))
    for clz in objects_to_migrate:
        count = 0
        if not isinstance(clz, str):
            logging.info("Migration of %s started", clz.__tablename__)
            for item in source_session.scalars(select(clz)).unique():
                target_session.merge(item)
                target_session.flush()
                count += 1
                if count % 100 == 0:
                    logging.info("Migration of chunk finished, %i migrated", count)
            logging.info("Migration of %s finished with %i rows", clz.__tablename__, count)
            target_session.commit()
    for clz in objects_to_migrate:
        count = 0
        if not isinstance(clz, str):
            if "id" in clz.__dict__:
                logging.info("Resetting sequence value for %s", clz.__tablename__)
                max = target_session.scalar(f"SELECT MAX(id) FROM {quote}{clz.__tablename__}{quote}")
                if max:
                    if dialect_name == "postgresql":
                        target_session.execute(
                            f"ALTER SEQUENCE {quote}{clz.__tablename__}_id_seq{quote} RESTART WITH {max+1}"
                        )
                    elif dialect_name == "sqlite":
                        pass  # nothing to be done for sqlite
                    elif dialect_name == "mysql":
                        target_session.execute(
                            f"ALTER TABLE {quote}{clz.__tablename__}{quote} AUTO_INCREMENT = {max+1}"
                        )
                    else:  # e.g. "mssql"
                        raise Exception(f"Database type {dialect_name} not supported")
                target_session.commit()


def main(extract: bool, restore: bool):
    if extract == restore:
        raise ValueError("Please specify what you want to do! Or use --help")

    if extract:
        logging.info("Creating migration database %s", temp_db_url)
        envs = os.environ.copy()
        envs["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"] = temp_db_url
        subprocess.check_call(args=["airflow", "db", "reset", "--yes"], env=envs)

    # source database
    airflow_engine = create_engine(airflow_db_url)
    temp_engine = create_engine(temp_db_url)
    logging.info("Connection to databases established")

    if extract:
        copy_airflow_tables(airflow_engine, temp_engine)
    else:
        copy_airflow_tables(temp_engine, airflow_engine)
    logging.info("Migration completed!")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--extract", help="Extracts the current Airflow database to a SQLite file", action="store_true"
    )
    parser.add_argument(
        "--restore", help="Restores from a SQLite to the current Airflow database", action="store_true"
    )
    args = parser.parse_args()
    main(args.extract, args.restore)
