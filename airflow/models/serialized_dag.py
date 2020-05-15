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

"""Serialzed DAG table in database."""

import logging
from datetime import timedelta
from typing import Any, Dict, List, Optional

import sqlalchemy_jsonfield
from sqlalchemy import BigInteger, Column, Index, String, and_
from sqlalchemy.sql import exists

from airflow.models.base import ID_LEN, Base
from airflow.models.dag import DAG, DagModel
from airflow.models.dagcode import DagCode
from airflow.serialization.serialized_objects import SerializedDAG
from airflow.settings import MIN_SERIALIZED_DAG_UPDATE_INTERVAL, STORE_SERIALIZED_DAGS, json
from airflow.utils import timezone
from airflow.utils.session import provide_session
from airflow.utils.sqlalchemy import UtcDateTime

log = logging.getLogger(__name__)


class SerializedDagModel(Base):
    """A table for serialized DAGs.

    serialized_dag table is a snapshot of DAG files synchronized by scheduler.
    This feature is controlled by:

    * ``[core] store_serialized_dags = True``: enable this feature
    * ``[core] min_serialized_dag_update_interval = 30`` (s):
      serialized DAGs are updated in DB when a file gets processed by scheduler,
      to reduce DB write rate, there is a minimal interval of updating serialized DAGs.
    * ``[scheduler] dag_dir_list_interval = 300`` (s):
      interval of deleting serialized DAGs in DB when the files are deleted, suggest
      to use a smaller interval such as 60

    It is used by webserver to load dagbags when ``store_serialized_dags=True``.
    Because reading from database is lightweight compared to importing from files,
    it solves the webserver scalability issue.
    """
    __tablename__ = 'serialized_dag'

    dag_id = Column(String(ID_LEN), primary_key=True)
    fileloc = Column(String(2000), nullable=False)
    # The max length of fileloc exceeds the limit of indexing.
    fileloc_hash = Column(BigInteger, nullable=False)
    data = Column(sqlalchemy_jsonfield.JSONField(json=json), nullable=False)
    last_updated = Column(UtcDateTime, nullable=False)

    __table_args__ = (
        Index('idx_fileloc_hash', fileloc_hash, unique=False),
    )

    def __init__(self, dag: DAG):
        self.dag_id = dag.dag_id
        self.fileloc = dag.full_filepath
        self.fileloc_hash = DagCode.dag_fileloc_hash(self.fileloc)
        self.data = SerializedDAG.to_dict(dag)
        self.last_updated = timezone.utcnow()

    @classmethod
    @provide_session
    def write_dag(cls, dag: DAG, min_update_interval: Optional[int] = None, session=None):
        """Serializes a DAG and writes it into database.

        :param dag: a DAG to be written into database
        :param min_update_interval: minimal interval in seconds to update serialized DAG
        :param session: ORM Session
        """
        # Checks if (Current Time - Time when the DAG was written to DB) < min_update_interval
        # If Yes, does nothing
        # If No or the DAG does not exists, updates / writes Serialized DAG to DB
        if min_update_interval is not None:
            if session.query(exists().where(
                and_(cls.dag_id == dag.dag_id,
                     (timezone.utcnow() - timedelta(seconds=min_update_interval)) < cls.last_updated))
            ).scalar():
                return
        log.debug("Writing DAG: %s to the DB", dag.dag_id)
        session.merge(cls(dag))
        log.debug("DAG: %s written to the DB", dag.dag_id)

    @classmethod
    @provide_session
    def read_all_dags(cls, session=None) -> Dict[str, 'SerializedDAG']:
        """Reads all DAGs in serialized_dag table.

        :param session: ORM Session
        :returns: a dict of DAGs read from database
        """
        serialized_dags = session.query(cls)

        dags = {}
        for row in serialized_dags:
            log.debug("Deserializing DAG: %s", row.dag_id)
            dag = row.dag

            # Sanity check.
            if dag.dag_id == row.dag_id:
                dags[row.dag_id] = dag
            else:
                log.warning(
                    "dag_id Mismatch in DB: Row with dag_id '%s' has Serialised DAG "
                    "with '%s' dag_id", row.dag_id, dag.dag_id)
        return dags

    @property
    def dag(self):
        """The DAG deserialized from the ``data`` column"""
        if isinstance(self.data, dict):
            dag = SerializedDAG.from_dict(self.data)  # type: Any
        else:
            # noinspection PyTypeChecker
            dag = SerializedDAG.from_json(self.data)
        return dag

    @classmethod
    @provide_session
    def remove_dag(cls, dag_id: str, session=None):
        """Deletes a DAG with given dag_id.

        :param dag_id: dag_id to be deleted
        :param session: ORM Session
        """
        # pylint: disable=no-member
        session.execute(cls.__table__.delete().where(cls.dag_id == dag_id))

    @classmethod
    @provide_session
    def remove_stale_dags(cls, expiration_date, session=None):
        """
        Deletes Serialized DAGs that were last touched by the scheduler before
        the expiration date. These DAGs were likely deleted.

        :param expiration_date: set inactive DAGs that were touched before this
            time
        :type expiration_date: datetime
        :return: None
        """
        log.debug("Deleting Serialized DAGs that haven't been touched by the "
                  "scheduler since %s from %s table ", expiration_date, cls.__tablename__)

        session.execute(
            # pylint: disable=no-member
            cls.__table__.delete().where(cls.last_updated < expiration_date)
        )

    @classmethod
    @provide_session
    def has_dag(cls, dag_id: str, session=None) -> bool:
        """Checks a DAG exist in serialized_dag table.

        :param dag_id: the DAG to check
        :param session: ORM Session
        """
        return session.query(exists().where(cls.dag_id == dag_id)).scalar()

    @classmethod
    @provide_session
    def get(cls, dag_id: str, session=None) -> Optional['SerializedDagModel']:
        """
        Get the SerializedDAG for the given dag ID.
        It will cope with being passed the ID of a subdag by looking up the
        root dag_id from the DAG table.

        :param dag_id: the DAG to fetch
        :param session: ORM Session
        """
        row = session.query(cls).filter(cls.dag_id == dag_id).one_or_none()
        if row:
            return row

        # If we didn't find a matching DAG id then ask the DAG table to find
        # out the root dag
        root_dag_id = session.query(
            DagModel.root_dag_id).filter(DagModel.dag_id == dag_id).scalar()

        return session.query(cls).filter(cls.dag_id == root_dag_id).one_or_none()

    @staticmethod
    @provide_session
    def bulk_sync_to_db(dags: List[DAG], session=None):
        """
        Saves DAGs as Seralized DAG objects in the database. Each DAG is saved in a separate database query.

        :param dags: the DAG objects to save to the DB
        :type dags: List[airflow.models.dag.DAG]
        :return: None
        """
        if not STORE_SERIALIZED_DAGS:
            return

        for dag in dags:
            if not dag.is_subdag:
                SerializedDagModel.write_dag(
                    dag,
                    min_update_interval=MIN_SERIALIZED_DAG_UPDATE_INTERVAL,
                    session=session
                )
