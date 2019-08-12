# -*- coding: utf-8 -*-
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

import hashlib
from typing import Any, Dict, List, Optional, TYPE_CHECKING
from sqlalchemy import Column, Index, Integer, String, Text, and_
from sqlalchemy.sql import exists

from airflow.models.base import Base, ID_LEN
from airflow.utils import db, timezone
from airflow.utils.sqlalchemy import UtcDateTime


if TYPE_CHECKING:
    from airflow.dag.serialization.serialized_dag import SerializedDAG  # noqa: F401, E501; # pylint: disable=cyclic-import
    from airflow.models import DAG  # noqa: F401; # pylint: disable=cyclic-import


class SerializedDagModel(Base):
    """A table for serialized DAGs.

    serialized_dag table is a snapshot of DAG files synchronized by scheduler.
    This feature is controlled by:
        [core] dagcached = False: enable this feature
        [core] dagcached_min_update_interval = 30 (s):
            serialized DAGs are updated in DB when a file gets processed by scheduler,
            to reduce DB write rate, there is a minimal interval of updating serialized DAGs.
        [scheduler] dag_dir_list_interval = 300 (s):
            interval of deleting serialized DAGs in DB when the files are deleted, suggest
            to use a smaller interval such as 60

    It is used by webserver to load dagbags when dagcached=True. Because reading from
    database is lightweight compared to importing from files, it solves the webserver
    scalability issue.
    """
    __tablename__ = 'serialized_dag'

    dag_id = Column(String(ID_LEN), primary_key=True)
    fileloc = Column(String(2000))
    # The max length of fileloc exceeds the limit of indexing.
    fileloc_hash = Column(Integer)
    data = Column(Text)
    last_updated = Column(UtcDateTime)

    __table_args__ = (
        Index('idx_fileloc_hash', fileloc_hash, unique=False),
    )

    def __init__(self, dag):
        from airflow.dag.serialization import Serialization

        self.dag_id = dag.dag_id
        self.fileloc = dag.full_filepath
        self.fileloc_hash = self.dag_fileloc_hash(self.fileloc)
        self.data = Serialization.to_json(dag)
        self.last_updated = timezone.utcnow()

    @staticmethod
    def dag_fileloc_hash(full_filepath: str) -> int:
        """"Hashing file location for indexing.

        :param full_filepath: full filepath of DAG file
        :return: hashed full_filepath
        """
        # hashing is needed because the length of fileloc is 2000 as an Airflow convention,
        # which is over the limit of indexing. If we can reduce the length of fileloc, then
        # hashing is not needed.
        return int(0xFFFF & int(
            hashlib.sha1(full_filepath.encode('utf-8')).hexdigest(), 16))

    @classmethod
    @db.provide_session
    def write_dag(cls, dag: 'DAG', min_update_interval: Optional[int] = None, session=None):
        """Serializes a DAG and writes it into database.

        :param dag: a DAG to be written into database
        :param min_update_interval: minimal interval in seconds to update serialized DAG
        :param session: ORM Session
        """

        if min_update_interval is not None:
            result = session.query(cls.last_updated).filter(
                cls.dag_id == dag.dag_id)
            if result is not None and (
                    timezone.utcnow() - result.last_updated).total_seconds() < min_update_interval:
                return
        session.merge(cls(dag))

    @classmethod
    @db.provide_session
    def read_all_dags(cls, session=None) -> Dict[str, 'SerializedDAG']:
        """Reads all DAGs in serialized_dag table.
        :param session: ORM Session

        :returns: a dict of DAGs read from database
        """
        from airflow.dag.serialization import Serialization

        serialized_dags = session.query(cls.dag_id, cls.data).all()

        dags = {}
        for dag_id, data in serialized_dags:
            dag = Serialization.from_json(data)  # type: Any
            # Sanity check.
            if dag.dag_id == dag_id:
                dags[dag_id] = dag
        return dags

    @classmethod
    @db.provide_session
    def remove_dag(cls, dag_id: str, session=None):
        """Deletes a DAG with given dag_id.

        :param dag_id: dag_id to be deleted
        :param session: ORM Session
        """
        session.execute(cls.__table__.delete().where(cls.dag_id == dag_id))

    @classmethod
    @db.provide_session
    def remove_deleted_dags(cls, alive_dag_filelocs: List[str], session=None):
        """Deletes DAGs not included in alive_dag_filelocs.

        :param alive_dag_filelocs: file paths of alive DAGs
        :param session: ORM Session
        """
        alive_fileloc_hashes = [
            cls.dag_fileloc_hash(fileloc) for fileloc in alive_dag_filelocs]

        session.execute(
            cls.__table__.delete().where(
                and_(cls.fileloc_hash.notin_(alive_fileloc_hashes),
                     cls.fileloc.notin_(alive_dag_filelocs))))

    @classmethod
    @db.provide_session
    def has_dag(cls, dag_id: str, session=None) -> bool:
        """Checks a DAG exist in serialized_dag table.

        :param dag_id: the DAG to check
        :param session: ORM Session
        """
        return session.query(exists().where(cls.dag_id == dag_id)).scalar()
