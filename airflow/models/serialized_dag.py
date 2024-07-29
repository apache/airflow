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
"""Serialized DAG table in database."""

from __future__ import annotations

import logging
import zlib
from datetime import timedelta
from typing import TYPE_CHECKING, Collection

import sqlalchemy_jsonfield
from sqlalchemy import BigInteger, Column, Index, LargeBinary, String, and_, exc, or_, select
from sqlalchemy.orm import backref, foreign, relationship
from sqlalchemy.sql.expression import func, literal

from airflow.api_internal.internal_api_call import internal_api_call
from airflow.exceptions import TaskNotFound
from airflow.models.base import ID_LEN, Base
from airflow.models.dag import DagModel
from airflow.models.dagcode import DagCode
from airflow.models.dagrun import DagRun
from airflow.serialization.dag_dependency import DagDependency
from airflow.serialization.serialized_objects import SerializedDAG
from airflow.settings import COMPRESS_SERIALIZED_DAGS, MIN_SERIALIZED_DAG_UPDATE_INTERVAL, json
from airflow.utils import timezone
from airflow.utils.hashlib_wrapper import md5
from airflow.utils.session import NEW_SESSION, provide_session
from airflow.utils.sqlalchemy import UtcDateTime

if TYPE_CHECKING:
    from datetime import datetime

    from sqlalchemy.orm import Session

    from airflow.models import Operator
    from airflow.models.dag import DAG

log = logging.getLogger(__name__)


class SerializedDagModel(Base):
    """
    A table for serialized DAGs.

    serialized_dag table is a snapshot of DAG files synchronized by scheduler.
    This feature is controlled by:

    * ``[core] min_serialized_dag_update_interval = 30`` (s):
      serialized DAGs are updated in DB when a file gets processed by scheduler,
      to reduce DB write rate, there is a minimal interval of updating serialized DAGs.
    * ``[scheduler] dag_dir_list_interval = 300`` (s):
      interval of deleting serialized DAGs in DB when the files are deleted, suggest
      to use a smaller interval such as 60
    * ``[core] compress_serialized_dags``:
      whether compressing the dag data to the Database.

    It is used by webserver to load dags
    because reading from database is lightweight compared to importing from files,
    it solves the webserver scalability issue.
    """

    __tablename__ = "serialized_dag"

    dag_id = Column(String(ID_LEN), primary_key=True)
    fileloc = Column(String(2000), nullable=False)
    # The max length of fileloc exceeds the limit of indexing.
    fileloc_hash = Column(BigInteger(), nullable=False)
    _data = Column("data", sqlalchemy_jsonfield.JSONField(json=json), nullable=True)
    _data_compressed = Column("data_compressed", LargeBinary, nullable=True)
    last_updated = Column(UtcDateTime, nullable=False)
    dag_hash = Column(String(32), nullable=False)
    processor_subdir = Column(String(2000), nullable=True)

    __table_args__ = (Index("idx_fileloc_hash", fileloc_hash, unique=False),)

    dag_runs = relationship(
        DagRun,
        primaryjoin=dag_id == foreign(DagRun.dag_id),  # type: ignore
        backref=backref("serialized_dag", uselist=False, innerjoin=True),
    )

    dag_model = relationship(
        DagModel,
        primaryjoin=dag_id == DagModel.dag_id,  # type: ignore
        foreign_keys=dag_id,
        uselist=False,
        innerjoin=True,
        backref=backref("serialized_dag", uselist=False, innerjoin=True),
    )

    load_op_links = True

    def __init__(self, dag: DAG, processor_subdir: str | None = None) -> None:
        self.dag_id = dag.dag_id
        self.fileloc = dag.fileloc
        self.fileloc_hash = DagCode.dag_fileloc_hash(self.fileloc)
        self.last_updated = timezone.utcnow()
        self.processor_subdir = processor_subdir

        dag_data = SerializedDAG.to_dict(dag)
        dag_data_json = json.dumps(dag_data, sort_keys=True).encode("utf-8")

        self.dag_hash = md5(dag_data_json).hexdigest()

        if COMPRESS_SERIALIZED_DAGS:
            self._data = None
            self._data_compressed = zlib.compress(dag_data_json)
        else:
            self._data = dag_data
            self._data_compressed = None

        # serve as cache so no need to decompress and load, when accessing data field
        # when COMPRESS_SERIALIZED_DAGS is True
        self.__data_cache = dag_data

    def __repr__(self) -> str:
        return f"<SerializedDag: {self.dag_id}>"

    @classmethod
    @provide_session
    def write_dag(
        cls,
        dag: DAG,
        min_update_interval: int | None = None,
        processor_subdir: str | None = None,
        session: Session = NEW_SESSION,
    ) -> bool:
        """
        Serialize a DAG and writes it into database.

        If the record already exists, it checks if the Serialized DAG changed or not. If it is
        changed, it updates the record, ignores otherwise.

        :param dag: a DAG to be written into database
        :param min_update_interval: minimal interval in seconds to update serialized DAG
        :param session: ORM Session

        :returns: Boolean indicating if the DAG was written to the DB
        """
        # Checks if (Current Time - Time when the DAG was written to DB) < min_update_interval
        # If Yes, does nothing
        # If No or the DAG does not exists, updates / writes Serialized DAG to DB
        if min_update_interval is not None:
            if session.scalar(
                select(literal(True)).where(
                    cls.dag_id == dag.dag_id,
                    (timezone.utcnow() - timedelta(seconds=min_update_interval)) < cls.last_updated,
                )
            ):
                return False

        log.debug("Checking if DAG (%s) changed", dag.dag_id)
        new_serialized_dag = cls(dag, processor_subdir)
        serialized_dag_db = session.execute(
            select(cls.dag_hash, cls.processor_subdir).where(cls.dag_id == dag.dag_id)
        ).first()

        if (
            serialized_dag_db is not None
            and serialized_dag_db.dag_hash == new_serialized_dag.dag_hash
            and serialized_dag_db.processor_subdir == new_serialized_dag.processor_subdir
        ):
            log.debug("Serialized DAG (%s) is unchanged. Skipping writing to DB", dag.dag_id)
            return False

        log.debug("Writing Serialized DAG: %s to the DB", dag.dag_id)
        session.merge(new_serialized_dag)
        log.debug("DAG: %s written to the DB", dag.dag_id)
        return True

    @classmethod
    @provide_session
    def read_all_dags(cls, session: Session = NEW_SESSION) -> dict[str, SerializedDAG]:
        """
        Read all DAGs in serialized_dag table.

        :param session: ORM Session
        :returns: a dict of DAGs read from database
        """
        serialized_dags = session.scalars(select(cls))

        dags = {}
        for row in serialized_dags:
            log.debug("Deserializing DAG: %s", row.dag_id)
            dag = row.dag

            # Coherence check
            if dag.dag_id == row.dag_id:
                dags[row.dag_id] = dag
            else:
                log.warning(
                    "dag_id Mismatch in DB: Row with dag_id '%s' has Serialised DAG with '%s' dag_id",
                    row.dag_id,
                    dag.dag_id,
                )
        return dags

    @property
    def data(self) -> dict | None:
        # use __data_cache to avoid decompress and loads
        if not hasattr(self, "__data_cache") or self.__data_cache is None:
            if self._data_compressed:
                self.__data_cache = json.loads(zlib.decompress(self._data_compressed))
            else:
                self.__data_cache = self._data

        return self.__data_cache

    @property
    def dag(self) -> SerializedDAG:
        """The DAG deserialized from the ``data`` column."""
        SerializedDAG._load_operator_extra_links = self.load_op_links
        if isinstance(self.data, dict):
            data = self.data
        elif isinstance(self.data, str):
            data = json.loads(self.data)
        else:
            raise ValueError("invalid or missing serialized DAG data")
        return SerializedDAG.from_dict(data)

    @classmethod
    @provide_session
    def remove_dag(cls, dag_id: str, session: Session = NEW_SESSION) -> None:
        """
        Delete a DAG with given dag_id.

        :param dag_id: dag_id to be deleted
        :param session: ORM Session.
        """
        session.execute(cls.__table__.delete().where(cls.dag_id == dag_id))

    @classmethod
    @internal_api_call
    @provide_session
    def remove_deleted_dags(
        cls,
        alive_dag_filelocs: Collection[str],
        processor_subdir: str | None = None,
        session: Session = NEW_SESSION,
    ) -> None:
        """
        Delete DAGs not included in alive_dag_filelocs.

        :param alive_dag_filelocs: file paths of alive DAGs
        :param processor_subdir: dag processor subdir
        :param session: ORM Session
        """
        alive_fileloc_hashes = [DagCode.dag_fileloc_hash(fileloc) for fileloc in alive_dag_filelocs]

        log.debug(
            "Deleting Serialized DAGs (for which DAG files are deleted) from %s table ", cls.__tablename__
        )

        session.execute(
            cls.__table__.delete().where(
                and_(
                    cls.fileloc_hash.notin_(alive_fileloc_hashes),
                    cls.fileloc.notin_(alive_dag_filelocs),
                    or_(
                        cls.processor_subdir.is_(None),
                        cls.processor_subdir == processor_subdir,
                    ),
                )
            )
        )

    @classmethod
    @provide_session
    def has_dag(cls, dag_id: str, session: Session = NEW_SESSION) -> bool:
        """
        Check a DAG exist in serialized_dag table.

        :param dag_id: the DAG to check
        :param session: ORM Session
        """
        return session.scalar(select(literal(True)).where(cls.dag_id == dag_id).limit(1)) is not None

    @classmethod
    @provide_session
    def get_dag(cls, dag_id: str, session: Session = NEW_SESSION) -> SerializedDAG | None:
        row = cls.get(dag_id, session=session)
        if row:
            return row.dag
        return None

    @classmethod
    @provide_session
    def get(cls, dag_id: str, session: Session = NEW_SESSION) -> SerializedDagModel | None:
        """
        Get the SerializedDAG for the given dag ID.

        It will cope with being passed the ID of a subdag by looking up the root dag_id from the DAG table.

        :param dag_id: the DAG to fetch
        :param session: ORM Session
        """
        row = session.scalar(select(cls).where(cls.dag_id == dag_id))
        if row:
            return row

        # If we didn't find a matching DAG id then ask the DAG table to find
        # out the root dag
        root_dag_id = session.scalar(select(DagModel.root_dag_id).where(DagModel.dag_id == dag_id))

        return session.scalar(select(cls).where(cls.dag_id == root_dag_id))

    @staticmethod
    @provide_session
    def bulk_sync_to_db(
        dags: list[DAG],
        processor_subdir: str | None = None,
        session: Session = NEW_SESSION,
    ) -> None:
        """
        Save DAGs as Serialized DAG objects in the database.

        Each DAG is saved in a separate database query.

        :param dags: the DAG objects to save to the DB
        :param session: ORM Session
        :return: None
        """
        for dag in dags:
            if not dag.is_subdag:
                SerializedDagModel.write_dag(
                    dag=dag,
                    min_update_interval=MIN_SERIALIZED_DAG_UPDATE_INTERVAL,
                    processor_subdir=processor_subdir,
                    session=session,
                )

    @classmethod
    @provide_session
    def get_last_updated_datetime(cls, dag_id: str, session: Session = NEW_SESSION) -> datetime | None:
        """
        Get the date when the Serialized DAG associated to DAG was last updated in serialized_dag table.

        :param dag_id: DAG ID
        :param session: ORM Session
        """
        return session.scalar(select(cls.last_updated).where(cls.dag_id == dag_id))

    @classmethod
    @provide_session
    def get_max_last_updated_datetime(cls, session: Session = NEW_SESSION) -> datetime | None:
        """
        Get the maximum date when any DAG was last updated in serialized_dag table.

        :param session: ORM Session
        """
        return session.scalar(select(func.max(cls.last_updated)))

    @classmethod
    @provide_session
    def get_latest_version_hash(cls, dag_id: str, session: Session = NEW_SESSION) -> str | None:
        """
        Get the latest DAG version for a given DAG ID.

        :param dag_id: DAG ID
        :param session: ORM Session
        :return: DAG Hash, or None if the DAG is not found
        """
        return session.scalar(select(cls.dag_hash).where(cls.dag_id == dag_id))

    @classmethod
    def get_latest_version_hash_and_updated_datetime(
        cls,
        dag_id: str,
        *,
        session: Session,
    ) -> tuple[str, datetime] | None:
        """
        Get the latest version for a DAG ID and the date it was last updated in serialized_dag table.

        :meta private:
        :param dag_id: DAG ID
        :param session: ORM Session
        :return: A tuple of DAG Hash and last updated datetime, or None if the DAG is not found
        """
        return session.execute(
            select(cls.dag_hash, cls.last_updated).where(cls.dag_id == dag_id)
        ).one_or_none()

    @classmethod
    @provide_session
    def get_dag_dependencies(cls, session: Session = NEW_SESSION) -> dict[str, list[DagDependency]]:
        """
        Get the dependencies between DAGs.

        :param session: ORM Session
        """
        if session.bind.dialect.name in ["sqlite", "mysql"]:
            query = session.execute(
                select(cls.dag_id, func.json_extract(cls._data, "$.dag.dag_dependencies"))
            )
            iterator = ((dag_id, json.loads(deps_data) if deps_data else []) for dag_id, deps_data in query)
        else:
            iterator = session.execute(
                select(cls.dag_id, func.json_extract_path(cls._data, "dag", "dag_dependencies"))
            )
        return {dag_id: [DagDependency(**d) for d in (deps_data or [])] for dag_id, deps_data in iterator}

    @staticmethod
    @internal_api_call
    @provide_session
    def get_serialized_dag(dag_id: str, task_id: str, session: Session = NEW_SESSION) -> Operator | None:
        from airflow.models.serialized_dag import SerializedDagModel

        try:
            model = session.get(SerializedDagModel, dag_id)
            if model:
                return model.dag.get_task(task_id)
        except (exc.NoResultFound, TaskNotFound):
            return None

        return None
