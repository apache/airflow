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
from typing import TYPE_CHECKING, Any

import sqlalchemy_jsonfield
import uuid6
from sqlalchemy import Column, ForeignKey, LargeBinary, String, exc, select
from sqlalchemy.orm import backref, foreign, relationship
from sqlalchemy.sql.expression import func, literal
from sqlalchemy_utils import UUIDType

from airflow.exceptions import TaskNotFound
from airflow.models.base import ID_LEN, Base
from airflow.models.dag import DagModel
from airflow.models.dag_version import DagVersion
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
    from airflow.serialization.serialized_objects import LazyDeserializedDAG

log = logging.getLogger(__name__)


class SerializedDagModel(Base):
    """
    A table for serialized DAGs.

    serialized_dag table is a snapshot of DAG files synchronized by scheduler.
    This feature is controlled by:

    * ``[core] min_serialized_dag_update_interval = 30`` (s):
      serialized DAGs are updated in DB when a file gets processed by scheduler,
      to reduce DB write rate, there is a minimal interval of updating serialized DAGs.
    * ``[dag_processor] refresh_interval = 300`` (s):
      interval of deleting serialized DAGs in DB when the files are deleted, suggest
      to use a smaller interval such as 60
    * ``[core] compress_serialized_dags``:
      whether compressing the dag data to the Database.

    It is used by webserver to load dags
    because reading from database is lightweight compared to importing from files,
    it solves the webserver scalability issue.
    """

    __tablename__ = "serialized_dag"
    id = Column(UUIDType(binary=False), primary_key=True, default=uuid6.uuid7)
    dag_id = Column(String(ID_LEN), nullable=False)
    _data = Column("data", sqlalchemy_jsonfield.JSONField(json=json), nullable=True)
    _data_compressed = Column("data_compressed", LargeBinary, nullable=True)
    created_at = Column(UtcDateTime, nullable=False, default=timezone.utcnow)
    last_updated = Column(UtcDateTime, nullable=False, default=timezone.utcnow, onupdate=timezone.utcnow)
    dag_hash = Column(String(32), nullable=False)

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
    dag_version_id = Column(
        UUIDType(binary=False), ForeignKey("dag_version.id", ondelete="CASCADE"), nullable=False, unique=True
    )
    dag_version = relationship("DagVersion", back_populates="serialized_dag")

    load_op_links = True

    def __init__(self, dag: DAG | LazyDeserializedDAG) -> None:
        from airflow.models.dag import DAG

        self.dag_id = dag.dag_id
        dag_data = {}
        if isinstance(dag, DAG):
            dag_data = SerializedDAG.to_dict(dag)
        else:
            dag_data = dag.data

        self.dag_hash = SerializedDagModel.hash(dag_data)

        # partially ordered json data
        dag_data_json = json.dumps(dag_data, sort_keys=True).encode("utf-8")

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
    def hash(cls, dag_data):
        """Hash the data to get the dag_hash."""
        dag_data = cls._sort_serialized_dag_dict(dag_data)
        data_json = json.dumps(dag_data, sort_keys=True).encode("utf-8")
        return md5(data_json).hexdigest()

    @classmethod
    def _sort_serialized_dag_dict(cls, serialized_dag: Any):
        """Recursively sort json_dict and its nested dictionaries and lists."""
        if isinstance(serialized_dag, dict):
            return {k: cls._sort_serialized_dag_dict(v) for k, v in sorted(serialized_dag.items())}
        elif isinstance(serialized_dag, list):
            if all(isinstance(i, dict) for i in serialized_dag):
                if all("task_id" in i.get("__var", {}) for i in serialized_dag):
                    return sorted(
                        [cls._sort_serialized_dag_dict(i) for i in serialized_dag],
                        key=lambda x: x["__var"]["task_id"],
                    )
            elif all(isinstance(item, str) for item in serialized_dag):
                return sorted(serialized_dag)
            return [cls._sort_serialized_dag_dict(i) for i in serialized_dag]
        return serialized_dag

    @classmethod
    @provide_session
    def write_dag(
        cls,
        dag: DAG | LazyDeserializedDAG,
        bundle_name: str,
        bundle_version: str | None = None,
        min_update_interval: int | None = None,
        session: Session = NEW_SESSION,
    ) -> bool:
        """
        Serialize a DAG and writes it into database.

        If the record already exists, it checks if the Serialized DAG changed or not. If it is
        changed, it updates the record, ignores otherwise.

        :param dag: a DAG to be written into database
        :param bundle_name: bundle name of the DAG
        :param bundle_version: bundle version of the DAG
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
                    (timezone.utcnow() - timedelta(seconds=min_update_interval)) < cls.created_at,
                )
            ):
                return False

        log.debug("Checking if DAG (%s) changed", dag.dag_id)
        new_serialized_dag = cls(dag)
        serialized_dag_hash = session.scalars(
            select(cls.dag_hash).where(cls.dag_id == dag.dag_id).order_by(cls.created_at.desc())
        ).first()

        if serialized_dag_hash is not None and serialized_dag_hash == new_serialized_dag.dag_hash:
            log.debug("Serialized DAG (%s) is unchanged. Skipping writing to DB", dag.dag_id)
            return False
        dag_version = DagVersion.get_latest_version(dag.dag_id, session=session)
        if dag_version and not dag_version.task_instances:
            # This is for dynamic DAGs that the hashes changes often. We should update
            # the serialized dag, the dag_version and the dag_code instead of a new version
            # if the dag_version is not associated with any task instances
            latest_ser_dag = cls.get(dag.dag_id, session=session)
            if TYPE_CHECKING:
                assert latest_ser_dag is not None
            # Update the serialized DAG with the new_serialized_dag
            latest_ser_dag._data = new_serialized_dag._data
            latest_ser_dag._data_compressed = new_serialized_dag._data_compressed
            latest_ser_dag.dag_hash = new_serialized_dag.dag_hash
            session.merge(latest_ser_dag)
            # The dag_version and dag_code may not have changed, still we should
            # do the below actions:
            # Update the latest dag version
            dag_version.bundle_name = bundle_name
            dag_version.bundle_version = bundle_version
            session.merge(dag_version)
            # Update the latest DagCode
            DagCode.update_source_code(dag_id=dag.dag_id, fileloc=dag.fileloc, session=session)
            return True

        dagv = DagVersion.write_dag(
            dag_id=dag.dag_id,
            bundle_name=bundle_name,
            bundle_version=bundle_version,
            session=session,
        )
        log.debug("Writing Serialized DAG: %s to the DB", dag.dag_id)
        new_serialized_dag.dag_version = dagv
        session.add(new_serialized_dag)
        log.debug("DAG: %s written to the DB", dag.dag_id)
        DagCode.write_code(dagv, dag.fileloc, session=session)
        return True

    @classmethod
    def latest_item_select_object(cls, dag_id):
        return select(cls).where(cls.dag_id == dag_id).order_by(cls.created_at.desc()).limit(1)

    @classmethod
    @provide_session
    def get_latest_serialized_dags(
        cls, *, dag_ids: list[str], session: Session = NEW_SESSION
    ) -> list[SerializedDagModel]:
        """
        Get the latest serialized dags of given DAGs.

        :param dag_ids: The list of DAG IDs.
        :param session: The database session.
        :return: The latest serialized dag of the DAGs.
        """
        # Subquery to get the latest serdag per dag_id
        latest_serdag_subquery = (
            session.query(cls.dag_id, func.max(cls.created_at).label("created_at"))
            .filter(cls.dag_id.in_(dag_ids))
            .group_by(cls.dag_id)
            .subquery()
        )
        latest_serdags = session.scalars(
            select(cls)
            .join(
                latest_serdag_subquery,
                cls.created_at == latest_serdag_subquery.c.created_at,
            )
            .where(cls.dag_id.in_(dag_ids))
        ).all()
        return latest_serdags or []

    @classmethod
    @provide_session
    def read_all_dags(cls, session: Session = NEW_SESSION) -> dict[str, SerializedDAG]:
        """
        Read all DAGs in serialized_dag table.

        :param session: ORM Session
        :returns: a dict of DAGs read from database
        """
        latest_serialized_dag_subquery = (
            session.query(cls.dag_id, func.max(cls.created_at).label("max_created"))
            .group_by(cls.dag_id)
            .subquery()
        )
        serialized_dags = session.scalars(
            select(cls).join(
                latest_serialized_dag_subquery,
                (cls.dag_id == latest_serialized_dag_subquery.c.dag_id)
                and (cls.created_at == latest_serialized_dag_subquery.c.max_created),
            )
        )

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
        if not hasattr(self, "_SerializedDagModel__data_cache") or self.__data_cache is None:
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

        :param dag_id: the DAG to fetch
        :param session: ORM Session
        """
        return session.scalar(cls.latest_item_select_object(dag_id))

    @staticmethod
    @provide_session
    def bulk_sync_to_db(
        dags: list[DAG] | list[LazyDeserializedDAG],
        bundle_name: str,
        bundle_version: str | None = None,
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
            SerializedDagModel.write_dag(
                dag=dag,
                bundle_name=bundle_name,
                bundle_version=bundle_version,
                min_update_interval=MIN_SERIALIZED_DAG_UPDATE_INTERVAL,
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
        return session.scalar(
            select(cls.created_at).where(cls.dag_id == dag_id).order_by(cls.created_at.desc()).limit(1)
        )

    @classmethod
    @provide_session
    def get_max_last_updated_datetime(cls, session: Session = NEW_SESSION) -> datetime | None:
        """
        Get the maximum date when any DAG was last updated in serialized_dag table.

        :param session: ORM Session
        """
        return session.scalar(select(func.max(cls.created_at)))

    @classmethod
    @provide_session
    def get_latest_version_hash(cls, dag_id: str, session: Session = NEW_SESSION) -> str | None:
        """
        Get the latest DAG version for a given DAG ID.

        :param dag_id: DAG ID
        :param session: ORM Session
        :return: DAG Hash, or None if the DAG is not found
        """
        return session.scalar(
            select(cls.dag_hash).where(cls.dag_id == dag_id).order_by(cls.created_at.desc()).limit(1)
        )

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
            select(cls.dag_hash, cls.created_at)
            .where(cls.dag_id == dag_id)
            .order_by(cls.created_at.desc())
            .limit(1)
        ).one_or_none()

    @classmethod
    @provide_session
    def get_dag_dependencies(cls, session: Session = NEW_SESSION) -> dict[str, list[DagDependency]]:
        """
        Get the dependencies between DAGs.

        :param session: ORM Session
        """
        latest_sdag_subquery = (
            session.query(cls.dag_id, func.max(cls.created_at).label("max_created"))
            .group_by(cls.dag_id)
            .subquery()
        )
        if session.bind.dialect.name in ["sqlite", "mysql"]:
            query = session.execute(
                select(cls.dag_id, func.json_extract(cls._data, "$.dag.dag_dependencies")).join(
                    latest_sdag_subquery,
                    (cls.dag_id == latest_sdag_subquery.c.dag_id)
                    and (cls.created_at == latest_sdag_subquery.c.max_created),
                )
            )
            iterator = ((dag_id, json.loads(deps_data) if deps_data else []) for dag_id, deps_data in query)
        else:
            iterator = session.execute(
                select(cls.dag_id, func.json_extract_path(cls._data, "dag", "dag_dependencies")).join(
                    latest_sdag_subquery,
                    (cls.dag_id == latest_sdag_subquery.c.dag_id)
                    and (cls.created_at == latest_sdag_subquery.c.max_created),
                )
            )
        return {dag_id: [DagDependency(**d) for d in (deps_data or [])] for dag_id, deps_data in iterator}

    @staticmethod
    @provide_session
    def get_serialized_dag(dag_id: str, task_id: str, session: Session = NEW_SESSION) -> Operator | None:
        try:
            # get the latest version of the DAG
            model = session.scalar(SerializedDagModel.latest_item_select_object(dag_id))
            if model:
                return model.dag.get_task(task_id)
        except (exc.NoResultFound, TaskNotFound):
            return None

        return None
