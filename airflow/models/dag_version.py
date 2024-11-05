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

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import uuid6
from sqlalchemy import Column, ForeignKey, Integer, UniqueConstraint, func, select
from sqlalchemy.orm import relationship
from sqlalchemy_utils import UUIDType

from airflow.models.base import Base, StringID
from airflow.utils import timezone
from airflow.utils.session import NEW_SESSION, provide_session
from airflow.utils.sqlalchemy import UtcDateTime, with_row_locks

if TYPE_CHECKING:
    from sqlalchemy.orm import Session
    from sqlalchemy.sql import Select

log = logging.getLogger(__name__)


class DagVersion(Base):
    """Model to track the versions of DAGs in the database."""

    __tablename__ = "dag_version"
    id = Column(UUIDType(binary=False), primary_key=True, default=uuid6.uuid7)
    version_number = Column(Integer, nullable=False, default=1)
    version_name = Column(StringID())
    dag_id = Column(StringID(), ForeignKey("dag.dag_id", ondelete="CASCADE"), nullable=False)
    dag_model = relationship("DagModel", back_populates="dag_versions")
    dag_code = relationship(
        "DagCode",
        back_populates="dag_version",
        uselist=False,
        cascade="all, delete, delete-orphan",
        cascade_backrefs=False,
    )
    serialized_dag = relationship(
        "SerializedDagModel",
        back_populates="dag_version",
        uselist=False,
        cascade="all, delete, delete-orphan",
        cascade_backrefs=False,
    )
    dag_runs = relationship("DagRun", back_populates="dag_version", cascade="all, delete, delete-orphan")
    task_instances = relationship("TaskInstance", back_populates="dag_version")
    created_at = Column(UtcDateTime, default=timezone.utcnow)

    __table_args__ = (
        UniqueConstraint("dag_id", "version_number", name="dag_id_v_name_v_number_unique_constraint"),
    )

    def __repr__(self):
        """Represent the object as a string."""
        return f"<DagVersion {self.dag_id} {self.version}>"

    @classmethod
    @provide_session
    def write_dag(
        cls,
        *,
        dag_id: str,
        version_name: str | None = None,
        version_number: int = 1,
        session: Session = NEW_SESSION,
    ) -> DagVersion:
        """
        Write a new DagVersion into database.

        Checks if a version of the DAG exists and increments the version number if it does.

        :param dag_id: The DAG ID.
        :param version_name: The version name.
        :param version_number: The version number.
        :param session: The database session.
        :return: The DagVersion object.
        """
        existing_dag_version = session.scalar(
            with_row_locks(cls._latest_version_select(dag_id), of=DagVersion, session=session, nowait=True)
        )
        if existing_dag_version:
            version_number = existing_dag_version.version_number + 1

        dag_version = DagVersion(
            dag_id=dag_id,
            version_number=version_number,
            version_name=version_name,
        )
        log.debug("Writing DagVersion %s to the DB", dag_version)
        session.add(dag_version)
        # Flush is necessary here due to the unique constraint and other linked tables
        session.flush()
        log.debug("DagVersion %s written to the DB", dag_version)
        return dag_version

    @classmethod
    def _latest_version_select(cls, dag_id: str) -> Select:
        """
        Get the select object to get the latest version of the DAG.

        :param dag_id: The DAG ID.
        :return: The select object.
        """
        return select(cls).where(cls.dag_id == dag_id).order_by(cls.created_at.desc()).limit(1)

    @classmethod
    @provide_session
    def get_latest_version(cls, dag_id: str, *, session: Session = NEW_SESSION) -> DagVersion | None:
        """
        Get the latest version of the DAG.

        :param dag_id: The DAG ID.
        :param session: The database session.
        :return: The latest version of the DAG or None if not found.
        """
        return session.scalar(cls._latest_version_select(dag_id))

    @classmethod
    @provide_session
    def get_version(
        cls,
        dag_id: str,
        version_number: int = 1,
        *,
        session: Session = NEW_SESSION,
    ) -> DagVersion | None:
        """
        Get the version of the DAG.

        :param dag_id: The DAG ID.
        :param version_number: The version number.
        :param session: The database session.
        :return: The version of the DAG or None if not found.
        """
        version_select_obj = (
            select(cls)
            .where(cls.dag_id == dag_id, cls.version_number == version_number)
            .order_by(cls.version_number.desc())
            .limit(1)
        )
        return session.scalar(version_select_obj)

    @property
    def version(self) -> str:
        """A human-friendly representation of the version."""
        name = f"{self.version_number}"
        if self.version_name:
            name = f"{self.version_name}-{self.version_number}"
        return name

    @classmethod
    @provide_session
    def get_latest_dag_versions(
        cls, *, dag_ids: list[str], session: Session = NEW_SESSION
    ) -> list[DagVersion]:
        """
        Get the latest version of DAGs.

        :param dag_ids: The list of DAG IDs.
        :param session: The database session.
        :return: The latest version of the DAGs.
        """
        # Subquery to get the latest version number per dag_id
        latest_version_subquery = (
            session.query(cls.dag_id, func.max(cls.created_at).label("created_at"))
            .filter(cls.dag_id.in_(dag_ids))
            .group_by(cls.dag_id)
            .subquery()
        )
        latest_versions = session.scalars(
            select(cls)
            .join(
                latest_version_subquery,
                cls.created_at == latest_version_subquery.c.created_at,
            )
            .where(cls.dag_id.in_(dag_ids))
        ).all()
        return latest_versions or []
