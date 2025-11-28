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
from sqlalchemy import Column, ForeignKey, Integer, UniqueConstraint, select
from sqlalchemy.orm import joinedload, relationship
from sqlalchemy_utils import UUIDType

from airflow._shared.timezones import timezone
from airflow.dag_processing.bundles.manager import DagBundlesManager
from airflow.models.base import Base, StringID
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
    dag_id = Column(StringID(), ForeignKey("dag.dag_id", ondelete="CASCADE"), nullable=False)
    dag_model = relationship("DagModel", back_populates="dag_versions")
    bundle_name = Column(StringID(), nullable=True)
    bundle_version = Column(StringID())
    bundle = relationship(
        "DagBundleModel",
        primaryjoin="foreign(DagVersion.bundle_name) == DagBundleModel.name",
        uselist=False,
        viewonly=True,
    )
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
    task_instances = relationship("TaskInstance", back_populates="dag_version")
    created_at = Column(UtcDateTime, nullable=False, default=timezone.utcnow)
    last_updated = Column(UtcDateTime, nullable=False, default=timezone.utcnow, onupdate=timezone.utcnow)

    __table_args__ = (
        UniqueConstraint("dag_id", "version_number", name="dag_id_v_name_v_number_unique_constraint"),
    )

    def __repr__(self):
        """Represent the object as a string."""
        return f"<DagVersion {self.dag_id} {self.version}>"

    @property
    def bundle_url(self) -> str | None:
        """Render the bundle URL using the joined bundle metadata if available."""
        # Prefer using the joined bundle relationship when present to avoid extra queries
        if getattr(self, "bundle", None) is not None and hasattr(self.bundle, "signed_url_template"):
            return self.bundle.render_url(self.bundle_version)

        # fallback to the deprecated option if the bundle model does not have a signed_url_template
        # attribute
        try:
            return DagBundlesManager().view_url(self.bundle_name, self.bundle_version)
        except ValueError:
            return None

    @classmethod
    @provide_session
    def write_dag(
        cls,
        *,
        dag_id: str,
        bundle_name: str,
        bundle_version: str | None = None,
        version_number: int = 1,
        session: Session = NEW_SESSION,
    ) -> DagVersion:
        """
        Write a new DagVersion into database.

        Checks if a version of the DAG exists and increments the version number if it does.

        :param dag_id: The DAG ID.
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
            bundle_name=bundle_name,
            bundle_version=bundle_version,
        )
        log.debug("Writing DagVersion %s to the DB", dag_version)
        session.add(dag_version)
        log.debug("DagVersion %s written to the DB", dag_version)
        return dag_version

    @classmethod
    def _latest_version_select(
        cls,
        dag_id: str,
        bundle_version: str | None = None,
        load_dag_model: bool = False,
        load_bundle_model: bool = False,
    ) -> Select:
        """
        Get the select object to get the latest version of the DAG.

        :param dag_id: The DAG ID.
        :return: The select object.
        """
        query = select(cls).where(cls.dag_id == dag_id)
        if bundle_version:
            query = query.where(cls.bundle_version == bundle_version)

        if load_dag_model:
            query = query.options(joinedload(cls.dag_model))

        if load_bundle_model:
            query = query.options(joinedload(cls.bundle))

        query = query.order_by(cls.created_at.desc()).limit(1)
        return query

    @classmethod
    @provide_session
    def get_latest_version(
        cls,
        dag_id: str,
        *,
        bundle_version: str | None = None,
        load_dag_model: bool = False,
        load_bundle_model: bool = False,
        session: Session = NEW_SESSION,
    ) -> DagVersion | None:
        """
        Get the latest version of the DAG.

        :param dag_id: The DAG ID.
        :param session: The database session.
        :param load_dag_model: Whether to load the DAG model.
        :param load_bundle_model: Whether to load the DagBundle model.
        :return: The latest version of the DAG or None if not found.
        """
        return session.scalar(
            cls._latest_version_select(
                dag_id,
                bundle_version=bundle_version,
                load_dag_model=load_dag_model,
                load_bundle_model=load_bundle_model,
            )
        )

    @classmethod
    @provide_session
    def get_version(
        cls,
        dag_id: str,
        version_number: int | None = None,
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
        version_select_obj = select(cls).where(cls.dag_id == dag_id)
        if version_number:
            version_select_obj = version_select_obj.where(cls.version_number == version_number)

        return session.scalar(version_select_obj.order_by(cls.id.desc()).limit(1))

    @property
    def version(self) -> str:
        """A human-friendly representation of the version."""
        return f"{self.dag_id}-{self.version_number}"
