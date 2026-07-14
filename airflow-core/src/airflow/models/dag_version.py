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

import hashlib
import logging
from datetime import datetime
from typing import TYPE_CHECKING, Any, Literal, get_args
from uuid import UUID

import sqlalchemy as sa
import uuid6
from sqlalchemy import ForeignKey, Integer, UniqueConstraint, select
from sqlalchemy.orm import Mapped, joinedload, mapped_column, relationship

from airflow._shared.timezones import timezone
from airflow.dag_processing.bundles.manager import DagBundlesManager
from airflow.exceptions import DagVersionNotFound
from airflow.models.base import Base, StringID
from airflow.utils.session import NEW_SESSION, provide_session
from airflow.utils.sqlalchemy import UtcDateTime, with_row_locks

if TYPE_CHECKING:
    from sqlalchemy.orm import Session
    from sqlalchemy.sql import Select


log = logging.getLogger(__name__)

SourceStatus = Literal["current_stored_code", "redacted", "unavailable"]
ValuesStatus = Literal["available", "unavailable"]
_VALID_SOURCE_STATUSES = get_args(SourceStatus)
_VALID_VALUES_STATUSES = get_args(ValuesStatus)


class DagVersion(Base):
    """Model to track the versions of DAGs in the database."""

    __tablename__ = "dag_version"
    id: Mapped[UUID] = mapped_column(sa.Uuid(), primary_key=True, default=uuid6.uuid7)
    version_number: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    dag_id: Mapped[str] = mapped_column(
        StringID(), ForeignKey("dag.dag_id", ondelete="CASCADE"), nullable=False
    )
    dag_model = relationship("DagModel", back_populates="dag_versions")
    bundle_name: Mapped[str | None] = mapped_column(StringID(), nullable=True)
    bundle_version: Mapped[str | None] = mapped_column(StringID(), nullable=True)
    version_data: Mapped[dict | None] = mapped_column(sa.JSON(), nullable=True)
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
    created_at: Mapped[datetime] = mapped_column(UtcDateTime, nullable=False, default=timezone.utcnow)
    last_updated: Mapped[datetime] = mapped_column(
        UtcDateTime, nullable=False, default=timezone.utcnow, onupdate=timezone.utcnow
    )

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
        if self.bundle_name is None:
            return None
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
        version_data: dict | None = None,
        version_number: int = 1,
        session: Session = NEW_SESSION,
    ) -> DagVersion:
        """
        Write a new DagVersion into database.

        Checks if a version of the DAG exists and increments the version number if it does.

        :param dag_id: The DAG ID.
        :param bundle_name: The bundle name.
        :param bundle_version: The bundle version string.
        :param version_data: Optional structured data associated with this version (e.g., S3 manifest).
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
            version_data=version_data,
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
        load_serialized_dag: bool = False,
    ) -> Select:
        """
        Get the select object to get the latest version of the DAG.

        :param dag_id: The DAG ID.
        :return: The select object.
        """
        query = select(cls).where(cls.dag_id == dag_id)
        if bundle_version is not None:
            query = query.where(cls.bundle_version == bundle_version)

        if load_dag_model:
            query = query.options(joinedload(cls.dag_model))

        if load_bundle_model:
            query = query.options(joinedload(cls.bundle))

        if load_serialized_dag:
            query = query.options(joinedload(cls.serialized_dag))

        # Order by version_number, not created_at: version_number is monotonic and unique per
        # dag_id, so it is deterministic even when two versions share a created_at timestamp.
        # write_dag relies on this select to compute the next version_number; ordering by
        # created_at could pick a non-max row under a tie and collide with the
        # (dag_id, version_number) unique constraint.
        query = query.order_by(cls.version_number.desc()).limit(1)
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
        load_serialized_dag: bool = False,
        session: Session = NEW_SESSION,
    ) -> DagVersion | None:
        """
        Get the latest version of the DAG.

        :param dag_id: The DAG ID.
        :param session: The database session.
        :param load_dag_model: Whether to load the DAG model.
        :param load_bundle_model: Whether to load the DagBundle model.
        :param load_serialized_dag: Whether to eagerly load the serialized DAG.
        :return: The latest version of the DAG or None if not found.
        """
        return session.scalar(
            cls._latest_version_select(
                dag_id,
                bundle_version=bundle_version,
                load_dag_model=load_dag_model,
                load_bundle_model=load_bundle_model,
                load_serialized_dag=load_serialized_dag,
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

        return session.scalar(version_select_obj.order_by(cls.version_number.desc()).limit(1))

    @property
    def version(self) -> str:
        """A human-friendly representation of the version."""
        return f"{self.dag_id}-{self.version_number}"

    @classmethod
    @provide_session
    def get_diff(
        cls,
        dag_id: str,
        base_version_number: int,
        target_version_number: int,
        *,
        include_values: bool = False,
        include_source: bool = False,
        max_changes: int | None = None,
        source_status: SourceStatus | None = None,
        values_status: ValuesStatus | None = None,
        session: Session = NEW_SESSION,
    ) -> dict[str, Any]:
        """
        Compare two versions of a Dag using their currently stored state.

        ``source_status`` is supplied by callers that have an authorization context.  The CLI has
        operator-level authority and leaves it unset, while API callers calculate it before this
        method returns any source content. API callers use ``values_status`` to suppress raw values
        when the user cannot access Dag code; the structural diff remains available in redacted form.
        """
        # Keep this local to avoid the dag_version -> dag_version_diff -> serialized_objects cycle.
        from airflow.serialization.dag_version_diff import (
            DEFAULT_MAX_CHANGES,
            build_serialized_dag_diff,
        )

        if source_status is not None and source_status not in _VALID_SOURCE_STATUSES:
            raise ValueError(f"source_status must be one of {_VALID_SOURCE_STATUSES}, not {source_status!r}")
        if values_status is not None and values_status not in _VALID_VALUES_STATUSES:
            raise ValueError(f"values_status must be one of {_VALID_VALUES_STATUSES}, not {values_status!r}")

        if max_changes is None:
            max_changes = DEFAULT_MAX_CHANGES
        if base_version_number < 1 or target_version_number < 1:
            raise ValueError("Dag version numbers must be positive integers")

        if source_status is None:
            source_status = "current_stored_code" if include_source else "unavailable"

        query = (
            select(cls)
            .where(
                cls.dag_id == dag_id,
                cls.version_number.in_((base_version_number, target_version_number)),
            )
            .options(joinedload(cls.serialized_dag))
        )
        if include_source and source_status == "current_stored_code":
            query = query.options(joinedload(cls.dag_code))

        versions = {version.version_number: version for version in session.scalars(query).all()}
        missing_version = next(
            (
                version_number
                for version_number in (base_version_number, target_version_number)
                if version_number not in versions
            ),
            None,
        )
        if missing_version is not None:
            raise DagVersionNotFound(
                f"The DagVersion with dag_id: `{dag_id}` and version_number: `{missing_version}` was not found"
            )

        base_version = versions[base_version_number]
        target_version = versions[target_version_number]
        effective_include_values = include_values and values_status in {None, "available"}
        result = build_serialized_dag_diff(
            base_data=base_version.serialized_dag.data if base_version.serialized_dag else None,
            target_data=target_version.serialized_dag.data if target_version.serialized_dag else None,
            base_provenance=_get_provenance(base_version),
            target_provenance=_get_provenance(target_version),
            include_values=effective_include_values,
            max_changes=max_changes,
        )
        if include_values:
            values_available = effective_include_values and result["mode"] == "observed_state"
            result["values"] = {"status": "available" if values_available else "unavailable"}
        result["source"] = _get_source_diff(
            base_version,
            target_version,
            include_source=include_source,
            source_status=source_status,
        )
        return result


def _get_provenance(version: DagVersion) -> dict[str, Any]:
    return {
        "bundle_name": version.bundle_name,
        "bundle_version": version.bundle_version,
        "version_data": version.version_data,
    }


def _get_source_diff(
    base_version: DagVersion,
    target_version: DagVersion,
    *,
    include_source: bool,
    source_status: SourceStatus,
) -> dict[str, Any]:
    if not include_source:
        return {"status": "unavailable", "fidelity": "unavailable"}
    if source_status != "current_stored_code":
        return {"status": source_status, "fidelity": source_status}

    base_code = base_version.dag_code
    target_code = target_version.dag_code
    if base_code is None or target_code is None:
        return {"status": "unavailable", "fidelity": "unavailable"}

    base_source = base_code.source_code
    target_source = target_code.source_code
    return {
        "status": "current_stored_code",
        "fidelity": "current_stored_code",
        "changed": base_source != target_source,
        "base": {"digest": _get_source_digest(base_source), "content": base_source},
        "target": {"digest": _get_source_digest(target_source), "content": target_source},
    }


def _get_source_digest(source: str) -> str:
    return f"sha256:{hashlib.sha256(source.encode()).hexdigest()}"


def _resolve_version_data(
    dag_version: DagVersion | None, bundle_version: str | None
) -> dict[str, Any] | None:
    """Return a bundle version's ``version_data`` manifest, but only for pinned runs."""
    # Expose version_data only when the run is pinned (bundle_version set) and a DagVersion is
    # present, so the bundle initializes against the exact version the run used. Unpinned runs
    # follow the latest bundle state, and legacy rows have no DagVersion.
    if dag_version is not None and bundle_version is not None:
        return dag_version.version_data
    return None
