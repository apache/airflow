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
from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar

from sqlalchemy import Column, ForeignKey, Index, String, Table, inspect as sa_inspect, select
from sqlalchemy.orm import Mapped, mapped_column, relationship

from airflow.configuration import conf
from airflow.models.base import Base, StringID
from airflow.utils.session import NEW_SESSION, provide_session

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

dag_bundle_team_association_table = Table(
    "dag_bundle_team",
    Base.metadata,
    Column(
        "dag_bundle_name",
        StringID(length=250),
        ForeignKey("dag_bundle.name", ondelete="CASCADE"),
        primary_key=True,
    ),
    Column("team_name", String(50), ForeignKey("team.name", ondelete="CASCADE"), primary_key=True),
    Index("idx_dag_bundle_team_dag_bundle_name", "dag_bundle_name", unique=True),
    Index("idx_dag_bundle_team_team_name", "team_name"),
)


class Team(Base):
    """
    Contains the list of teams defined in the environment.

    This table is only used when Airflow is run in multi-team mode.
    """

    __tablename__ = "team"

    name: Mapped[str] = mapped_column(String(50), primary_key=True)
    dag_bundles = relationship(
        "DagBundleModel", secondary=dag_bundle_team_association_table, back_populates="teams"
    )

    def __repr__(self):
        return f"Team(name={self.name})"

    @classmethod
    @provide_session
    def get_name_if_exists(cls, name: str, *, session: Session = NEW_SESSION) -> str | None:
        """Return name if a Team row with that name exists, otherwise None."""
        return session.scalar(select(cls.name).where(cls.name == name))

    @classmethod
    @provide_session
    def get_all_team_names(cls, *, session: Session = NEW_SESSION) -> set[str]:
        """
        Return a set of all team names from the database.

        This method provides a convenient way to get just the team names for validation
        purposes, such as verifying team names in executor configurations.

        :return: Set of all team names
        """
        return set(session.scalars(select(Team.name)).all())


class TeamOwnedMixin:
    """
    Exposes ``team_name`` on models that reach a team through a relationship path.

    Teams only exist in multi-team mode, so the config is checked before any hop is
    walked: single-team deployments answer ``None`` without loading a relationship, and
    endpoints there pay for no extra join.  In multi-team mode the value comes from the
    already-loaded relationships when the query applied
    :func:`~airflow.api_fastapi.common.db.dags.eager_load_teams`, and falls back to the
    per-Dag cached resolver otherwise — so the attribute stays correct on paths that
    cannot eager load (an in-memory Dag run, a callback re-fetching by primary key)
    instead of tripping the ``lazy="raise"`` guard that keeps N+1 loads out.
    """

    #: Attribute names to walk from ``self`` to the owning :class:`DagModel`.
    _team_path: ClassVar[tuple[str, ...]] = ()

    if TYPE_CHECKING:
        # Every model mixing this in carries ``dag_id`` (it is the fallback lookup key).
        dag_id: str

    @property
    def team_name(self) -> str | None:
        """Name of the team owning this entity, or ``None`` when it is not team-owned."""
        if not conf.getboolean("core", "multi_team"):
            return None

        from airflow.models.dag import DagModel

        entity: Any = self
        for attribute in (*self._team_path, "bundle", "teams"):
            state = sa_inspect(entity)
            if attribute in state.unloaded:
                # Reuse this entity's own session. ``get_team_name`` is ``@provide_session``,
                # and the session it would open is the *same* scoped session the caller is
                # using, so closing it on exit detaches every object the caller still holds.
                if state.session is not None:
                    return DagModel.get_team_name(self.dag_id, session=state.session)
                return DagModel.get_team_name(self.dag_id)
            if (entity := getattr(entity, attribute)) is None:
                return None
        # A bundle maps to at most one team (unique index on dag_bundle_team.dag_bundle_name).
        return entity[0].name if entity else None
