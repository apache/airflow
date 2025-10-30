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

from typing import TYPE_CHECKING

import uuid6
from sqlalchemy import Column, ForeignKey, Index, String, Table, select
from sqlalchemy.orm import Mapped, relationship
from sqlalchemy_utils import UUIDType

from airflow.models.base import Base, StringID
from airflow.utils.session import NEW_SESSION, provide_session
from airflow.utils.sqlalchemy import mapped_column

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
    Column("team_id", UUIDType(binary=False), ForeignKey("team.id", ondelete="CASCADE"), primary_key=True),
    Index("idx_dag_bundle_team_dag_bundle_name", "dag_bundle_name", unique=True),
    Index("idx_dag_bundle_team_team_id", "team_id"),
)


class Team(Base):
    """
    Contains the list of teams defined in the environment.

    This table is only used when Airflow is run in multi-team mode.
    """

    __tablename__ = "team"

    id: Mapped[str] = mapped_column(UUIDType(binary=False), primary_key=True, default=uuid6.uuid7)
    name: Mapped[str] = mapped_column(String(50), unique=True, nullable=False)
    dag_bundles = relationship(
        "DagBundleModel", secondary=dag_bundle_team_association_table, back_populates="teams"
    )

    def __repr__(self):
        return f"Team(id={self.id},name={self.name})"

    @classmethod
    @provide_session
    def get_all_teams_id_to_name_mapping(cls, session: Session = NEW_SESSION) -> dict[str, str]:
        """
        Return a mapping of all team IDs to team names from the database.

        This method provides a reusable way to get team information that can be used
        across the codebase for validation and lookups.

        :param session: Database session
        :return: Dictionary mapping team UUIDs to team names
        """
        stmt = select(cls.id, cls.name)
        teams = session.execute(stmt).all()
        return {str(team_id): team_name for team_id, team_name in teams}

    @classmethod
    def get_all_team_names(cls) -> set[str]:
        """
        Return a set of all team names from the database.

        This method provides a convenient way to get just the team names for validation
        purposes, such as verifying team names in executor configurations.

        :return: Set of all team names
        """
        team_mapping = cls.get_all_teams_id_to_name_mapping()
        return set(team_mapping.values())
