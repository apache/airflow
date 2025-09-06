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

import uuid6
from sqlalchemy import Column, ForeignKey, Index, String, Table
from sqlalchemy.orm import relationship
from sqlalchemy_utils import UUIDType

from airflow.models.base import Base

dag_bundle_team_association_table = Table(
    "dag_bundle_team",
    Base.metadata,
    Column("dag_bundle_name", ForeignKey("dag_bundle.name", ondelete="CASCADE"), primary_key=True),
    Column("team_id", ForeignKey("team.id", ondelete="CASCADE"), primary_key=True),
    Index("idx_dag_bundle_team_dag_bundle_name", "dag_bundle_name", unique=True),
    Index("idx_dag_bundle_team_team_id", "team_id"),
)


class Team(Base):
    """
    Contains the list of teams defined in the environment.

    This table is only used when Airflow is run in multi-team mode.
    """

    __tablename__ = "team"

    id = Column(UUIDType(binary=False), primary_key=True, default=uuid6.uuid7)
    name = Column(String(50), unique=True, nullable=False)
    dag_bundles = relationship(
        "DagBundleModel", secondary=dag_bundle_team_association_table, back_populates="teams"
    )

    def __repr__(self):
        return f"Team(id={self.id},name={self.name})"
