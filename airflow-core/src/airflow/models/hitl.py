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

from typing import TypedDict

import sqlalchemy_jsonfield
from sqlalchemy import Boolean, Column, ForeignKeyConstraint, String, Text, func
from sqlalchemy.dialects import postgresql
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import relationship

from airflow.models.base import Base
from airflow.settings import json
from airflow.utils.sqlalchemy import UtcDateTime


class HITLUser(TypedDict):
    """Typed dict for saving a Human-in-the-loop user information."""

    id: str
    name: str


class HITLDetail(Base):
    """Human-in-the-loop request and corresponding response."""

    __tablename__ = "hitl_detail"
    ti_id = Column(
        String(36).with_variant(postgresql.UUID(as_uuid=False), "postgresql"),
        primary_key=True,
        nullable=False,
    )

    # User Request Detail
    options = Column(sqlalchemy_jsonfield.JSONField(json=json), nullable=False)
    subject = Column(Text, nullable=False)
    body = Column(Text, nullable=True)
    defaults = Column(sqlalchemy_jsonfield.JSONField(json=json), nullable=True)
    multiple = Column(Boolean, unique=False, default=False)
    params = Column(sqlalchemy_jsonfield.JSONField(json=json), nullable=False, default={})
    assignees = Column(sqlalchemy_jsonfield.JSONField(json=json), nullable=True)

    # Response Content Detail
    response_at = Column(UtcDateTime, nullable=True)
    responded_by = Column(sqlalchemy_jsonfield.JSONField(json=json), nullable=True)
    chosen_options = Column(
        sqlalchemy_jsonfield.JSONField(json=json),
        nullable=True,
        default=None,
    )
    params_input = Column(sqlalchemy_jsonfield.JSONField(json=json), nullable=False, default={})
    task_instance = relationship(
        "TaskInstance",
        lazy="joined",
        back_populates="hitl_detail",
    )

    __table_args__ = (
        ForeignKeyConstraint(
            (ti_id,),
            ["task_instance.id"],
            name="hitl_detail_ti_fkey",
            ondelete="CASCADE",
            onupdate="CASCADE",
        ),
    )

    @hybrid_property
    def response_received(self) -> bool:
        return self.response_at is not None

    @response_received.expression  # type: ignore[no-redef]
    def response_received(cls):
        return cls.response_at.is_not(None)

    @hybrid_property
    def responded_by_user_id(self):
        return (self.responded_by or {}).get("id")

    @responded_by_user_id.expression  # type: ignore[no-redef]
    def responded_by_user_id(cls):
        return func.json_extract(cls.responded_by, "$.id")

    @hybrid_property
    def responded_by_user_name(self):
        return (self.responded_by or {}).get("name")

    @responded_by_user_name.expression  # type: ignore[no-redef]
    def responded_by_user_name(cls):
        return func.json_extract(cls.responded_by, "$.name")

    @property
    def assigned_users(self) -> list[HITLUser]:
        if not self.assignees:
            return []
        return [HITLUser(id=row["id"], name=row["name"]) for row in self.assignees]

    @property
    def responded_by_user(self) -> HITLUser | None:
        if self.responded_by is None:
            return None
        return HITLUser(
            id=self.responded_by["id"],
            name=self.responded_by["name"],
        )

    DEFAULT_USER = HITLUser(
        id="Fallback to defaults",
        name="Fallback to defaults",
    )
