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

import sqlalchemy_jsonfield
from sqlalchemy import Boolean, Column, ForeignKeyConstraint, String, Text
from sqlalchemy.dialects import postgresql
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import relationship

from airflow.models.base import Base
from airflow.settings import json
from airflow.utils.sqlalchemy import UtcDateTime


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
    respondents = Column(sqlalchemy_jsonfield.JSONField(json=json), nullable=True)

    # Response Content Detail
    response_at = Column(UtcDateTime, nullable=True)
    responded_user_id = Column(String(128), nullable=True)
    responded_user_name = Column(String(128), nullable=True)
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

    DEFAULT_USER_NAME = "Fallback to defaults"
