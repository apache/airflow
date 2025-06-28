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

from typing import TYPE_CHECKING, Any

import sqlalchemy_jsonfield
from sqlalchemy import Boolean, Column, ForeignKey, ForeignKeyConstraint, Integer, MetaData, String, Text
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import registry, relationship

from airflow.models.base import Base, _get_schema, naming_convention
from airflow.models.taskinstance import TaskInstance
from airflow.settings import json
from airflow.utils import timezone
from airflow.utils.sqlalchemy import UtcDateTime

metadata = MetaData(schema=_get_schema(), naming_convention=naming_convention)
mapper_registry = registry(metadata=metadata)

if TYPE_CHECKING:
    Base = Any  # type: ignore[misc]
else:
    Base = mapper_registry.generate_base()


class HITLInputRequestModel(Base):
    """Human-in-the-loop input request."""

    __tablename__ = "hitl_input_request"
    id = Column(Integer, primary_key=True, autoincrement=True)
    options = Column(sqlalchemy_jsonfield.JSONField(json=json), nullable=False, default=[])
    subject = Column(Text, nullable=False)
    body = Column(Text, nullable=True)
    # Allow multiple defaults
    default = Column(sqlalchemy_jsonfield.JSONField(json=json), nullable=True)
    params = Column(sqlalchemy_jsonfield.JSONField(json=json), nullable=True)
    multiple = Column(Boolean, unique=False, default=False)

    ti_id = Column(
        String(36).with_variant(postgresql.UUID(as_uuid=False), "postgresql"),
        ForeignKey(
            TaskInstance.id,
            ondelete="CASCADE",
            onupdate="CASCADE",
            name="hitl_input_request_ti_fkey",
        ),
        nullable=False,
    )
    task_instance = relationship(
        "TaskInstance",
        primaryjoin="HITLInputRequestModel.ti_id == foreign(TaskInstance.id)",
        uselist=False,
    )


class HITLResponseModel(Base):
    """Human-in-the-loop received response."""

    __tablename__ = "hitl_response"
    id = Column(Integer, primary_key=True, autoincrement=True)
    created_at = Column(UtcDateTime, default=timezone.utcnow, nullable=False)
    content = Column(Text)
    user_id = Column(String(128), nullable=False)

    input_request_id = Column(
        Integer,
        nullable=False,
    )
    input_request = relationship(
        "HITLInputRequestModel",
        backref="response",
        uselist=False,
    )

    __table_args__ = (
        ForeignKeyConstraint(
            (input_request_id,),
            ("hitl_input_request.id",),
            name="hitl_response_input_request_fkey",
            ondelete="CASCADE",
            onupdate="CASCADE",
        ),
    )
