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
from sqlalchemy import Boolean, Column, ForeignKeyConstraint, Integer, String, Text
from sqlalchemy.dialects import postgresql

from airflow.models.base import Base
from airflow.settings import json
from airflow.utils import timezone
from airflow.utils.sqlalchemy import UtcDateTime


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
        nullable=False,
    )

    __table_args__ = (
        ForeignKeyConstraint(
            (ti_id,),
            ("task_instance.id",),
            name="hitl_input_request_ti_fkey",
            ondelete="CASCADE",
            onupdate="CASCADE",
        ),
    )


class HITLResponseModel(Base):
    """Human-in-the-loop received response."""

    __tablename__ = "hitl_response"
    id = Column(Integer, primary_key=True, autoincrement=True)
    created_at = Column(UtcDateTime, default=timezone.utcnow, nullable=False)
    content = Column(Text)
    user_id = Column(String(128), nullable=False)

    # TODO: set foreign key to HITLInputRequestModel instead
    ti_id = Column(
        String(36).with_variant(postgresql.UUID(as_uuid=False), "postgresql"),
        nullable=False,
    )

    __table_args__ = (
        ForeignKeyConstraint(
            (ti_id,),
            ("task_instance.id",),
            name="hitl_response_ti_fkey",
            ondelete="CASCADE",
            onupdate="CASCADE",
        ),
    )
