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
from sqlalchemy import Boolean, Column, MetaData, String, Text
from sqlalchemy.dialects import postgresql
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import registry

from airflow.models.base import _get_schema, naming_convention
from airflow.settings import json
from airflow.utils.sqlalchemy import UtcDateTime

metadata = MetaData(schema=_get_schema(), naming_convention=naming_convention)
mapper_registry = registry(metadata=metadata)

if TYPE_CHECKING:
    Base = Any  # type: ignore[misc]
else:
    Base = mapper_registry.generate_base()


class HITLResponseModel(Base):
    """Human-in-the-loop received response."""

    __tablename__ = "hitl_response"
    ti_id = Column(
        String(36).with_variant(postgresql.UUID(as_uuid=False), "postgresql"),
        primary_key=True,
        nullable=False,
    )

    # Input Request
    options = Column(sqlalchemy_jsonfield.JSONField(json=json), nullable=False)
    subject = Column(Text, nullable=False)
    body = Column(Text, nullable=True)
    default = Column(sqlalchemy_jsonfield.JSONField(json=json), nullable=True)
    multiple = Column(Boolean, unique=False, default=False)

    params = Column(sqlalchemy_jsonfield.JSONField(json=json), nullable=True)
    params_input = Column(sqlalchemy_jsonfield.JSONField(json=json), nullable=True)

    # Response Content Detail
    response_at = Column(UtcDateTime, nullable=True)
    user_id = Column(String(128), nullable=True)
    response_content = Column(
        sqlalchemy_jsonfield.JSONField(json=json),
        nullable=True,
        default=None,
    )

    @hybrid_property
    def response_received(self) -> bool:
        return self.response_at is not None
