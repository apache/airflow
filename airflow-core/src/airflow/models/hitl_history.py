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

import sqlalchemy_jsonfield
from sqlalchemy import Boolean, Column, ForeignKeyConstraint, String, Text
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import relationship

from airflow._shared.timezones import timezone
from airflow.models.base import Base
from airflow.models.hitl import HITLDetailPropertyMixin
from airflow.settings import json
from airflow.utils.sqlalchemy import UtcDateTime

if TYPE_CHECKING:
    from airflow.models.hitl import HITLDetail


class HITLDetailHistory(Base, HITLDetailPropertyMixin):
    """
    Store HITLDetail for old tries of TaskInstances.

    :meta private:
    """

    __tablename__ = "hitl_detail_history"
    ti_history_id = Column(
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
    created_at = Column(UtcDateTime, default=timezone.utcnow, nullable=False)

    # Response Content Detail
    responded_at = Column(UtcDateTime, nullable=True)
    responded_by = Column(sqlalchemy_jsonfield.JSONField(json=json), nullable=True)
    chosen_options = Column(
        sqlalchemy_jsonfield.JSONField(json=json),
        nullable=True,
        default=None,
    )
    params_input = Column(sqlalchemy_jsonfield.JSONField(json=json), nullable=False, default={})
    task_instance = relationship(
        "TaskInstanceHistory",
        lazy="joined",
        back_populates="hitl_detail",
    )

    def __init__(self, hitl_detail: HITLDetail):
        super().__init__()
        for column in self.__table__.columns:
            if column.name == "ti_history_id":
                setattr(self, column.name, hitl_detail.ti_id)
                continue
            setattr(self, column.name, getattr(hitl_detail, column.name))

    __table_args__ = (
        ForeignKeyConstraint(
            (ti_history_id,),
            ["task_instance_history.task_instance_id"],
            name="hitl_detail_history_tih_fkey",
            ondelete="CASCADE",
            onupdate="CASCADE",
        ),
    )
