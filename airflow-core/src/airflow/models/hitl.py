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

from datetime import datetime
from typing import TYPE_CHECKING, Any, TypedDict

import sqlalchemy_jsonfield
from sqlalchemy import Boolean, ForeignKeyConstraint, String, Text, func, literal
from sqlalchemy.dialects import postgresql
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import Mapped, relationship
from sqlalchemy.sql.functions import FunctionElement

from airflow._shared.timezones import timezone
from airflow.models.base import Base
from airflow.settings import json
from airflow.utils.sqlalchemy import UtcDateTime, mapped_column

if TYPE_CHECKING:
    from sqlalchemy.sql import ColumnElement
    from sqlalchemy.sql.compiler import SQLCompiler


class JSONExtract(FunctionElement):
    """
    Cross-dialect JSON key extractor.

    :meta: private
    """

    type = String()
    inherit_cache = True

    def __init__(self, column: ColumnElement[Any], key: str, **kwargs: dict[str, Any]) -> None:
        super().__init__(column, literal(key), **kwargs)


@compiles(JSONExtract, "postgresql")
def compile_postgres(element: JSONExtract, compiler: SQLCompiler, **kwargs: dict[str, Any]) -> str:
    """
    Compile JSONExtract for PostgreSQL.

    :meta: private
    """
    column, key = element.clauses
    return compiler.process(func.json_extract_path_text(column, key), **kwargs)


@compiles(JSONExtract, "sqlite")
@compiles(JSONExtract, "mysql")
def compile_sqlite_mysql(element: JSONExtract, compiler: SQLCompiler, **kwargs: dict[str, Any]) -> str:
    """
    Compile JSONExtract for SQLite/MySQL.

    :meta: private
    """
    column, key = element.clauses
    return compiler.process(func.json_extract(column, f"$.{key.value}"), **kwargs)


class HITLUser(TypedDict):
    """Typed dict for saving a Human-in-the-loop user information."""

    id: str
    name: str


class HITLDetailPropertyMixin:
    """The property part of HITLDetail and HITLDetailHistory."""

    responded_at: datetime | None
    responded_by: dict[str, Any] | None
    assignees: list[dict[str, str]] | None

    @hybrid_property
    def response_received(self) -> bool:
        return self.responded_at is not None

    @response_received.expression  # type: ignore[no-redef]
    def response_received(cls):
        return cls.responded_at.is_not(None)

    @hybrid_property
    def responded_by_user_id(self) -> str | None:
        return self.responded_by["id"] if self.responded_by else None

    @responded_by_user_id.expression  # type: ignore[no-redef]
    def responded_by_user_id(cls):
        return JSONExtract(cls.responded_by, "id")

    @hybrid_property
    def responded_by_user_name(self) -> str | None:
        return self.responded_by["name"] if self.responded_by else None

    @responded_by_user_name.expression  # type: ignore[no-redef]
    def responded_by_user_name(cls):
        return JSONExtract(cls.responded_by, "name")

    @hybrid_property
    def assigned_users(self) -> list[HITLUser]:
        if not self.assignees:
            return []
        return [
            HITLUser(
                id=assignee["id"],
                name=assignee["name"],
            )
            for assignee in self.assignees
        ]

    @hybrid_property
    def responded_by_user(self) -> HITLUser | None:
        if self.responded_by is None:
            return None
        return HITLUser(
            id=self.responded_by["id"],
            name=self.responded_by["name"],
        )


class HITLDetail(Base, HITLDetailPropertyMixin):
    """Human-in-the-loop request and corresponding response."""

    __tablename__ = "hitl_detail"
    ti_id: Mapped[str] = mapped_column(
        String(36).with_variant(postgresql.UUID(as_uuid=False), "postgresql"),
        primary_key=True,
        nullable=False,
    )

    # User Request Detail
    options: Mapped[dict] = mapped_column(sqlalchemy_jsonfield.JSONField(json=json), nullable=False)
    subject: Mapped[str] = mapped_column(Text, nullable=False)
    body: Mapped[str | None] = mapped_column(Text, nullable=True)
    defaults: Mapped[dict | None] = mapped_column(sqlalchemy_jsonfield.JSONField(json=json), nullable=True)
    multiple: Mapped[bool | None] = mapped_column(Boolean, unique=False, default=False, nullable=True)
    params: Mapped[dict] = mapped_column(
        sqlalchemy_jsonfield.JSONField(json=json), nullable=False, default={}
    )
    assignees: Mapped[list[dict[str, str]] | None] = mapped_column(
        sqlalchemy_jsonfield.JSONField(json=json), nullable=True
    )
    created_at: Mapped[datetime] = mapped_column(UtcDateTime, default=timezone.utcnow, nullable=False)

    # Response Content Detail
    responded_at: Mapped[datetime | None] = mapped_column(UtcDateTime, nullable=True)
    responded_by: Mapped[dict | None] = mapped_column(
        sqlalchemy_jsonfield.JSONField(json=json), nullable=True
    )
    chosen_options: Mapped[list[str] | None] = mapped_column(
        sqlalchemy_jsonfield.JSONField(json=json),
        nullable=True,
        default=None,
    )
    params_input: Mapped[dict] = mapped_column(
        sqlalchemy_jsonfield.JSONField(json=json), nullable=False, default={}
    )
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
