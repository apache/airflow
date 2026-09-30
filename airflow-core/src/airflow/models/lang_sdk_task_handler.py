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

from collections.abc import Callable
from datetime import datetime
from typing import TYPE_CHECKING, Any
from uuid import UUID

import sqlalchemy as sa
import uuid6
from sqlalchemy import BigInteger, ForeignKeyConstraint, Index, String, UniqueConstraint, Uuid
from sqlalchemy.orm import Mapped, mapped_column, validates

from airflow._shared.timezones import timezone
from airflow.models.base import Base, StringID
from airflow.utils.hashlib_wrapper import md5
from airflow.utils.sqlalchemy import UtcDateTime

if TYPE_CHECKING:
    from sqlalchemy.engine.default import DefaultExecutionContext


def compute_fileloc_hash(relative_fileloc: str) -> str:
    """
    Return the md5 hex digest that the task handler tables index in place of a relative file location.

    A 2000-character path exceeds MySQL's 3072-byte key limit and Postgres's 2704-byte btree entry limit.
    """
    return md5(relative_fileloc.encode()).hexdigest()


# The column default fills a hash on INSERT, ORM or Core. ORM updates resync it in each model's
# @validates hook instead of an onupdate default, which would fire on every UPDATE, even one that
# leaves the path alone. A Core UPDATE or upsert that changes a path must set its hash itself.
def _build_fileloc_hash_default(path_key: str) -> Callable[[DefaultExecutionContext], str]:
    def compute(context: DefaultExecutionContext) -> str:
        return compute_fileloc_hash(context.get_current_parameters()[path_key])

    return compute


class LangSDKTaskHandlerArtifact(Base):
    """
    A Language SDK artifact in a task handler Dag bundle, cached with its fingerprint and probe answer.

    ``task_handlers`` is the runtime's whole answer, ``{dag_id: [TaskHandlerDeclaration as JSON, ...]}``.
    It is written only from a probe and together with the fingerprint, so the two always match.
    """

    __tablename__ = "lang_sdk_task_handler_artifact"

    id: Mapped[UUID] = mapped_column(Uuid(), primary_key=True, default=uuid6.uuid7)
    bundle_name: Mapped[str] = mapped_column(StringID(), nullable=False)
    relative_fileloc: Mapped[str] = mapped_column(String(2000), nullable=False)
    relative_fileloc_hash: Mapped[str] = mapped_column(
        String(32), nullable=False, default=_build_fileloc_hash_default("relative_fileloc")
    )
    size_bytes: Mapped[int] = mapped_column(BigInteger, nullable=False)
    # NULL means the artifact stores no fingerprint and is probed again on every parse.
    cache_digest: Mapped[str | None] = mapped_column(String(128), nullable=True)
    task_handlers: Mapped[dict[str, list[dict[str, Any]]]] = mapped_column(sa.JSON(), nullable=False)
    last_probed_at: Mapped[datetime] = mapped_column(UtcDateTime, nullable=False, default=timezone.utcnow)

    __table_args__ = (
        UniqueConstraint(
            bundle_name,
            relative_fileloc_hash,
            name="lang_sdk_task_handler_artifact_bundle_fileloc_uq",
        ),
        Index("idx_lang_sdk_task_handler_artifact_last_probed_at", last_probed_at),
    )

    @validates("relative_fileloc")
    def _sync_relative_fileloc_hash(self, key: str, relative_fileloc: str) -> str:
        self.relative_fileloc_hash = compute_fileloc_hash(relative_fileloc)
        return relative_fileloc


class LangSDKTaskHandler(Base):
    """The binding of one stub task, by ``(dag_id, task_id)``, to the artifact that registers its handler."""

    __tablename__ = "lang_sdk_task_handler"

    dag_id: Mapped[str] = mapped_column(StringID(), primary_key=True)
    task_id: Mapped[str] = mapped_column(StringID(), primary_key=True)
    artifact_id: Mapped[UUID] = mapped_column(Uuid(), nullable=False)
    dag_bundle_name: Mapped[str] = mapped_column(StringID(), nullable=False)
    dag_relative_fileloc: Mapped[str] = mapped_column(String(2000), nullable=False)
    dag_relative_fileloc_hash: Mapped[str] = mapped_column(
        String(32), nullable=False, default=_build_fileloc_hash_default("dag_relative_fileloc")
    )

    __table_args__ = (
        ForeignKeyConstraint(
            (dag_id,),
            ["dag.dag_id"],
            name="lang_sdk_task_handler_dag_id_fkey",
            ondelete="CASCADE",
        ),
        # No ON DELETE: deleting an artifact that a handler still references must fail.
        ForeignKeyConstraint(
            (artifact_id,),
            ["lang_sdk_task_handler_artifact.id"],
            name="lang_sdk_task_handler_artifact_id_fkey",
        ),
        Index("idx_lang_sdk_task_handler_dag_file", dag_bundle_name, dag_relative_fileloc_hash),
        Index("idx_lang_sdk_task_handler_artifact_id", artifact_id),
    )

    @validates("dag_relative_fileloc")
    def _sync_dag_relative_fileloc_hash(self, key: str, dag_relative_fileloc: str) -> str:
        self.dag_relative_fileloc_hash = compute_fileloc_hash(dag_relative_fileloc)
        return dag_relative_fileloc
