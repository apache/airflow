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
from uuid import UUID

from sqlalchemy import BigInteger, ForeignKey, String, Uuid
from sqlalchemy.orm import Mapped, mapped_column

from airflow.models.base import Base
from airflow.utils.sqlalchemy import UtcDateTime


class DagParseCheckpoint(Base):
    """Latest publication per Job and source; removed when its Job is purged."""

    __tablename__ = "dag_parse_checkpoint"

    job_id: Mapped[int] = mapped_column(ForeignKey("job.id", ondelete="CASCADE"), primary_key=True)
    source_key: Mapped[str] = mapped_column(String(64), primary_key=True)
    attempt_id: Mapped[UUID] = mapped_column(Uuid, nullable=False)
    dispatch_sequence: Mapped[int] = mapped_column(BigInteger, nullable=False)
    payload_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    accepted_at: Mapped[datetime] = mapped_column(UtcDateTime, nullable=False)
    bundle_revision: Mapped[UUID | None] = mapped_column(Uuid, nullable=True)
