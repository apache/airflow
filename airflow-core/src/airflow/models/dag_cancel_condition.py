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

from sqlalchemy import JSON, ForeignKeyConstraint, Index, PrimaryKeyConstraint, String
from sqlalchemy.orm import Mapped, mapped_column

from airflow._shared.webhook_naming.matching import MAX_CORRELATION_KEY_CONF_LENGTH
from airflow.models.base import Base, StringID

#: The ``task_id`` of a Dag's own conditions. A primary key column cannot be NULL, and no task has an
#: empty id.
DAG_LEVEL_TASK_ID = ""


class DagCancelCondition(Base):
    """
    A ``cancel_on`` condition of a Dag or of one of its tasks, as the Dag processor last stored it.

    .. note:: |experimental|
    """

    __tablename__ = "dag_cancel_condition"

    dag_id: Mapped[str] = mapped_column(StringID(), primary_key=True)
    task_id: Mapped[str] = mapped_column(StringID(), primary_key=True)
    webhook: Mapped[str] = mapped_column(StringID(length=64), primary_key=True)
    # The digest of the whole condition. correlation_key_conf is nullable, and NULLs never collide in a
    # unique key, so a key over the columns themselves would let one condition be stored twice.
    condition_digest: Mapped[str] = mapped_column(String(64), primary_key=True)
    # none_as_null: plain JSON writes an explicit None as the JSON text 'null', which IS NULL misses.
    events: Mapped[list[str] | None] = mapped_column(JSON(none_as_null=True), nullable=True)
    match: Mapped[dict[str, list[str]]] = mapped_column(JSON(), nullable=False)
    correlation_key_conf: Mapped[str | None] = mapped_column(
        String(MAX_CORRELATION_KEY_CONF_LENGTH), nullable=True
    )
    # The state the task ends in; NULL for a Dag's own conditions, which fail the whole run.
    cancel_state: Mapped[str | None] = mapped_column(String(20), nullable=True)

    __table_args__ = (
        PrimaryKeyConstraint(dag_id, task_id, webhook, condition_digest, name="dag_cancel_condition_pkey"),
        ForeignKeyConstraint(
            (dag_id,),
            ["dag.dag_id"],
            name="dag_cancel_condition_dag_id_fkey",
            ondelete="CASCADE",
        ),
        # A delivery with a correlation key reads the conditions of its webhook.
        Index("idx_dag_cancel_condition_webhook", webhook),
    )
