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

import math
from typing import TYPE_CHECKING

from sqlalchemy import and_, bindparam, delete, func, or_, select

from airflow._shared.timezones import timezone
from airflow.configuration import conf
from airflow.models.asset import AssetPartitionDagRun
from airflow.models.dag import DagModel
from airflow.models.dagwarning import DagWarning, DagWarningType
from airflow.utils.session import create_session
from airflow.utils.sqlalchemy import build_upsert_stmt, get_dialect_name

if TYPE_CHECKING:
    from typing import TypeAlias

    from sqlalchemy.orm import Session

    PendingRunTimeouts: TypeAlias = dict[str, float]


class AssetPartitionWaitMonitor:
    """Refresh advisory warnings about pending partition age, independently of run creation."""

    _batch_size = 100

    def __init__(self) -> None:
        self._cursor: str | None = None

    def check(self) -> None:
        # Validate before reconciling: invalid configuration must not clear existing warnings.
        configured = conf.getjson("scheduler", "pending_partitioned_dag_run_timeout")
        if not isinstance(configured, dict):
            raise ValueError(
                "pending_partitioned_dag_run_timeout must be a JSON object of consumer Dag IDs to seconds"
            )
        timeouts: PendingRunTimeouts = {}
        for dag_id, value in configured.items():
            if (
                not isinstance(dag_id, str)
                or not dag_id.strip()
                or isinstance(value, bool)
                or not isinstance(value, (int, float))
                or value <= 0
                or not math.isfinite(value)
            ):
                raise ValueError(
                    "pending_partitioned_dag_run_timeout requires nonempty consumer Dag IDs and finite positive seconds"
                )
            timeouts[dag_id] = value

        with create_session(scoped=False) as session:
            dag_ids = self._get_next_batch(timeouts, after=self._cursor, session=session)
            if not dag_ids and self._cursor is not None:
                dag_ids = self._get_next_batch(timeouts, after=None, session=session)
            if dag_ids:
                self._refresh(dag_ids, timeouts, session=session)
        # Do not advance on rollback: the same batch needs another opportunity next time.
        self._cursor = dag_ids[-1] if len(dag_ids) == self._batch_size else None

    def _get_next_batch(
        self, timeouts: PendingRunTimeouts, *, session: Session, after: str | None
    ) -> list[str]:
        # Use database ordering for both candidates and cursor comparisons. Python ordering
        # can differ from the database collation. Include existing warnings even with no policy.
        query = (
            select(DagModel.dag_id)
            .outerjoin(
                DagWarning,
                and_(
                    DagWarning.dag_id == DagModel.dag_id,
                    DagWarning.warning_type == DagWarningType.ASSET_PARTITION_PENDING,
                ),
            )
            .where(or_(DagModel.dag_id.in_(timeouts), DagWarning.dag_id.is_not(None)))
            .order_by(DagModel.dag_id)
            .limit(self._batch_size)
        )
        if after is not None:
            query = query.where(DagModel.dag_id > after)
        return list(session.scalars(query))

    def _refresh(self, dag_ids: list[str], timeouts: PendingRunTimeouts, *, session: Session) -> None:
        existing = {
            dag_id: message
            for dag_id, message in session.execute(
                select(DagWarning.dag_id, DagWarning.message).where(
                    DagWarning.dag_id.in_(dag_ids),
                    DagWarning.warning_type == DagWarningType.ASSET_PARTITION_PENDING,
                )
            )
        }
        now = timezone.utcnow()
        overdue: dict[str, str] = {}
        if monitored := [dag_id for dag_id in dag_ids if dag_id in timeouts]:
            pending = session.execute(
                select(
                    AssetPartitionDagRun.target_dag_id,
                    func.count(),
                    func.min(AssetPartitionDagRun.created_at),
                )
                .join(DagModel, DagModel.dag_id == AssetPartitionDagRun.target_dag_id)
                .where(
                    AssetPartitionDagRun.target_dag_id.in_(monitored),
                    AssetPartitionDagRun.created_dag_run_id.is_(None),
                    DagModel.timetable_partitioned.is_(True),
                    DagModel.is_paused.is_(False),
                    DagModel.is_draining.is_(False),
                    DagModel.is_stale.is_(False),
                )
                .group_by(AssetPartitionDagRun.target_dag_id)
            )
            for dag_id, count, oldest in pending:
                timeout = timeouts[dag_id]
                if (now - oldest).total_seconds() > timeout:
                    overdue[dag_id] = (
                        f"{count} pending partitioned Dag runs; oldest pending since {oldest.isoformat()}. "
                        f"The configured wait threshold is {timeout:g} seconds. "
                        "Inspect the pending asset inputs for this Dag."
                    )

        if cleared := existing.keys() - overdue.keys():
            session.execute(
                delete(DagWarning).where(
                    DagWarning.dag_id.in_(cleared),
                    DagWarning.warning_type == DagWarningType.ASSET_PARTITION_PENDING,
                )
            )
        changed = [
            {
                "dag_id": dag_id,
                "warning_type": DagWarningType.ASSET_PARTITION_PENDING.value,
                "message": message,
                "timestamp": now,
                "pending_message": message,
            }
            for dag_id, message in overdue.items()
            if existing.get(dag_id) != message
        ]
        if changed:
            # Atomic upserts allow multiple schedulers to discover the same warning. Preserve
            # its first-detected timestamp; stable messages avoid writes on unchanged polls.
            session.execute(
                build_upsert_stmt(
                    get_dialect_name(session),
                    DagWarning.__table__,
                    conflict_cols=["dag_id", "warning_type"],
                    values={},
                    update_fields={"message": bindparam("pending_message")},
                ),
                changed,
            )
