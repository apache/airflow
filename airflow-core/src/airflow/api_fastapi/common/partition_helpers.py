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

import structlog

from airflow.exceptions import DeserializationError
from airflow.models.serialized_dag import SerializedDagModel
from airflow.timetables.simple import PartitionedAssetTimetable

if TYPE_CHECKING:
    from sqlalchemy.orm import Session


log = structlog.get_logger(logger_name=__name__)


def _extract_partitioned_timetable(serdag: SerializedDagModel) -> PartitionedAssetTimetable | None:
    """Return the ``PartitionedAssetTimetable`` carried by *serdag*, or ``None``."""
    try:
        timetable = serdag.dag.timetable
    except (DeserializationError, TypeError):
        # ``DeserializationError`` covers structural serialization failures so
        # a corrupted serialized Dag silently degrades to non-partitioned
        # rather than 500-ing the read-only UI page. ``TypeError`` covers the
        # eager validation in ``RollupMapper.__init__`` (raised during
        # timetable deserialization when an upstream mapper / window pair is
        # incompatible), so a misconfigured rollup Dag also degrades to
        # non-partitioned here rather than 500-ing.
        # ``KeyError`` / ``AttributeError`` / ``ImportError`` are intentionally
        # not caught: refactor bugs that rename an attribute on ``serdag.dag``
        # or break an import path must surface to the caller rather than
        # silently downgrading the route to non-rollup.
        log.warning("Failed to deserialize timetable for Dag", dag_id=serdag.dag_id, exc_info=True)
        return None
    if not timetable.partitioned:
        return None
    if TYPE_CHECKING:
        assert isinstance(timetable, PartitionedAssetTimetable)
    return timetable


def load_partitioned_timetable(dag_id: str, session: Session) -> PartitionedAssetTimetable | None:
    """
    Return the PartitionedAssetTimetable for *dag_id*, or None if absent or not partitioned.

    Callers gate this behind ``DagModel.has_rollup_mappers``, which is only
    populated for ``PartitionedAssetTimetable``. The ``TYPE_CHECKING`` assert
    narrows the type for mypy without a runtime ``isinstance`` cost.
    """
    serdag = SerializedDagModel.get(dag_id=dag_id, session=session)
    if serdag is None:
        return None
    return _extract_partitioned_timetable(serdag)


def load_partitioned_timetables(
    dag_ids: list[str], session: Session
) -> dict[str, PartitionedAssetTimetable | None]:
    """
    Batch-load PartitionedAssetTimetables for *dag_ids* in a single query.

    Routes that already gate per-Dag on ``DagModel.has_rollup_mappers`` should
    use this when iterating over many Dags so ``SerializedDagModel`` is hit
    once instead of once per Dag. Returns a dict keyed by ``dag_id``; entries
    whose timetable failed to deserialize or is not partitioned are ``None``.
    """
    if not dag_ids:
        return {}
    return {
        serdag.dag_id: _extract_partitioned_timetable(serdag)
        for serdag in SerializedDagModel.get_latest_serialized_dags(dag_ids=dag_ids, session=session)
    }
