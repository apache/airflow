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

from datetime import timedelta
from typing import TYPE_CHECKING

import structlog
from sqlalchemy import and_, case, exists, func, or_, select

from airflow._shared.timezones import timezone
from airflow.jobs.job import Job
from airflow.models.callback import DagProcessorCallback, UndecodableCallbackError
from airflow.utils.sqlalchemy import with_row_locks

if TYPE_CHECKING:
    from collections.abc import Collection, Sequence

    from sqlalchemy import ColumnElement, SQLColumnExpression
    from sqlalchemy.orm import Session

    from airflow.callbacks.callback_requests import CallbackRequest
    from airflow.utils.sqlalchemy import Select

log = structlog.get_logger(__name__)

# Short enough for an upgraded API server or Dag processor to pick the row up during a rolling upgrade.
UNDECODABLE_CALLBACK_RETRY_INTERVAL = timedelta(minutes=5)


def get_decodable_callback_filter() -> ColumnElement[bool]:
    """Skip callbacks that could not be decoded until their retry time."""
    retry_after = DagProcessorCallback.decode_retry_after
    return or_(retry_after.is_(None), retry_after <= timezone.utcnow())


def order_callback_queue(query: Select[DagProcessorCallback]) -> Select[DagProcessorCallback]:
    """Serve fresh work already waiting at the oldest eligible retry's deadline, then due retries."""
    model = DagProcessorCallback
    retry = model.decode_retry_after
    oldest_retry = (
        query.where(retry.is_not(None)).with_only_columns(func.min(retry)).correlate(None).scalar_subquery()
    )
    # New arrivals cannot overtake the oldest retry. Deferring it again moves its deadline
    # behind work already waiting, including when the next poll comes from a different Job.
    group = case(
        (and_(retry.is_(None), model.created_at < oldest_retry), 0), (retry.is_not(None), 1), else_=2
    )
    return query.order_by(group, retry, model.priority_weight.desc(), model.id)


def fetch_callback_batch(
    query: Select[DagProcessorCallback], limit: int, *, session: Session
) -> list[tuple[DagProcessorCallback, CallbackRequest]]:
    """Lock and decode at most one delivery batch, giving failed retries' slots to fresh work."""
    model = DagProcessorCallback
    eligible = query.where(get_decodable_callback_filter())
    ordered = order_callback_queue(eligible)
    rows = session.scalars(
        with_row_locks(ordered.limit(limit), of=model, session=session, skip_locked=True)
    ).all()
    decoded: list[tuple[DagProcessorCallback, CallbackRequest]] = []

    def decode(batch: Sequence[DagProcessorCallback]) -> int:
        failed_retries = 0
        for row in batch:
            try:
                request = row.get_callback_request()
            except UndecodableCallbackError as error:
                if row.decode_retry_after is not None:
                    failed_retries += 1
                defer_callback(row)
                log.error(
                    "Stored callback cannot be decoded; releasing it for a later retry",
                    id=row.id,
                    reason=str(error),
                    retry_after=row.decode_retry_after,
                )
            else:
                decoded.append((row, request))
        return failed_retries

    if slots := decode(rows):
        # When enqueue timestamps are ahead of the reader's clock, the same retry can head every poll.
        fresh = eligible.where(model.decode_retry_after.is_(None), model.id.not_in([row.id for row in rows]))
        fresh = fresh.order_by(model.priority_weight.desc(), model.id).limit(slots)
        decode(session.scalars(with_row_locks(fresh, of=model, session=session, skip_locked=True)).all())
    # Queue selection decides which rows fit. Delivery and replay use the same stable priority order.
    return sorted(decoded, key=lambda pair: (-pair[0].priority_weight, pair[0].id))


def defer_callback(work: DagProcessorCallback) -> None:
    work.processor_job_id = None
    work.processor_claim_id = None
    work.decode_retry_after = timezone.utcnow() + UNDECODABLE_CALLBACK_RETRY_INTERVAL


def get_available_work_filter(processor_job_id: SQLColumnExpression[int | None]) -> ColumnElement[bool]:
    """Select unowned work or work whose owning Job has ended."""
    # Correlate by owner so polling an empty queue never materializes the entire Job history.
    retired_owner = exists().where(Job.id == processor_job_id, Job.end_date.is_not(None))
    return or_(processor_job_id.is_(None), retired_owner)


def prepare_callback_source_filter(
    bundle_names: Collection[str],
    pinned_only: Collection[str],
    *,
    limit: int,
    session: Session,
) -> ColumnElement[bool]:
    """Select runnable callbacks before applying the queue limit, including rows from older writers."""
    model = DagProcessorCallback
    restricted = set(bundle_names).intersection(pinned_only)
    if not restricted:
        return model.bundle_name.in_(bundle_names)

    # Older writers only recorded the version inside the serialized request. Classify at most one
    # delivery batch per poll so a rolling upgrade or a large backlog cannot stall other bundles.
    legacy = select(model).where(
        model.bundle_name.in_(restricted),
        model.is_version_pinned.is_(None),
        get_available_work_filter(model.processor_job_id),
    )
    for row, request in fetch_callback_batch(legacy, limit, session=session):
        row.is_version_pinned = request.bundle_version is not None
    session.flush()
    return or_(
        model.bundle_name.in_(set(bundle_names) - restricted),
        and_(model.bundle_name.in_(restricted), model.is_version_pinned.is_(True)),
    )
