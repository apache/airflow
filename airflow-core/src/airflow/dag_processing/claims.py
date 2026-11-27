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

from sqlalchemy import and_, exists, or_, select

from airflow._shared.timezones import timezone
from airflow.jobs.job import Job
from airflow.models.callback import DagProcessorCallback, UndecodableCallbackError
from airflow.utils.sqlalchemy import with_row_locks

if TYPE_CHECKING:
    from collections.abc import Collection

    from sqlalchemy import ColumnElement, SQLColumnExpression
    from sqlalchemy.orm import Session

# Short enough for an upgraded API server or Dag processor to pick the row up during a rolling upgrade.
UNDECODABLE_CALLBACK_RETRY_INTERVAL = timedelta(minutes=5)


def get_decodable_callback_filter() -> ColumnElement[bool]:
    """Skip callbacks that could not be decoded until their retry time."""
    retry_after = DagProcessorCallback.decode_retry_after
    return or_(retry_after.is_(None), retry_after <= timezone.utcnow())


def get_available_work_filter(processor_job_id: SQLColumnExpression[int | None]) -> ColumnElement[bool]:
    """Select unowned work or work whose owning Job has ended."""
    # Correlate by owner so polling an empty queue never materializes the entire Job history.
    retired_owner = exists().where(Job.id == processor_job_id, Job.end_date.is_not(None))
    return or_(processor_job_id.is_(None), retired_owner)


def prepare_callback_source_filter(
    bundle_names: Collection[str], pinned_only: Collection[str], *, limit: int, session: Session
) -> ColumnElement[bool]:
    """Select runnable callbacks before applying the queue limit, including rows from older writers."""
    model = DagProcessorCallback
    restricted = set(bundle_names).intersection(pinned_only)
    if not restricted:
        return model.bundle_name.in_(bundle_names)

    # Older writers only recorded the version inside the serialized request. Classify at most one
    # batch per poll so a rolling upgrade or a large backlog cannot stall other bundles.
    legacy = (
        select(model)
        .where(
            model.bundle_name.in_(restricted),
            model.is_version_pinned.is_(None),
            get_available_work_filter(model.processor_job_id),
        )
        .order_by(model.id)
        .limit(limit)
    )
    for row in session.scalars(with_row_locks(legacy, session=session, skip_locked=True)):
        try:
            row.is_version_pinned = row.get_callback_request().bundle_version is not None
        except UndecodableCallbackError:
            row.is_version_pinned = False
    session.flush()
    return or_(
        model.bundle_name.in_(set(bundle_names) - restricted),
        and_(model.bundle_name.in_(restricted), model.is_version_pinned.is_(True)),
    )
