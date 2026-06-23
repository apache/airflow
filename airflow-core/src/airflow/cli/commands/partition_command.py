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
"""Partitions sub-commands."""

from __future__ import annotations

import datetime
from typing import TYPE_CHECKING

from sqlalchemy import or_, select

from airflow._shared.timezones.timezone import parse as parsedate
from airflow.models.dagrun import DagRun
from airflow.models.taskinstance import TaskInstance, clear_task_instances
from airflow.utils import cli as cli_utils
from airflow.utils.cli import get_db_dag
from airflow.utils.providers_configuration_loader import providers_configuration_loaded
from airflow.utils.session import NEW_SESSION, provide_session

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

TI_CHUNK_SIZE = 500


def _flush_buffer(
    buffer: list[str],
    carry: list[TaskInstance],
    session: Session,
    *,
    drain: bool = False,
) -> int:
    """
    Fetch TIs for buffered run_ids, extend carry, send full TI_CHUNK_SIZE slices.

    If drain=True, also send the final partial slice (used at end of run).
    Returns the total number of TIs sent to clear_task_instances by this call.
    """
    flushed = 0
    if buffer:
        chunk_tis = list(session.scalars(select(TaskInstance).where(TaskInstance.run_id.in_(buffer))))
        buffer.clear()
        carry.extend(chunk_tis)
    while len(carry) >= TI_CHUNK_SIZE:
        slice_tis = carry[:TI_CHUNK_SIZE]
        del carry[:TI_CHUNK_SIZE]
        clear_task_instances(slice_tis, session=session)
        flushed += len(slice_tis)
    if drain and carry:
        clear_task_instances(carry, session=session)
        flushed += len(carry)
    return flushed


@cli_utils.action_cli
@providers_configuration_loaded
@provide_session
def clear(args, *, session: Session = NEW_SESSION) -> None:
    """
    Clear the partition_key and partition_date of matching DagRuns.

    When a partition_date window is given, both bounds are **day-granular** and
    anchored in the timetable's timezone for tz-aware partitioned timetables.
    --start-date is the inclusive start local calendar day; --end-date is the
    inclusive end local calendar day (any time-of-day or timezone-offset
    component in either value is ignored; only the calendar date is used).
    """
    has_range = args.start_date is not None or args.end_date is not None or args.date is not None
    selectors_used = sum([args.run_id is not None, args.partition_key is not None, has_range])
    if selectors_used != 1:
        raise SystemExit(
            "Specify exactly one of --run-id, --partition-key, or a partition_date range "
            "(--start-date/--end-date or --date)."
        )

    if args.date is not None:
        if args.start_date is not None or args.end_date is not None:
            raise SystemExit("--date cannot be combined with --start-date / --end-date.")
        raw = args.date
        parts = raw.split("~", 1)
        if len(parts) != 2 or not parts[0].strip() or not parts[1].strip():
            raise SystemExit("--date must be in the form 'a~b', e.g. '2026-01-01~2026-01-31'.")
        try:
            args.start_date = parsedate(parts[0].strip())
            args.end_date = parsedate(parts[1].strip())
        except ValueError:
            raise SystemExit("--date sides must be parseable as a date or datetime.")

    stmt = select(DagRun).where(DagRun.dag_id == args.dag_id)
    if args.run_id:
        stmt = stmt.where(DagRun.run_id == args.run_id)
    elif args.partition_key is not None:
        stmt = stmt.where(DagRun.partition_key == args.partition_key)
    else:
        stmt = stmt.where(or_(DagRun.partition_key.is_not(None), DagRun.partition_date.is_not(None)))
        if args.start_date is not None or args.end_date is not None:
            dag = get_db_dag(bundle_names=None, dag_id=args.dag_id)
            if args.start_date is not None:
                lower = dag.timetable.resolve_day_bound(args.start_date.date())
                stmt = stmt.where(DagRun.partition_date >= lower)
            if args.end_date is not None:
                upper = dag.timetable.resolve_day_bound(args.end_date.date() + datetime.timedelta(days=1))
                stmt = stmt.where(DagRun.partition_date < upper)
    stmt = stmt.order_by(DagRun.partition_date, DagRun.run_id)

    clear_tis = bool(args.clear_task_instances)
    cleared = 0
    processed_any = False

    # For --clear-task-instances: run_ids are buffered so that TIs are fetched with a single
    # SELECT IN per chunk (avoiding N+1). The fetched TIs are then flushed to
    # clear_task_instances in slices of TI_CHUNK_SIZE, batched by TI count rather than
    # DagRun count. Any leftover TIs that do not fill a full slice are carried forward and
    # combined with the next SELECT's results before the next set of slices is cut.
    ti_buffer_run_ids: list[str] = []
    ti_carry: list[TaskInstance] = []
    tis_cleared_total = 0
    runs_for_ti_total = 0
    tis_dry_total = 0
    runs_for_ti_dry = 0

    for run in session.scalars(stmt).yield_per(100):
        processed_any = True
        fields_already_cleared = run.partition_key is None and run.partition_date is None
        if fields_already_cleared and not clear_tis:
            print(f"DagRun {run.run_id}: already cleared, skipping.")
            continue
        if not fields_already_cleared:
            print(
                f"DagRun {run.run_id}: "
                f"partition_key={run.partition_key!r} -> None, "
                f"partition_date={run.partition_date.isoformat() if run.partition_date else None} -> None"
            )
            if not args.dry_run:
                run.partition_key = None
                run.partition_date = None
            cleared += 1
        if clear_tis:
            if args.dry_run:
                run_tis = session.scalars(select(TaskInstance).where(TaskInstance.run_id == run.run_id)).all()
                tis_dry_total += len(run_tis)
                runs_for_ti_dry += 1
            else:
                ti_buffer_run_ids.append(run.run_id)
                runs_for_ti_total += 1
                if len(ti_buffer_run_ids) >= TI_CHUNK_SIZE:
                    tis_cleared_total += _flush_buffer(ti_buffer_run_ids, ti_carry, session)

    if not processed_any:
        print(f"No matching DagRuns found for dag_id={args.dag_id}.")
        return

    # Flush the tail: fetch any remaining buffered run_ids, combine with carry, then
    # cut full slices and send the final partial slice.
    if clear_tis:
        if args.dry_run:
            print(
                f"Dry run: would clear task instances on {runs_for_ti_dry} "
                f"DagRun(s) ({tis_dry_total} task instance(s))."
            )
        else:
            tis_cleared_total += _flush_buffer(ti_buffer_run_ids, ti_carry, session, drain=True)
            print(
                f"Cleared task instances on {runs_for_ti_total} "
                f"DagRun(s) ({tis_cleared_total} task instance(s))."
            )

    if args.dry_run:
        print(f"Dry run: would clear {cleared} DagRun(s). No changes written.")
    else:
        print(f"Cleared partition fields on {cleared} DagRun(s).")
