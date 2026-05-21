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

from typing import TYPE_CHECKING

from sqlalchemy import or_, select

from airflow._shared.timezones.timezone import parse as parsedate
from airflow.models.dagrun import DagRun
from airflow.models.taskinstance import TaskInstance, clear_task_instances
from airflow.utils import cli as cli_utils
from airflow.utils.providers_configuration_loader import providers_configuration_loaded
from airflow.utils.session import NEW_SESSION, provide_session

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

DR_CHUNK_SIZE = 500


@cli_utils.action_cli
@providers_configuration_loaded
@provide_session
def clear(args, *, session: Session = NEW_SESSION) -> None:
    """Clear the partition_key and partition_date of matching DagRuns."""
    has_range = args.start_date is not None or args.end_date is not None or args.date is not None
    selectors_used = sum([args.run_id is not None, args.partition_key is not None, has_range])
    if selectors_used == 0:
        raise SystemExit(
            "Specify exactly one of --run-id, --partition-key, or a partition_date range "
            "(--start-date/--end-date or --date)."
        )
    if selectors_used > 1:
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
            raise SystemExit("--date must be in the form 'a~b', e.g. '2026-01-01~2026-01-31'.")

    if args.end_date is not None:
        args.end_date = args.end_date.replace(hour=23, minute=59, second=59, microsecond=999999)

    stmt = select(DagRun).where(DagRun.dag_id == args.dag_id)
    if args.run_id:
        stmt = stmt.where(DagRun.run_id == args.run_id)
    elif args.partition_key is not None:
        stmt = stmt.where(DagRun.partition_key == args.partition_key)
    else:
        stmt = stmt.where(or_(DagRun.partition_key.is_not(None), DagRun.partition_date.is_not(None)))
        if args.start_date is not None:
            stmt = stmt.where(DagRun.partition_date >= args.start_date)
        if args.end_date is not None:
            stmt = stmt.where(DagRun.partition_date <= args.end_date)
    stmt = stmt.order_by(DagRun.partition_date, DagRun.run_id)

    clear_tis = bool(args.clear_task_instances)
    cleared = 0
    processed_any = False

    # For --clear-task-instances: buffer run_ids, flush in DR_CHUNK_SIZE batches.
    # task_instances is a lazy-select relationship; avoid N+1 by issuing one explicit
    # SELECT per chunk instead of accessing run.task_instances directly.
    ti_buffer_run_ids: list[str] = []
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
                if len(ti_buffer_run_ids) >= DR_CHUNK_SIZE:
                    chunk_tis = list(
                        session.scalars(
                            select(TaskInstance).where(TaskInstance.run_id.in_(ti_buffer_run_ids))
                        )
                    )
                    clear_task_instances(chunk_tis, session=session)
                    tis_cleared_total += len(chunk_tis)
                    ti_buffer_run_ids.clear()

    if not processed_any:
        print(f"No matching DagRuns found for dag_id={args.dag_id}.")
        return

    # Flush the tail of the TI buffer after the loop.
    if clear_tis:
        if args.dry_run:
            print(
                f"Dry run: would clear task instances on {runs_for_ti_dry} "
                f"DagRun(s) ({tis_dry_total} task instance(s))."
            )
        else:
            if ti_buffer_run_ids:
                chunk_tis = list(
                    session.scalars(select(TaskInstance).where(TaskInstance.run_id.in_(ti_buffer_run_ids)))
                )
                clear_task_instances(chunk_tis, session=session)
                tis_cleared_total += len(chunk_tis)
            print(
                f"Cleared task instances on {runs_for_ti_total} "
                f"DagRun(s) ({tis_cleared_total} task instance(s))."
            )

    if args.dry_run:
        print(f"Dry run: would clear {cleared} DagRun(s). No changes written.")
    else:
        print(f"Cleared partition fields on {cleared} DagRun(s).")
