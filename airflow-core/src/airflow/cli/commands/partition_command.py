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

from airflow._shared.timezones.timezone import parse as parsedate
from airflow.models.dagrun import DagRun, clear_partition_runs
from airflow.utils import cli as cli_utils
from airflow.utils.cli import get_db_dag
from airflow.utils.providers_configuration_loader import providers_configuration_loaded
from airflow.utils.session import NEW_SESSION, provide_session

if TYPE_CHECKING:
    from sqlalchemy.orm import Session


@cli_utils.action_cli
@providers_configuration_loaded
@provide_session
def clear(args, *, session: Session = NEW_SESSION) -> None:
    """
    Clear the partition_key and partition_date of matching DagRuns.

    When a partition_date window is given, both bounds are inclusive and their
    wall-clock value is re-interpreted in the Dag's timetable timezone. The time
    component is honoured, so sub-day windows on sub-daily schedules select only
    the matching partitions; a date-only value (no time) is treated as local
    midnight.
    """
    has_start = args.start_date is not None
    has_end = args.end_date is not None
    has_range = has_start or has_end or args.date is not None
    selectors_used = sum([args.run_id is not None, args.partition_key is not None, has_range])
    if selectors_used != 1:
        raise SystemExit(
            "Specify exactly one of --run-id, --partition-key, or a partition_date range "
            "(--start-date/--end-date or --date)."
        )

    from_date_flag = args.date is not None

    if from_date_flag:
        if has_start or has_end:
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
        has_start = has_end = True

    has_date_window = has_start or has_end
    if has_date_window:
        dag = get_db_dag(bundle_names=None, dag_id=args.dag_id)
        if has_start and has_end:
            lower = dag.timetable.localize_partition_datetime(args.start_date)
            upper = dag.timetable.localize_partition_datetime(args.end_date)
            if lower > upper:
                if from_date_flag:
                    raise SystemExit(
                        f"--date: the start of the range ({parts[0].strip()!r}) must be on or before "
                        f"the end ({parts[1].strip()!r})."
                    )
                raise SystemExit("--start-date must be on or before --end-date.")
    else:
        dag = None

    clear_tis = bool(args.clear_task_instances)

    processed_any = False
    ti_run_count = 0

    def _print_run(run: DagRun, had_partition_fields: bool) -> None:
        nonlocal processed_any, ti_run_count
        processed_any = True
        if clear_tis:
            ti_run_count += 1
        if had_partition_fields:
            print(
                f"DagRun {run.run_id}: "
                f"partition_key={run.partition_key!r} -> None, "
                f"partition_date={run.partition_date.isoformat() if run.partition_date else None} -> None"
            )
        elif not clear_tis:
            print(f"DagRun {run.run_id}: already cleared, skipping.")

    cleared, tis_cleared_total = clear_partition_runs(
        dag=dag,
        dag_id=args.dag_id,
        run_id=args.run_id,
        partition_key=args.partition_key,
        partition_date_start=args.start_date,
        partition_date_end=args.end_date,
        clear_tis=clear_tis,
        dry_run=args.dry_run,
        session=session,
        on_run_matched=_print_run,
    )

    if not processed_any:
        print(f"No matching DagRuns found for dag_id={args.dag_id}.")
        return

    if clear_tis:
        if args.dry_run:
            print(
                f"Dry run: would clear task instances on {ti_run_count} "
                f"DagRun(s) ({tis_cleared_total} task instance(s))."
            )
        else:
            print(
                f"Cleared task instances on {ti_run_count} DagRun(s) ({tis_cleared_total} task instance(s))."
            )

    if args.dry_run:
        print(f"Dry run: would clear {cleared} DagRun(s). No changes written.")
    else:
        print(f"Cleared partition fields on {cleared} DagRun(s).")
