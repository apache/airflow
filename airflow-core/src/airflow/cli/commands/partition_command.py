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

from airflow.models.dagrun import DagRun
from airflow.utils import cli as cli_utils
from airflow.utils.providers_configuration_loader import providers_configuration_loaded
from airflow.utils.session import NEW_SESSION, provide_session

if TYPE_CHECKING:
    from sqlalchemy.orm import Session


@cli_utils.action_cli
@providers_configuration_loaded
@provide_session
def clear(args, *, session: Session = NEW_SESSION) -> None:
    """Clear the partition_key and partition_date of matching DagRuns."""
    has_range = args.start_date is not None or args.end_date is not None
    if not args.run_id and not has_range:
        raise SystemExit(
            "Specify either --run-id for a single DagRun or a partition_date range "
            "via --start-date / --end-date."
        )
    if args.run_id and has_range:
        raise SystemExit("--run-id cannot be combined with --start-date / --end-date.")

    stmt = select(DagRun).where(DagRun.dag_id == args.dag_id)
    if args.run_id:
        stmt = stmt.where(DagRun.run_id == args.run_id)
    else:
        stmt = stmt.where(or_(DagRun.partition_key.is_not(None), DagRun.partition_date.is_not(None)))
        if args.start_date is not None:
            stmt = stmt.where(DagRun.partition_date >= args.start_date)
        if args.end_date is not None:
            stmt = stmt.where(DagRun.partition_date <= args.end_date)
    stmt = stmt.order_by(DagRun.partition_date, DagRun.run_id)

    runs = session.scalars(stmt).all()
    if not runs:
        print(f"No matching DagRuns found for dag_id={args.dag_id}.")
        return

    cleared = 0
    for run in runs:
        if run.partition_key is None and run.partition_date is None:
            print(f"DagRun {run.run_id}: already cleared, skipping.")
            continue
        print(
            f"DagRun {run.run_id}: "
            f"partition_key={run.partition_key!r} -> None, "
            f"partition_date={run.partition_date.isoformat() if run.partition_date else None} -> None"
        )
        if not args.dry_run:
            run.partition_key = None
            run.partition_date = None
        cleared += 1

    if args.dry_run:
        print(f"Dry run: would clear {cleared} DagRun(s). No changes written.")
    else:
        print(f"Cleared partition fields on {cleared} DagRun(s).")
