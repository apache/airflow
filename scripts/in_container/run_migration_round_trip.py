#!/usr/bin/env python
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
"""
Migration round-trip regression check — intended to run inside Breeze.

Walks every revision base → head (upgrade direction) and head → base
(downgrade direction) one rev at a time, on a single fresh SQLite DB
with ``PRAGMA foreign_keys = ON``.  Before each step the seed fixture is
idempotently restored so any FK chain a rebuild needs to fire is in
place.

Per-migration walking is essential.  ``airflow_db.upgradedb`` /
``airflow_db.downgrade`` reconfigures Airflow's engine via
``_single_connection_pool`` for every call, so each migration starts
on a freshly-connected SQLAlchemy engine where ``setup_event_handlers``
fires ``PRAGMA foreign_keys=ON``.  In contrast a single-call
``upgradedb(to_revision=tip)`` walks all migrations on one shared
connection: once an earlier migration's ``disable_sqlite_fkeys`` flips
FK off on that connection, the off state simply persists into later
migrations, silently masking broken or missing wrappers downstream.

The placement convention every migration must follow (only DML breaks
PRAGMA, but the safe rule is strictly stronger) and the FK chains
exercised by the seed are documented in
``contributing-docs/26_migration_round_trip_check.rst``.
"""

from __future__ import annotations

import os
import sys
import tempfile
from collections.abc import Callable
from pathlib import Path
from typing import Any

from alembic.script import ScriptDirectory
from rich.console import Console
from rich.panel import Panel
from rich.rule import Rule
from sqlalchemy import text

console = Console(width=140, color_system="standard", highlight=False)

SEED_DAG_ID = "round_trip_dag"
SEED_DAG_VERSION_ID = "abcdef0123456789abcdef0123456789"
SEED_BUNDLE_NAME = "dags-folder"
SEED_RUN_ID = "manual_run_1"
SEED_TASK_ID = "task1"
SEED_TASK_INSTANCE_ID = "fedcba9876543210fedcba9876543210"

# Hardcoded INSERT values for tables we seed.  Each entry maps column name
# to a SQL literal.  When a column is not present in the live schema (e.g.
# an older revision predating its addition), it is dropped from the INSERT
# — provided every NOT-NULL-no-default column at that revision is covered.
# When a future migration introduces a brand-new NOT NULL column without a
# default, ``_insert_or_skip`` raises with a clear message pointing at the
# table that needs an entry here.
SEED_VALUES: dict[str, dict[str, str]] = {
    "log_template": {
        "id": "1",
        "filename": "'{{ ti.dag_id }}/{{ ts }}.log'",
        "elasticsearch_id": "'{{ ti.dag_id }}-{{ ts }}'",
        "created_at": "'2024-01-01 00:00:00.000000+00:00'",
    },
    "dag_bundle": {
        "name": f"'{SEED_BUNDLE_NAME}'",
        "active": "1",
        "version": "NULL",
        "last_refreshed": "NULL",
        "signed_url_template": "NULL",
        "url_template": "NULL",
        "bundle_template_params": "NULL",
    },
    "dag": {
        "dag_id": f"'{SEED_DAG_ID}'",
        "max_active_tasks": "16",
        "has_task_concurrency_limits": "0",
        "is_paused": "0",
        "is_active": "1",
        "is_stale": "0",
        "fileloc": "'/tmp/round_trip_dag.py'",
        "fail_fast": "0",
        "max_consecutive_failed_dag_runs": "0",
        "bundle_name": f"'{SEED_BUNDLE_NAME}'",
        "has_import_errors": "0",
        "max_active_runs": "16",
        "_default_view": "'grid'",
        "exceeds_max_non_backfill": "0",
        "timetable_type": "'cron'",
        "timetable_partitioned": "0",
        "timetable_periodic": "0",
    },
    "dag_version": {
        "id": f"'{SEED_DAG_VERSION_ID}'",
        "version_number": "1",
        "dag_id": f"'{SEED_DAG_ID}'",
        "bundle_name": f"'{SEED_BUNDLE_NAME}'",
        "bundle_version": "NULL",
        "created_at": "'2024-01-01 00:00:00+00:00'",
        "last_updated": "'2024-01-01 00:00:00+00:00'",
    },
    "dag_run": {
        "id": "1",
        "dag_id": f"'{SEED_DAG_ID}'",
        "execution_date": "'2024-01-01 00:00:00.000000+00:00'",
        "logical_date": "'2024-01-01 00:00:00.000000+00:00'",
        "run_id": f"'{SEED_RUN_ID}'",
        "run_type": "'manual'",
        "state": "'success'",
        "log_template_id": "1",
        "run_after": "'2024-01-01 00:00:00+00:00'",
        "span_status": "'not_started'",
        "clear_number": "0",
    },
    "task_instance": {
        "id": f"'{SEED_TASK_INSTANCE_ID}'",
        "task_id": f"'{SEED_TASK_ID}'",
        "dag_id": f"'{SEED_DAG_ID}'",
        "run_id": f"'{SEED_RUN_ID}'",
        "map_index": "-1",
        "pool": "'default'",
        "pool_slots": "1",
        "state": "'success'",
        "try_number": "1",
        "max_tries": "0",
    },
}

# FK-dependency-respecting insertion order.
SEED_ORDER = ("log_template", "dag_bundle", "dag", "dag_version", "dag_run", "task_instance")


def _make_fresh_db() -> Path:
    """Create a fresh SQLite file and re-point Airflow's engine at it."""
    fd, path = tempfile.mkstemp(prefix="round_trip_", suffix=".db")
    os.close(fd)
    db = Path(path)
    db.unlink()  # Airflow creates the file on first connect

    new_conn = f"sqlite:///{db}"
    os.environ["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"] = new_conn

    # Delayed: AIRFLOW__DATABASE__SQL_ALCHEMY_CONN must be set in the environment
    # before Airflow initialises its configuration, so settings.configure_orm()
    # picks up the fresh DB path rather than whatever was in the environment at
    # process start.
    from airflow import settings

    settings.SQL_ALCHEMY_CONN = new_conn
    settings.dispose_orm()
    settings.configure_orm()
    return db


def _step(msg: str) -> None:
    console.print()
    console.print(Rule(f"[bold cyan]{msg}[/]", style="cyan"))


def _table_info(conn, table: str) -> list[tuple[Any, ...]]:
    return list(conn.execute(text(f"PRAGMA table_info('{table}')")).fetchall())


def _table_exists(conn, table: str) -> bool:
    return bool(
        conn.execute(
            text("SELECT 1 FROM sqlite_master WHERE type='table' AND name=:n"),
            {"n": table},
        ).scalar()
    )


def _insert_or_skip(conn, table: str) -> None:
    """Insert a seed row into ``table`` if the table exists and the row is missing.

    Only columns present in the live schema and listed in ``SEED_VALUES`` are
    included.  Raises if a NOT-NULL-no-default column at the live revision is
    not in ``SEED_VALUES``.
    """
    if not _table_exists(conn, table):
        return

    info = _table_info(conn, table)
    live_cols = {r[1] for r in info}
    not_null_no_default = {r[1] for r in info if r[3] and r[4] is None}

    seed = SEED_VALUES[table]
    use = {k: v for k, v in seed.items() if k in live_cols}
    missing = not_null_no_default - set(use)
    if missing:
        raise RuntimeError(
            f"Cannot seed `{table}`: NOT-NULL columns without defaults are missing seed values: "
            f"{sorted(missing)}. Add them to SEED_VALUES['{table}'] in this script."
        )

    key = "dag_id" if "dag_id" in use else ("id" if "id" in use else next(iter(use)))
    if conn.execute(text(f"SELECT 1 FROM {table} WHERE {key}={use[key]}")).scalar():
        return

    cols = ", ".join(use)
    vals = ", ".join(use.values())
    conn.execute(text(f"INSERT INTO {table} ({cols}) VALUES ({vals})"))


def _seed_for_current_schema(get_engine: Callable[[], Any]) -> None:
    """Idempotently seed every table the live schema can accept.

    Run before each migration step so the seed is in place to fire FK-cascade
    chains during the rebuild that step performs (if any).
    """
    with get_engine().begin() as conn:
        for table in SEED_ORDER:
            _insert_or_skip(conn, table)

        ti_cols = (
            {r[1] for r in _table_info(conn, "task_instance")}
            if _table_exists(conn, "task_instance")
            else set()
        )
        if "dag_version_id" in ti_cols and _table_exists(conn, "dag_version"):
            conn.execute(
                text(
                    "UPDATE task_instance SET dag_version_id = :dvid "
                    "WHERE task_id = :tid AND dag_id = :did AND dag_version_id IS NULL"
                ),
                {"dvid": SEED_DAG_VERSION_ID, "tid": SEED_TASK_ID, "did": SEED_DAG_ID},
            )


def _all_revisions() -> list[str]:
    """Return every migration revision in topological order, base → head."""
    from airflow.utils.db import _get_alembic_config

    cfg = _get_alembic_config()
    script = ScriptDirectory.from_config(cfg)
    return [r.revision for r in reversed(list(script.walk_revisions(base="base", head="heads")))]


def main() -> None:
    revs = _all_revisions()
    console.print(
        Panel(
            f"Walking {len(revs)} revisions: base → head, then head → base.",
            title="migration round-trip",
            border_style="cyan",
            expand=False,
        )
    )

    db = _make_fresh_db()
    try:
        # settings was imported and configured by _make_fresh_db(); importing
        # it here just retrieves the already-initialised module from sys.modules.
        from airflow import settings
        from airflow.utils import db as airflow_db
        from airflow.utils.db import _SKIP_EXTERNAL_DB_MANAGERS_UPGRADE

        token = _SKIP_EXTERNAL_DB_MANAGERS_UPGRADE.set(True)
        try:
            _step(f"Walk UP base → {revs[-1][:8]}…")
            for rev in revs:
                try:
                    # Pass settings.get_engine (not the engine itself) so each
                    # call picks up the engine that upgradedb/downgrade may have
                    # just reconfigured via _single_connection_pool.
                    _seed_for_current_schema(settings.get_engine)
                except Exception as exc:
                    _fail("seed", rev, exc)
                    return
                try:
                    airflow_db.upgradedb(to_revision=rev)
                except Exception as exc:
                    _fail("up", rev, exc)
                    return

            _step(f"Walk DOWN {revs[-1][:8]}… → {revs[0][:8]}…")
            # Stop at the first migration (the squashed-migrations file).  Going
            # all the way to ``base`` means executing the squashed migration's
            # downgrade, which "drop everything we created" can fail on objects
            # that later migrations have already renamed or removed (e.g. the
            # ``sm_dag`` index).  That's a pre-existing limitation of the
            # squashed-migrations file, not a FK round-trip concern.
            down_revs = list(reversed(revs[1:]))
            down_targets = list(reversed(revs[:-1]))
            for downgrading_rev, target in zip(down_revs, down_targets):
                try:
                    _seed_for_current_schema(settings.get_engine)
                except Exception as exc:
                    _fail("seed", downgrading_rev, exc)
                    return
                try:
                    airflow_db.downgrade(to_revision=target)
                except Exception as exc:
                    _fail("down", downgrading_rev, exc)
                    return

            console.print()
            console.print(
                Panel(
                    f"[bold green]PASS[/] — {len(revs)} revisions round-tripped clean",
                    border_style="green",
                    expand=False,
                )
            )
        finally:
            _SKIP_EXTERNAL_DB_MANAGERS_UPGRADE.reset(token)
    finally:
        db.unlink(missing_ok=True)


def _fail(direction: str, rev: str, exc: Exception) -> None:
    console.print()
    console.print(
        Panel(
            f"[bold red]{direction.upper()} {rev}[/]\n"
            f"  {type(exc).__name__}: {exc}\n\n"
            "[dim]See contributing-docs/26_migration_round_trip_check.rst for the "
            "disable_sqlite_fkeys placement convention.[/]",
            title="[bold red]FAIL[/]",
            border_style="red",
            expand=False,
        )
    )
    sys.exit(1)


if __name__ == "__main__":
    main()
