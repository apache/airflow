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
Benchmark: get_task_instances query — before vs after fix for #62027.

Demonstrates the duplicate-JOIN elimination by:
  1. Showing the SQL JOIN structure of both queries.
  2. Running EXPLAIN ANALYZE (PostgreSQL) or timing (SQLite) both queries.

Usage (inside Breeze):
    # SQLite (default):
    python dev/benchmark_get_task_instances.py [--runs N] [--repeats N]

    # PostgreSQL (shows row-multiplication effect clearly):
    breeze run --backend postgres python dev/benchmark_get_task_instances.py --runs 2000 --repeats 5
"""

from __future__ import annotations

import argparse
import re
import time
import uuid

from sqlalchemy import select, text
from sqlalchemy.orm import contains_eager, joinedload

from airflow.api_fastapi.common.db.task_instances import eager_load_TI_and_TIH_for_validation
from airflow.dag_processing.bundles.manager import DagBundlesManager
from airflow.dag_processing.dagbag import DagBag, sync_bag_to_db
from airflow.models.dag_version import DagVersion
from airflow.models.dagrun import DagRun
from airflow.models.taskinstance import TaskInstance as TI
from airflow.utils.db import resetdb
from airflow.utils.session import create_session
from airflow.utils.state import DagRunState
from airflow.utils.types import DagRunType

DAG_ID = "example_python_operator"


def get_dialect_name(session) -> str:
    return session.bind.dialect.name  # "sqlite" or "postgresql"


def setup_db() -> None:
    print("Resetting DB and loading example DAGs...")
    resetdb(skip_init=False)
    DagBundlesManager().sync_bundles_to_db()
    dagbag = DagBag(include_examples=True)
    with create_session() as session:
        sync_bag_to_db(dagbag, "dags-folder", None, session=session)
    n = sum(1 for _ in dagbag.dags)
    print(f"  Loaded {n} example DAGs")


def seed_runs(n_runs: int) -> int:
    """
    Create DagRuns + TaskInstances.
    - SQLite: disables FK checks via PRAGMA for synthetic data.
    - PostgreSQL: inserts in FK-satisfying order (DagModel+DagVersion already exist
      after sync_bag_to_db, so DagRun → TI order is sufficient).
    """
    import pickle

    empty_config = pickle.dumps({})

    with create_session() as session:
        dialect = get_dialect_name(session)

        dag_version = DagVersion.get_latest_version(DAG_ID, session=session)
        if dag_version is None:
            raise RuntimeError(f"DagVersion for '{DAG_ID}' not found — did sync_bag_to_db run?")

        # Use a fixed set of synthetic task_ids (no real task objects needed)
        task_ids = [f"bench_task_{i}" for i in range(10)]

        if dialect == "sqlite":
            # Disable FK checks for seeding synthetic data (SQLite / benchmark only)
            session.execute(text("PRAGMA foreign_keys = OFF"))
        elif dialect == "postgresql":
            # Disable FK triggers for the session to allow synthetic task_ids
            session.execute(text("SET session_replication_role = replica"))

        total = 0
        for i in range(n_runs):
            run_id = f"bench_run_{i}_{uuid.uuid4().hex[:6]}"
            dr = DagRun(
                run_id=run_id,
                dag_id=DAG_ID,
                run_type=DagRunType.MANUAL,
                state=DagRunState.SUCCESS,
            )
            dr.dag_version_id = dag_version.id
            session.add(dr)
            session.flush()

            for task_id in task_ids:
                session.execute(
                    text(
                        "INSERT INTO task_instance "
                        "(id, task_id, dag_id, run_id, map_index, state, try_number, max_tries, "
                        " hostname, unixname, pool, pool_slots, queue, priority_weight, "
                        " custom_operator_name, executor_config, span_status, dag_version_id) "
                        "VALUES (:id, :task_id, :dag_id, :run_id, -1, 'success', 1, 0, "
                        "        '', '', 'default_pool', 1, 'default', 1, '', :cfg, "
                        "        'NOT_STARTED', :dag_version_id)"
                    ),
                    {
                        "id": str(uuid.uuid4()),
                        "task_id": task_id,
                        "dag_id": DAG_ID,
                        "run_id": run_id,
                        "cfg": empty_config,
                        "dag_version_id": str(dag_version.id),
                    },
                )
                total += 1

        session.commit()
    return total


def build_before_query():
    """Original: joinedload triggers duplicate JOINs on already-joined tables."""
    return (
        select(TI).join(TI.dag_run).outerjoin(TI.dag_version).options(*eager_load_TI_and_TIH_for_validation())
    )


def build_after_query():
    """Fixed: contains_eager reuses existing JOINs — no duplicates."""
    return (
        select(TI)
        .join(TI.dag_run)
        .outerjoin(TI.dag_version)
        .options(
            contains_eager(TI.dag_run).joinedload(DagRun.dag_model),
            contains_eager(TI.dag_version).joinedload(DagVersion.bundle),
            joinedload(TI.task_instance_note),
        )
    )


def analyse_sql(label: str, query, dialect_name: str = "sqlite") -> None:
    if dialect_name == "postgresql":
        from sqlalchemy.dialects import postgresql

        sql = str(query.compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}))
    else:
        from sqlalchemy.dialects import sqlite

        sql = str(query.compile(dialect=sqlite.dialect(), compile_kwargs={"literal_binds": True}))
    upper = sql.upper()
    n_joins = upper.count(" JOIN ")
    joined_tables = re.findall(r"JOIN\s+(\w+)(?:\s+AS\s+\w+)?", sql, re.IGNORECASE)
    print(f"\n  [{label}] — {n_joins} JOINs")
    for tbl in joined_tables:
        print(f"    + {tbl}")


def explain_analyze(session, query, label: str) -> None:
    """Run EXPLAIN ANALYZE and print the plan — PostgreSQL only."""
    from sqlalchemy.dialects import postgresql

    compiled_sql = str(query.compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}))
    result = session.execute(text(f"EXPLAIN ANALYZE {compiled_sql}"))
    print(f"\n[EXPLAIN ANALYZE — {label}]")
    for row in result:
        print(" ", row[0])


def time_query(session, query, label: str, repeats: int) -> float:
    # Warm-up
    list(session.scalars(query))
    session.expunge_all()

    times = []
    for _ in range(repeats):
        t0 = time.perf_counter()
        results = list(session.scalars(query))
        elapsed = time.perf_counter() - t0
        times.append(elapsed)
        session.expunge_all()

    avg = sum(times) / len(times)
    mn, mx = min(times), max(times)
    print(
        f"  {label}: avg={avg * 1000:6.1f}ms  min={mn * 1000:5.1f}ms  max={mx * 1000:5.1f}ms"
        f"  ({len(results)} rows, {repeats} repeats)"
    )
    return avg


def main():
    parser = argparse.ArgumentParser(description="Benchmark get_task_instances query (#62027 fix)")
    parser.add_argument("--runs", type=int, default=200, help="DAG runs to seed (default: 200)")
    parser.add_argument("--repeats", type=int, default=5, help="Timing repeats per query (default: 5)")
    args = parser.parse_args()

    setup_db()

    print(f"\nSeeding {args.runs} DAG runs...")
    total = seed_runs(args.runs)
    print(f"  Seeded {total} task instances total")

    before_q = build_before_query()
    after_q = build_after_query()

    with create_session() as session:
        dialect_name = get_dialect_name(session)

    print(f"\n--- SQL structure (dialect: {dialect_name}) ---")
    analyse_sql("BEFORE (joinedload — duplicate JOINs)", before_q, dialect_name)
    analyse_sql("AFTER  (contains_eager — no duplicates)", after_q, dialect_name)

    if dialect_name == "postgresql":
        print(f"\n--- EXPLAIN ANALYZE on {total} rows ---")
        with create_session() as session:
            explain_analyze(session, before_q, "BEFORE (joinedload — duplicate JOINs)")
            explain_analyze(session, after_q, "AFTER  (contains_eager — no duplicates)")

    print(f"\n--- Timing on {total} rows ({args.repeats} repeats) ---")
    with create_session() as session:
        avg_before = time_query(session, before_q, "BEFORE", args.repeats)
        avg_after = time_query(session, after_q, "AFTER ", args.repeats)

    if avg_after > 0:
        ratio = avg_before / avg_after
        faster = ratio >= 1
        print(
            f"\n  AFTER is {ratio:.2f}x {'faster' if faster else 'slower'} than BEFORE  "
            f"({avg_before * 1000:.1f}ms → {avg_after * 1000:.1f}ms)"
        )
        if dialect_name == "sqlite":
            print(
                "  (SQLite shows small gains; row-multiplication effect is much larger on PostgreSQL at scale)"
            )
        else:
            print("  (PostgreSQL shows the full row-multiplication effect at scale)")


if __name__ == "__main__":
    main()
