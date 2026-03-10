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
Benchmark write performance on DagRun table with and without start_date/end_date indexes.

Tests both sequential and concurrent writes to simulate real scheduler behavior.

Usage:
    breeze run --backend postgres python dev/benchmark_dagrun_write.py
"""

from __future__ import annotations

import threading
import time
from datetime import timedelta

from sqlalchemy import text

from airflow._shared.timezones import timezone
from airflow.models.dag import DagModel
from airflow.models.dagbundle import DagBundleModel
from airflow.models.dagrun import DagRun
from airflow.utils import db
from airflow.utils.state import DagRunState
from airflow.utils.types import DagRunType

# BATCH_SIZE only applies to INSERT (bulk_save_objects is chunked in Python).
# UPDATE and DELETE are single SQL statements — Postgres handles all rows internally.
INSERT_BATCH_SIZE = 500
NUM_ROWS = 10000
DAG_ID = "write_bench_dag"

# Concurrent write config: simulate N scheduler processes each inserting ROWS_PER_SCHEDULER rows
CONCURRENT_SCHEDULERS = 8
ROWS_PER_SCHEDULER = 500  # ~realistic scheduler burst per loop


def ensure_dag(*, session):
    bundle = session.execute(
        text("SELECT name FROM dag_bundle WHERE name = 'write_bench_bundle'")
    ).scalar_one_or_none()
    if not bundle:
        session.add(DagBundleModel(name="write_bench_bundle", version="1.0"))
        session.flush()

    dag = session.execute(
        text("SELECT dag_id FROM dag WHERE dag_id = :dag_id").bindparams(dag_id=DAG_ID)
    ).scalar_one_or_none()
    if not dag:
        session.add(DagModel(dag_id=DAG_ID, bundle_name="write_bench_bundle"))
        session.flush()


def drop_test_indexes(*, session):
    with session.bind.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
        conn.execute(text("DROP INDEX IF EXISTS idx_dag_run_start_date"))
        conn.execute(text("DROP INDEX IF EXISTS idx_dag_run_end_date"))


def create_test_indexes(*, session):
    # Runs outside a transaction block — required for Postgres DDL without locking
    with session.bind.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_dag_run_start_date ON dag_run (start_date)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_dag_run_end_date ON dag_run (end_date)"))


def cleanup_bench_rows(*, session):
    session.execute(text("DELETE FROM dag_run WHERE dag_id LIKE 'write_bench%'"))


def build_dag_runs(n, dag_id, offset=0):
    base = timezone.utcnow() - timedelta(days=30)
    runs = []
    for i in range(n):
        run_date = base + timedelta(hours=offset + i)
        dr = DagRun(
            dag_id=dag_id,
            run_id=f"bench_run_{offset + i}",
            run_type=DagRunType.SCHEDULED.value,
            logical_date=run_date,
        )
        dr.state = DagRunState.SUCCESS
        dr.start_date = run_date
        dr.end_date = run_date + timedelta(minutes=30)
        dr.data_interval_start = run_date
        dr.data_interval_end = run_date + timedelta(minutes=30)
        runs.append(dr)
    return runs


# ---------------------------------------------------------------------------
# Sequential benchmarks
# Note: session.commit() is intentionally inside the timing window for
# bench_* functions — we are measuring full write cost including WAL flush
# and index maintenance at commit, which is the overhead we care about.
# ---------------------------------------------------------------------------


def bench_insert(*, session, label):
    cleanup_bench_rows(session=session)
    session.commit()
    runs = build_dag_runs(NUM_ROWS, DAG_ID)

    t0 = time.monotonic()
    for i in range(0, NUM_ROWS, INSERT_BATCH_SIZE):
        session.bulk_save_objects(runs[i : i + INSERT_BATCH_SIZE])
        session.flush()
    session.commit()
    elapsed = time.monotonic() - t0
    print(f"  INSERT {NUM_ROWS} rows [{label}]: {elapsed:.4f}s")
    return elapsed


def bench_update(*, session, label):
    # Single SQL statement — Postgres updates all rows internally
    new_end = timezone.utcnow()
    t0 = time.monotonic()
    session.execute(
        text("UPDATE dag_run SET end_date = :end_date WHERE dag_id = :dag_id").bindparams(
            end_date=new_end, dag_id=DAG_ID
        )
    )
    session.commit()
    elapsed = time.monotonic() - t0
    print(f"  UPDATE {NUM_ROWS} rows [{label}]: {elapsed:.4f}s")
    return elapsed


def bench_delete(*, session, label):
    # Single SQL statement — Postgres deletes all rows internally
    t0 = time.monotonic()
    session.execute(text("DELETE FROM dag_run WHERE dag_id = :dag_id").bindparams(dag_id=DAG_ID))
    session.commit()
    elapsed = time.monotonic() - t0
    print(f"  DELETE {NUM_ROWS} rows [{label}]: {elapsed:.4f}s")
    return elapsed


def run_sequential_suite(*, session, label):
    i = bench_insert(session=session, label=label)
    u = bench_update(session=session, label=label)
    d = bench_delete(session=session, label=label)
    return i, u, d


# ---------------------------------------------------------------------------
# Concurrent INSERT benchmark — simulates N schedulers inserting simultaneously
# ---------------------------------------------------------------------------


def _scheduler_worker(engine, scheduler_id, rows, results, errors):
    """Each thread owns its own session (simulates a separate scheduler process)."""
    from airflow.settings import Session  # lazy import for worker isolation

    session = Session(bind=engine)
    dag_id = f"write_bench_sched_{scheduler_id}"
    try:
        existing = session.execute(
            text("SELECT dag_id FROM dag WHERE dag_id = :dag_id").bindparams(dag_id=dag_id)
        ).scalar_one_or_none()
        if not existing:
            session.add(DagModel(dag_id=dag_id, bundle_name="write_bench_bundle"))
            session.commit()

        runs = build_dag_runs(rows, dag_id, offset=scheduler_id * rows)

        t0 = time.monotonic()
        for i in range(0, rows, INSERT_BATCH_SIZE):
            session.bulk_save_objects(runs[i : i + INSERT_BATCH_SIZE])
            session.flush()
        session.commit()
        results[scheduler_id] = time.monotonic() - t0
    except Exception as e:
        errors[scheduler_id] = str(e)
        session.rollback()
    finally:
        session.close()


def bench_concurrent_insert(engine, label):
    results = [None] * CONCURRENT_SCHEDULERS
    errors = {}

    threads = [
        threading.Thread(
            target=_scheduler_worker,
            args=(engine, i, ROWS_PER_SCHEDULER, results, errors),
        )
        for i in range(CONCURRENT_SCHEDULERS)
    ]

    wall_start = time.monotonic()
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    wall_elapsed = time.monotonic() - wall_start

    if errors:
        print(f"  CONCURRENT INSERT [{label}]: ERRORS — {errors}")
        return None, None

    avg_per_thread = sum(r for r in results if r is not None) / CONCURRENT_SCHEDULERS
    total_rows = CONCURRENT_SCHEDULERS * ROWS_PER_SCHEDULER
    print(
        f"  CONCURRENT INSERT {CONCURRENT_SCHEDULERS} schedulers x {ROWS_PER_SCHEDULER} rows "
        f"= {total_rows} total [{label}]: "
        f"wall={wall_elapsed:.4f}s  avg-per-scheduler={avg_per_thread:.4f}s"
    )
    return wall_elapsed, avg_per_thread


def main():
    print("=" * 70)
    print("DagRun Write Performance Benchmark (Postgres)")
    print(f"Sequential: {NUM_ROWS} rows  |  INSERT batch: {INSERT_BATCH_SIZE}")
    print(f"Concurrent: {CONCURRENT_SCHEDULERS} schedulers x {ROWS_PER_SCHEDULER} rows each")
    print("=" * 70)

    db.initdb()
    from airflow.settings import Session  # lazy import for worker isolation

    session = Session()
    engine = session.bind

    try:
        ensure_dag(session=session)
        session.commit()

        # ---- Sequential: without indexes ----
        print("\nDropping test indexes (if present)...")
        drop_test_indexes(session=session)

        print("\nWarm-up (no indexes)...")
        run_sequential_suite(session=session, label="warm-up")

        print("\nIteration 1 (no indexes):")
        r1_no = run_sequential_suite(session=session, label="no indexes")
        print("\nIteration 2 (no indexes):")
        r2_no = run_sequential_suite(session=session, label="no indexes")
        avg_no = tuple((a + b) / 2 for a, b in zip(r1_no, r2_no))

        # ---- Sequential: with indexes ----
        print("\nCreating test indexes...")
        create_test_indexes(session=session)

        print("\nWarm-up (with indexes)...")
        run_sequential_suite(session=session, label="warm-up")

        print("\nIteration 1 (with indexes):")
        r1_wi = run_sequential_suite(session=session, label="with indexes")
        print("\nIteration 2 (with indexes):")
        r2_wi = run_sequential_suite(session=session, label="with indexes")
        avg_wi = tuple((a + b) / 2 for a, b in zip(r1_wi, r2_wi))

        # ---- Sequential results ----
        print("\n" + "=" * 70)
        print("SEQUENTIAL RESULTS")
        print("=" * 70)
        for op, no, wi in zip(("INSERT", "UPDATE", "DELETE"), avg_no, avg_wi):
            overhead = ((wi - no) / no) * 100
            sign = "+" if overhead >= 0 else ""
            print(f"  {op:6s}  no-index: {no:.4f}s  with-index: {wi:.4f}s  overhead: {sign}{overhead:.1f}%")

        # ---- Concurrent INSERT: without indexes ----
        print("\n" + "=" * 70)
        print("CONCURRENT INSERT (simulating multiple schedulers)")
        print("=" * 70)
        cleanup_bench_rows(session=session)
        session.commit()

        print("\nDropping test indexes...")
        drop_test_indexes(session=session)

        print("\nWarm-up (no indexes)...")
        bench_concurrent_insert(engine, "warm-up")
        cleanup_bench_rows(session=session)
        session.commit()

        print("\nIteration 1 (no indexes):")
        wall1_no, avg1_no = bench_concurrent_insert(engine, "no indexes")
        cleanup_bench_rows(session=session)
        session.commit()
        print("\nIteration 2 (no indexes):")
        wall2_no, avg2_no = bench_concurrent_insert(engine, "no indexes")
        cleanup_bench_rows(session=session)
        session.commit()

        # ---- Concurrent INSERT: with indexes ----
        print("\nCreating test indexes...")
        create_test_indexes(session=session)

        print("\nWarm-up (with indexes)...")
        bench_concurrent_insert(engine, "warm-up")
        cleanup_bench_rows(session=session)
        session.commit()

        print("\nIteration 1 (with indexes):")
        wall1_wi, avg1_wi = bench_concurrent_insert(engine, "with indexes")
        cleanup_bench_rows(session=session)
        session.commit()
        print("\nIteration 2 (with indexes):")
        wall2_wi, avg2_wi = bench_concurrent_insert(engine, "with indexes")
        cleanup_bench_rows(session=session)
        session.commit()

        # ---- Concurrent results ----
        avg_wall_no = (wall1_no + wall2_no) / 2
        avg_wall_wi = (wall1_wi + wall2_wi) / 2
        avg_thread_no = (avg1_no + avg2_no) / 2
        avg_thread_wi = (avg1_wi + avg2_wi) / 2

        wall_overhead = ((avg_wall_wi - avg_wall_no) / avg_wall_no) * 100
        thread_overhead = ((avg_thread_wi - avg_thread_no) / avg_thread_no) * 100

        print("\n" + "=" * 70)
        print("CONCURRENT INSERT RESULTS")
        print("=" * 70)
        print(
            f"  Wall time   no-index: {avg_wall_no:.4f}s  with-index: {avg_wall_wi:.4f}s  "
            f"overhead: {'+' if wall_overhead >= 0 else ''}{wall_overhead:.1f}%"
        )
        print(
            f"  Per-sched   no-index: {avg_thread_no:.4f}s  with-index: {avg_thread_wi:.4f}s  "
            f"overhead: {'+' if thread_overhead >= 0 else ''}{thread_overhead:.1f}%"
        )
        print("=" * 70)

    finally:
        cleanup_bench_rows(session=session)
        session.commit()
        drop_test_indexes(session=session)
        session.close()


if __name__ == "__main__":
    main()
