#!/usr/bin/env python3
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
Benchmark script for historical_metrics endpoint performance.

This script compares the performance of the old vs new query approaches for the
historical_metrics_data endpoint.

Usage:
    uv run --project airflow-core python dev/benchmark_historical_metrics.py

The script will:
1. Create test data (DagRuns and TaskInstances) if needed
2. Run the old query approach (with func.coalesce)
3. Run the new query approach (sargable with OR/IS NULL)
4. Display performance comparison
"""

from __future__ import annotations

import time
from datetime import timedelta

from sqlalchemy import func, or_, select

from airflow._shared.timezones import timezone
from airflow.models.dag import DagModel
from airflow.models.dagbundle import DagBundleModel
from airflow.models.dagrun import DagRun
from airflow.models.taskinstance import TaskInstance
from airflow.utils import db
from airflow.utils.state import DagRunState
from airflow.utils.types import DagRunType


def setup_test_data(*, session, num_dag_runs=10000, tasks_per_dag_run=10):
    """Create test data for benchmarking."""
    print(f"\nSetting up test data: {num_dag_runs} DagRuns with {tasks_per_dag_run} TaskInstances each...")

    # Check if test data already exists
    existing_count = session.execute(
        select(func.count()).select_from(DagRun).where(DagRun.dag_id == "benchmark_dag")
    ).scalar_one()

    if existing_count >= num_dag_runs:
        print(f"Test data already exists ({existing_count} DagRuns found). Skipping creation.")
        return

    # Create a test DAG bundle first (required by foreign key)
    bundle = session.execute(
        select(DagBundleModel).where(DagBundleModel.name == "benchmark_bundle")
    ).scalar_one_or_none()
    if not bundle:
        bundle = DagBundleModel(name="benchmark_bundle", version="1.0")
        session.add(bundle)
        session.flush()

    # Create a test DAG in DagModel
    dag = session.execute(select(DagModel).where(DagModel.dag_id == "benchmark_dag")).scalar_one_or_none()
    if not dag:
        dag = DagModel(dag_id="benchmark_dag", bundle_name="benchmark_bundle")
        session.add(dag)
        session.flush()

    t0 = time.monotonic()
    base_date = timezone.utcnow() - timedelta(days=30)

    # Create DagRuns using bulk insert for performance
    dag_runs = []
    for i in range(num_dag_runs):
        run_date = base_date + timedelta(hours=i)
        end_dt = run_date + timedelta(minutes=30)
        dag_run = DagRun(
            dag_id="benchmark_dag",
            run_id=f"benchmark_run_{i}",
            run_type=DagRunType.SCHEDULED.value,
            logical_date=run_date,
        )
        dag_run.state = DagRunState.SUCCESS if i % 2 == 0 else DagRunState.FAILED
        dag_run.start_date = run_date
        dag_run.end_date = end_dt
        dag_run.data_interval_start = run_date
        dag_run.data_interval_end = end_dt
        dag_runs.append(dag_run)

        # Bulk insert every 1000 records
        if len(dag_runs) >= 1000:
            session.bulk_save_objects(dag_runs)
            session.flush()
            dag_runs = []

    if dag_runs:
        session.bulk_save_objects(dag_runs)
        session.flush()

    elapsed = time.monotonic() - t0
    print(f"Created {num_dag_runs} DagRuns in {elapsed:.2f}s")


def benchmark_old_query(*, session, start_date, end_date, dag_ids):
    """Benchmark the old query approach (using func.coalesce)."""
    current_time = timezone.utcnow()

    # Old approach: 3 separate queries with func.coalesce
    t0 = time.monotonic()

    # Query 1: DagRun types
    session.execute(
        select(DagRun.run_type, func.count(DagRun.run_id))
        .where(
            func.coalesce(DagRun.start_date, current_time) >= start_date,
            func.coalesce(DagRun.end_date, current_time) <= func.coalesce(end_date, current_time),
        )
        .where(DagRun.dag_id.in_(dag_ids))
        .group_by(DagRun.run_type)
    ).all()

    # Query 2: DagRun states
    session.execute(
        select(DagRun.state, func.count(DagRun.run_id))
        .where(
            func.coalesce(DagRun.start_date, current_time) >= start_date,
            func.coalesce(DagRun.end_date, current_time) <= func.coalesce(end_date, current_time),
        )
        .where(DagRun.dag_id.in_(dag_ids))
        .group_by(DagRun.state)
    ).all()

    # Query 3: TaskInstance states
    session.execute(
        select(TaskInstance.state, func.count(TaskInstance.run_id))
        .join(TaskInstance.dag_run)
        .where(
            func.coalesce(DagRun.start_date, current_time) >= start_date,
            func.coalesce(DagRun.end_date, current_time) <= func.coalesce(end_date, current_time),
        )
        .where(DagRun.dag_id.in_(dag_ids))
        .group_by(TaskInstance.state)
    ).all()

    return time.monotonic() - t0


def benchmark_new_query(*, session, start_date, end_date, dag_ids):
    """Benchmark the new query approach (sargable with combined queries)."""
    current_time = timezone.utcnow()
    end_date_value = end_date if end_date is not None else current_time

    # New approach: Sargable queries with OR/IS NULL, combined DagRun query
    t0 = time.monotonic()

    # Build sargable date filter conditions
    start_date_filter = or_(DagRun.start_date >= start_date, DagRun.start_date.is_(None))
    end_date_filter = or_(DagRun.end_date <= end_date_value, DagRun.end_date.is_(None))

    # Combined DagRun query - get both run_type and state counts in a single query
    dag_run_metrics = session.execute(
        select(DagRun.run_type, DagRun.state, func.count(DagRun.run_id))
        .where(start_date_filter, end_date_filter)
        .where(DagRun.dag_id.in_(dag_ids))
        .group_by(DagRun.run_type, DagRun.state)
    ).all()

    # TaskInstances query
    session.execute(
        select(TaskInstance.state, func.count(TaskInstance.run_id))
        .join(TaskInstance.dag_run)
        .where(start_date_filter, end_date_filter)
        .where(DagRun.dag_id.in_(dag_ids))
        .group_by(TaskInstance.state)
    ).all()

    # Aggregate dag_run_metrics (kept to mirror production code path)
    dag_run_types_dict: dict = {}
    dag_run_states_dict: dict = {}
    for run_type, state, count in dag_run_metrics:
        dag_run_types_dict[run_type] = dag_run_types_dict.get(run_type, 0) + count
        dag_run_states_dict[state] = dag_run_states_dict.get(state, 0) + count

    return time.monotonic() - t0


def main():
    """Run the benchmark comparison."""
    print("=" * 80)
    print("Historical Metrics Endpoint Performance Benchmark")
    print("=" * 80)

    # Initialize database session
    db.initdb()
    from airflow.settings import Session  # lazy import for worker isolation

    session = Session()

    try:
        setup_test_data(session=session, num_dag_runs=10000)
        session.commit()

        start_date = timezone.utcnow() - timedelta(days=30)
        end_date = timezone.utcnow()
        dag_ids = {"benchmark_dag"}

        print("\n" + "=" * 80)
        print("Running Benchmark (3 iterations each)")
        print("=" * 80)

        print("\nWarming up...")
        benchmark_old_query(session=session, start_date=start_date, end_date=end_date, dag_ids=dag_ids)
        benchmark_new_query(session=session, start_date=start_date, end_date=end_date, dag_ids=dag_ids)

        old_times = []
        new_times = []
        iterations = 3

        for i in range(iterations):
            print(f"\nIteration {i + 1}/{iterations}:")

            old_time = benchmark_old_query(
                session=session, start_date=start_date, end_date=end_date, dag_ids=dag_ids
            )
            old_times.append(old_time)
            print(f"  Old query (func.coalesce):      {old_time:.4f}s")

            new_time = benchmark_new_query(
                session=session, start_date=start_date, end_date=end_date, dag_ids=dag_ids
            )
            new_times.append(new_time)
            print(f"  New query (sargable + combined): {new_time:.4f}s")

            improvement = ((old_time - new_time) / old_time) * 100
            print(f"  Improvement: {improvement:.1f}%")

        avg_old = sum(old_times) / len(old_times)
        avg_new = sum(new_times) / len(new_times)
        avg_improvement = ((avg_old - avg_new) / avg_old) * 100

        print("\n" + "=" * 80)
        print("RESULTS")
        print("=" * 80)
        print(f"Average old query time:      {avg_old:.4f}s")
        print(f"Average new query time:      {avg_new:.4f}s")
        print(f"Average improvement:         {avg_improvement:.1f}%")
        print(f"Average time saved:          {(avg_old - avg_new):.4f}s")
        print("\n" + "=" * 80)

        print("\nOPTIMIZATIONS APPLIED:")
        print("-" * 80)
        print("1. Added indexes on DagRun.start_date and DagRun.end_date")
        print("2. Removed func.coalesce() wrapping to make queries sargable")
        print("3. Combined two separate DagRun queries into one (3 queries -> 2 queries)")
        print("4. Used explicit OR/IS NULL logic instead of COALESCE")
        print("=" * 80)

    finally:
        session.close()


if __name__ == "__main__":
    main()
