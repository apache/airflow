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
Show EXPLAIN ANALYZE plans for old (coalesce) vs new (sargable) queries.

Seeds 50,000 DagRun rows so Postgres has enough data to prefer index scans
over sequential scans, making the index usage difference visible.

Usage:
    breeze run --backend postgres python dev/explain_query_plans.py
"""

from __future__ import annotations

from datetime import timedelta

from sqlalchemy import func, or_, select, text

from airflow._shared.timezones import timezone
from airflow.models.dag import DagModel
from airflow.models.dagbundle import DagBundleModel
from airflow.models.dagrun import DagRun
from airflow.utils import db
from airflow.utils.state import DagRunState
from airflow.utils.types import DagRunType

DAG_ID = "explain_plan_dag"
NUM_ROWS = 5_000_000
BATCH_SIZE = 1_000


def ensure_dag(*, session):
    bundle = session.execute(
        text("SELECT name FROM dag_bundle WHERE name = 'explain_bundle'")
    ).scalar_one_or_none()
    if not bundle:
        session.add(DagBundleModel(name="explain_bundle", version="1.0"))
        session.flush()

    dag = session.execute(
        text("SELECT dag_id FROM dag WHERE dag_id = :dag_id").bindparams(dag_id=DAG_ID)
    ).scalar_one_or_none()
    if not dag:
        session.add(DagModel(dag_id=DAG_ID, bundle_name="explain_bundle"))
        session.flush()


def seed_dag_runs(*, session):
    existing = session.execute(
        select(func.count()).select_from(DagRun).where(DagRun.dag_id == DAG_ID)
    ).scalar_one()
    if existing >= NUM_ROWS:
        print(f"  {existing} rows already present — skipping seed")
        return

    print(f"  Seeding {NUM_ROWS} DagRun rows...")
    base = timezone.utcnow() - timedelta(days=60)
    batch = []
    for i in range(NUM_ROWS):
        run_date = base + timedelta(minutes=i)
        dr = DagRun(
            dag_id=DAG_ID,
            run_id=f"explain_run_{i}",
            run_type=DagRunType.SCHEDULED.value,
            logical_date=run_date,
        )
        dr.state = DagRunState.SUCCESS
        dr.start_date = run_date
        dr.end_date = run_date + timedelta(minutes=30)
        dr.data_interval_start = run_date
        dr.data_interval_end = run_date + timedelta(minutes=30)
        batch.append(dr)
        if len(batch) >= BATCH_SIZE:
            session.bulk_save_objects(batch)
            session.flush()
            batch = []
    if batch:
        session.bulk_save_objects(batch)
        session.flush()
    print(f"  Seeded {NUM_ROWS} rows")


def ensure_indexes(*, session):
    with session.bind.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_dag_run_start_date ON dag_run (start_date)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_dag_run_end_date ON dag_run (end_date)"))
        conn.execute(text("ANALYZE dag_run"))


def run_explain(*, session, label, query):
    sql = query.compile(session.bind, compile_kwargs={"literal_binds": True})
    explain_sql = f"EXPLAIN ANALYZE {sql}"
    result = session.execute(text(explain_sql)).fetchall()
    print(f"\n{'=' * 70}")
    print(f"QUERY PLAN: {label}")
    print("=" * 70)
    for row in result:
        print(row[0])


def cleanup(*, session):
    session.execute(text("DELETE FROM dag_run WHERE dag_id = :dag_id").bindparams(dag_id=DAG_ID))


def main():
    db.initdb()
    from airflow.settings import Session  # lazy import for worker isolation

    session = Session()
    current_time = timezone.utcnow()
    start_date = current_time - timedelta(days=30)
    end_date = current_time

    try:
        ensure_dag(session=session)
        seed_dag_runs(session=session)
        session.commit()

        ensure_indexes(session=session)

        # ---- OLD: coalesce (non-sargable) ----
        old_query = (
            select(DagRun.run_type, func.count(DagRun.run_id))
            .where(
                func.coalesce(DagRun.start_date, current_time) >= start_date,
                func.coalesce(DagRun.end_date, current_time) <= end_date,
            )
            .group_by(DagRun.run_type)
        )
        run_explain(session=session, label="OLD — func.coalesce (non-sargable)", query=old_query)

        # ---- NEW: sargable OR/IS NULL ----
        start_date_filter = or_(
            DagRun.start_date >= start_date,
            DagRun.start_date.is_(None) & (current_time >= start_date),
        )
        end_date_filter = or_(
            DagRun.end_date <= end_date,
            DagRun.end_date.is_(None) & (current_time <= end_date),
        )
        new_query = (
            select(DagRun.run_type, DagRun.state, func.count(DagRun.run_id))
            .where(start_date_filter, end_date_filter)
            .group_by(DagRun.run_type, DagRun.state)
        )
        run_explain(session=session, label="NEW — sargable OR/IS NULL", query=new_query)

    finally:
        cleanup(session=session)
        session.commit()
        session.close()


if __name__ == "__main__":
    main()
