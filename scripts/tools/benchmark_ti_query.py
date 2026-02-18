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
SQL comparison & benchmark tool for TaskInstance list query optimization.

Produces reviewer-quality output for PR #62108 — eliminates duplicate JOINs
in the ``GET /taskInstances`` family of endpoints by switching from
``joinedload`` to ``contains_eager`` when the relationship table is already
explicitly joined for filtering / sorting.

Designed to run inside **Breeze** (``breeze shell``) or any environment
where Airflow is installed in editable mode (``pip install -e ./airflow-core``).

Capabilities
------------
1. Validates the runtime environment (Breeze / editable install).
2. Captures the **exact** ORM-generated SQL via temporary ``sqlalchemy.event``
   hooks — no production code is modified.
3. Builds both the *before* (joinedload) and *after* (contains_eager) query
   variants using real Airflow ORM models.
4. Benchmarks query execution with ``time.perf_counter`` (warmup + measured
   runs).
5. Optionally runs ``EXPLAIN ANALYZE`` on PostgreSQL / MySQL 8+.
6. Generates synthetic dataset if the database is empty or too small.
7. Outputs a **GitHub-Markdown** report ready to paste into PR comments.

Usage (inside breeze shell)
---------------------------
::

    # Full run — captures SQL, benchmarks, produces markdown report
    python scripts/tools/benchmark_ti_query.py

    # Skip dataset seeding (DB already has data)
    python scripts/tools/benchmark_ti_query.py --skip-seed

    # SQL capture only (no benchmark)
    python scripts/tools/benchmark_ti_query.py --sql-only

    # Custom dataset size & iterations
    python scripts/tools/benchmark_ti_query.py --num-dags 20 --tis-per-dag 200 -n 50

    # Save outputs to files
    python scripts/tools/benchmark_ti_query.py -o benchmark_results.json \\
        --sql-before sql_before.txt --sql-after sql_after.txt

    # Compare two saved JSON reports
    python scripts/tools/benchmark_ti_query.py --compare before.json after.json
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import statistics
import subprocess
import sys
import textwrap
import time
from datetime import datetime, timezone
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[2]

# Suppress noisy Airflow startup logging.
logging.basicConfig(level=logging.WARNING)


# ============================================================================
# 1. Environment validation
# ============================================================================
def validate_environment() -> str:
    """
    Detect runtime environment.

    Returns one of: "breeze", "editable", "unknown".
    Prints guidance when the environment is not suitable.
    """
    # Check Breeze
    if os.environ.get("BREEZE", "") or Path("/opt/airflow").is_dir():
        return "breeze"
    # Check editable install
    try:
        import airflow  # noqa: F401

        return "editable"
    except ImportError:
        pass
    return "unknown"


def _abort_if_no_env(env: str) -> None:
    if env == "unknown":
        print(
            textwrap.dedent("""\
            ================================================================
            ERROR: Airflow is not importable in this environment.

            This script must run inside one of:

              a) Breeze container:
                    breeze shell
                    python scripts/tools/benchmark_ti_query.py

              b) Editable install:
                    pip install -e ./airflow-core
                    python scripts/tools/benchmark_ti_query.py

            ================================================================
            """)
        )
        sys.exit(1)


# ============================================================================
# 2. Git info
# ============================================================================
def _get_git_info() -> dict[str, str]:
    """Return current branch name and short SHA."""
    info: dict[str, str] = {}
    try:
        info["branch"] = (
            subprocess.check_output(
                ["git", "branch", "--show-current"],
                cwd=str(_REPO_ROOT),
                stderr=subprocess.DEVNULL,
            )
            .decode()
            .strip()
        )
        info["sha"] = (
            subprocess.check_output(
                ["git", "rev-parse", "--short", "HEAD"],
                cwd=str(_REPO_ROOT),
                stderr=subprocess.DEVNULL,
            )
            .decode()
            .strip()
        )
    except Exception:
        info.setdefault("branch", "unknown")
        info.setdefault("sha", "unknown")
    return info


# ============================================================================
# 3. SQL Capture via SQLAlchemy event hooks (temporary, no code changes)
# ============================================================================
class SQLCapture:
    """Context manager that records every SQL statement emitted on an engine."""

    def __init__(self, engine):
        from sqlalchemy import event

        self.engine = engine
        self.statements: list[dict] = []
        self._event = event

    def __enter__(self):
        self.statements.clear()
        self._event.listen(self.engine, "before_cursor_execute", self._before)
        self._event.listen(self.engine, "after_cursor_execute", self._after)
        return self

    def _before(self, conn, cursor, statement, parameters, context, executemany):
        conn.info["_bench_t0"] = time.perf_counter()
        self.statements.append(
            {
                "sql": statement,
                "params": repr(parameters)[:500] if parameters else "",
                "time_ms": 0.0,
            }
        )

    def _after(self, conn, cursor, statement, parameters, context, executemany):
        t0 = conn.info.pop("_bench_t0", None)
        if t0 is not None and self.statements:
            self.statements[-1]["time_ms"] = round((time.perf_counter() - t0) * 1000, 4)

    def __exit__(self, *exc):
        self._event.remove(self.engine, "before_cursor_execute", self._before)
        self._event.remove(self.engine, "after_cursor_execute", self._after)


# ============================================================================
# 4. Query builders — replicate exact ORM query patterns
# ============================================================================
def build_query_before():
    """
    Build the BEFORE (baseline) query — ``joinedload`` on already-joined tables.

    This mirrors what ``get_task_instances`` did on ``main`` before the PR:
    explicit ``.join(TI.dag_run).outerjoin(TI.dag_version)`` **plus**
    ``joinedload`` for those same relationships, causing duplicate JOINs.
    """
    from sqlalchemy import select
    from sqlalchemy.orm import joinedload

    from airflow.models.dag_version import DagVersion
    from airflow.models.dagrun import DagRun
    from airflow.models.taskinstance import TaskInstance as TI

    return (
        select(TI)
        .join(TI.dag_run)
        .outerjoin(TI.dag_version)
        .options(
            joinedload(TI.dag_version).joinedload(DagVersion.bundle),
            joinedload(TI.dag_run).options(joinedload(DagRun.dag_model)),
            joinedload(TI.task_instance_note),
        )
    )


def build_query_after():
    """
    Build the AFTER (optimized) query — ``contains_eager`` reuses existing joins.

    Uses the ``eager_load_TI_and_TIH_for_validation`` helper with
    ``dag_run_joined=True, dag_version_joined=True`` so ``contains_eager``
    tells SQLAlchemy to populate the relationship from the already-joined
    columns instead of emitting additional LEFT OUTER JOINs.
    """
    from sqlalchemy import select

    from airflow.models.taskinstance import TaskInstance as TI

    try:
        from airflow.api_fastapi.common.db.task_instances import (
            eager_load_TI_and_TIH_for_validation,
        )

        return (
            select(TI)
            .join(TI.dag_run)
            .outerjoin(TI.dag_version)
            .options(
                *eager_load_TI_and_TIH_for_validation(
                    dag_run_joined=True, dag_version_joined=True
                )
            )
        )
    except TypeError:
        # Pre-optimization branch — helper doesn't accept keyword flags.
        from airflow.api_fastapi.common.db.task_instances import (
            eager_load_TI_and_TIH_for_validation,
        )

        return (
            select(TI)
            .join(TI.dag_run)
            .outerjoin(TI.dag_version)
            .options(*eager_load_TI_and_TIH_for_validation())
        )


def build_batch_query_before():
    """Batch endpoint BEFORE: triple-joined (initial joinedload + post-paginate joinedload)."""
    from sqlalchemy import select
    from sqlalchemy.orm import joinedload

    from airflow.models.dag_version import DagVersion
    from airflow.models.dagrun import DagRun
    from airflow.models.taskinstance import TaskInstance as TI

    return (
        select(TI)
        .join(TI.dag_run)
        .outerjoin(TI.dag_version)
        .options(
            # From eager_load helper (joinedload — duplicates the explicit joins)
            joinedload(TI.dag_version).joinedload(DagVersion.bundle),
            joinedload(TI.dag_run).options(joinedload(DagRun.dag_model)),
            joinedload(TI.task_instance_note),
            # Post-paginate options — third redundant copy
            joinedload(TI.rendered_task_instance_fields),
            joinedload(TI.task_instance_note),
            joinedload(TI.dag_run).options(joinedload(DagRun.dag_model)),
        )
    )


def build_batch_query_after():
    """Batch endpoint AFTER: contains_eager reuses explicit joins, only rendered_fields added."""
    from sqlalchemy import select
    from sqlalchemy.orm import joinedload

    from airflow.models.taskinstance import TaskInstance as TI

    try:
        from airflow.api_fastapi.common.db.task_instances import (
            eager_load_TI_and_TIH_for_validation,
        )

        return (
            select(TI)
            .join(TI.dag_run)
            .outerjoin(TI.dag_version)
            .options(
                *eager_load_TI_and_TIH_for_validation(
                    dag_run_joined=True, dag_version_joined=True
                )
            )
            .options(joinedload(TI.rendered_task_instance_fields))
        )
    except TypeError:
        from airflow.api_fastapi.common.db.task_instances import (
            eager_load_TI_and_TIH_for_validation,
        )

        return (
            select(TI)
            .join(TI.dag_run)
            .outerjoin(TI.dag_version)
            .options(*eager_load_TI_and_TIH_for_validation())
            .options(joinedload(TI.rendered_task_instance_fields))
        )


# ============================================================================
# 5. SQL compilation helper
# ============================================================================
def compile_query(query, engine) -> str:
    """Compile a SQLAlchemy Select to a dialect-specific SQL string."""
    return str(query.compile(engine, compile_kwargs={"literal_binds": True}))


# ============================================================================
# 6. Benchmarking
# ============================================================================
def benchmark(session_factory, query, *, warmup: int = 3, iterations: int = 20) -> dict:
    """
    Execute *query* repeatedly and return timing statistics.

    Uses ``time.perf_counter`` for high-resolution timing.
    """
    times: list[float] = []

    for i in range(warmup + iterations):
        session = session_factory()
        try:
            t0 = time.perf_counter()
            list(session.execute(query).scalars())
            t1 = time.perf_counter()
            if i >= warmup:
                times.append((t1 - t0) * 1000)
        finally:
            session.close()

    def _stats(data: list[float]) -> dict:
        if len(data) < 2:
            return {
                "min": round(data[0], 4),
                "max": round(data[0], 4),
                "mean": round(data[0], 4),
                "median": round(data[0], 4),
                "stdev": 0.0,
            }
        return {
            "min": round(min(data), 4),
            "max": round(max(data), 4),
            "mean": round(statistics.mean(data), 4),
            "median": round(statistics.median(data), 4),
            "stdev": round(statistics.stdev(data), 4),
        }

    return {"query_ms": _stats(times), "warmup": warmup, "iterations": iterations}


# ============================================================================
# 7. EXPLAIN ANALYZE (optional — Postgres / MySQL 8+)
# ============================================================================
def explain_analyze(session, query, engine) -> str | None:
    """Run EXPLAIN ANALYZE if the dialect supports it. Returns plan text or None."""
    dialect = engine.dialect.name
    sql_text = compile_query(query, engine)
    try:
        if dialect == "postgresql":
            rows = session.execute(
                __import__("sqlalchemy").text(f"EXPLAIN ANALYZE {sql_text}")
            ).fetchall()
            return "\n".join(r[0] for r in rows)
        elif dialect == "mysql":
            rows = session.execute(
                __import__("sqlalchemy").text(f"EXPLAIN ANALYZE {sql_text}")
            ).fetchall()
            return "\n".join(str(r) for r in rows)
    except Exception as e:
        return f"EXPLAIN ANALYZE not available: {e}"
    return None


# ============================================================================
# 8. Dataset seeding
# ============================================================================
def get_dataset_counts(session) -> dict[str, int]:
    """Return row counts for key tables."""
    from sqlalchemy import func, select

    from airflow.models.dagrun import DagRun
    from airflow.models.taskinstance import TaskInstance as TI

    ti_count = session.scalar(select(func.count()).select_from(TI)) or 0
    dr_count = session.scalar(select(func.count()).select_from(DagRun)) or 0
    return {"task_instances": ti_count, "dag_runs": dr_count}


def seed_dataset(session, *, num_dags: int = 10, tis_per_dag: int = 100) -> dict[str, int]:
    """
    Insert synthetic DAGs, DagRuns, and TaskInstances.

    Safe: uses ORM inserts with proper foreign keys, committed in batches.
    """
    import uuid6

    from airflow.models.dag import DagModel
    from airflow.models.dagbundle import DagBundleModel
    from airflow.models.dag_version import DagVersion
    from airflow.models.dagrun import DagRun
    from airflow.models.taskinstance import TaskInstance

    now = datetime.now(tz=timezone.utc)

    # Ensure a bundle exists
    bundle = session.get(DagBundleModel, "benchmark_bundle")
    if not bundle:
        bundle = DagBundleModel(name="benchmark_bundle", active=True)
        session.add(bundle)
        session.flush()

    total_tis = 0
    total_drs = 0
    batch_size = 500

    for d in range(num_dags):
        dag_id = f"benchmark_dag_{d:04d}"

        # DagModel
        dag_model = session.get(DagModel, dag_id)
        if not dag_model:
            dag_model = DagModel(dag_id=dag_id, bundle_name="benchmark_bundle", fileloc="/tmp/bench.py")
            session.add(dag_model)
            session.flush()

        # DagVersion
        dag_version = DagVersion(
            id=uuid6.uuid7(),
            dag_id=dag_id,
            version_number=1,
            bundle_name="benchmark_bundle",
            created_at=now,
            last_updated=now,
        )
        session.add(dag_version)
        session.flush()

        # DagRun
        run_id = f"bench_run_{d:04d}"
        dag_run = DagRun(
            dag_id=dag_id,
            run_id=run_id,
            run_after=now,
            state="success",
            created_dag_version_id=dag_version.id,
        )
        session.add(dag_run)
        session.flush()
        total_drs += 1

        # TaskInstances
        for t in range(tis_per_dag):
            ti = TaskInstance(
                id=uuid6.uuid7(),
                task_id=f"task_{t:04d}",
                dag_id=dag_id,
                run_id=run_id,
                dag_version_id=dag_version.id,
                state="success",
                try_number=1,
                start_date=now,
                end_date=now,
            )
            session.add(ti)
            total_tis += 1

            if total_tis % batch_size == 0:
                session.flush()

    session.commit()
    print(f"  Seeded {total_tis} TaskInstances across {total_drs} DagRuns ({num_dags} DAGs)")
    return {"task_instances_inserted": total_tis, "dag_runs_inserted": total_drs}


# ============================================================================
# 9. Report generation (GitHub Markdown)
# ============================================================================
def _count_joins(sql: str) -> int:
    return sql.upper().count(" JOIN ")


def generate_markdown_report(
    *,
    git_info: dict,
    sql_before: str,
    sql_after: str,
    bench_before: dict | None,
    bench_after: dict | None,
    dataset: dict,
    explain_before: str | None = None,
    explain_after: str | None = None,
    dialect: str = "postgresql",
    batch_sql_before: str | None = None,
    batch_sql_after: str | None = None,
    batch_bench_before: dict | None = None,
    batch_bench_after: dict | None = None,
) -> str:
    """Generate a complete GitHub-Markdown comment ready to paste."""
    lines: list[str] = []

    lines.append("## SQL Comparison & Benchmark — TaskInstance List Query Optimization")
    lines.append("")
    lines.append(f"**Branch:** `{git_info.get('branch', 'N/A')}` "
                 f"(`{git_info.get('sha', 'N/A')}`)")
    lines.append(f"**Dialect:** `{dialect}`")
    lines.append("")

    # --- SQL Comparison: get_task_instances ---
    lines.append("### `GET /dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances` — SQL Comparison")
    lines.append("")
    lines.append(f"**Before** ({_count_joins(sql_before)} JOINs, {len(sql_before)} chars):")
    lines.append("```sql")
    lines.append(sql_before)
    lines.append("```")
    lines.append("")
    lines.append(f"**After** ({_count_joins(sql_after)} JOINs, {len(sql_after)} chars):")
    lines.append("```sql")
    lines.append(sql_after)
    lines.append("```")
    lines.append("")

    # --- SQL Comparison: batch endpoint ---
    if batch_sql_before and batch_sql_after:
        lines.append("### `POST /dags/~/dagRuns/~/taskInstances/list` (Batch) — SQL Comparison")
        lines.append("")
        lines.append(
            f"**Before** ({_count_joins(batch_sql_before)} JOINs, "
            f"{len(batch_sql_before)} chars):"
        )
        lines.append("```sql")
        lines.append(batch_sql_before)
        lines.append("```")
        lines.append("")
        lines.append(
            f"**After** ({_count_joins(batch_sql_after)} JOINs, "
            f"{len(batch_sql_after)} chars):"
        )
        lines.append("```sql")
        lines.append(batch_sql_after)
        lines.append("```")
        lines.append("")

    # --- Benchmark Results ---
    lines.append("### Benchmark Results")
    lines.append("")
    if bench_before and bench_after:
        lines.append("#### `get_task_instances` endpoint query")
        lines.append("")
        lines.append("| Metric | Before | After | Change |")
        lines.append("|--------|--------|-------|--------|")

        bm = bench_before["query_ms"]["mean"]
        am = bench_after["query_ms"]["mean"]
        pct = ((am - bm) / bm * 100) if bm > 0 else 0
        lines.append(
            f"| Avg Query Time | {bm:.4f} ms | {am:.4f} ms | "
            f"{pct:+.1f}% |"
        )

        bmed = bench_before["query_ms"]["median"]
        amed = bench_after["query_ms"]["median"]
        pct_med = ((amed - bmed) / bmed * 100) if bmed > 0 else 0
        lines.append(
            f"| Median Query Time | {bmed:.4f} ms | {amed:.4f} ms | "
            f"{pct_med:+.1f}% |"
        )

        lines.append(
            f"| Min Query Time | {bench_before['query_ms']['min']:.4f} ms | "
            f"{bench_after['query_ms']['min']:.4f} ms | |"
        )
        lines.append(
            f"| Max Query Time | {bench_before['query_ms']['max']:.4f} ms | "
            f"{bench_after['query_ms']['max']:.4f} ms | |"
        )
        lines.append(
            f"| Std Dev | {bench_before['query_ms']['stdev']:.4f} ms | "
            f"{bench_after['query_ms']['stdev']:.4f} ms | |"
        )
        lines.append(f"| Iterations | {bench_before['iterations']} | {bench_after['iterations']} | |")
        lines.append(f"| SQL JOINs | {_count_joins(sql_before)} | {_count_joins(sql_after)} | "
                     f"{_count_joins(sql_after) - _count_joins(sql_before)} |")
        lines.append("")

    if batch_bench_before and batch_bench_after:
        lines.append("#### `get_task_instances_batch` endpoint query")
        lines.append("")
        lines.append("| Metric | Before | After | Change |")
        lines.append("|--------|--------|-------|--------|")

        bm = batch_bench_before["query_ms"]["mean"]
        am = batch_bench_after["query_ms"]["mean"]
        pct = ((am - bm) / bm * 100) if bm > 0 else 0
        lines.append(f"| Avg Query Time | {bm:.4f} ms | {am:.4f} ms | {pct:+.1f}% |")
        lines.append(
            f"| Median Query Time | {batch_bench_before['query_ms']['median']:.4f} ms | "
            f"{batch_bench_after['query_ms']['median']:.4f} ms | |"
        )
        lines.append(
            f"| SQL JOINs | {_count_joins(batch_sql_before)} | "
            f"{_count_joins(batch_sql_after)} | "
            f"{_count_joins(batch_sql_after) - _count_joins(batch_sql_before)} |"
        )
        lines.append("")

    # --- EXPLAIN ANALYZE ---
    if explain_before or explain_after:
        lines.append("### EXPLAIN ANALYZE")
        lines.append("")
        if explain_before:
            lines.append("**Before:**")
            lines.append("```")
            lines.append(explain_before)
            lines.append("```")
        if explain_after:
            lines.append("**After:**")
            lines.append("```")
            lines.append(explain_after)
            lines.append("```")
        lines.append("")

    # --- Dataset ---
    lines.append("### Dataset")
    lines.append("")
    lines.append(f"- **TaskInstances:** {dataset.get('task_instances', 'N/A')} rows")
    lines.append(f"- **DagRuns:** {dataset.get('dag_runs', 'N/A')} rows")
    lines.append("")

    # --- Observations ---
    joins_before = _count_joins(sql_before)
    joins_after = _count_joins(sql_after)
    joins_reduced = joins_before - joins_after

    lines.append("### Observations")
    lines.append("")
    lines.append(
        f"1. **JOIN reduction:** The `get_task_instances` query went from "
        f"**{joins_before}** to **{joins_after}** JOINs "
        f"(eliminated **{joins_reduced}** duplicate JOINs)."
    )
    lines.append(
        "2. **Root cause:** Before this PR, `joinedload(TI.dag_run)` and "
        "`joinedload(TI.dag_version)` caused SQLAlchemy to emit *additional* "
        "LEFT OUTER JOINs for relationships that were *already* explicitly "
        "joined via `.join(TI.dag_run).outerjoin(TI.dag_version)` for "
        "filtering/sorting."
    )
    lines.append(
        "3. **Fix:** Replaced `joinedload` with `contains_eager` when the "
        "caller has already performed the explicit join. `contains_eager` "
        "tells SQLAlchemy to populate the relationship attribute from "
        "the already-joined columns — zero additional JOINs."
    )
    lines.append(
        "4. **Backward compatible:** The `dag_run_joined` / `dag_version_joined` "
        "flags default to `False`, preserving existing behavior for callers "
        "that don't explicitly join (e.g., TaskInstanceHistory endpoints)."
    )
    if batch_sql_before and batch_sql_after:
        bb = _count_joins(batch_sql_before)
        ba = _count_joins(batch_sql_after)
        lines.append(
            f"5. **Batch endpoint:** Additionally reduced from **{bb}** to "
            f"**{ba}** JOINs by removing a redundant post-paginate "
            "`joinedload(TI.task_instance_note)` and "
            "`joinedload(TI.dag_run)` that were already handled by the "
            "initial eager-load options."
        )
    lines.append("")
    return "\n".join(lines)


# ============================================================================
# 10. Compare two saved JSON reports
# ============================================================================
def compare_reports(before_path: str, after_path: str) -> None:
    with open(before_path) as f:
        before = json.load(f)
    with open(after_path) as f:
        after = json.load(f)

    print("=" * 72)
    print("  TaskInstance Query Benchmark Comparison")
    print("=" * 72)
    print(f"  BEFORE: {before['git']['branch']} ({before['git']['sha']})")
    print(f"  AFTER:  {after['git']['branch']} ({after['git']['sha']})")
    print("-" * 72)

    for label, key in [("get_task_instances", "sql"), ("batch", "batch_sql")]:
        if key not in before or key not in after:
            continue
        bj = _count_joins(before[key])
        aj = _count_joins(after[key])
        print(f"\n  [{label}] JOIN count:  before={bj}  after={aj}  delta={aj - bj}")
        print(f"  [{label}] SQL length:  before={len(before[key])}  after={len(after[key])}")

    for label, key in [("get_task_instances", "benchmark"), ("batch", "batch_benchmark")]:
        if key not in before or key not in after:
            continue
        bm = before[key]["query_ms"]["mean"]
        am = after[key]["query_ms"]["mean"]
        pct = ((am - bm) / bm * 100) if bm > 0 else 0
        direction = "faster" if pct < 0 else "slower"
        print(f"\n  [{label}] query_ms:")
        print(f"    before mean: {bm:.4f} ms")
        print(f"    after  mean: {am:.4f} ms")
        print(f"    change:      {pct:+.2f}% ({direction})")

    print("\n" + "=" * 72)


# ============================================================================
# Main
# ============================================================================
def main() -> None:
    parser = argparse.ArgumentParser(
        description="SQL comparison & benchmark for TaskInstance query optimization (PR #62108).",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=textwrap.dedent("""\
            Examples (run inside `breeze shell`):

              # Full run with default dataset
              python scripts/tools/benchmark_ti_query.py

              # SQL only, no benchmarks
              python scripts/tools/benchmark_ti_query.py --sql-only

              # Custom dataset & iterations
              python scripts/tools/benchmark_ti_query.py --num-dags 20 --tis-per-dag 200 -n 50

              # Save results
              python scripts/tools/benchmark_ti_query.py -o results.json \\
                  --sql-before sql_before.txt --sql-after sql_after.txt

              # Compare two saved reports
              python scripts/tools/benchmark_ti_query.py --compare before.json after.json
        """),
    )
    parser.add_argument("-o", "--output", help="Save JSON report to file.")
    parser.add_argument("--sql-before", help="Save BEFORE SQL to file.")
    parser.add_argument("--sql-after", help="Save AFTER SQL to file.")
    parser.add_argument("--compare", nargs=2, metavar=("BEFORE", "AFTER"),
                        help="Compare two saved JSON reports and exit.")
    parser.add_argument("-n", "--iterations", type=int, default=20,
                        help="Number of benchmark iterations (default: 20).")
    parser.add_argument("--warmup", type=int, default=3,
                        help="Number of warmup iterations (default: 3).")
    parser.add_argument("--sql-only", action="store_true",
                        help="Print compiled SQL for both variants and exit.")
    parser.add_argument("--skip-seed", action="store_true",
                        help="Skip synthetic dataset seeding.")
    parser.add_argument("--num-dags", type=int, default=10,
                        help="Number of DAGs for synthetic dataset (default: 10).")
    parser.add_argument("--tis-per-dag", type=int, default=100,
                        help="TaskInstances per DAG for synthetic dataset (default: 100).")
    parser.add_argument("--explain", action="store_true",
                        help="Run EXPLAIN ANALYZE (PostgreSQL / MySQL 8+).")
    parser.add_argument("--markdown-file", help="Save markdown report to file.")
    args = parser.parse_args()

    # ---- Compare mode (no Airflow needed) ----
    if args.compare:
        compare_reports(args.compare[0], args.compare[1])
        return

    # ---- Environment check ----
    env = validate_environment()
    _abort_if_no_env(env)
    print(f"[env] Detected: {env}")

    # ---- Airflow imports ----
    from airflow import settings as airflow_settings

    airflow_settings.configure_orm()
    engine = airflow_settings.engine
    Session = airflow_settings.Session  # noqa: N806
    dialect = engine.dialect.name
    print(f"[db]  Dialect: {dialect}")

    # ---- Build queries ----
    q_before = build_query_before()
    q_after = build_query_after()
    q_batch_before = build_batch_query_before()
    q_batch_after = build_batch_query_after()

    sql_before = compile_query(q_before, engine)
    sql_after = compile_query(q_after, engine)
    batch_sql_before = compile_query(q_batch_before, engine)
    batch_sql_after = compile_query(q_batch_after, engine)

    # ---- SQL-only mode ----
    if args.sql_only:
        print("\n--- BEFORE (get_task_instances) ---")
        print(sql_before)
        print(f"\n  JOINs: {_count_joins(sql_before)}")
        print("\n--- AFTER (get_task_instances) ---")
        print(sql_after)
        print(f"\n  JOINs: {_count_joins(sql_after)}")
        print("\n--- BEFORE (batch) ---")
        print(batch_sql_before)
        print(f"\n  JOINs: {_count_joins(batch_sql_before)}")
        print("\n--- AFTER (batch) ---")
        print(batch_sql_after)
        print(f"\n  JOINs: {_count_joins(batch_sql_after)}")
        if args.sql_before:
            Path(args.sql_before).write_text(sql_before)
            print(f"\nSaved BEFORE SQL to {args.sql_before}")
        if args.sql_after:
            Path(args.sql_after).write_text(sql_after)
            print(f"\nSaved AFTER SQL to {args.sql_after}")
        return

    # ---- Dataset validation / seeding ----
    session = Session()
    try:
        counts = get_dataset_counts(session)
        print(f"[data] Current: {counts['task_instances']} TIs, {counts['dag_runs']} DagRuns")

        if not args.skip_seed and counts["task_instances"] < args.num_dags * args.tis_per_dag:
            print("[data] Seeding synthetic dataset...")
            seed_dataset(session, num_dags=args.num_dags, tis_per_dag=args.tis_per_dag)
            counts = get_dataset_counts(session)
            print(f"[data] After seeding: {counts['task_instances']} TIs, {counts['dag_runs']} DagRuns")
    finally:
        session.close()

    # ---- SQL Capture via event hooks ----
    print("[sql]  Capturing generated SQL via engine event hooks...")
    with SQLCapture(engine) as capture:
        s = Session()
        try:
            list(s.execute(q_after).scalars())
        finally:
            s.close()
    print(f"[sql]  Captured {len(capture.statements)} statement(s)")

    # ---- Benchmark ----
    print(f"[bench] Running benchmarks ({args.warmup} warmup + {args.iterations} measured)...")

    print("[bench] BEFORE (get_task_instances)...")
    bench_before = benchmark(Session, q_before, warmup=args.warmup, iterations=args.iterations)

    print("[bench] AFTER  (get_task_instances)...")
    bench_after = benchmark(Session, q_after, warmup=args.warmup, iterations=args.iterations)

    print("[bench] BEFORE (batch)...")
    batch_bench_before = benchmark(Session, q_batch_before, warmup=args.warmup, iterations=args.iterations)

    print("[bench] AFTER  (batch)...")
    batch_bench_after = benchmark(Session, q_batch_after, warmup=args.warmup, iterations=args.iterations)

    # ---- EXPLAIN ANALYZE (optional) ----
    explain_before_text = None
    explain_after_text = None
    if args.explain:
        print("[explain] Running EXPLAIN ANALYZE...")
        s = Session()
        try:
            explain_before_text = explain_analyze(s, q_before, engine)
            explain_after_text = explain_analyze(s, q_after, engine)
        finally:
            s.close()

    # ---- Generate markdown report ----
    git_info = _get_git_info()
    md = generate_markdown_report(
        git_info=git_info,
        sql_before=sql_before,
        sql_after=sql_after,
        bench_before=bench_before,
        bench_after=bench_after,
        dataset=counts,
        explain_before=explain_before_text,
        explain_after=explain_after_text,
        dialect=dialect,
        batch_sql_before=batch_sql_before,
        batch_sql_after=batch_sql_after,
        batch_bench_before=batch_bench_before,
        batch_bench_after=batch_bench_after,
    )

    # ---- Print report ----
    print("\n" + "=" * 72)
    print(md)
    print("=" * 72)

    # ---- Save files ----
    if args.sql_before:
        Path(args.sql_before).write_text(sql_before)
        print(f"Saved BEFORE SQL to {args.sql_before}")
    if args.sql_after:
        Path(args.sql_after).write_text(sql_after)
        print(f"Saved AFTER SQL to {args.sql_after}")
    if args.markdown_file:
        Path(args.markdown_file).write_text(md)
        print(f"Saved markdown report to {args.markdown_file}")
    if args.output:
        report = {
            "git": git_info,
            "dialect": dialect,
            "dataset": counts,
            "sql": sql_before,
            "sql_after": sql_after,
            "batch_sql": batch_sql_before,
            "batch_sql_after": batch_sql_after,
            "benchmark": bench_before,
            "benchmark_after": bench_after,
            "batch_benchmark": batch_bench_before,
            "batch_benchmark_after": batch_bench_after,
        }
        with open(args.output, "w") as f:
            json.dump(report, f, indent=2)
        print(f"Saved JSON report to {args.output}")


if __name__ == "__main__":
    main()
