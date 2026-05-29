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
Bottleneck #3 — DAG serialization (``DagSerialization.to_dict`` -> ``serialize_dag``).

This is the CPU hot path inside the parsing subprocess: every parse serializes
each DAG (serialized_objects.py :: serialize_dag), which serializes every task
(``cls.serialize(task)``) and runs dependency detection
(``OperatorSerialization.detect_dependencies`` per task). Known sub-costs:

  * ``inspect.signature`` per task (largely addressed by caching
    ``_FORBIDDEN_TEMPLATE_FIELDS`` — PR #67701);
  * ``ensure_serialized_asset()`` doing an encode->decode roundtrip per asset
    outlet inside ``detect_task_dependencies`` just to read ``.name``/``.uri``.

This script scales the task count and (optionally) the number of asset outlets
per task so the asset path is exercised. Use ``--profile`` to get a cProfile
attribution instead of timings.

Run:
    uv run --project airflow-core python dev/dag_processing_benchmarks/bench_serialize_dag.py
    uv run --project airflow-core python dev/dag_processing_benchmarks/bench_serialize_dag.py --tasks 200 --assets 3 --profile
"""

from __future__ import annotations

import argparse
import cProfile
import datetime
import pstats

from _common import bench, print_header, print_row

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, Asset
from airflow.serialization.serialized_objects import DagSerialization


def build_dag(num_tasks: int, assets_per_task: int = 0) -> DAG:
    with DAG(
        dag_id="bench_serialize_dag",
        schedule=None,
        start_date=datetime.datetime(2024, 1, 1),
        tags=["bench"],
    ) as dag:
        prev = None
        for i in range(num_tasks):
            outlets = [Asset(f"s3://bench-bucket/task_{i}/asset_{j}") for j in range(assets_per_task)]
            task = BashOperator(
                task_id=f"task_{i}",
                bash_command=f"echo {i}",
                outlets=outlets,
            )
            if prev is not None:
                prev >> task
            prev = task
    return dag


def run_profile(num_tasks: int, assets_per_task: int) -> None:
    dag = build_dag(num_tasks, assets_per_task)
    profiler = cProfile.Profile()
    profiler.enable()
    for _ in range(50):
        DagSerialization.to_dict(dag)
    profiler.disable()
    stats = pstats.Stats(profiler)
    stats.sort_stats("cumulative")
    print_header(f"cProfile: to_dict x50, {num_tasks} tasks, {assets_per_task} assets/task")
    stats.print_stats(30)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tasks", default="10,50,100,250", help="comma-separated task counts")
    parser.add_argument(
        "--assets",
        type=int,
        default=0,
        help="asset outlets per task (exercises the ensure_serialized_asset roundtrip)",
    )
    parser.add_argument("--profile", action="store_true", help="cProfile instead of timing")
    args = parser.parse_args()

    if args.profile:
        counts = [int(t) for t in args.tasks.split(",")]
        run_profile(counts[-1], args.assets)
        return

    task_counts = [int(t) for t in args.tasks.split(",")]
    print_header(f"DagSerialization.to_dict scaling (assets/task={args.assets}, best-of-7, ms)")
    widths = (10, 16, 18, 16)
    print_row("tasks", "to_dict (ms)", "median (ms)", "µs/task", widths=widths)
    for n in task_counts:
        dag = build_dag(n, args.assets)
        timing = bench(lambda: DagSerialization.to_dict(dag), repeat=7, warmup=3)
        per_task_us = timing.best / n * 1e6
        print_row(
            n,
            f"{timing.best_ms:.3f}",
            f"{timing.median_ms:.3f}",
            f"{per_task_us:.2f}",
            widths=widths,
        )

    print(
        "\nReading this: 'µs/task' should be ~flat if serialization is linear in task\n"
        "count. Re-run with --assets 3 to surface the per-outlet asset roundtrip cost,\n"
        "and with --profile to see where the time concentrates.\n"
    )


if __name__ == "__main__":
    main()
