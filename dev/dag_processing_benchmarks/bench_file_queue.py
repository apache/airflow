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
Bottleneck #1 — file-queue de-duplication is O(N x queue-size).

``DagFileProcessorManager`` keeps to-parse files in a ``collections.deque``
(``self._file_queue``) and de-duplicates on insert with a linear membership test:

    # manager.py :: _add_files_to_queue
    elif mode == "front":
        new_files = list(f for f in files if f not in self._file_queue)   # f not in deque == O(Q)
        self._file_queue.extendleft(new_files)
    ...
    if mode == "frontprio":
        for file in files:
            with contextlib.suppress(ValueError):
                self._file_queue.remove(file)   # O(Q) per file
            self._file_queue.appendleft(file)

``x in deque`` and ``deque.remove`` are both ``O(Q)``. So:

  * Filling an **empty** queue in one shot is fine — O(N) (the membership scan
    runs against the still-empty deque).
  * But re-adding files that are **already queued** (``front`` mode), the
    ``frontprio`` priority path (always remove+appendleft), and dripping files in
    **incrementally** (one callback/priority file per loop) are each ``O(N x Q)``
    -> quadratic in the steady-state file count.

This script drives the *real* manager methods with synthetic ``DagFileInfo``
objects so the linear baseline and the quadratic paths can be compared directly.
No database is touched.

Run:
    uv run --project airflow-core python dev/dag_processing_benchmarks/bench_file_queue.py
    uv run --project airflow-core python dev/dag_processing_benchmarks/bench_file_queue.py --sizes 500,1000,2000,4000
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

from _common import bench, print_header, print_row

from airflow.dag_processing.manager import DagFileInfo, DagFileProcessorManager

# The manager logs an INFO line per call; silence it so the table is readable.
logging.getLogger("airflow.dag_processing.manager.DagFileProcessorManager").setLevel(logging.WARNING)


def make_files(n: int, *, bundle: str = "bench_bundle") -> list[DagFileInfo]:
    return [DagFileInfo(rel_path=Path(f"dag_{i:06d}.py"), bundle_name=bundle) for i in range(n)]


def fresh_manager() -> DagFileProcessorManager:
    # max_runs is the only required field; everything else comes from config defaults.
    return DagFileProcessorManager(max_runs=1)


def bench_fill_empty(n: int) -> float:
    """LINEAR baseline: fill an empty queue in one shot (dedup scans empty deque)."""
    files = make_files(n)

    def run() -> None:
        mgr = fresh_manager()
        mgr._add_files_to_queue(files, mode="back")

    return bench(run, repeat=5, warmup=1).best


def bench_front_readd(n: int) -> float:
    """QUADRATIC: re-add files already in the queue (front-mode dedup scans full queue)."""
    files = make_files(n)

    def run() -> None:
        mgr = fresh_manager()
        mgr._add_files_to_queue(files, mode="back")
        mgr._add_files_to_queue(files, mode="front")

    return bench(run, repeat=5, warmup=1).best


def bench_frontprio_readd(n: int) -> float:
    """QUADRATIC: frontprio re-prioritises with remove()+appendleft() per file."""
    files = make_files(n)

    def run() -> None:
        mgr = fresh_manager()
        mgr._add_files_to_queue(files, mode="back")
        mgr._add_files_to_queue(files, mode="frontprio")

    return bench(run, repeat=5, warmup=1).best


def bench_incremental_front(n: int) -> float:
    """QUADRATIC: drip files in one-at-a-time (models per-loop callback/priority adds)."""
    files = make_files(n)

    def run() -> None:
        mgr = fresh_manager()
        for f in files:
            mgr._add_files_to_queue([f], mode="front")

    return bench(run, repeat=3, warmup=1).best


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sizes", default="500,1000,2000,4000", help="comma-separated file counts")
    args = parser.parse_args()
    sizes = [int(s) for s in args.sizes.split(",")]

    print_header("File-queue ops scaling (best-of-N, ms) — watch the quadratic columns 4x per doubling")
    widths = (8, 16, 18, 20, 20, 16)
    print_row(
        "files",
        "fill empty (lin)",
        "front re-add",
        "frontprio re-add",
        "incremental drip",
        "prio: ms/N²×1e6",
        widths=widths,
    )
    for n in sizes:
        linear = bench_fill_empty(n)
        front = bench_front_readd(n)
        prio = bench_frontprio_readd(n)
        drip = bench_incremental_front(n)
        # Normalise the frontprio (clearly quadratic) op by N**2 — a flat column == O(N²).
        quad_norm = prio / (n * n) * 1e9
        print_row(
            n,
            f"{linear * 1000:.2f}",
            f"{front * 1000:.2f}",
            f"{prio * 1000:.2f}",
            f"{drip * 1000:.2f}",
            f"{quad_norm:.3f}",
            widths=widths,
        )

    print(
        "\nReading this:\n"
        "  * 'fill empty' grows ~linearly (≈2x per doubling) — the one-shot path is fine.\n"
        "  * 'front re-add', 'frontprio re-add', 'incremental drip' grow ~4x per doubling\n"
        "    (quadratic). The 'ms/N²×1e6' column stays ~flat, confirming O(N²).\n"
        "  * frontprio is the priority-parse path; incremental drip models callback adds.\n"
    )


if __name__ == "__main__":
    main()
