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
Bottleneck #2 — every DAG file is read and AST-parsed several times per parse.

For a single file, before the user code even executes, the pipeline does:

  * ``might_contain_dag`` — reads the whole file as bytes and ``.lower()``-copies
    it, and calls ``conf.getimport(...)`` on *every* call
    (utils/file.py :: might_contain_dag / might_contain_dag_via_default_heuristic).
    This runs during directory scan *and* again inside the importer.
  * ``iter_airflow_imports`` — ``ast.parse`` of the file bytes to find airflow
    imports to pre-import (utils/file.py :: iter_airflow_imports, driven by
    processor.py :: _pre_import_airflow_modules).
  * ``check_dag_file_stability`` — ``ast.parse`` again **and** a full
    ``ast.NodeVisitor`` walk of the tree on every parse, regardless of whether
    the file changed (dag_version_inflation_checker.py :: check_dag_file_stability).

So the same bytes are read 2–3x and ``ast.parse``-d 2x per file per loop. This
script generates synthetic DAG files of varying size and reports the per-stage
cost plus the combined "pre-execution overhead".

Run:
    uv run --project airflow-core python dev/dag_processing_benchmarks/bench_file_scan.py
    uv run --project airflow-core python dev/dag_processing_benchmarks/bench_file_scan.py --tasks 10,50,200
"""

from __future__ import annotations

import argparse
import tempfile
import textwrap
from pathlib import Path

from _common import bench, print_header, print_row

from airflow.utils.dag_version_inflation_checker import check_dag_file_stability
from airflow.utils.file import iter_airflow_imports, might_contain_dag


def generate_dag_source(num_tasks: int) -> str:
    """Build a realistic-ish DAG file with ``num_tasks`` tasks and some varying-value bait."""
    header = textwrap.dedent(
        """
        from __future__ import annotations

        import datetime
        import random
        from airflow.sdk import DAG
        from airflow.providers.standard.operators.bash import BashOperator

        DEFAULT_ARGS = {"owner": "bench", "retries": 1}

        with DAG(
            dag_id="bench_scan_dag",
            schedule="@daily",
            start_date=datetime.datetime(2024, 1, 1),
            default_args=DEFAULT_ARGS,
            tags=["bench", "scan"],
        ) as dag:
        """
    )
    body_lines = []
    for i in range(num_tasks):
        body_lines.append(f'    t{i} = BashOperator(task_id="task_{i}", bash_command="echo {i} && sleep 0")')
    # A couple of chaining lines so the AST has dependency expressions too.
    for i in range(1, num_tasks):
        body_lines.append(f"    t{i - 1} >> t{i}")
    return header + "\n".join(body_lines) + "\n"


def write_temp_dag(num_tasks: int, tmpdir: Path) -> Path:
    path = tmpdir / f"dag_{num_tasks}.py"
    path.write_text(generate_dag_source(num_tasks))
    return path


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tasks", default="10,50,200,500", help="comma-separated task counts")
    args = parser.parse_args()
    task_counts = [int(t) for t in args.tasks.split(",")]

    print_header("Per-file pre-execution scan cost (best-of-7, ms)")
    widths = (10, 12, 18, 22, 22, 14)
    print_row(
        "tasks",
        "bytes",
        "might_contain_dag",
        "iter_airflow_imports",
        "check_dag_stability",
        "combined",
        widths=widths,
    )

    with tempfile.TemporaryDirectory() as tmp:
        tmpdir = Path(tmp)
        for n in task_counts:
            path = write_temp_dag(n, tmpdir)
            fp = str(path)
            size = path.stat().st_size

            might = bench(lambda: might_contain_dag(fp, True), repeat=7, warmup=2).best
            imports = bench(lambda: list(iter_airflow_imports(fp)), repeat=7, warmup=2).best
            stability = bench(lambda: check_dag_file_stability(fp), repeat=7, warmup=2).best

            def combined() -> None:
                might_contain_dag(fp, True)
                list(iter_airflow_imports(fp))
                check_dag_file_stability(fp)

            total = bench(combined, repeat=7, warmup=2).best

            print_row(
                n,
                size,
                f"{might * 1000:.3f}",
                f"{imports * 1000:.3f}",
                f"{stability * 1000:.3f}",
                f"{total * 1000:.3f}",
                widths=widths,
            )

    print(
        "\nReading this: 'combined' is paid per file on every parse loop, on top of\n"
        "the actual import/exec of user code. 'check_dag_stability' (full AST walk)\n"
        "and the duplicate ast.parse in 'iter_airflow_imports' dominate for big files.\n"
    )


if __name__ == "__main__":
    main()
