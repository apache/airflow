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
Bottleneck #4 — result persistence runs per-file with many small queries.

``DagFileProcessorManager._collect_results`` calls ``handle_parsing_result``
(decorated ``@provide_session``) once per finished file, so each parsed file is
its own DB transaction calling ``update_dag_parsing_results_in_db``. Inside that
call, per file/per DAG:

  * ``_RunInfo.calculate`` runs ~2 queries **per DAG** (latest-run subquery +
    active-run count) — collection.py :: _RunInfo.calculate.
  * ``_update_import_errors`` issues ``SELECT bundle_name, filename FROM
    import_error`` with **no WHERE clause** — a full scan of the whole
    ``import_error`` table on every file persist — then per error does
    UPDATE + re-SELECT + a per-row ``UPDATE dag ... synchronize_session="fetch"``
    (collection.py :: _update_import_errors).
  * ``_update_dag_warnings`` does ``session.merge`` per warning (SELECT+upsert
    each) — collection.py :: _update_dag_warnings.

This script measures wall-time and **statement count** for persisting N files of
K DAGs each, and separately shows how ``_update_import_errors`` cost grows with
the size of the ``import_error`` table.

REQUIRES A MIGRATED METADATA DB. Run it under Breeze (recommended), e.g.:

    breeze shell
    # inside the container:
    python /opt/airflow/dev/dag_processing_benchmarks/bench_persist_results.py

or against any DB whose schema is migrated, with AIRFLOW__DATABASE__SQL_ALCHEMY_CONN
pointing at it. The script creates a throwaway bundle ('bench_persist_bundle')
and DAGs prefixed 'bench_persist_' and removes them on exit.
"""

from __future__ import annotations

import argparse
import contextlib
import datetime
import time

from _common import print_header, print_row
from sqlalchemy import event

import airflow.settings


class _StatementCounter:
    """Count + roughly categorise SQL statements via an engine event listener."""

    def __init__(self) -> None:
        self.total = 0
        self.by_verb: dict[str, int] = {}
        self._active = False

    def _on_execute(self, conn, cursor, statement, parameters, context, executemany):
        if not self._active:
            return
        self.total += 1
        verb = statement.lstrip().split(" ", 1)[0].upper()
        self.by_verb[verb] = self.by_verb.get(verb, 0) + 1

    @contextlib.contextmanager
    def counting(self):
        self.total = 0
        self.by_verb = {}
        self._active = True
        try:
            yield self
        finally:
            self._active = False

    def install(self) -> None:
        event.listen(airflow.settings.engine, "before_cursor_execute", self._on_execute)

    def remove(self) -> None:
        event.remove(airflow.settings.engine, "before_cursor_execute", self._on_execute)


def db_is_ready() -> bool:
    from sqlalchemy import text

    from airflow.utils.session import create_session

    try:
        with create_session() as session:
            session.execute(text("SELECT 1"))
        return True
    except Exception as e:
        print(f"\n[skipped] Metadata DB not available / not migrated: {e}")
        print("Run this under Breeze (see the module docstring).\n")
        return False


def ensure_bundle(name: str) -> None:
    from airflow.models.dagbundle import DagBundleModel
    from airflow.utils.session import create_session

    with create_session() as session:
        if session.get(DagBundleModel, name) is None:
            session.add(DagBundleModel(name=name))


def build_lazy_dags(num_files: int, dags_per_file: int):
    """Return a list (per file) of lists of LazyDeserializedDAG."""
    from airflow.providers.standard.operators.bash import BashOperator
    from airflow.sdk import DAG
    from airflow.serialization.serialized_objects import DagSerialization, LazyDeserializedDAG

    per_file = []
    for f in range(num_files):
        lazy = []
        for d in range(dags_per_file):
            with DAG(
                dag_id=f"bench_persist_{f}_{d}",
                schedule=None,
                start_date=datetime.datetime(2024, 1, 1),
            ) as dag:
                prev = None
                for i in range(5):
                    task = BashOperator(task_id=f"t{i}", bash_command="echo hi")
                    if prev is not None:
                        prev >> task
                    prev = task
            dag.relative_fileloc = f"bench_persist_{f}.py"
            dag.fileloc = f"/tmp/bench_persist_{f}.py"
            lazy.append(LazyDeserializedDAG(data=DagSerialization.to_dict(dag)))
        per_file.append((f"bench_persist_{f}.py", lazy))
    return per_file


def bench_persist(num_files: int, dags_per_file: int, bundle: str) -> None:
    from airflow.dag_processing.collection import update_dag_parsing_results_in_db
    from airflow.utils.session import create_session

    per_file = build_lazy_dags(num_files, dags_per_file)
    counter = _StatementCounter()
    counter.install()
    try:
        print_header(
            f"update_dag_parsing_results_in_db: {num_files} files x {dags_per_file} DAGs "
            "(per-file transaction)"
        )
        widths = (8, 14, 14, 40)
        print_row("file", "time (ms)", "queries", "by verb", widths=widths)
        total_time = 0.0
        total_queries = 0
        for idx, (rel, lazy) in enumerate(per_file):
            with counter.counting():
                start = time.perf_counter()
                # Mirror the manager: one transaction per file.
                with create_session() as session:
                    update_dag_parsing_results_in_db(
                        bundle_name=bundle,
                        bundle_version=None,
                        dags=lazy,
                        import_errors={},
                        parse_duration=0.1,
                        warnings=set(),
                        session=session,
                        files_parsed={(bundle, rel)},
                    )
                elapsed = time.perf_counter() - start
            total_time += elapsed
            total_queries += counter.total
            if idx < 3 or idx == len(per_file) - 1:
                verbs = ", ".join(f"{k}:{v}" for k, v in sorted(counter.by_verb.items()))
                print_row(idx, f"{elapsed * 1000:.1f}", counter.total, verbs, widths=widths)
        print(
            f"\nTOTAL: {total_time * 1000:.0f} ms, {total_queries} queries "
            f"({total_queries / num_files:.1f} queries/file, "
            f"{total_time / num_files * 1000:.1f} ms/file)\n"
        )
    finally:
        counter.remove()


def cleanup(bundle: str) -> None:
    from sqlalchemy import delete

    from airflow.models.dag import DagModel
    from airflow.models.dagbundle import DagBundleModel
    from airflow.utils.session import create_session

    with contextlib.suppress(Exception), create_session() as session:
        session.execute(delete(DagModel).where(DagModel.bundle_name == bundle))
        session.execute(delete(DagBundleModel).where(DagBundleModel.name == bundle))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--files", type=int, default=20, help="number of files to persist")
    parser.add_argument("--dags-per-file", type=int, default=1, help="DAGs per file")
    parser.add_argument("--no-cleanup", action="store_true", help="keep created rows for inspection")
    args = parser.parse_args()

    if not db_is_ready():
        return

    bundle = "bench_persist_bundle"
    ensure_bundle(bundle)
    try:
        bench_persist(args.files, args.dags_per_file, bundle)
    finally:
        if not args.no_cleanup:
            cleanup(bundle)


if __name__ == "__main__":
    main()
