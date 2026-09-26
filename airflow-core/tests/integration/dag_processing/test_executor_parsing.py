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
from __future__ import annotations

import json
import os
import signal
import sqlite3
import subprocess
from zipfile import ZipFile

import pytest

from airflow.dag_processing.discovery import discover_python_bundle
from airflow.dag_processing.executor_manager import ROUTE
from airflow.dag_processing.orchestrator import OrchestrationStore, ParseOrchestrator
from airflow.executors.workloads import BundleInfo

pytestmark = pytest.mark.db_test


def run_airflow(args, env):
    with subprocess.Popen(
        ["airflow", *args],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        start_new_session=True,
    ) as process:
        try:
            output, _ = process.communicate(timeout=180)
        except subprocess.TimeoutExpired:
            os.killpg(process.pid, signal.SIGKILL)
            output, _ = process.communicate()
            pytest.fail(f"Command timed out: {args}\n{output}")
        assert process.returncode == 0, output
        return output


def test_dag_processor_parses_and_reparses_through_executor(tmp_path):
    """Exercise the documented command, including its real API, runner and SDK importer."""
    source = tmp_path / "dags"
    source.mkdir()
    imported = tmp_path / "imports.txt"
    dag_file = source / "example.py"

    def write_dag(revision):
        dag_file.write_text(
            "from airflow.sdk import DAG\n"
            "from pathlib import Path\n"
            f"with Path({str(imported)!r}).open('a') as stream: stream.write('imported\\n')\n"
            f"dag = DAG('executor_poc', schedule=None, doc_md='revision {revision}')\n"
        )

    write_dag(1)
    (source / "broken.py").write_text("raise ValueError('expected prototype import error')\n")
    with ZipFile(source / "archive.zip", "w") as archive:
        archive.writestr("zipped.py", "from airflow.sdk import DAG\ndag = DAG('zipped_poc', schedule=None)\n")
    database = tmp_path / "airflow.db"
    logs = tmp_path / "parse-logs"
    env = {
        name: value
        for name, value in os.environ.items()
        if not name.startswith(("AIRFLOW__", "AIRFLOW_DAG_PARSING_POC_", "_AIRFLOW"))
        and name not in {"AIRFLOW_HOME", "AIRFLOW_CONFIG"}
    }
    env.update(
        AIRFLOW_HOME=str(tmp_path),
        AIRFLOW_CONFIG=str(tmp_path / "airflow.cfg"),
        AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=f"sqlite:///{database}",
        AIRFLOW__CORE__LOAD_EXAMPLES="False",
        AIRFLOW__CORE__EXECUTOR="LocalExecutor",
        AIRFLOW__CORE__PARALLELISM="7",
        AIRFLOW__CORE__MIN_SERIALIZED_DAG_UPDATE_INTERVAL="0",
        AIRFLOW__DAG_PROCESSOR__PARSING_PROCESSES="1",
        AIRFLOW__DAG_PROCESSOR__MIN_FILE_PROCESS_INTERVAL="0",
        AIRFLOW__LOGGING__DAG_PROCESSOR_CHILD_PROCESS_LOG_DIRECTORY=str(logs),
        AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST=json.dumps(
            [
                {
                    "name": "verification",
                    "classpath": "airflow.dag_processing.bundles.local.LocalDagBundle",
                    "kwargs": {"path": str(source)},
                }
            ]
        ),
    )
    run_airflow(["db", "migrate"], env)
    command = ["dag-processor", "--executor-parsing", "--bundle-name", "verification", "--num-runs", "1"]
    unsent_workload = None
    for revision in (1, 2):
        if revision == 2:
            write_dag("unsent")
            store = OrchestrationStore(database)
            orchestrator = ParseOrchestrator(store, route=ROUTE, bundle="verification")
            orchestrator.update_inventory(
                BundleInfo(name="verification", version=None),
                discover_python_bundle(source, bundle_name="verification"),
            )
            unsent_workload = orchestrator.step(eligible_paths={"example.py"}).workload_id
            assert unsent_workload is not None
        write_dag(revision)
        run_airflow(command, env)
        with sqlite3.connect(database) as connection:
            assert connection.execute("SELECT dag_id FROM serialized_dag ORDER BY dag_id").fetchall() == [
                ("executor_poc",),
                ("zipped_poc",),
            ]
            assert connection.execute(
                "SELECT count(*) FROM admissions WHERE state != 'released'"
            ).fetchone() == (0,)
            assert connection.execute(
                "SELECT accepted_count FROM parse_sources ORDER BY path"
            ).fetchall() == [(revision,), (revision,), (revision,)]
            assert connection.execute("SELECT count(*) FROM import_error").fetchone() == (1,)
            assert any(
                f"revision {revision}" in row[0]
                for row in connection.execute("SELECT source_code FROM dag_code")
            )
        assert imported.read_text().splitlines() == ["imported"] * revision
    assert {row["status"] for row in store.get_attempts(unsent_workload)} == {"retired"}
    assert any(path.is_file() for path in logs.rglob("*"))
    assert run_airflow(["config", "get-value", "core", "parallelism"], env).strip().endswith("7")
    assert run_airflow(["config", "get-value", "core", "executor"], env).strip().endswith("LocalExecutor")
