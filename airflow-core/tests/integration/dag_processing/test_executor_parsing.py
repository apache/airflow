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
import sys
import textwrap
from zipfile import ZipFile

import pytest

pytestmark = pytest.mark.db_test


def run_command(command, env, *, timeout=180):
    with subprocess.Popen(
        command,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        start_new_session=True,
    ) as process:
        try:
            output, _ = process.communicate(timeout=timeout)
        except subprocess.TimeoutExpired:
            os.killpg(process.pid, signal.SIGKILL)
            output, _ = process.communicate()
            pytest.fail(f"Command timed out: {command}\n{output}")
        assert process.returncode == 0, output
        return output


@pytest.mark.parametrize("start_method", ["fork", "spawn"])
def test_existing_manager_parses_and_reparses_through_local_executor(tmp_path, start_method):
    source = tmp_path / "dags"
    source.mkdir()
    imported = tmp_path / "imports.txt"
    dag_file = source / "example.py"
    (source / "broken.py").write_text("raise ValueError('expected import error')\n")
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
        AIRFLOW__CORE__DAG_DISCOVERY_SAFE_MODE="False",
        AIRFLOW__DAG_PROCESSOR__MP_START_METHOD=start_method,
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
    run_command(["airflow", "db", "migrate"], env)
    command = [
        "airflow",
        "dag-processor",
        "--executor-parsing",
        "--bundle-name",
        "verification",
        "--num-runs",
        "1",
    ]
    for revision in (1, 2):
        dag_file.write_text(
            "from airflow.sdk import DAG, Variable\n"
            "from pathlib import Path\n"
            f"with Path({str(imported)!r}).open('a') as stream: stream.write('imported\\n')\n"
            "value = Variable.get('local_executor_parse_probe', default='fallback')\n"
            f"dag = DAG('executor_poc', schedule=None, doc_md='revision {revision} ' + value)\n"
        )
        run_command(command, env)
        with sqlite3.connect(database) as connection:
            assert connection.execute("SELECT dag_id FROM serialized_dag ORDER BY dag_id").fetchall() == [
                ("executor_poc",),
                ("zipped_poc",),
            ]
            assert connection.execute("SELECT count(*) FROM import_error").fetchone() == (1,)
            assert any(
                f"revision {revision}" in row[0]
                for row in connection.execute("SELECT source_code FROM dag_code")
            )
            assert not connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table' AND name LIKE 'dag_parse_%'"
            ).fetchall()
        assert imported.read_text().splitlines() == ["imported"] * revision
    assert any(path.is_file() for path in logs.rglob("*.log"))
    assert run_command(["airflow", "config", "get-value", "core", "parallelism"], env).strip().endswith("7")
    if start_method == "spawn":
        (source / "timeout.py").write_text(
            "import time\nfrom airflow.sdk import DAG\ntime.sleep(3600)\ndag = DAG('never_published')\n"
        )
        env["AIRFLOW__DAG_PROCESSOR__DAG_FILE_PROCESSOR_TIMEOUT"] = "5"
        run_command(command, env, timeout=45)
        with sqlite3.connect(database) as connection:
            assert connection.execute("SELECT dag_id FROM serialized_dag ORDER BY dag_id").fetchall() == [
                ("executor_poc",),
                ("zipped_poc",),
            ]


def test_spawn_consumer_starts_before_large_parsing_delivery(tmp_path):
    script = textwrap.dedent(
        f"""
        import multiprocessing
        import time
        from pathlib import Path
        from uuid import uuid4
        from airflow import settings
        from airflow.executors.local_executor import LocalExecutor
        from airflow.executors.workloads import BundleInfo, ParseDagFile, WorkloadType
        from airflow.executors.workloads.parsing import ParseDagFileState

        if __name__ == '__main__':
            multiprocessing.set_start_method('spawn', force=True)
            workload = ParseDagFile(
                workload_id=uuid4(), bundle_info=BundleInfo(name='local'),
                bundle_path=Path({str(tmp_path)!r}), relative_path='dag.py',
                log_path={str(tmp_path / "parser.log")!r}, control_dir=Path({str(tmp_path)!r}),
                timeout=10, callbacks=['x' * 256000],
            )
            workload.cancel_path.touch()
            executor = LocalExecutor(parallelism=1)
            executor.supported_workload_types = frozenset({{WorkloadType.PARSE_DAG_FILE}})
            executor.start()
            try:
                with settings.Session() as session:
                    executor.queue_workload(workload, session=session)
                deadline = time.monotonic() + 30
                while time.monotonic() < deadline:
                    executor.heartbeat()
                    if events := executor.get_event_buffer():
                        assert events[workload.key][0] == ParseDagFileState.SUCCESS
                        break
                    time.sleep(0.05)
                else:
                    raise TimeoutError('Parsing delivery did not finish')
            except BaseException:
                executor.terminate()
                raise
            finally:
                executor.end()
        """
    )
    run_command([sys.executable, "-c", script], os.environ.copy(), timeout=60)
