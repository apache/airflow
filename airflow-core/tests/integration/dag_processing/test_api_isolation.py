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

"""Run with Breeze and PostgreSQL; the processor joins an API-only internal network."""

from __future__ import annotations

import json
import secrets
import shutil
import socket
import subprocess
import time
from pathlib import Path
from urllib.parse import urlsplit

import pytest
from sqlalchemy import delete, select
from sqlalchemy.engine import make_url
from uuid6 import uuid7

from airflow.callbacks.callback_requests import DagCallbackRequest
from airflow.configuration import conf
from airflow.jobs.job import Job
from airflow.models.callback import DagProcessorCallback
from airflow.models.dag import DagModel
from airflow.models.dagbag import DagPriorityParsingRequest
from airflow.models.dagbundle import DagBundleModel
from airflow.models.dagcode import DagCode
from airflow.models.variable import Variable

from tests_common.test_utils.db import clear_db_callbacks, clear_db_dag_bundles, clear_db_dags
from unit.api_fastapi.execution_api.conftest import async_db_engine as async_db_engine
from unit.api_fastapi.execution_api.test_dag_processor_client import (
    api_requests as api_requests,
    api_url as api_url,
    clean_db as clean_db,
    freeze_time as freeze_time,
    provision_token as provision_token,
)

pytestmark = [
    pytest.mark.db_test,
    pytest.mark.skipif(
        not Path("/var/run/docker.sock").exists() or not shutil.which("docker"),
        reason="Run in Breeze with its Docker socket mounted",
    ),
]


@pytest.fixture
def api_bind_host():
    return "0.0.0.0"


@pytest.fixture
def api_secret():
    return secrets.token_hex(32)


def run_docker(*args, input=None, check=True, timeout=30):
    return subprocess.run(
        ["docker", *args],
        input=input,
        capture_output=True,
        text=True,
        check=check,
        timeout=timeout,
    )


@pytest.mark.parametrize("shutdown", ["signal", "api_restart"])
def test_processor_without_database_network_or_server_credentials(
    api_url,
    provision_token,
    session,
    tmp_path,
    shutdown,
):
    database = make_url(conf.get("database", "sql_alchemy_conn"))
    if not database.host:
        pytest.skip("Requires PostgreSQL or MySQL in Breeze")
    database_ip = socket.gethostbyname(database.host)
    database_port = database.port or (5432 if database.drivername.startswith("postgres") else 3306)
    current = json.loads(run_docker("inspect", socket.gethostname()).stdout)[0]
    mounts = {mount["Destination"]: mount["Source"] for mount in current["Mounts"]}
    suffix = uuid7().hex
    network = f"processor-isolation-{suffix}"
    worker = f"processor-isolation-worker-{suffix}"
    clear_db_callbacks()
    clear_db_dags()
    clear_db_dag_bundles()
    session.execute(delete(DagPriorityParsingRequest))
    session.add(DagBundleModel(name="bundle-a"))
    session.add(DagPriorityParsingRequest(bundle_name="bundle-a", relative_fileloc="a.py"))
    callback = DagProcessorCallback(
        priority_weight=1,
        callback=DagCallbackRequest(
            filepath="a.py",
            bundle_name="bundle-a",
            bundle_version=None,
            dag_id="isolated",
            run_id="run",
        ),
    )
    session.add(callback)
    session.commit()
    callback_id = callback.id
    Variable.set("isolation-value", "from-api")
    Variable.delete("isolation-callback")
    source_code = (
        "from airflow.sdk import DAG, Variable\n"
        "def callback(context):\n"
        "    Variable.set('isolation-callback', 'done')\n"
        "dag = DAG('isolated', schedule=None, description=Variable.get('isolation-value'), "
        "on_failure_callback=callback)\n"
    )
    run_docker("network", "create", "--internal", network)
    try:
        run_docker("network", "connect", network, current["Id"])
        inspected = json.loads(run_docker("inspect", current["Id"]).stdout)[0]
        api_ip = inspected["NetworkSettings"]["Networks"][network]["IPAddress"]
        worker_api_url = f"http://{api_ip}:{urlsplit(api_url).port}/execution/"
        environment = {
            "USER": "airflow",
            "AIRFLOW_HOME": "/tmp/isolated-airflow",
            "AIRFLOW_CONFIG": "/tmp/isolated-airflow/airflow.cfg",
            "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN": f"postgresql+psycopg://blocked:blocked@{database_ip}:{database_port}/blocked",
            "AIRFLOW__CORE__FERNET_KEY": "",
            "AIRFLOW__API_AUTH__JWT_SECRET": "",
            "AIRFLOW__API_AUTH__JWT_PRIVATE_KEY_PATH": "",
            "AIRFLOW__CORE__LOAD_EXAMPLES": "False",
            "AIRFLOW__CORE__EXECUTION_API_SERVER_URL": worker_api_url,
            "AIRFLOW__DAG_PROCESSOR__EXECUTION_API_TOKEN_FILE": "/tmp/processor.jwt",
            "AIRFLOW__DAG_PROCESSOR__HEALTH_CHECK_THRESHOLD": "5",
            "AIRFLOW__SCHEDULER__JOB_HEARTBEAT_SEC": "1",
            "AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST": json.dumps(
                [
                    {
                        "name": "bundle-a",
                        "classpath": "airflow.dag_processing.bundles.local.LocalDagBundle",
                        "kwargs": {"path": "/tmp/worker-only-dags"},
                    }
                ]
            ),
        }
        arguments = [
            "run",
            "--detach",
            "--name",
            worker,
            "--network",
            network,
            "--cap-drop",
            "ALL",
            "--security-opt",
            "no-new-privileges",
            "--user",
            "50000:0",
            "--read-only",
            "--tmpfs",
            "/tmp:rw,exec,size=256m",
            "--entrypoint",
            "sleep",
        ]
        for package in ("airflow-core", "task-sdk", "shared", "providers", "devel-common"):
            arguments += [
                "--mount",
                f"type=bind,source={mounts[f'/opt/airflow/{package}']},target=/opt/airflow/{package},readonly",
            ]
        for key, value in environment.items():
            arguments += ["--env", f"{key}={value}"]
        arguments += [current["Image"], "600"]
        run_docker(*arguments)
        run_docker(
            "exec",
            "-i",
            worker,
            "sh",
            "-c",
            "umask 077; cat > /tmp/processor.jwt",
            input=provision_token().read_text(),
        )
        run_docker(
            "exec",
            "-i",
            worker,
            "sh",
            "-c",
            "mkdir -p /tmp/worker-only-dags; cat > /tmp/worker-only-dags/a.py",
            input=source_code,
        )
        assert not Path("/tmp/worker-only-dags/a.py").exists()
        denied = run_docker(
            "exec",
            worker,
            "python",
            "-c",
            (
                "import socket,sys\n"
                f"try: socket.create_connection(({database_ip!r}, {database_port}), timeout=2)\n"
                "except OSError: sys.exit(0)\n"
                "sys.exit(1)\n"
            ),
        )
        assert denied.returncode == 0
        with (tmp_path / "processor.log").open("w+") as output:
            process = subprocess.Popen(
                ["docker", "exec", worker, "airflow", "dag-processor", "--bundle-name", "bundle-a"],
                stdout=output,
                stderr=subprocess.STDOUT,
            )
            deadline = time.monotonic() + 90
            while time.monotonic() < deadline:
                session.expire_all()
                dag = session.get(DagModel, "isolated")
                delivered = session.get(DagProcessorCallback, callback_id)
                if dag and delivered is None:
                    break
                if process.poll() is not None:
                    output.seek(0)
                    pytest.fail(output.read())
                time.sleep(0.2)
            else:
                output.seek(0)
                pytest.fail(output.read())
            assert Variable.get("isolation-callback") == "done"
            assert session.scalar(select(DagPriorityParsingRequest)) is None
            assert (
                session.scalar(select(DagCode.source_code).where(DagCode.dag_id == "isolated")) == source_code
            )
            run_docker("exec", worker, "airflow", "dag-processor", "--check-ready")
            pause = (
                "import json,os,signal; "
                "pid=json.load(open('/tmp/isolated-airflow/dag_processor_health.json'))['pid']; "
                "os.kill(pid, signal.SIGSTOP)"
            )
            run_docker("exec", worker, "python", "-c", pause)
            time.sleep(6)
            assert (
                run_docker(
                    "exec", worker, "airflow", "dag-processor", "--check-health", check=False
                ).returncode
                != 0
            )
            run_docker("exec", worker, "python", "-c", pause.replace("SIGSTOP", "SIGCONT"))
            if shutdown == "signal":
                run_docker("exec", worker, "python", "-c", pause.replace("SIGSTOP", "SIGTERM"))
            else:
                session.scalar(select(Job)).state = "restarting"
                session.commit()
            assert (process.wait(timeout=30) == 0) == (shutdown == "signal")
            session.expire_all()
            assert session.scalar(select(Job.state)) == ("success" if shutdown == "signal" else "failed")
    finally:
        run_docker("rm", "--force", worker, check=False)
        run_docker("network", "disconnect", network, current["Id"], check=False)
        run_docker("network", "rm", network, check=False)
        clear_db_callbacks()
        clear_db_dags()
        clear_db_dag_bundles()
        session.execute(delete(DagPriorityParsingRequest))
        session.commit()
        Variable.delete("isolation-value")
        Variable.delete("isolation-callback")
