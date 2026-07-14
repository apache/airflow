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
import logging
import os
import subprocess
import time

from sqlalchemy import func, select

from airflow.dag_processing.bundles.manager import DagBundlesManager
from airflow.dag_processing.dagbag import DagBag
from airflow.models import DagRun
from airflow.models.serialized_dag import SerializedDagModel
from airflow.serialization.definitions.dag import SerializedDAG
from airflow.utils.session import create_session
from airflow.utils.state import State

from tests_common.test_utils.dag import create_scheduler_dag
from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS, AIRFLOW_V_3_1_PLUS

try:
    from airflow.sdk import timezone
except ImportError:
    from airflow.utils import timezone  # type: ignore[no-redef,attr-defined]

log = logging.getLogger(__name__)


def unpause_trigger_dag_and_get_run_id(dag_id: str, conf: dict | None = None) -> str:
    unpause_command = ["airflow", "dags", "unpause", dag_id]

    # Unpause the dag using the cli.
    subprocess.run(unpause_command, check=True, env=os.environ.copy())

    execution_date = timezone.utcnow()
    run_id = f"manual__{execution_date.isoformat()}"

    trigger_command = [
        "airflow",
        "dags",
        "trigger",
        dag_id,
        "--run-id",
        run_id,
        "--logical-date",
        execution_date.isoformat(),
    ]

    if conf:
        trigger_command += ["--conf", json.dumps(conf)]

    # Trigger the dag using the cli.
    subprocess.run(trigger_command, check=True, env=os.environ.copy())

    return run_id


def start_scheduler(capture_output: bool = False):
    stdout = None if capture_output else subprocess.DEVNULL
    stderr = None if capture_output else subprocess.DEVNULL

    scheduler_command_args = [
        "airflow",
        "scheduler",
    ]

    apiserver_command_args = [
        "airflow",
        "api-server",
        "--port",
        "8080",
        "--daemon",
    ]

    scheduler_process = subprocess.Popen(
        scheduler_command_args,
        env=os.environ.copy(),
        stdout=stdout,
        stderr=stderr,
    )

    apiserver_process = subprocess.Popen(
        apiserver_command_args,
        env=os.environ.copy(),
        stdout=stdout,
        stderr=stderr,
    )

    # Wait to ensure both processes have started.
    time.sleep(10)

    return scheduler_process, apiserver_process


def serialize_and_get_dags(dag_folder) -> dict[str, SerializedDAG]:
    log.info("Serializing Dags from directory %s", dag_folder)
    # Load DAGs from the dag directory.
    dag_bag = DagBag(dag_folder=dag_folder)

    dag_ids = dag_bag.dag_ids

    dag_dict: dict[str, SerializedDAG] = {}
    with create_session() as session:
        for dag_id in dag_ids:
            dag = dag_bag.get_dag(dag_id)
            assert dag is not None, f"DAG with ID {dag_id} not found."
            # Sync the DAG to the database.
            if AIRFLOW_V_3_0_PLUS:
                from airflow.models.dagbundle import DagBundleModel

                count = session.scalar(
                    select(func.count()).select_from(DagBundleModel).where(DagBundleModel.name == "testing")
                )
                if count == 0:
                    session.add(DagBundleModel(name="testing"))
                    session.commit()
                SerializedDAG.bulk_write_to_db(
                    bundle_name="testing", bundle_version=None, dags=[dag], session=session
                )
                dag_dict[dag_id] = create_scheduler_dag(dag)
            else:
                dag.sync_to_db(session=session)
                dag_dict[dag_id] = dag
            # Manually serialize the dag and write it to the db to avoid a db error.
            if AIRFLOW_V_3_1_PLUS:
                from airflow.serialization.serialized_objects import LazyDeserializedDAG

                SerializedDagModel.write_dag(
                    LazyDeserializedDAG.from_dag(dag), bundle_name="testing", session=session
                )
            else:
                SerializedDagModel.write_dag(dag, bundle_name="testing", session=session)

        session.commit()

    TESTING_BUNDLE_CONFIG = [
        {
            "name": "testing",
            "classpath": "airflow.dag_processing.bundles.local.LocalDagBundle",
            "kwargs": {"path": f"{dag_folder}", "refresh_interval": 1},
        }
    ]

    os.environ["AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST"] = json.dumps(TESTING_BUNDLE_CONFIG)
    # Initial add
    manager = DagBundlesManager()
    manager.sync_bundles_to_db()

    return dag_dict


def wait_for_dag_run(dag_id: str, run_id: str, max_wait_time: int):
    # max_wait_time, is the timeout for the DAG run to complete. The value is in seconds.
    start_time = timezone.utcnow().timestamp()

    while timezone.utcnow().timestamp() - start_time < max_wait_time:
        with create_session() as session:
            dag_run = session.scalar(
                select(DagRun).where(
                    DagRun.dag_id == dag_id,
                    DagRun.run_id == run_id,
                )
            )

            if dag_run is None:
                time.sleep(5)
                continue

            dag_run_state = dag_run.state
            log.debug("DAG Run state: %s.", dag_run_state)

            if dag_run_state in [State.SUCCESS, State.FAILED]:
                break
    return dag_run_state


def terminate_process(proc: subprocess.Popen, timeout: int = 30) -> None:
    # SIGKILL is the fallback if the process is still alive after timeout.
    proc.terminate()
    try:
        proc.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()
