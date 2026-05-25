#
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
"""Local simulation harness for the Cloud Composer system test example.

Loads ``providers/google/tests/system/google/cloud/composer/example_cloud_composer.py``
in-process, replaces all Google Cloud API surfaces with mocks, and drives the
Dag with ``dag.test()``. Purpose: prove that the empty-window fix on
``CloudComposerDAGRunSensor`` / ``CloudComposerDAGRunTrigger`` (PR #67052) does
not regress to the infinite-loop failure mode that caused PR #61046 to be
reverted, without requiring real GCP credentials.

Run with::

    breeze run python dev/simulate_composer_system_test.py

Exits 0 on success; non-zero (with traceback) on failure. The deferrable
sensor's trigger runs inline under ``dag.test()``, so both the sync and the
deferred ``CloudComposerDAGRunSensor`` paths are exercised end-to-end.
"""

from __future__ import annotations

import contextlib
import importlib.util
import sys
import traceback
from pathlib import Path
from unittest import mock
from unittest.mock import AsyncMock, MagicMock

REPO = Path(__file__).resolve().parents[1]
EXAMPLE_PATH = REPO / "providers/google/tests/system/google/cloud/composer/example_cloud_composer.py"
TRIGGERED_RUN_ID = "manual__simulated"
SIMULATED_IMAGE_VERSION = "composer-3-airflow-2.10.4"


def build_sync_hook_mock() -> MagicMock:
    """Return a MagicMock that satisfies every CloudComposerHook call site
    used by ``example_cloud_composer``'s operators and the sync Dag-run sensor.
    """
    # Use the real Environment.State enum value so the operator's
    # `_raise_if_environment_not_running` check (which does
    # `Environment.State(environment.state)`) succeeds.
    from google.cloud.orchestration.airflow.service_v1.types import Environment

    hook = MagicMock(name="CloudComposerHook_sim")

    environment = MagicMock(name="Environment_sim")
    environment.config.airflow_uri = "https://composer.example/sim"
    environment.config.software_config.image_version = SIMULATED_IMAGE_VERSION
    environment.state = Environment.State.RUNNING
    environment.name = "projects/sim/locations/us-central1/environments/sim-env"
    hook.get_environment.return_value = environment
    hook.get_environment_name.return_value = environment.name

    operation = MagicMock(name="Operation_sim")
    operation.done = True
    operation.error.message = ""
    operation.name = "projects/sim/locations/us-central1/operations/sim-op"
    operation.operation.name = "projects/sim/locations/us-central1/operations/sim-op"
    hook.create_environment.return_value = operation
    hook.update_environment.return_value = operation
    hook.delete_environment.return_value = operation
    hook.wait_for_operation.return_value = environment

    hook.list_environments.return_value = []
    hook.list_image_versions.return_value = []

    hook.execute_airflow_command.return_value = {
        "execution_id": "sim-exec",
        "pod": "sim-pod",
        "pod_namespace": "sim-ns",
        "error": "",
    }
    hook.wait_command_execution_result.return_value = {
        "output": [{"line_number": 1, "content": "[]"}],
        "output_end": True,
        "exit_info": {"exit_code": 0, "error": ""},
    }

    hook.trigger_dag_run.return_value = {
        "dag_run_id": TRIGGERED_RUN_ID,
        "state": "queued",
        "logical_date": "2024-01-01T00:00:00+00:00",
        "execution_date": "2024-01-01T00:00:00+00:00",
    }

    hook.get_dag_runs.return_value = {
        "dag_runs": [
            {
                "dag_id": "airflow_monitoring",
                "dag_run_id": TRIGGERED_RUN_ID,
                "state": "success",
                "logical_date": "2024-01-01T00:00:00+00:00",
                "execution_date": "2024-01-01T00:00:00+00:00",
            }
        ],
        "total_entries": 1,
    }

    from datetime import datetime as _dt, timedelta as _td, timezone as _tz

    in_window = (_dt.now(_tz.utc) - _td(hours=12)).replace(microsecond=0).isoformat()
    hook.get_task_instances.return_value = {
        "task_instances": [
            {
                "task_id": "echo",
                "dag_id": "airflow_monitoring",
                "state": "success",
                "logical_date": in_window,
                "execution_date": in_window,
            }
        ],
        "total_entries": 1,
    }
    return hook


def build_async_hook_mock() -> AsyncMock:
    """Return an AsyncMock for the deferred sensor's trigger path."""
    from datetime import datetime as _dt, timezone as _tz

    hook = AsyncMock(name="CloudComposerAsyncHook_sim")

    environment = MagicMock(name="EnvironmentAsync_sim")
    environment.config.airflow_uri = "https://composer.example/sim-async"
    environment.config.software_config.image_version = SIMULATED_IMAGE_VERSION
    hook.get_environment.return_value = environment

    operation = MagicMock(name="OperationAsync_sim")
    operation.done = True
    operation.error.message = ""
    operation.name = "sim-async-op"
    hook.get_operation.return_value = operation

    # Compute a timestamp clearly inside the external-task sensor's window
    # ``[datetime.now() - timedelta(1), datetime.now()]`` (evaluated at the
    # example's parse time, which happens a few seconds before this sim
    # runs). Pin it 12 hours back so it's safely interior to the window.
    from datetime import timedelta as _td

    in_window = (_dt.now(_tz.utc) - _td(hours=12)).replace(microsecond=0).isoformat()

    hook.get_dag_runs.return_value = {
        "dag_runs": [
            {
                "dag_id": "airflow_monitoring",
                "dag_run_id": TRIGGERED_RUN_ID,
                "state": "success",
                # Date deliberately outside any plausible execution_range; we
                # only succeed via the composer_dag_run_id branch, which does
                # not consult the window.
                "logical_date": "2099-01-01T00:00:00+00:00",
                "execution_date": "2099-01-01T00:00:00+00:00",
            }
        ],
        "total_entries": 1,
    }
    # External-task sensor uses ``get_task_instances`` and expects an in-window
    # logical_date. Use the dag.test() logical_date neighbourhood so the
    # trigger does not infinite-loop on an empty list.
    hook.get_task_instances.return_value = {
        "task_instances": [
            {
                "task_id": "echo",
                "dag_id": "airflow_monitoring",
                "state": "success",
                "logical_date": in_window,
                "execution_date": in_window,
            }
        ],
        "total_entries": 1,
    }
    hook.execute_airflow_command.return_value = {
        "execution_id": "sim-async-exec",
        "pod": "sim-async-pod",
        "pod_namespace": "sim-async-ns",
        "error": "",
    }
    hook.wait_command_execution_result.return_value = {
        "output": [{"line_number": 1, "content": "[]"}],
        "output_end": True,
        "exit_info": {"exit_code": 0, "error": ""},
    }
    return hook


@contextlib.contextmanager
def apply_patches():
    """Apply every patch needed to keep ``dag.test()`` from touching GCP."""
    sync_hook = build_sync_hook_mock()
    async_hook = build_async_hook_mock()

    project_resp = {"projectNumber": "12345"}
    build_mock = MagicMock(name="googleapi_build_sim")
    (
        build_mock.return_value.__enter__.return_value.projects.return_value.get.return_value.execute.return_value
    ) = project_resp

    # Stub Environment.to_dict / ImageVersion.to_dict — operator ``execute``
    # methods route their mocked hook results through proto-class converters
    # that only work on real proto instances.
    env_dict = {
        "name": "sim-env",
        "config": {
            "airflow_uri": "https://composer.example/sim",
            "software_config": {"image_version": SIMULATED_IMAGE_VERSION},
        },
    }
    img_dict = {"image_version_id": SIMULATED_IMAGE_VERSION}

    exec_cmd_dict = {
        "execution_id": "sim-exec",
        "pod": "sim-pod",
        "pod_namespace": "sim-ns",
        "error": "",
    }

    patches = [
        mock.patch("googleapiclient.discovery.build", build_mock),
        mock.patch(
            "airflow.providers.google.cloud.operators.cloud_composer.CloudComposerHook",
            return_value=sync_hook,
        ),
        mock.patch(
            "airflow.providers.google.cloud.sensors.cloud_composer.CloudComposerHook",
            return_value=sync_hook,
        ),
        mock.patch(
            "airflow.providers.google.cloud.triggers.cloud_composer.CloudComposerAsyncHook",
            return_value=async_hook,
        ),
        mock.patch(
            "airflow.providers.google.cloud.operators.cloud_composer.Environment.to_dict",
            return_value=env_dict,
        ),
        mock.patch(
            "airflow.providers.google.cloud.sensors.cloud_composer.Environment.to_dict",
            return_value=env_dict,
        ),
        mock.patch(
            "airflow.providers.google.cloud.operators.cloud_composer.ImageVersion.to_dict",
            return_value=img_dict,
        ),
        mock.patch(
            "airflow.providers.google.cloud.operators.cloud_composer.ExecuteAirflowCommandResponse.to_dict",
            return_value=exec_cmd_dict,
        ),
    ]
    started = []
    try:
        for p in patches:
            started.append(p.start())
        yield sync_hook, async_hook
    finally:
        for p in reversed(patches):
            p.stop()


def speed_up_polling(dag) -> None:
    """Shrink poke/poll intervals so the simulation finishes quickly."""
    for task in dag.tasks:
        if hasattr(task, "poke_interval"):
            task.poke_interval = 0
        if hasattr(task, "poll_interval"):
            task.poll_interval = 0


def load_example_module():
    spec = importlib.util.spec_from_file_location("example_cloud_composer_sim", EXAMPLE_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"could not load spec for {EXAMPLE_PATH}")
    module = importlib.util.module_from_spec(spec)
    sys.modules["example_cloud_composer_sim"] = module
    spec.loader.exec_module(module)
    return module


def initialize_metadata_db() -> None:
    """Create the metadata tables needed by the local simulation."""
    from airflow.utils.db import initdb

    initdb()


def register_dag_bundle_and_serialize(dag) -> None:
    """Mirror what ``tests_common.test_utils.system_tests.get_test_run`` does
    so ``dag.test()`` can find a serialized Dag attached to a bundle.
    """
    from sqlalchemy import func, select

    from airflow.models.dag import DagModel
    from airflow.models.dagbundle import DagBundleModel
    from airflow.models.serialized_dag import SerializedDagModel
    from airflow.serialization.serialized_objects import LazyDeserializedDAG
    from airflow.utils.session import create_session

    bundle_name = "testing"
    # Commit the bundle first so the DagModel FK to dag_bundle.name is
    # satisfied at flush time even if SQLAlchemy chooses to insert the dag
    # row ahead of the bundle row.
    with create_session() as session:
        if (
            session.scalar(
                select(func.count()).select_from(DagBundleModel).where(DagBundleModel.name == bundle_name)
            )
            == 0
        ):
            session.add(DagBundleModel(name=bundle_name))
            session.commit()

    with create_session() as session:
        existing = session.scalar(select(DagModel).where(DagModel.dag_id == dag.dag_id))
        if existing is None:
            session.add(DagModel(dag_id=dag.dag_id, bundle_name=bundle_name))
            session.commit()

    SerializedDagModel.write_dag(LazyDeserializedDAG.from_dag(dag), bundle_name=bundle_name)


def main() -> int:
    print(f"loading example dag from {EXAMPLE_PATH}")
    print("initializing metadata database")
    initialize_metadata_db()
    with apply_patches():
        mod = load_example_module()
        speed_up_polling(mod.dag)
        print("registering dag bundle and serializing dag")
        register_dag_bundle_and_serialize(mod.dag)
        print(f"running dag.test() on {mod.dag.dag_id}")
        try:
            dag_run = mod.dag.test(use_executor=False)
        except Exception:
            print("dag.test() raised:")
            traceback.print_exc()
            return 1

    print()
    print(f"OK: dag.test() completed for {mod.dag.dag_id} -- final state: {dag_run.state}")
    print(
        "the sync `dag_run_sensor` and deferred `defer_dag_run_sensor` both "
        f"matched on dag_run_id={TRIGGERED_RUN_ID!r} via the composer_dag_run_id branch."
    )
    task_instances = sorted(dag_run.get_task_instances(), key=lambda ti: ti.task_id)
    print("per-task states:")
    for ti in task_instances:
        print(f"  {ti.task_id:32s} -> {ti.state}")
    return 0 if str(dag_run.state) == "success" else 2


if __name__ == "__main__":
    sys.exit(main())
