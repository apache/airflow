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
Helpers for the Lang-SDK e2e modes: what the Dag processor recorded for the stub tasks.

The Dag processor binds each stub task of a Dag file to the artifact that registers its task handler, and
records the bindings in the metadata database. Nothing in the REST API shows them, so they are read from
the ``postgres`` service of the stack.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import TYPE_CHECKING

from airflow_e2e_tests.constants import DAGS_BUNDLE_NAME

if TYPE_CHECKING:
    from collections.abc import Collection

    from testcontainers.compose import DockerCompose

    from airflow_e2e_tests.e2e_test_utils.clients import AirflowClient

# How long to wait before asking again, in seconds.
_POLL_INTERVAL = 5


@dataclass(frozen=True)
class ArtifactRef:
    """An artifact, as the Dag processor names it: its Dag bundle and its path in that bundle."""

    bundle_name: str
    rel_path: str


def query_metadata_db(compose: DockerCompose, sql: str) -> list[list[str]]:
    """
    Run *sql* in the ``postgres`` service and return its rows.

    It runs ``psql -v ON_ERROR_STOP=1 -At -F '\\t'``, so an SQL error raises and each row is one line of
    tab-separated values.
    """
    stdout, _, _ = compose.exec_in_container(
        command=[
            "psql",
            "-U",
            "airflow",
            "-d",
            "airflow",
            "-v",
            "ON_ERROR_STOP=1",
            "-At",
            "-F",
            "\t",
            "-c",
            sql,
        ],
        service_name="postgres",
    )
    return [line.split("\t") for line in stdout.splitlines()]


def get_task_handler_bindings(compose: DockerCompose) -> dict[tuple[str, str], ArtifactRef]:
    """Return each recorded binding, by Dag id and task id, joined to its artifact."""
    rows = query_metadata_db(
        compose,
        "SELECT h.dag_id, h.task_id, a.bundle_name, a.relative_fileloc "
        "FROM lang_sdk_task_handler h JOIN lang_sdk_task_handler_artifact a ON a.id = h.artifact_id",
    )
    return {
        (dag_id, task_id): ArtifactRef(bundle_name, rel_path)
        for dag_id, task_id, bundle_name, rel_path in rows
    }


def get_task_handler_artifacts(compose: DockerCompose) -> dict[ArtifactRef, str]:
    """Return each recorded artifact with its ``last_probed_at``, as text."""
    rows = query_metadata_db(
        compose,
        "SELECT bundle_name, relative_fileloc, last_probed_at FROM lang_sdk_task_handler_artifact",
    )
    return {ArtifactRef(bundle_name, rel_path): probed_at for bundle_name, rel_path, probed_at in rows}


def get_routed_stub_tasks(client: AirflowClient, queues: Collection[str]) -> dict[tuple[str, str], str]:
    """
    Return the queue of each task on *queues* in a non-stale ``dags-folder`` Dag without import errors.

    The e2e Dag files put no Python task on a routed queue, and one on it would fail anyway, so a task on one
    of *queues* is a stub task.
    """
    tasks: dict[tuple[str, str], str] = {}
    for dag in client.list_dags(bundle_name=DAGS_BUNDLE_NAME, exclude_stale=True, has_import_errors=False):
        for task in client.get_dag_tasks(dag["dag_id"]):
            if task["queue"] in queues:
                tasks[dag["dag_id"], task["task_id"]] = task["queue"]
    return tasks


def wait_until_stub_tasks_are_bound(
    client: AirflowClient,
    compose: DockerCompose,
    *,
    queues: Collection[str],
    dag_files: Collection[str],
    expected_import_errors: Collection[str],
    timeout: float,
) -> None:
    """
    Wait until the Dag files are parsed, every routed stub task is bound and the import errors are expected.

    A stub task on one of *queues* in a Dag that serialized is bound once its Dag file has been parsed with
    the artifacts in place, and its Dag file is the one that has an import error if a task does not match its
    task handler. Each Dag file that still has an unbound stub task is asked once for a parse, which makes
    the Dag processor parse it next, so the wait does not have to outlast its parse interval.

    :param queues: The queues that the Dag processor routes to a Lang-SDK coordinator.
    :param dag_files: The ``dags-folder`` files of the Lang SDK that the stack was given. Until the Dag
        processor has parsed them, no stub task is there to be unbound.
    :param expected_import_errors: The ``dags-folder`` files that fail to import by design.
    :raises TimeoutError: when a Dag file is still not parsed, a stub task is still unbound or an import error
        is still missing or unexpected after *timeout* seconds, with what is missing and the text of the
        unexpected errors.
    """
    deadline = time.monotonic() + timeout
    asked_for_parse: set[str] = set()
    while True:
        bindings = get_task_handler_bindings(compose)
        unbound = sorted(task for task in get_routed_stub_tasks(client, queues) if task not in bindings)
        import_errors = {
            error["filename"]: error["stack_trace"]
            for error in client.list_import_errors(bundle_name=DAGS_BUNDLE_NAME)
        }
        missing = sorted(set(expected_import_errors) - set(import_errors))
        unexpected = {
            name: text for name, text in import_errors.items() if name not in expected_import_errors
        }
        parsed_files = {
            dag["relative_fileloc"]
            for dag in client.list_dags(bundle_name=DAGS_BUNDLE_NAME, exclude_stale=False)
        } | set(import_errors)
        unparsed = sorted(set(dag_files) - parsed_files)
        if not (unparsed or unbound or missing or unexpected):
            return
        if time.monotonic() >= deadline:
            lines = [f"The Dag processor did not settle within {timeout:.0f}s."]
            if unparsed:
                lines.append(f"Dag files that the Dag processor has not parsed: {unparsed}")
            if unbound:
                lines.append(f"Stub tasks without a binding (Dag id, task id): {unbound}")
            if missing:
                lines.append(f"Files that should have an import error and have none: {missing}")
            lines.extend(f"Unexpected import error in {name}:\n{text}" for name, text in unexpected.items())
            raise TimeoutError("\n".join(lines))
        if unbound:
            unbound_dag_ids = {dag_id for dag_id, _ in unbound}
            for dag in client.list_dags(bundle_name=DAGS_BUNDLE_NAME, exclude_stale=True):
                if dag["dag_id"] in unbound_dag_ids and dag["relative_fileloc"] not in asked_for_parse:
                    asked_for_parse.add(dag["relative_fileloc"])
                    client.reparse_dag_file(dag["file_token"])
        time.sleep(_POLL_INTERVAL)
