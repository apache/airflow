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

import json
import time
from collections import Counter
from dataclasses import dataclass
from typing import TYPE_CHECKING

from airflow_e2e_tests.constants import DAGS_BUNDLE_NAME

if TYPE_CHECKING:
    from collections.abc import Collection, Mapping
    from pathlib import Path

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


def read_parse_attempts(logs_path: Path, relative_fileloc: str) -> list[list[dict]]:
    """
    Return the log records of each parse of a ``dags-folder`` file by the Dag processor, oldest first.

    The Dag processor appends the log of each parse of a file to ``dag_processor/<date>/<bundle>/<file>.log``
    in the logs folder, one JSON record per line, and every parse starts with ``Filling up the DagBag``. A
    line that is not JSON yet, because the Dag processor is still writing it, is skipped.
    """
    attempts: list[list[dict]] = []
    log_files = sorted(logs_path.glob(f"dag_processor/????-??-??/{DAGS_BUNDLE_NAME}/{relative_fileloc}.log"))
    for log_file in log_files:
        for line in log_file.read_text().splitlines():
            try:
                record = json.loads(line)
            except ValueError:
                continue
            if not isinstance(record, dict):
                continue
            if str(record.get("event", "")).startswith("Filling up the DagBag from"):
                attempts.append([])
            if attempts:
                attempts[-1].append(record)
    return attempts


def assert_later_parses_probe_nothing(
    client: AirflowClient,
    compose: DockerCompose,
    logs_path: Path,
    dag_id_by_file: Mapping[str, str],
    *,
    timeout: float = 180,
) -> None:
    """
    Ask the Dag processor to parse each ``dags-folder`` file again, and check that it probes no artifact.

    The artifacts are probed on the first parse, and their answers are recorded with a fingerprint, so a
    parse that finds the same artifacts probes none. That holds for the parse requested here, and for
    every parse of each file: the log of a file holds at most one probe of each artifact, and the recorded
    probe time of each artifact is the same afterwards.

    :param dag_id_by_file: A Dag of each file, by the path of the file in the Dags folder. It must be in the
        Dag processor's database, as it is for a file that failed to import.
    :raises TimeoutError: when a file is not parsed again within *timeout* seconds.
    """
    artifacts_before = get_task_handler_artifacts(compose)
    assert artifacts_before, "No artifact is recorded, so no later parse could skip probing one."
    parse_counts = {file: len(read_parse_attempts(logs_path, file)) for file in dag_id_by_file}
    dags_before = {dag["dag_id"]: dag for dag in _list_dags_of_folder(client)}
    markers_before = {
        file: _get_parse_marker(client, dag_id, file) for file, dag_id in dag_id_by_file.items()
    }
    for dag_id in dag_id_by_file.values():
        client.reparse_dag_file(dags_before[dag_id]["file_token"])

    deadline = time.monotonic() + timeout
    while True:
        waiting_for = [
            file
            for file, dag_id in dag_id_by_file.items()
            if _get_parse_marker(client, dag_id, file) == markers_before[file]
            or len(read_parse_attempts(logs_path, file)) <= parse_counts[file]
        ]
        if not waiting_for:
            break
        if time.monotonic() >= deadline:
            raise TimeoutError(f"The Dag processor did not parse {waiting_for} again within {timeout:.0f}s.")
        time.sleep(_POLL_INTERVAL)

    for file in dag_id_by_file:
        attempts = read_parse_attempts(logs_path, file)
        probing = [
            record
            for attempt in attempts[parse_counts[file] :]
            for record in attempt
            if record.get("event") == "Probing a task handler artifact"
        ]
        assert not probing, f"A later parse of {file} probed artifacts: {probing}"
        probes = Counter(
            (record.get("bundle_name"), record.get("path"))
            for attempt in attempts
            for record in attempt
            if record.get("event") == "Probed a task handler artifact"
        )
        assert all(count == 1 for count in probes.values()), (
            f"The log of {file} holds more than one probe of an artifact: {probes}"
        )
    assert get_task_handler_artifacts(compose) == artifacts_before, (
        "A later parse recorded another probe of an artifact."
    )


def _list_dags_of_folder(client: AirflowClient) -> list[dict]:
    """Return every Dag of the Dags folder, also a stale one, which is how a file that failed to import is."""
    return client.list_dags(bundle_name=DAGS_BUNDLE_NAME, exclude_stale=False)


def _get_parse_marker(
    client: AirflowClient, dag_id: str, relative_fileloc: str
) -> tuple[str | None, str | None]:
    """
    Return what the Dag processor writes when it parses a file.

    That is the parse time of the Dag, and the time of the import error of the file if it has one.
    """
    dag = next(dag for dag in _list_dags_of_folder(client) if dag["dag_id"] == dag_id)
    error_times = [
        error["timestamp"]
        for error in client.list_import_errors(bundle_name=DAGS_BUNDLE_NAME)
        if error["filename"] == relative_fileloc
    ]
    return dag["last_parsed_time"], error_times[0] if error_times else None
