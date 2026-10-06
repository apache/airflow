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
Helpers for the Lang-SDK e2e modes: what the Dag processor did with the stub tasks of a Dag file.

The check runs at parse time, against the artifact the worker's own pick would run; it has no binding and
no recorded answer, so what it did is read from the Dag processor's parse logs and the REST API, never from
the metadata database.
"""

from __future__ import annotations

import contextlib
import json
import time
from collections import Counter
from dataclasses import dataclass
from typing import TYPE_CHECKING

import requests

from airflow_e2e_tests.constants import DAGS_BUNDLE_NAME

if TYPE_CHECKING:
    from collections.abc import Collection, Mapping
    from pathlib import Path

    from airflow_e2e_tests.e2e_test_utils.clients import AirflowClient

# The Dag processor logs one before, and one (only on success) after, each probe.
PROBING_EVENT = "Probing a task handler artifact"
PROBED_EVENT = "Probed a task handler artifact"

# How long to wait before asking again, in seconds.
_POLL_INTERVAL = 5


@dataclass(frozen=True)
class ArtifactRef:
    """An artifact, as the Dag processor names it: its Dag bundle and its path in that bundle."""

    bundle_name: str
    rel_path: str


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


def count_probes(attempt: list[dict]) -> Counter[ArtifactRef]:
    """Return how many times one parse attempt probed each artifact, from its ``Probed`` records."""
    return Counter(
        ArtifactRef(record["bundle_name"], record["path"])
        for record in attempt
        if record.get("event") == PROBED_EVENT
    )


def _probe_started(logs_path: Path, file: str) -> bool:
    """Return whether any parse attempt of *file* logged the start of a task handler probe."""
    return any(
        any(record.get("event") == PROBING_EVENT for record in attempt)
        for attempt in read_parse_attempts(logs_path, file)
    )


def get_import_errors(client: AirflowClient, files: Collection[str]) -> dict[str, str]:
    """Return the text of the import error of each of the ``dags-folder`` *files* that has one, by file."""
    return {
        error["filename"]: error["stack_trace"]
        for error in client.list_import_errors(bundle_name=DAGS_BUNDLE_NAME)
        if error["filename"] in files
    }


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


def wait_until_stub_tasks_are_checked(
    client: AirflowClient,
    logs_path: Path,
    *,
    dag_files: Collection[str],
    expected_import_errors: Collection[str],
    timeout: float,
) -> None:
    """
    Wait until the Dag files are parsed, each has probed a task handler artifact, and the import errors fit.

    A file is parsed once it is among the Dags of ``dags-folder``, stale ones included (a file with a check
    import error is still serialized, but stale). Once parsed, its parse log must hold at
    least one ``Probed a task handler artifact`` record: a probe that fails is only a warning, so a file whose
    artifact cannot be probed gets no import error at all, and the only sign that something is wrong is the
    missing probe record. Each file that is parsed but still has no probe record is asked once for a reparse,
    through the ``file_token`` of one of its Dags, so the wait does not have to outlast the parse interval.

    :param dag_files: The ``dags-folder`` files of the Lang SDK that the stack was given.
    :param expected_import_errors: The files among *dag_files* that fail to import by design. Only these
        files' import errors are checked: another file of the Dags folder, such as a stock example Dag, may
        fail to import for other reasons.
    :raises TimeoutError: when a Dag file is still not parsed, still has no probe record, or an import error
        is still missing or unexpected, after *timeout* seconds, with what is missing and the text of the
        unexpected errors.
    """
    deadline = time.monotonic() + timeout
    asked_for_parse: set[str] = set()
    while True:
        import_errors = get_import_errors(client, dag_files)
        parsed_files = {dag["relative_fileloc"] for dag in _list_dags_of_folder(client)} | set(import_errors)
        unparsed = sorted(set(dag_files) - parsed_files)
        not_probed = sorted(
            file
            for file in dag_files
            if file not in unparsed
            and not any(count_probes(attempt) for attempt in read_parse_attempts(logs_path, file))
        )
        missing = sorted(set(expected_import_errors) - set(import_errors))
        unexpected = {
            name: text for name, text in import_errors.items() if name not in expected_import_errors
        }
        if not (unparsed or not_probed or missing or unexpected):
            return
        if time.monotonic() >= deadline:
            lines = [f"The Dag processor did not settle within {timeout:.0f}s."]
            if unparsed:
                lines.append(f"Dag files that the Dag processor has not parsed: {unparsed}")
            if not_probed:
                never_started = [file for file in not_probed if not _probe_started(logs_path, file)]
                started_no_result = [file for file in not_probed if file not in never_started]
                if never_started:
                    lines.append(f"Dag files whose task handler probe never started: {never_started}")
                if started_no_result:
                    lines.append(
                        "Dag files whose task handler probe started but produced no result: "
                        f"{started_no_result}"
                    )
            if missing:
                lines.append(f"Files that should have an import error and have none: {missing}")
            lines.extend(f"Unexpected import error in {name}:\n{text}" for name, text in unexpected.items())
            raise TimeoutError("\n".join(lines))
        if not_probed:
            dags_by_file = {dag["relative_fileloc"]: dag for dag in _list_dags_of_folder(client)}
            for file in not_probed:
                if file not in asked_for_parse and file in dags_by_file:
                    asked_for_parse.add(file)
                    client.reparse_dag_file(dags_by_file[file]["file_token"])
        time.sleep(_POLL_INTERVAL)


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


def assert_every_file_is_probed_once(
    logs_path: Path, expected_artifacts_by_file: Mapping[str, Collection[ArtifactRef]]
) -> None:
    """
    Check that each file's probe records are exactly *expected_artifacts_by_file*, with no duplicate.

    At least one parse attempt of a file holds a ``Probed`` record, no single attempt probes the same
    artifact twice (the probe cache lives for one parse of one file), and the union of every attempt's
    probes is exactly the expected set: no file probes an artifact it should not, and none is missing.
    """
    for file, expected in expected_artifacts_by_file.items():
        attempts = read_parse_attempts(logs_path, file)
        assert attempts, f"{file} was not parsed."
        probed: set[ArtifactRef] = set()
        for attempt in attempts:
            counts = count_probes(attempt)
            duplicated = {ref: count for ref, count in counts.items() if count > 1}
            assert not duplicated, f"A parse of {file} probed an artifact more than once: {duplicated}"
            probed |= set(counts)
        assert probed == set(expected), f"{file}: probed {probed}, expected {set(expected)}"


def assert_later_parses_probe_nothing_new(
    client: AirflowClient,
    logs_path: Path,
    dag_id_by_file: Mapping[str, str],
    *,
    timeout: float = 180,
) -> None:
    """
    Ask the Dag processor to parse each file again, and check that it probes no artifact new to that file.

    Nothing records the answer, so a later parse probes the same artifacts again: "nothing new" means no
    artifact that an earlier parse of the file did not already probe, and no new attempt probes one artifact
    more than once. A later parse that probes nothing at all is also wrong: the probe cache must not outlive
    the parse that filled it.

    :param dag_id_by_file: A Dag of each file, by the path of the file in the Dags folder. It must be in the
        Dag processor's database, as it is for a file that failed to import.
    :raises TimeoutError: when a file is not parsed again, or its parse marker has not changed, within
        *timeout* seconds.
    """
    attempt_counts_before = {file: len(read_parse_attempts(logs_path, file)) for file in dag_id_by_file}
    probed_before = {
        file: {ref for attempt in read_parse_attempts(logs_path, file) for ref in count_probes(attempt)}
        for file in dag_id_by_file
    }
    dags_before = {dag["dag_id"]: dag for dag in _list_dags_of_folder(client)}
    markers_before = {
        file: _get_parse_marker(client, dag_id, file) for file, dag_id in dag_id_by_file.items()
    }
    for dag_id in dag_id_by_file.values():
        # A reparse request for this file may already be pending, from the readiness gate asking for one
        # before this function ran; the Dag processor serves that one instead, so the wait below still
        # succeeds.
        with contextlib.suppress(requests.HTTPError):
            client.reparse_dag_file(dags_before[dag_id]["file_token"])

    deadline = time.monotonic() + timeout
    while True:
        waiting_for = [
            file
            for file, dag_id in dag_id_by_file.items()
            if _get_parse_marker(client, dag_id, file) == markers_before[file]
            or len(read_parse_attempts(logs_path, file)) <= attempt_counts_before[file]
        ]
        if not waiting_for:
            break
        if time.monotonic() >= deadline:
            raise TimeoutError(f"The Dag processor did not parse {waiting_for} again within {timeout:.0f}s.")
        time.sleep(_POLL_INTERVAL)

    for file in dag_id_by_file:
        attempts = read_parse_attempts(logs_path, file)
        new_attempts = attempts[attempt_counts_before[file] :]
        new_probed: set[ArtifactRef] = set()
        for attempt in new_attempts:
            counts = count_probes(attempt)
            duplicated = {ref: count for ref, count in counts.items() if count > 1}
            assert not duplicated, f"A later parse of {file} probed an artifact more than once: {duplicated}"
            new_probed |= set(counts)
        assert new_probed, f"A later parse of {file} probed nothing: the probe cache outlived its parse."
        assert new_probed <= probed_before[file], (
            f"A later parse of {file} probed artifacts an earlier parse did not: "
            f"{new_probed - probed_before[file]}"
        )
