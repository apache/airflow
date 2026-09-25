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
Experimental executor worker supervising SDK definition importing.

Core still supplies validation, policies and serialization; this is not an SDK-only runtime.
LocalExecutor shares its host's trust boundary. Remote isolation additionally requires
separate configuration, credentials, filesystems and network policy at deployment time.
"""

from __future__ import annotations

import hashlib
import json
import os
import selectors
import signal
import time
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path, PurePosixPath
from typing import TYPE_CHECKING
from uuid import uuid4

import httpx
import structlog

from airflow._shared.timezones import timezone
from airflow.configuration import conf
from airflow.executors.workloads.parsing import MAX_PARSING_REQUEST_BYTES, DagDefinitionResult
from airflow.sdk.api.client import Client
from airflow.sdk.log import logging_processors

if TYPE_CHECKING:
    from datetime import datetime

    from airflow.executors.workloads.parsing import DagDefinitionAttempt, ParseDagDefinitions


_CREDENTIAL_OPTIONS = (
    ("database", "sql_alchemy_conn"),
    ("database", "sql_alchemy_conn_async"),
    ("core", "sql_alchemy_conn"),
    ("api_auth", "jwt_secret"),
    ("api_auth", "jwt_private_key_path"),
    ("core", "fernet_key"),
)
_CREDENTIAL_ENV_NAMES = {
    f"AIRFLOW__{section.upper()}__{option.upper()}{suffix}"
    for section, option in _CREDENTIAL_OPTIONS
    for suffix in ("", "_CMD", "_SECRET")
}
_ACK_RECOVERY_SECONDS = 5.0
_CLAIM_RECONCILIATION_SECONDS = 5.0
_OVERSIZED_RESULT_DIAGNOSTIC = "Serialized parsing result exceeds the request size limit"
_UNENCODABLE_RESULT_DIAGNOSTIC = "Parsing result cannot be encoded as JSON"


class ParsingWorkerError(RuntimeError):
    """The worker cannot safely complete or publish a registered parsing attempt."""


class ParsingAttemptAlreadyClaimedError(ParsingWorkerError):
    """Another execution owns this attempt; a duplicate delivery must not replace its status."""


class ParsingClaimDispositionUnknownError(ParsingWorkerError):
    """A claim could not be reconciled; another delivery may still own the attempt."""


class ParsingRequestTooLargeError(ParsingWorkerError):
    """The API rejected the payload, possibly after an uncertain earlier submission."""

    def __init__(self, message: str, uncertain: bool = False):
        super().__init__(message)
        self.uncertain = uncertain


class ParsingAPIClient(Client):
    """Use the SDK request handlers without its task-oriented, long retry schedule."""

    def request(self, *args, **kwargs):
        return httpx.Client.request(self, *args, **kwargs)


def validate_remote_credentials() -> None:
    """Reject known server credentials; this check is not a sandbox or a secrets inventory."""
    if os.environ.get("AIRFLOW_DAG_PARSING_POC_REMOTE") != "1":
        return
    forbidden = sorted(name for name in _CREDENTIAL_ENV_NAMES if os.environ.get(name))
    if forbidden:
        raise ParsingWorkerError(f"Remote parsing worker contains server credential settings: {forbidden}")
    for section, option in _CREDENTIAL_OPTIONS:
        value = conf.get(section, option, fallback="")
        if value and not (option == "sql_alchemy_conn" and value == "sqlite:///:memory:"):
            raise ParsingWorkerError(f"Remote parsing worker must not configure [{section}] {option}")


@contextmanager
def _set_client_environment() -> Iterator[None]:
    overrides = {
        "_AIRFLOW_PROCESS_CONTEXT": "client",
        "AIRFLOW__DAG_PROCESSOR__PARSING_PRE_IMPORT_MODULES": "false",
    }
    names = _CREDENTIAL_ENV_NAMES | overrides.keys()
    previous = {name: os.environ.get(name) for name in names}
    for name in _CREDENTIAL_ENV_NAMES:
        os.environ.pop(name, None)
    os.environ.update(overrides)
    try:
        yield
    finally:
        for name, value in previous.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value


def get_bundle_root(workload: ParseDagDefinitions) -> Path:
    """Resolve a deployment-configured root; never accept a source host's absolute path."""
    roots = json.loads(os.environ.get("AIRFLOW_DAG_PARSING_POC_BUNDLE_ROOTS", "{}"))
    entry = roots.get(workload.bundle_info.name)
    if not isinstance(entry, dict) or not isinstance(entry.get("path"), str):
        raise ParsingWorkerError("The workload bundle has no configured worker root")
    if entry.get("version") != workload.bundle_info.version:
        raise ParsingWorkerError("Configured worker bundle version does not match the workload")
    root = Path(entry["path"])
    if not root.is_absolute() or not root.is_dir():
        raise ParsingWorkerError("Configured worker bundle root must be an existing absolute directory")
    return root.resolve(strict=True)


def resolve_definition_path(bundle_root: Path, definition: DagDefinitionAttempt) -> Path:
    """Validate the source file or containing archive, including symlink containment."""
    source_path = definition.archive_path or definition.relative_path
    path = bundle_root.joinpath(*PurePosixPath(source_path).parts).resolve(strict=True)
    if not path.is_relative_to(bundle_root.resolve(strict=True)):
        raise ParsingWorkerError("Definition path escapes its configured bundle root")
    if not path.is_file() or path.suffix not in {".py", ".zip"}:
        raise ParsingWorkerError("The parsing PoC only supports Python files and ZIP members")
    revision = definition.archive_revision or definition.source_revision
    if compute_source_revision(path) != revision:
        raise ParsingWorkerError("Definition content does not match its registered source revision")
    return path


def compute_source_revision(path: Path) -> str:
    """Return the initial filesystem bridge's SHA-256 source identity."""
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def parse_definition(
    workload: ParseDagDefinitions,
    definition: DagDefinitionAttempt,
    *,
    bundle_root: Path,
    client: Client,
    log_dir: Path,
    legacy: bool = False,
) -> DagDefinitionResult:
    """Run one claimed definition; the legacy path remains available for baseline comparisons."""
    from airflow.dag_processing.executor_importer import (
        SdkDagDefinitionProcess,
        SdkDagParseRequest,
        SdkDagParsingResult,
        run_sdk_importer,
    )
    from airflow.dag_processing.processor import DagFileProcessorProcess
    from airflow.sdk.execution_time.supervisor import _should_use_exec

    started = time.monotonic()
    result = DagDefinitionResult(
        attempt_id=definition.attempt_id,
        relative_path=definition.relative_path,
        source_revision=definition.source_revision,
        outcome="worker_error",
        duration_seconds=0,
    )
    process: DagFileProcessorProcess | SdkDagDefinitionProcess | None = None
    selector = selectors.DefaultSelector()
    log_handle = None
    try:
        path = resolve_definition_path(bundle_root, definition)
        # Keep a small publication window; the API remains authoritative about expiry.
        remaining = (workload.stop_deadline - timezone.utcnow()).total_seconds() - 2
        timeout = min(definition.timeout_seconds, remaining)
        if timeout <= 0:
            result.outcome = "timeout"
            result.diagnostics.append("Batch deadline leaves no time for another import")
            return result
        log_dir.mkdir(parents=True, exist_ok=True)
        log_handle = (log_dir / f"{definition.attempt_id}.jsonl").open("ab")
        with _set_client_environment():
            logger = structlog.wrap_logger(
                structlog.BytesLogger(log_handle), processors=logging_processors(json_output=True)
            ).bind()
            try:
                if legacy:
                    if definition.archive_path is not None:
                        raise ParsingWorkerError("The legacy baseline does not accept archive members")
                    process = DagFileProcessorProcess.start(
                        id=definition.attempt_id,
                        path=path,
                        bundle_path=bundle_root,
                        bundle_name=workload.bundle_info.name,
                        dag_file_rel_path=definition.relative_path,
                        callbacks=[],
                        logger=logger,
                        logger_filehandle=log_handle,
                        selector=selector,
                        subprocess_logs_to_stdout=True,
                        client=client,
                        new_process_group=True,
                    )
                else:
                    process = SdkDagDefinitionProcess.start(
                        id=definition.attempt_id,
                        target=run_sdk_importer,
                        use_exec=_should_use_exec(),
                        logger=logger,
                        selector=selector,
                        subprocess_logs_to_stdout=True,
                        client=client,
                        new_process_group=True,
                    )
                    process.send_msg(
                        SdkDagParseRequest(
                            definition=definition,
                            bundle_path=bundle_root,
                            bundle_name=workload.bundle_info.name,
                            include_source=os.environ.get("AIRFLOW_DAG_PARSING_POC_INCLUDE_SOURCE") == "1",
                        ),
                        request_id=0,
                    )
                deadline = time.monotonic() + timeout
                while not process.is_ready:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        process.kill(signal.SIGKILL, escalation_delay=1, force=True)
                        result.outcome = "timeout"
                        result.diagnostics.append("Definition exceeded its supervised import timeout")
                        return result
                    process._service_subprocess(min(0.1, remaining))
                parsed = process.parsing_result
                if parsed is None:
                    result.diagnostics.append("Parser subprocess exited without a serialized result")
                    return result
                # Inputs are expected to be immutable; discard output from a source that changed during import.
                resolve_definition_path(bundle_root, definition)
                if isinstance(parsed, SdkDagParsingResult):
                    result.diagnostics.extend(parsed.diagnostics)
                    if parsed.worker_error:
                        return result
                    result.source_code = parsed.source_code
                elif os.environ.get("AIRFLOW_DAG_PARSING_POC_INCLUDE_SOURCE") == "1":
                    if path.suffix != ".py":
                        raise ParsingWorkerError("Metadata persistence currently requires a Python file")
                    source = path.read_bytes()
                    if hashlib.sha256(source).hexdigest() != definition.source_revision:
                        raise ParsingWorkerError("Definition changed before source publication")
                    result.source_code = source.decode("utf-8")
                result.serialized_dags = [dag.data for dag in parsed.serialized_dags]
                result.import_errors = parsed.import_errors or {}
                result.warnings = parsed.warnings or []
                result.outcome = "import_error" if result.import_errors else "success"
                return result
            finally:
                if process is not None:
                    if process._check_subprocess_exit() is None:
                        process.kill(signal.SIGKILL, escalation_delay=1, force=True)
                    process.close()
    except Exception as error:
        result.diagnostics.append(f"{type(error).__name__}: {error}")
        return result
    finally:
        selector.close()
        if log_handle is not None:
            log_handle.close()
        result.duration_seconds = time.monotonic() - started


def _encode_payload(payload: dict) -> bytes:
    return json.dumps(payload, ensure_ascii=False, allow_nan=False, separators=(",", ":")).encode("utf-8")


def _post_with_retry(
    client: Client,
    path: str,
    payload: dict | bytes,
    *,
    deadline: datetime,
    allow_late_claim: bool = False,
) -> dict:
    is_claim = path.rsplit("/", 1)[-1] == "claim"
    if allow_late_claim and not is_claim:
        raise ValueError("Late reconciliation is only valid for claim requests")
    body = payload if isinstance(payload, bytes) else _encode_payload(payload)
    recovery_deadline = None
    for retry in range(2):
        # This bounds retry admission; HTTPX timeouts bound individual I/O waits, not total response time.
        remaining = (
            (deadline - timezone.utcnow()).total_seconds()
            if recovery_deadline is None
            else recovery_deadline - time.monotonic()
        )
        if remaining <= 0:
            if recovery_deadline is not None:
                error_type = ParsingClaimDispositionUnknownError if is_claim else ParsingWorkerError
                raise error_type("Parsing API acknowledgment recovery budget expired")
            if not allow_late_claim:
                raise ParsingWorkerError("Batch deadline expired before API publication")
            # The API can report existing ownership/receipts after expiry while rejecting new late claims.
            remaining = _CLAIM_RECONCILIATION_SECONDS
        try:
            response = client.post(
                path, content=body, headers={"Content-Type": "application/json"}, timeout=min(5, remaining)
            )
            response.raise_for_status()
            return response.json()
        except httpx.HTTPError as error:
            status = error.response.status_code if isinstance(error, httpx.HTTPStatusError) else None
            if isinstance(error, httpx.HTTPStatusError) and status == 409 and is_claim:
                try:
                    conflict = error.response.json()
                except ValueError:
                    conflict = None
                if isinstance(conflict, dict) and conflict.get("detail") == (
                    "Another execution already claimed this attempt"
                ):
                    raise ParsingAttemptAlreadyClaimedError(
                        "Another execution already claimed this attempt"
                    ) from None
            if status == 413:
                raise ParsingRequestTooLargeError(
                    "Parsing API request failed: HTTP 413", uncertain=bool(retry)
                ) from None
            retryable = isinstance(error, httpx.TransportError) or (status is not None and status >= 500)
            if retry or not retryable:
                # HTTP exceptions cannot always round-trip through LocalExecutor's multiprocessing queue.
                detail = f"HTTP {status}" if status is not None else type(error).__name__
                error_type = (
                    ParsingClaimDispositionUnknownError if is_claim and retryable else ParsingWorkerError
                )
                raise error_type(f"Parsing API request failed: {detail}") from None
            # Replay may recover an accepted claim/result; it does not authorize new work after expiry.
            recovery_deadline = time.monotonic() + _ACK_RECOVERY_SECONDS
    raise ParsingWorkerError("Unreachable request retry state")


def _encode_result_error(
    execution_id: str, definition: DagDefinitionAttempt, result: DagDefinitionResult, diagnostic: str
) -> bytes:
    compact = DagDefinitionResult(
        attempt_id=definition.attempt_id,
        relative_path=definition.relative_path,
        source_revision=definition.source_revision,
        outcome="worker_error",
        duration_seconds=result.duration_seconds,
        diagnostics=[diagnostic],
    )
    body = _encode_payload({"execution_id": execution_id, "result": compact.model_dump(mode="json")})
    if len(body) > MAX_PARSING_REQUEST_BYTES:
        raise ParsingWorkerError("Parsing result identity and compact error exceed the request size limit")
    return body


def _publish_result(
    client: Client,
    path: str,
    execution_id: str,
    definition: DagDefinitionAttempt,
    result: DagDefinitionResult,
    *,
    deadline: datetime,
) -> None:
    try:
        body = _encode_payload({"execution_id": execution_id, "result": result.model_dump(mode="json")})
    except (TypeError, ValueError):
        body = _encode_result_error(execution_id, definition, result, _UNENCODABLE_RESULT_DIAGNOSTIC)
        compact = True
    else:
        compact = len(body) > MAX_PARSING_REQUEST_BYTES
        if compact:
            body = _encode_result_error(execution_id, definition, result, _OVERSIZED_RESULT_DIAGNOSTIC)
    try:
        _post_with_retry(client, path, body, deadline=deadline)
    except ParsingRequestTooLargeError as error:
        if compact or error.uncertain:
            raise
        body = _encode_result_error(execution_id, definition, result, _OVERSIZED_RESULT_DIAGNOSTIC)
        _post_with_retry(client, path, body, deadline=deadline)


def supervise_dag_parse(workload: ParseDagDefinitions, *, server: str) -> int:
    """Claim, import and publish a batch through the explicitly configured HTTP API."""
    if not server or httpx.URL(server).scheme not in {"http", "https"}:
        raise ParsingWorkerError("Executor parsing requires an explicit HTTP Execution API URL")
    validate_remote_credentials()
    execution_id = str(uuid4())
    log_dir = Path(os.environ.get("AIRFLOW_DAG_PARSING_POC_LOG_DIR", "./dag-parsing-poc-logs"))
    with ParsingAPIClient(base_url=server.rstrip("/") + "/", token=workload.token, timeout=5) as client:
        for definition in workload.definitions:
            path = f"poc/parsing/workloads/{workload.workload_id}/attempts/{definition.attempt_id}"
            claim = _post_with_retry(
                client,
                f"{path}/claim",
                {"execution_id": execution_id},
                deadline=workload.start_deadline,
                allow_late_claim=True,
            )
            if claim["status"] == "accepted":
                continue
            if claim["status"] not in {"claimed", "already_claimed"}:
                raise ParsingWorkerError("API returned an unknown claim state")
            if timezone.utcnow() >= workload.stop_deadline:
                raise ParsingWorkerError("Batch stop deadline expired before bundle access")
            try:
                root = get_bundle_root(workload)
            except (ParsingWorkerError, ValueError, OSError) as error:
                result = DagDefinitionResult(
                    attempt_id=definition.attempt_id,
                    relative_path=definition.relative_path,
                    source_revision=definition.source_revision,
                    outcome="worker_error",
                    diagnostics=[f"{type(error).__name__}: {error}"],
                    duration_seconds=0,
                )
            else:
                result = parse_definition(
                    workload, definition, bundle_root=root, client=client, log_dir=log_dir
                )
            _publish_result(
                client,
                f"{path}/result",
                execution_id,
                definition,
                result,
                deadline=workload.stop_deadline,
            )
    return 0
