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
"""Supervised SDK importing for the executor PoC; core still supplies validation and serialization."""

from __future__ import annotations

import hashlib
import os
from pathlib import Path
from typing import TYPE_CHECKING, Annotated, ClassVar, Literal, cast

import attrs
import structlog
from pydantic import BaseModel, Field, TypeAdapter

from airflow.dag_processing.bundles.local import LocalDagBundle
from airflow.dag_processing.dagbag import BundleDagBag, _assign_default_team_pools, _validate_executor_fields
from airflow.dag_processing.processor import (
    DagFileParsingResult,
    DagFileProcessorProcess,
    ToDagProcessor,
    ToManager,
    _serialize_dags,
)
from airflow.exceptions import AirflowClusterPolicySkipDag
from airflow.executors.workloads.parsing import DagDefinitionAttempt  # noqa: TC001 - Pydantic field
from airflow.sdk.execution_time import comms, task_runner
from airflow.sdk.execution_time.supervisor import WatchedSubprocess, register_request_method
from airflow.sdk.importers.base import DagImporterRegistry, FileDagDefinition, FilesystemDagDefinition
from airflow.sdk.importers.zip_importer import ZipMemberDagDefinition
from airflow.serialization.serialized_objects import LazyDeserializedDAG  # noqa: TC001 - Pydantic field
from airflow.utils.dag_version_inflation_checker import check_dag_file_stability

if TYPE_CHECKING:
    from structlog.typing import FilteringBoundLogger

    from airflow.sdk.api.client import Client
    from airflow.sdk.execution_time.supervisor import RequestHandler, RequestResult


class SdkDagParseRequest(BaseModel):
    """A token-free definition reference sent to the importing child."""

    definition: DagDefinitionAttempt
    bundle_path: Path
    bundle_name: str
    include_source: bool = False
    type: Literal["SdkDagParseRequest"] = "SdkDagParseRequest"


class SdkDagParsingResult(BaseModel):
    """Only serialized data leaves the importing child."""

    serialized_dags: list[LazyDeserializedDAG] = Field(default_factory=list)
    import_errors: dict[str, str] = Field(default_factory=dict)
    warnings: list = Field(default_factory=list)
    diagnostics: list[str] = Field(default_factory=list)
    source_code: str | None = None
    worker_error: bool = False
    type: Literal["SdkDagParsingResult"] = "SdkDagParsingResult"


ToSdkImporter = Annotated[SdkDagParseRequest | ToDagProcessor, Field(discriminator="type")]
FromSdkImporter = Annotated[SdkDagParsingResult | ToManager, Field(discriminator="type")]


@attrs.define(kw_only=True)
class SdkDagDefinitionProcess(WatchedSubprocess):
    """Reuse subprocess supervision and the parser's shared API request handlers."""

    client: Client
    parsing_result: SdkDagParsingResult | None = None
    decoder: ClassVar[TypeAdapter[FromSdkImporter]] = TypeAdapter(FromSdkImporter)

    def _handle_parsing_result(
        self, msg: SdkDagParsingResult, log: FilteringBoundLogger, req_id: int
    ) -> RequestResult:
        self.parsing_result = msg
        return None, {}

    _request_handlers: ClassVar[dict[type[BaseModel], RequestHandler[SdkDagDefinitionProcess]]] = {
        **WatchedSubprocess._get_shared_request_handlers(
            *(
                message
                for message in DagFileProcessorProcess._request_handlers
                if message is not DagFileParsingResult
            )
        ),
        **dict([register_request_method(SdkDagParsingResult, _handle_parsing_result)]),
    }

    @property
    def is_ready(self) -> bool:
        return self._check_subprocess_exit() is not None and not self._open_sockets

    def close(self) -> None:
        self.cleanup_sockets_after_kill()


def build_sdk_definition(request: SdkDagParseRequest) -> FileDagDefinition:
    from airflow.dag_processing.executor_worker import ParsingWorkerError, resolve_definition_path

    attempt = request.definition
    resolve_definition_path(request.bundle_path, attempt)
    if attempt.archive_path is not None:
        definition: FileDagDefinition = ZipMemberDagDefinition(
            zip_path=request.bundle_path / attempt.archive_path,
            file_path=attempt.relative_path[len(attempt.archive_path) + 1 :],
        )
    else:
        if Path(attempt.relative_path).suffix == ".zip":
            raise ParsingWorkerError("SDK parsing requires one reference per archive member")
        definition = FilesystemDagDefinition(path=request.bundle_path / attempt.relative_path)
    if hashlib.sha256(definition.read_bytes()).hexdigest() != attempt.source_revision:
        raise ParsingWorkerError("Definition content does not match its registered source revision")
    return definition


def import_sdk_definition(request: SdkDagParseRequest) -> SdkDagParsingResult:
    """Import, validate and serialize entirely inside the disposable importing process."""
    from airflow.dag_processing.executor_worker import ParsingWorkerError

    log = structlog.get_logger(logger_name="task")
    result = SdkDagParsingResult()
    attempt = request.definition
    try:
        definition = build_sdk_definition(request)
        bundle = LocalDagBundle(name=request.bundle_name, path=str(request.bundle_path))
        registry = DagImporterRegistry.from_config(request.bundle_name)
        importer = registry.get_importer(
            Path(attempt.archive_path) if attempt.archive_path is not None else definition
        )
        if importer is None:
            raise ParsingWorkerError("No SDK importer is configured for this definition")
        with definition.as_file() as source_path:
            stability = check_dag_file_stability(str(source_path))
        if stability_errors := stability.get_error_format_dict(attempt.relative_path, None):
            result.import_errors.update(stability_errors)
            return result

        bag = BundleDagBag(
            dag_folder=request.bundle_path,
            bundle_path=request.bundle_path,
            bundle_name=request.bundle_name,
            collect_dags=False,
            load_op_links=False,
        )
        imported = importer.import_definition(definition, bundle)
        if imported.errors:
            result.import_errors[attempt.relative_path] = "\n".join(
                error.stacktrace or error.format_message() for error in imported.errors
            )
        result.diagnostics.extend(
            f"{warning.warning_type}: {warning.message}" for warning in imported.warnings
        )
        for dag in imported.dags:
            try:
                dag.bundle_name = request.bundle_name
                dag.fileloc = repr(definition)
                dag.relative_fileloc = attempt.relative_path
                dag.validate()
                _validate_executor_fields(dag, request.bundle_name)
                _assign_default_team_pools(dag, request.bundle_name)
                bag.bag_dag(dag)
            except AirflowClusterPolicySkipDag:
                continue
            except Exception as error:
                result.import_errors[attempt.relative_path] = f"{type(error).__name__}: {error}"
        serialized, serialization_errors = _serialize_dags(bag, log)
        result.import_errors.update(serialization_errors)
        result.warnings = stability.get_formatted_warnings(bag.dag_ids)
        # ZIP definitions cache content. Reconstruct one to validate the bytes after importing.
        fresh_definition = build_sdk_definition(request)
        if request.include_source:
            source = importer.get_source_code(fresh_definition).source_code
            if hashlib.sha256(source.encode("utf-8")).hexdigest() != attempt.source_revision:
                raise ParsingWorkerError("Source publication does not match its registered revision")
            result.source_code = source
        result.serialized_dags = serialized
        return result
    except Exception as error:
        return SdkDagParsingResult(worker_error=True, diagnostics=[f"{type(error).__name__}: {error}"])


def run_sdk_importer() -> None:
    os.environ["_AIRFLOW_PROCESS_CONTEXT"] = "client"
    decoder = comms.CommsDecoder[ToSdkImporter, FromSdkImporter](
        body_decoder=TypeAdapter[ToSdkImporter](ToSdkImporter)
    )
    request = decoder._get_response()
    if not isinstance(request, SdkDagParseRequest):
        raise RuntimeError("SDK importing requires a definition request")
    # Parsing uses the shared SDK requests on the same process-global channel as tasks.
    task_runner.SUPERVISOR_COMMS = cast("comms.CommsDecoder", decoder)
    decoder.send(import_sdk_definition(request))
