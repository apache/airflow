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
"""Run the existing file parser inside a LocalExecutor worker."""

from __future__ import annotations

import selectors
import signal
import time
from functools import lru_cache
from typing import TYPE_CHECKING

import structlog
from httpx import URL
from pydantic import TypeAdapter

from airflow.callbacks.callback_requests import CallbackRequest
from airflow.configuration import conf
from airflow.dag_processing.manager import _make_execution_api
from airflow.dag_processing.processor import DagFileProcessorProcess
from airflow.sdk.api.client import Client
from airflow.sdk.log import init_log_file, logging_processors

if TYPE_CHECKING:
    from airflow.executors.workloads.parsing import ParseDagFile


@lru_cache(maxsize=1)
def _get_execution_api():
    # Construct this in each executor worker, never inherit an API event-loop thread through fork.
    return _make_execution_api()


def supervise_dag_parse(workload: ParseDagFile) -> int:
    """Publish serialized results locally after the importing subprocess has stopped."""
    if workload.cancel_path.exists():
        return 0
    process = None
    callbacks: list[CallbackRequest] = [
        TypeAdapter(CallbackRequest).validate_json(value) for value in workload.callbacks
    ]
    with (
        selectors.DefaultSelector() as selector,
        init_log_file(workload.log_path).open("ab") as log_handle,
        Client(
            base_url=None,
            token="",
            dry_run=True,
            transport=_get_execution_api().transport,
        ) as client,
    ):
        client.base_url = URL("http://in-process.invalid./")
        logger = structlog.wrap_logger(
            structlog.BytesLogger(log_handle), processors=logging_processors(json_output=True)
        ).bind()
        try:
            process = DagFileProcessorProcess.start(
                id=workload.workload_id,
                path=workload.bundle_path / workload.relative_path,
                bundle_path=workload.bundle_path,
                bundle_name=workload.bundle_info.name,
                dag_file_rel_path=workload.relative_path,
                callbacks=callbacks,
                logger=logger,
                logger_filehandle=log_handle,
                selector=selector,
                client=client,
                new_process_group=True,
                subprocess_logs_to_stdout=conf.get("logging", "dag_processor_log_target") == "stdout",
            )
            deadline = time.monotonic() + workload.timeout
            while not process.is_ready:
                if workload.cancel_path.exists():
                    return 0
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError(f"Parsing {workload.relative_path} exceeded {workload.timeout}s")
                process._service_subprocess(min(0.1, remaining))
            if process.parsing_result is None and not callbacks:
                raise RuntimeError("Parser exited without a serialized result")
            payload = process.parsing_result.model_dump_json() if process.parsing_result else "null"
        finally:
            if process is not None:
                try:
                    if process._check_subprocess_exit() is None:
                        process.kill(signal.SIGKILL, escalation_delay=1, force=True)
                        if process._check_subprocess_exit() is None:
                            raise RuntimeError("Parser subprocess did not stop")
                finally:
                    process.close()
    if not workload.cancel_path.exists():
        temporary = workload.result_path.with_suffix(".tmp")
        temporary.write_text(payload)
        temporary.replace(workload.result_path)
    return 0
