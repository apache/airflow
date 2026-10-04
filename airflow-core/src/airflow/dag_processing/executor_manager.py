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
"""Replace the existing manager's process pool with a dedicated LocalExecutor."""

from __future__ import annotations

import time
from contextlib import ExitStack
from dataclasses import dataclass, field
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import TYPE_CHECKING
from uuid import uuid4

from airflow.dag_processing.manager import DagFileProcessorManager
from airflow.dag_processing.processor import DagFileParsingResult
from airflow.executors.local_executor import LocalExecutor
from airflow.executors.workloads import BundleInfo, WorkloadType
from airflow.executors.workloads.parsing import ParseDagFile, ParseDagFileKey, ParseDagFileState
from airflow.utils.session import create_session

if TYPE_CHECKING:
    import signal

    from airflow.dag_processing.manager import DagFileInfo


@dataclass
class ExecutorDagFileProcess:
    """Adapt executor completion to the manager's existing result and timeout handling."""

    workload: ParseDagFile
    executor: LocalExecutor
    start_time: float = field(default_factory=time.monotonic)
    pid: None = None
    parsing_result: DagFileParsingResult | None = None
    is_ready: bool = False

    @property
    def had_callbacks(self) -> bool:
        return bool(self.workload.callbacks)

    def kill(self, signal_to_send: signal.Signals, escalation_delay: float = 5.0) -> None:
        """Cancel locally; the worker supervises termination and the manager discards its result."""
        # LocalExecutor only accepts completion for registered dispatches; retain the slot until then.
        self.workload.cancel_path.touch()
        self.executor.executor_queues[self.workload.type].pop(self.workload.key, None)

    def close(self) -> None:
        """Keep cancellation markers until executor shutdown, including for deliveries still queued."""
        self.workload.result_path.unlink(missing_ok=True)


class ExecutorDagFileProcessorManager(DagFileProcessorManager):
    """Keep discovery, callbacks, priority, refresh and persistence in the existing manager."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._executor: LocalExecutor | None = None
        self._resources: ExitStack | None = None
        self._control_dir: Path | None = None

    def before_run(self) -> None:
        super().before_run()
        self._resources = ExitStack()
        try:
            self._control_dir = Path(
                self._resources.enter_context(TemporaryDirectory(prefix="airflow-parsing-"))
            )
            self._executor = LocalExecutor(parallelism=self._parallelism)
            self._executor.supported_workload_types = frozenset({WorkloadType.PARSE_DAG_FILE})
            self._executor.start()
            self._resources.callback(self._executor.end)
        except BaseException:
            self.end()
            raise

    def _create_process(self, dag_file: DagFileInfo) -> ExecutorDagFileProcess:
        if self._executor is None or self._control_dir is None:
            raise RuntimeError("Start the parsing executor before submitting files")
        if dag_file.bundle_path is None:
            raise ValueError("Local parsing requires a prepared bundle path")
        workload = ParseDagFile(
            workload_id=uuid4(),
            bundle_info=BundleInfo(name=dag_file.bundle_name, version=dag_file.bundle_version),
            bundle_path=dag_file.bundle_path,
            relative_path=str(dag_file.rel_path),
            log_path=self._render_log_filename(dag_file),
            control_dir=self._control_dir,
            timeout=self.processor_timeout,
            callbacks=[item.to_json() for item in self._callback_to_execute.pop(dag_file, [])],
        )
        with create_session() as session:
            self._executor.queue_workload(workload, session=session)
        return ExecutorDagFileProcess(workload, self._executor)

    def _service_processor_sockets(self, timeout: float | None = 1.0) -> None:
        if self._executor is None or self._control_dir is None:
            raise RuntimeError("Start the parsing executor before polling")
        self._executor.heartbeat()
        if timeout:
            time.sleep(timeout)
        self._executor.sync()
        processes = {
            proc.workload.key: proc
            for proc in self._processors.values()
            if isinstance(proc, ExecutorDagFileProcess)
        }
        for key, (state, info) in self._executor.get_event_buffer().items():
            if not isinstance(key, ParseDagFileKey) or state not in {
                ParseDagFileState.SUCCESS,
                ParseDagFileState.FAILED,
            }:
                continue
            (self._control_dir / f"{key.id}.cancel").unlink(missing_ok=True)
            if (proc := processes.get(key)) is None:
                (self._control_dir / f"{key.id}.json").unlink(missing_ok=True)
                continue
            if state == ParseDagFileState.SUCCESS:
                try:
                    payload = proc.workload.result_path.read_text()
                    if payload != "null":
                        proc.parsing_result = DagFileParsingResult.model_validate_json(payload)
                except (OSError, ValueError):
                    self.log.exception("Unable to read parsing result for %s", proc.workload.display_name)
            else:
                self.log.error("Parsing workload %s failed: %s", proc.workload.display_name, info)
            proc.is_ready = True

    def terminate(self) -> None:
        if self._resources is not None:
            super().terminate()

    def end(self) -> None:
        if self._resources is not None:
            resources, self._resources = self._resources, None
            try:
                resources.close()
            finally:
                self._processors.clear()
                self._executor = None
                self._control_dir = None

    def after_run(self) -> None:
        try:
            self.terminate()
        finally:
            self.end()
