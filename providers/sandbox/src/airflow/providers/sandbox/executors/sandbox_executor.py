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
"""``SandboxExecutor`` — runs each Airflow task instance in an ephemeral cloud
sandbox (Daytona / E2B / Modal / islo) behind a pluggable provider layer.

Topology follows Airflow 3's EdgeExecutor + Task SDK model, **not**
KubernetesExecutor's DB-coupled watcher: the in-sandbox Task SDK supervisor
heartbeats and ships logs straight to the api-server. The executor only
reconciles terminal *exit state* from a polling watcher. The api-server remains
the single source of truth for task-instance state.
"""

from __future__ import annotations

import queue
import threading
import time
from collections import defaultdict
from typing import TYPE_CHECKING, Any, Sequence

from airflow.configuration import conf
from airflow.executors.base_executor import BaseExecutor

from airflow.providers.sandbox.provider_loader import load_provider
from airflow.providers.sandbox.backends.base import (
    ExecResult,
    SandboxProvider,
    SandboxSpec,
    SandboxState,
)

if TYPE_CHECKING:
    from airflow.executors import workloads
    from airflow.models.taskinstance import TaskInstance
    from airflow.models.taskinstancekey import TaskInstanceKey


# Treat a sandbox as vanished only after this many consecutive UNKNOWN polls,
# so a transient provider API blip never kills a healthy running task.
_GONE_AFTER_CONSECUTIVE_UNKNOWN = 3


class SandboxExecutor(BaseExecutor):
    """A remote, non-local executor. One sandbox per task try."""

    # --- BaseExecutor capability flags ---
    is_local: bool = False
    is_single_threaded: bool = False          # remote → not SQLite-bound
    is_production: bool = True
    # Per-task sandbox provisioning is unsuited to interactive `airflow tasks test`.
    supports_ad_hoc_ti_run: bool = False
    # Logs are pushed by the in-sandbox Task SDK supervisor to remote storage.
    serve_logs: bool = False

    if TYPE_CHECKING:
        # The Airflow-3 workload type accepted by _process_workloads.
        from airflow.executors.workloads import ExecuteTask  # noqa: F401

    def __init__(self) -> None:
        super().__init__()
        self.provider: SandboxProvider = load_provider(conf.get("sandbox", "provider"))
        # In-process queues + watcher → plain queue.Queue, not multiprocessing.
        self._task_queue: queue.Queue[tuple[TaskInstanceKey, Any, SandboxSpec]] = queue.Queue()
        self._result_queue: queue.Queue[tuple[TaskInstanceKey, ExecResult, str]] = queue.Queue()
        # handle + exec_ref per live key. Guarded by _lock (shared with watcher).
        self._inflight: dict[TaskInstanceKey, tuple[str, str]] = {}
        self._lock = threading.Lock()
        self._watcher: SandboxWatcher | None = None
        self._poll_interval = conf.getint("sandbox", "poll_interval", fallback=5)
        self._batch = conf.getint("sandbox", "creation_batch_size", fallback=8)

    # ----------------------------------------------------------------- lifecycle
    def start(self) -> None:
        # Logs cannot be recovered after a sandbox is destroyed, and get_task_log
        # runs in a *different process* than this executor (see its docstring), so
        # remote logging is mandatory rather than advisory.
        if not conf.getboolean("logging", "remote_logging", fallback=False):
            raise RuntimeError(
                "SandboxExecutor requires [logging] remote_logging=True: task logs "
                "are produced inside ephemeral sandboxes and must be shipped to "
                "remote storage by the in-sandbox Task SDK supervisor, or they are "
                "lost on teardown."
            )
        self.provider.authenticate()
        self._watcher = SandboxWatcher(
            self.provider, self._inflight, self._lock, self._result_queue, self._poll_interval
        )
        self._watcher.start()

    def end(self) -> None:
        # Drain the queue ourselves — the heartbeat that calls sync() has stopped,
        # so a plain queue.join() here would deadlock on never-launched workloads.
        deadline = time.monotonic() + conf.getint("sandbox", "shutdown_timeout", fallback=120)
        while time.monotonic() < deadline:
            self.sync()
            with self._lock:
                pending = not self._task_queue.empty() or bool(self._inflight)
            if not pending:
                break
            time.sleep(self._poll_interval)
        self._stop_watcher()

    def terminate(self) -> None:
        with self._lock:
            handles = [h for h, _ in self._inflight.values()]
            self._inflight.clear()
        for handle in handles:
            try:
                self.provider.destroy(handle)
            except Exception:
                self.log.exception("destroy failed for %s", handle)
        self._stop_watcher()

    # ----------------------------------------------- workload intake (Airflow 3)
    def _process_workloads(self, workloads: list["workloads.ExecuteTask"]) -> None:
        # BaseExecutor has already popped these from queued_tasks and marked them
        # QUEUED — do NOT re-emit queued()/pop here.
        for wl in workloads:
            key = wl.ti.key
            self._task_queue.put((key, wl, self._build_spec(wl)))

    # ------------------------------------------------------------------ heartbeat
    def sync(self) -> None:
        self._drain_results()
        self._launch_batch()

    def _drain_results(self) -> None:
        while True:
            try:
                key, res, handle = self._result_queue.get_nowait()
            except queue.Empty:
                break
            # The executor only asserts terminal state from an actual exit code.
            # On GONE we do NOT unconditionally fail: in A3 the api-server owns TI
            # state, so the scheduler reconciles (zombie detection) if the task
            # already reported terminal. We surface fail only as a best-effort
            # signal that the sandbox vanished without reporting an exit code.
            if res.state is SandboxState.SUCCEEDED:
                self.success(key)
            elif res.state is SandboxState.FAILED:
                self.fail(key)
            elif res.state is SandboxState.GONE:
                self.log.warning(
                    "sandbox %s for %s vanished without an exit code; "
                    "letting the scheduler reconcile against the api-server", handle, key
                )
                self.fail(key)
            else:
                continue  # RUNNING/UNKNOWN — leave in flight
            with self._lock:
                self._inflight.pop(key, None)
            self._maybe_destroy(handle)

    def _launch_batch(self) -> None:
        slots = self.slots_available
        launched = 0
        while launched < min(self._batch, slots):
            try:
                key, payload, spec = self._task_queue.get_nowait()
            except queue.Empty:
                break
            try:
                handle = self.provider.create_sandbox(spec)
                # Carry the serialized workload + api-server URL into the sandbox via
                # env (works for every backend, incl. upload-less ones like Modal).
                run_env = {**spec.env, **self._workload_env(payload)}
                exec_ref = self.provider.run(
                    handle, self._render_command(payload), env=run_env, timeout=spec.timeout
                )
                with self._lock:
                    self._inflight[key] = (handle, exec_ref)
                self.running.add(key)
                # Persist the handle as external_executor_id ASAP for adoption.
                self.running_state(key, info=handle)
            except Exception as exc:  # noqa: BLE001
                self.log.exception("sandbox launch failed for %s", key)
                self.fail(key, info=str(exc))
            launched += 1

    @property
    def slots_available(self) -> int:
        # parallelism == 0 means unbounded.
        if self.parallelism == 0:
            return self._batch
        return super().slots_available

    # ------------------------------------------------------------------ adoption
    def try_adopt_task_instances(
        self, tis: Sequence["TaskInstance"]
    ) -> Sequence["TaskInstance"]:
        not_adopted: list[TaskInstance] = []
        for ti in tis:
            handle = ti.external_executor_id
            if not handle or not self.provider.capabilities.supports_reattach:
                not_adopted.append(ti)  # opaque-handle providers: best-effort only
                continue
            try:
                res = self.provider.poll_status(handle, exec_ref=handle)
            except Exception:
                res = ExecResult(state=SandboxState.GONE)
            if res.state is SandboxState.GONE:
                not_adopted.append(ti)  # scheduler reschedules
            else:
                with self._lock:
                    self._inflight[ti.key] = (handle, handle)
                self.running.add(ti.key)
        return not_adopted  # contract: return the ones you could NOT adopt

    # --------------------------------------------------------------------- logs
    def get_task_log(self, ti: "TaskInstance", try_number: int) -> tuple[list[str], list[str]]:
        # WARNING: this method is usually invoked by the api-server/log-reader
        # process, whose executor instance never ran sync() — so _inflight is
        # empty there and this returns ([], []). The real log path is remote
        # logging via the in-sandbox Task SDK supervisor. This is a best-effort
        # fallback only for the rare same-process case.
        with self._lock:
            pair = self._inflight.get(ti.key)
        if not pair:
            return [], []
        try:
            return self.provider.fetch_logs(*pair)
        except Exception:
            return ["sandbox-executor: could not fetch logs from sandbox"], []

    # ----------------------------------------------------------------- revoke
    def revoke_task(self, *, ti: "TaskInstance") -> None:
        with self._lock:
            pair = self._inflight.pop(ti.key, None)
        if pair:
            self._maybe_destroy(pair[0])

    @staticmethod
    def get_cli_commands() -> list:
        return []  # room for `airflow sandbox list/clean`

    # ----------------------------------------------------------------- internals
    def _maybe_destroy(self, handle: str) -> None:
        try:
            self.provider.destroy(handle)
        except Exception:
            self.log.exception("destroy failed for %s", handle)

    def _build_spec(self, wl: "workloads.ExecuteTask") -> SandboxSpec:
        ti = wl.ti
        override: dict[str, Any] = {}
        # executor_config["sandbox_override"] parallels K8s pod_override.
        cfg = getattr(ti, "executor_config", None) or {}
        if isinstance(cfg, dict):
            override = dict(cfg.get("sandbox_override", {}))
        # Deterministic name → adoptable across ALL providers, not only labelled ones.
        name = f"af-{ti.dag_id}-{ti.task_id}-{ti.run_id}-{ti.try_number}"
        name = "".join(c if c.isalnum() or c in "-" else "-" for c in name)[:63]
        spec = SandboxSpec(
            name=name,
            image=override.get("image") or conf.get("sandbox", "default_image", fallback=None),
            cpu=override.get("cpu") or conf.getint("sandbox", "default_cpu", fallback=None),
            memory_mb=override.get("memory_mb")
            or conf.getint("sandbox", "default_memory_mb", fallback=None),
            disk_gb=override.get("disk_gb")
            or conf.getint("sandbox", "default_disk_gb", fallback=None),
            timeout=override.get("timeout")
            or conf.getint("sandbox", "default_timeout", fallback=600),
            env=dict(override.get("env", {})),
            labels={
                "airflow_dag_id": ti.dag_id,
                "airflow_task_id": ti.task_id,
                "airflow_run_id": ti.run_id,
                "airflow_try_number": str(ti.try_number),
            },
            keep=bool(override.get("keep", False)),
        )
        return spec

    def _workload_env(self, payload: Any) -> dict[str, str]:
        """Serialize the A3 ExecuteTask workload + api-server URL for the sandbox.

        The workload (with its short-lived JWT) is base64-encoded into an env var
        the in-sandbox runner decodes; this works for every backend, including
        upload-less ones (Modal/islo). For the legacy raw-command path, fall back
        to running that command directly.
        """
        env: dict[str, str] = {}
        dump = getattr(payload, "model_dump_json", None)
        if callable(dump):
            import base64

            env["AIRFLOW_SANDBOX_WORKLOAD"] = base64.b64encode(dump().encode()).decode()
        server = conf.get("core", "execution_api_server_url", fallback=None)
        if server:
            env["AIRFLOW__CORE__EXECUTION_API_SERVER_URL"] = server
        return env

    def _render_command(self, payload: Any) -> list[str]:
        # A3 workload: run the Task SDK supervisor inside the sandbox against the
        # api-server (same mechanism as Celery/Edge). The sandbox image must have
        # apache-airflow installed and network reach to the api-server.
        if hasattr(payload, "model_dump_json"):
            return ["python", "-m", "airflow.providers.sandbox.execution_time.run_workload"]
        # Legacy/raw-command fallback (e.g. ad-hoc CommandType from execute_async).
        if isinstance(payload, (list, tuple)):
            return list(payload)
        return ["sh", "-c", str(payload)]

    def _stop_watcher(self) -> None:
        if self._watcher:
            self._watcher.stop()
            self._watcher.join(timeout=10)
            self._watcher = None


class SandboxWatcher(threading.Thread):
    """In-process polling watcher (no provider offers a cluster watch stream).

    Iterates inflight handles, emits terminal ``(key, ExecResult, handle)`` onto
    the result queue. Distinguishes a transient ``UNKNOWN`` from a confirmed
    ``GONE`` via a consecutive-failure threshold so a single API blip never kills
    a healthy task.
    """

    def __init__(self, provider, inflight, lock, result_queue, interval):
        super().__init__(daemon=True, name="sandbox-watcher")
        self.provider = provider
        self._inflight = inflight
        self._lock = lock
        self._result_queue = result_queue
        self._interval = interval
        self._stopped = threading.Event()
        self._unknown_streak: dict[Any, int] = defaultdict(int)

    def run(self) -> None:
        while not self._stopped.is_set():
            with self._lock:
                snapshot = list(self._inflight.items())
            for key, (handle, exec_ref) in snapshot:
                try:
                    res = self.provider.poll_status(handle, exec_ref)
                except Exception:
                    res = ExecResult(state=SandboxState.UNKNOWN)

                if res.state is SandboxState.UNKNOWN:
                    self._unknown_streak[key] += 1
                    if self._unknown_streak[key] >= _GONE_AFTER_CONSECUTIVE_UNKNOWN:
                        self._emit(key, ExecResult(state=SandboxState.GONE), handle)
                    continue

                self._unknown_streak.pop(key, None)
                if res.state in (SandboxState.SUCCEEDED, SandboxState.FAILED, SandboxState.GONE):
                    self._emit(key, res, handle)
            self._stopped.wait(self._interval)

    def _emit(self, key, res, handle) -> None:
        # Drop from inflight under lock first, so sync() and the watcher never
        # double-emit / double-destroy the same handle.
        with self._lock:
            if key not in self._inflight:
                return
            del self._inflight[key]
        self._unknown_streak.pop(key, None)
        self._result_queue.put((key, res, handle))

    def stop(self) -> None:
        self._stopped.set()
