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
Run the Dag and task callbacks, and task emails, that the scheduler routes to executors.

These are the callbacks a Dag processor otherwise runs inside a parse of the Dag file. The worker
parses the file at the Dag run's bundle version, finds the callbacks on the Dag or task, and runs
them with the context the scheduler recorded when it requested them.
"""

from __future__ import annotations

import os
from collections.abc import Sequence
from pathlib import Path
from typing import TYPE_CHECKING, Any

from airflow.dag_processing.bundles.base import BundleVersionLock  # noqa: SDK002
from airflow.sdk.api.datamodels._generated import (
    DagCallbackContext,
    EmailCallbackContext,
    TaskCallbackContext,
    TaskInstanceState,
    TIRunContext,
)
from airflow.sdk.definitions.dag import DAG
from airflow.sdk.exceptions import TaskNotFound

if TYPE_CHECKING:
    from structlog.typing import FilteringBoundLogger

    from airflow.sdk.api.datamodels._generated import BundleInfo
    from airflow.sdk.definitions.context import Context

    CallbackContext = DagCallbackContext | TaskCallbackContext | EmailCallbackContext

__all__ = ["run_dag_attribute_callbacks"]

_FAILURE_CALLBACK_TYPES = frozenset(
    (TaskInstanceState.FAILED, TaskInstanceState.UP_FOR_RETRY, TaskInstanceState.UPSTREAM_FAILED)
)


def run_dag_attribute_callbacks(
    context: CallbackContext,
    *,
    dag_rel_path: os.PathLike[str],
    bundle_info: BundleInfo,
    log: FilteringBoundLogger,
) -> list[str]:
    """
    Parse the callback's Dag file and run its callbacks, returning one message per failure.

    Each callback runs even if an earlier one failed, as in the Dag processor.
    """
    from airflow.sdk.execution_time.bundles import initialize_ti_bundle

    try:
        bundle = initialize_ti_bundle(bundle_info)
        with BundleVersionLock(bundle_name=bundle.name, bundle_version=bundle.version):
            dag = _load_dag(_get_dag_id(context), Path(bundle.path, dag_rel_path), bundle.path, bundle.name)
            if isinstance(context, DagCallbackContext):
                return _run_dag_callbacks(context, dag, log)
            if isinstance(context, TaskCallbackContext):
                return _run_task_callbacks(context, dag, log)
            return _send_task_email(context, dag, log)
    except Exception as e:
        log.exception("Could not run the callbacks", dag_rel_path=os.fspath(dag_rel_path))
        return [f"{type(e).__name__}: {e}"]


def _get_dag_id(context: CallbackContext) -> str:
    return context.dag_id if isinstance(context, DagCallbackContext) else context.ti.dag_id


def _load_dag(dag_id: str, dag_path: Path, bundle_path: Path, bundle_name: str) -> DAG:
    from airflow.dag_processing.dagbag import BundleDagBag  # noqa: SDK002
    from airflow.sdk.execution_time.task_runner import _find_native_dag_importer

    if (importer := _find_native_dag_importer(os.fspath(dag_path), bundle_name)) is not None:
        raise ValueError(
            f"Dag {dag_id!r} is a native Lang-SDK Dag parsed by {importer.coordinator_classpath}, "
            "whose callbacks do not run in Python"
        )
    bag = BundleDagBag(
        dag_folder=os.fspath(dag_path),
        safe_mode=False,
        load_op_links=False,
        bundle_path=bundle_path,
        bundle_name=bundle_name,
    )
    if (dag := bag.dags.get(dag_id)) is None:
        raise ValueError(f"Dag {dag_id!r} was not found in {dag_path}")
    if not isinstance(dag, DAG):
        raise ValueError(f"Dag {dag_id!r} is a native Lang-SDK Dag, whose callbacks do not run in Python")
    return dag


def _get_task(dag: DAG, task_id: str) -> Any:
    try:
        return dag.get_task(task_id)
    except TaskNotFound:
        raise ValueError(f"Task {task_id!r} was not found in Dag {dag.dag_id!r}") from None


def _get_callback_name(callback: Any) -> str:
    return getattr(callback, "__name__", None) or type(callback).__name__


def _run_callbacks(callbacks: Any, template_context: Context, log: FilteringBoundLogger) -> list[str]:
    errors = []
    for callback in callbacks if isinstance(callbacks, Sequence) else [callbacks]:
        name = _get_callback_name(callback)
        log.info("Executing callback", callback=name)
        try:
            callback(template_context)
        except Exception as e:
            log.exception("Callback failed", callback=name)
            errors.append(f"{name}: {type(e).__name__}: {e}")
    return errors


def _run_dag_callbacks(context: DagCallbackContext, dag: DAG, log: FilteringBoundLogger) -> list[str]:
    from airflow.sdk.execution_time.task_runner import RuntimeTaskInstance

    callbacks = dag.on_failure_callback if context.is_failure_callback else dag.on_success_callback
    if not callbacks:
        log.warning("Callback requested, but the Dag has none", dag_id=context.dag_id)
        return []

    template_context: Context = {"dag": dag, "run_id": context.run_id, "reason": context.msg}
    if context.last_ti is not None:
        try:
            task = dag.get_task(context.last_ti.task_id)
        except TaskNotFound:
            # The task only enriches the context: a task removed since the run must not cost the
            # user the callback itself.
            log.warning(
                "Task from callback context no longer exists in the Dag; running callback with minimal context",
                dag_id=context.dag_id,
                task_id=context.last_ti.task_id,
            )
        else:
            runtime_ti = RuntimeTaskInstance.model_construct(
                **context.last_ti.model_dump(exclude_unset=True),
                task=task,
                _ti_context_from_server=TIRunContext.model_construct(
                    dag_run=context.dag_run, max_tries=task.retries
                ),
            )
            template_context = runtime_ti.get_template_context()
            template_context["reason"] = context.msg

    log.info(
        "Executing on_%s Dag callbacks",
        "failure" if context.is_failure_callback else "success",
        dag_id=context.dag_id,
    )
    return _run_callbacks(callbacks, template_context, log)


def _run_task_callbacks(context: TaskCallbackContext, dag: DAG, log: FilteringBoundLogger) -> list[str]:
    from airflow.sdk.execution_time.task_runner import RuntimeTaskInstance

    ti = context.ti
    if context.task_callback_type is not None and context.task_callback_type not in _FAILURE_CALLBACK_TYPES:
        log.warning(
            "Task callback requested but is not a failure callback",
            dag_id=ti.dag_id,
            task_id=ti.task_id,
            run_id=ti.run_id,
        )
        return []

    task = _get_task(dag, ti.task_id)
    if context.task_callback_type == TaskInstanceState.UP_FOR_RETRY:
        callbacks = task.on_retry_callback
    else:
        callbacks = task.on_failure_callback
    if not callbacks:
        log.warning("Callback requested, but the task has none", dag_id=ti.dag_id, task_id=ti.task_id)
        return []

    if context.ti_context is not None:
        runtime_ti = RuntimeTaskInstance.model_construct(
            **ti.model_dump(exclude_unset=True),
            task=task,
            _ti_context_from_server=context.ti_context,
            max_tries=context.ti_context.max_tries,
        )
    else:
        runtime_ti = RuntimeTaskInstance.model_construct(**ti.model_dump(exclude_unset=True), task=task)
    return _run_callbacks(callbacks, runtime_ti.get_template_context(), log)


def _send_task_email(context: EmailCallbackContext, dag: DAG, log: FilteringBoundLogger) -> list[str]:
    from airflow.sdk.execution_time.task_runner import RuntimeTaskInstance, _send_error_email_notification

    ti = context.ti
    task = _get_task(dag, ti.task_id)
    if not task.email:
        log.warning("Email requested, but the task has no email configured", task_id=ti.task_id)
        return []
    email_type = context.email_type.value
    if not (task.email_on_failure if email_type == "failure" else task.email_on_retry):
        log.info("Email not sent: the task sets email_on_%s=False", email_type, task_id=ti.task_id)
        return []

    runtime_ti = RuntimeTaskInstance.model_construct(
        **ti.model_dump(exclude_unset=True),
        task=task,
        _ti_context_from_server=context.ti_context,
        max_tries=context.ti_context.max_tries,
    )
    log.info("Sending %s email", email_type, dag_id=ti.dag_id, task_id=ti.task_id, run_id=ti.run_id)
    try:
        error = Exception(context.msg) if context.msg else None
        _send_error_email_notification(task, runtime_ti, runtime_ti.get_template_context(), error, log)
    except Exception as e:
        log.exception("Failed to send %s email", email_type, task_id=ti.task_id)
        return [f"{type(e).__name__}: {e}"]
    return []
