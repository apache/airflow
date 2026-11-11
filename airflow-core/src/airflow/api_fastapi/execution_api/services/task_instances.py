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

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from airflow.api_fastapi.execution_api.services.stub_arg_bindings import (
    UndeliverableMappedArgBindingsError,
    derive_mapped_arg_bindings,
)
from airflow.serialization.definitions.mappedoperator import is_mapped

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from airflow.api_fastapi.execution_api.datamodels.task_arg_binding import TaskArgBinding
    from airflow.models.dagbag import DBDagBag
    from airflow.serialization.serialized_objects import SerializedOperator


def client_supports_arg_bindings() -> bool:
    """
    Whether the request's negotiated API version can receive ``arg_bindings``.

    Clients on older versions never see the field (the version migration strips it from
    the response), so the derivation must not run for them.

    Rather than comparing the negotiated version by date, we check the
    ``VersionChangeWithSideEffects`` subclass's ``is_applied`` flag; see
    https://docs.cadwyn.dev/concepts/version_changes/#version-changes-with-side-effects
    """
    # Imported locally: the versions package transitively imports the routes, which import
    # this module, so a top-level import here would be circular.
    from airflow.api_fastapi.execution_api.versions.v2026_10_30 import AddArgBindingsToTIRunContext

    return AddArgBindingsToTIRunContext.is_applied


def get_task(dag_bag: DBDagBag, ti: Any, *, session: Session) -> SerializedOperator | None:
    """Return the task of *ti* from its Dag version, or ``None`` if it cannot be found."""
    if ti.dag_version_id is None:
        return None
    if (dag := dag_bag.get_dag(ti.dag_version_id, session=session)) is None:
        return None
    return dag.task_dict.get(ti.task_id)


def client_supports_mapped_arg_bindings() -> bool:
    """Whether the request's negotiated API version can receive the arg bindings of a mapped stub."""
    from airflow.api_fastapi.execution_api.versions.v2027_02_28 import AddMappedStubArgBindings

    return AddMappedStubArgBindings.is_applied


def get_arg_bindings(
    task: SerializedOperator, ti: Any, *, dag_bag: DBDagBag, session: Session
) -> list[TaskArgBinding] | list[dict[str, Any]] | None:
    """
    Return the TaskFlow arg spec of *task* if it is a stub task.

    A stub whose arguments differ per map index gets its spec derived for this map index; a Dag
    serialized before such a stub had bindings keeps the legacy ignored-args behavior. So does a
    mapped stub for a client older than 2027-02-28.

    :raises UndeliverableMappedArgBindingsError: if an argument has no value at this map index, or
        the client is too old to receive the stub's bindings.
    """
    if not task.is_stub or (arg_bindings := task.arg_bindings) is None:
        return None
    per_map_index = is_mapped(task) or task.get_closest_mapped_task_group() is not None
    if not client_supports_mapped_arg_bindings():
        if is_mapped(task):
            return None
        if per_map_index or any(entry["kind"] == "sequence" for entry in arg_bindings):
            raise UndeliverableMappedArgBindingsError(
                "they need a client at Execution API version 2027-02-28 or later"
            )
        return arg_bindings
    if per_map_index:
        return derive_mapped_arg_bindings(task, ti, arg_bindings, dag_bag=dag_bag, session=session)
    return arg_bindings


def get_execution_timeout(task: SerializedOperator) -> float | None:
    """
    Return the ``execution_timeout`` of *task* in seconds, or ``None`` if it sets none.

    The value is sent as it is, so a runtime handles it the way the Python task runner does: zero
    means no timeout, and a negative one has already elapsed.
    """
    if task.execution_timeout is None:
        return None
    return task.execution_timeout.total_seconds()
