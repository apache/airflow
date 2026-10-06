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
"""Business logic backing the task-instance execution routes."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from airflow.serialization.definitions.operatorlink import is_link_xcom_key

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from airflow.models.dagbag import DBDagBag


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


def get_arg_bindings(dag_bag: DBDagBag, ti: Any, *, session: Session) -> list | None:
    """
    Extract the stub task's TaskFlow arg spec from its Dag version.

    Mapped (``.expand()``) stubs never capture a parse-time spec, so they resolve to
    ``None`` here and keep the legacy ignored-args behavior; per-map-index delivery
    lands in a follow-up.
    """
    if ti.dag_version_id is None:
        return None
    if (dag := dag_bag.get_dag(ti.dag_version_id, session=session)) is None:
        return None
    if (task := dag.task_dict.get(ti.task_id)) is None or not task.is_stub:
        return None
    return task.arg_bindings


def drop_operator_link_keys(keys: list[str], dag_bag: DBDagBag, ti: Any, *, session: Session) -> list[str]:
    """
    Remove the task's operator-link xcom keys from ``keys``.

    Links recorded by earlier tries stay readable in the UI, so a retry must not purge
    them. The server owns that decision because every SDK receives this list and would
    otherwise each have to own the logic for link key naming convention.
    """
    if ti.dag_version_id is None:
        return keys
    if (dag := dag_bag.get_dag(ti.dag_version_id, session=session)) is None:
        return keys
    if (task := dag.task_dict.get(ti.task_id)) is None:
        return keys
    link_xcom_keys = {link.xcom_key for link in task.operator_extra_link_dict.values()}
    if not link_xcom_keys:
        return keys
    return [key for key in keys if not is_link_xcom_key(key, link_xcom_keys)]
