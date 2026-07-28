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

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from airflow.models.dagbag import DBDagBag

# Task type recorded on the TI row (``TaskInstance.operator``) for
# ``airflow.providers.standard.decorators.stub._StubOperator``. Used to gate the
# serialized-Dag lookup for ``arg_bindings`` so regular tasks never pay for it.
# The gate matches the exact class name; a subclass would need its own entry here.
STUB_TASK_TYPE = "_StubOperator"


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
    if (task := dag.task_dict.get(ti.task_id)) is None:
        return None
    return getattr(task, "_arg_bindings", None)
