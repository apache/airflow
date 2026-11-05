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

from airflow.sdk.definitions.parse_warning_record import DagParseWarningRecord


def emit_dag_warning(
    dag_id: str,
    message: str,
    *,
    stacktrace: str | None = None,
) -> None:
    """Record a parse-time warning for a Dag id that could not be loaded (#29275)."""
    if not dag_id:
        raise ValueError("dag_id must be non-empty")
    from airflow.sdk.definitions._internal.contextmanager import DagContext

    if DagContext.current_autoregister_module_name is None:
        raise RuntimeError("emit_dag_warning() can only be called while a Dag file is being parsed")
    if stacktrace:
        message = f"{message}\n\n{stacktrace}"
    DagContext.parse_warnings.append(DagParseWarningRecord(dag_id=dag_id, message=message))


def drain_parse_warnings() -> list[DagParseWarningRecord]:
    from airflow.sdk.definitions._internal.contextmanager import DagContext

    warnings = DagContext.parse_warnings
    DagContext.parse_warnings = []
    return warnings
