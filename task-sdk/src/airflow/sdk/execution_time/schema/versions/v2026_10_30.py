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
Supervisor schema version 2026-10-30.

A brand-new message body needs no field-level migration instructions here: a lang-SDK
pinned to an older version simply never sends it, so there is nothing to strip on the
way down. Only *changes* to bodies that already existed at an earlier dated version
require a ``VersionChange`` entry below.
"""

from __future__ import annotations

import json

from cadwyn import ResponseInfo, VersionChange, convert_response_to_previous_version_for, schema

from airflow.dag_processing.processor import DagFileParsingResult  # noqa: SDK002
from airflow.sdk.api.datamodels._generated import TIRunContext
from airflow.sdk.execution_time.comms import StartupDetails, TaskState


class AddArgBindingsToSupervisorTIRunContext(VersionChange):
    """
    Add the ``arg_bindings`` argument-binding spec for stub (foreign-runtime) tasks.

    Each entry is a discriminated union of ``XComArgBinding`` and ``LiteralArgBinding``
    keyed on ``kind``. The supervisor-schema mirror of the execution API's
    ``AddArgBindingsToTIRunContext``, named apart so the two migrations are not confused.
    """

    description = __doc__

    instructions_to_migrate_to_previous_version = (schema(TIRunContext).field("arg_bindings").didnt_exist,)


class AddDagRunConfJsonToSupervisorTIRunContext(VersionChange):
    """Add compact DagRun configuration JSON to the supervisor task context."""

    description = __doc__
    instructions_to_migrate_to_previous_version = (
        schema(TIRunContext).field("dag_run_conf_json").didnt_exist,
    )

    @convert_response_to_previous_version_for(StartupDetails)  # type: ignore[arg-type]
    def deserialize_conf_for_previous_versions(response: ResponseInfo) -> None:  # type: ignore[misc]
        """Preserve the dictionary contract for older language SDK clients."""
        ti_context = response.body.get("ti_context")
        if not isinstance(ti_context, dict):
            return
        dag_run = ti_context.get("dag_run")
        if not isinstance(dag_run, dict):
            return
        dag_run_conf_json = ti_context.pop("dag_run_conf_json", None)
        if isinstance(dag_run_conf_json, str):
            dag_run["conf"] = json.loads(dag_run_conf_json)


class AddRetryReasonToTaskState(VersionChange):
    """Add `retry_reason` to `TaskState`."""

    description = __doc__

    instructions_to_migrate_to_previous_version = (schema(TaskState).field("retry_reason").didnt_exist,)


class AddDagDefinitionsToDagFileParsingResult(VersionChange):
    """Add the imported Dag definitions and their source code to `DagFileParsingResult`."""

    description = __doc__

    instructions_to_migrate_to_previous_version = (
        schema(DagFileParsingResult).field("parsed_definitions").didnt_exist,
        schema(DagFileParsingResult).field("dag_source_codes").didnt_exist,
    )
