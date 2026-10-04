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

from cadwyn import VersionChange, schema

from airflow.dag_processing.processor import DagFileParsingResult  # noqa: SDK002
from airflow.sdk.api.datamodels._generated import PreviousTIResponse, TaskInstance, TIRunContext
from airflow.sdk.execution_time.comms import (
    TaskState,
)


class AddRegionCoordinatesToSupervisorTaskInstance(VersionChange):
    """Carry task coordinates in supervisor startup and previous-instance messages."""

    description = __doc__
    instructions_to_migrate_to_previous_version = (
        schema(TaskInstance).field("region_id").didnt_exist,
        schema(TaskInstance).field("region_index").didnt_exist,
        schema(PreviousTIResponse).field("region_id").didnt_exist,
        schema(PreviousTIResponse).field("region_index").didnt_exist,
    )


class AddArgBindingsToSupervisorTIRunContext(VersionChange):
    """
    Add the ``arg_bindings`` argument-binding spec for stub (foreign-runtime) tasks.

    Each entry is a discriminated union of ``XComArgBinding`` and ``LiteralArgBinding``
    keyed on ``kind``. The supervisor-schema mirror of the execution API's
    ``AddArgBindingsToTIRunContext``, named apart so the two migrations are not confused.
    """

    description = __doc__

    instructions_to_migrate_to_previous_version = (schema(TIRunContext).field("arg_bindings").didnt_exist,)


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
