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

from cadwyn import VersionChange, schema

from airflow.sdk.api.datamodels._generated import TIRunContext


class AddXComBatchMessages(VersionChange):
    """
    Add the ``GetXComBatch``/``XComBatchResult`` message pair to the task-execution channel.

    A wholly new discriminated-union member, not a field change on an existing one, so
    there is nothing for older schema consumers to migrate away from -- the head shape
    already is the schema for this body (see schema/AGENTS.md). This entry exists only
    to satisfy the per-commit ``check-supervisor-schemas-versions`` snapshot check.
    """

    description = __doc__

    instructions_to_migrate_to_previous_version = ()


class AddArgBindingsToSupervisorTIRunContext(VersionChange):
    """
    Add the ``arg_bindings`` argument-binding spec for stub (foreign-runtime) tasks.

    Each entry is a discriminated union of ``XComArgBinding`` and ``LiteralArgBinding``
    keyed on ``kind``. The supervisor-schema mirror of the execution API's
    ``AddArgBindingsToTIRunContext``, named apart so the two migrations are not confused.
    """

    description = __doc__

    instructions_to_migrate_to_previous_version = (schema(TIRunContext).field("arg_bindings").didnt_exist,)
