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
"""Supervisor schema version 2026-12-01."""

from __future__ import annotations

from cadwyn import VersionChange, schema
from pydantic import JsonValue

from airflow.sdk.api.datamodels._generated import TIDeferredStatePayload


class RemoveStringTriggerKwargsFromDeferTask(VersionChange):
    """
    Stop accepting ``trigger_kwargs`` as a string in ``DeferTask``.

    The supervisor-schema mirror of the execution API's ``RemoveStringTriggerKwargs``.
    """

    description = __doc__

    instructions_to_migrate_to_previous_version = (
        # DeferTask inherits the field from the generated TIDeferredStatePayload, and Cadwyn only
        # changes a field on the class that declares it.
        schema(TIDeferredStatePayload)
        .field("trigger_kwargs")
        .had(type=dict[str, JsonValue | None] | str | None),
    )
