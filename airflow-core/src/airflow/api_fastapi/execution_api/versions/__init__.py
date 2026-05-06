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

from cadwyn import HeadVersion, Version, VersionBundle

from airflow.api_fastapi.execution_api.versions.v2025_04_28 import AddRenderedMapIndexField
from airflow.api_fastapi.execution_api.versions.v2025_05_20 import DowngradeUpstreamMapIndexes
from airflow.api_fastapi.execution_api.versions.v2025_08_10 import (
    AddDagRunStateFieldAndPreviousEndpoint,
    AddIncludePriorDatesToGetXComSlice,
)
from airflow.api_fastapi.execution_api.versions.v2025_09_23 import AddDagVersionIdField
from airflow.api_fastapi.execution_api.versions.v2025_10_27 import MakeDagRunConfNullable
from airflow.api_fastapi.execution_api.versions.v2025_11_05 import AddTriggeringUserNameField
from airflow.api_fastapi.execution_api.versions.v2026_04_06 import (
    AddDagEndpoint,
    AddDagRunDetailEndpoint,
    AddNoteField,
    AddPartitionKeyField,
    AddRunAfterField,
    AddTaskInstanceStartDateField,
    MakeDagRunStartDateNullable,
    ModifyDeferredTaskKwargsToJsonValue,
    MovePreviousRunEndpoint,
    RemoveUpstreamMapIndexesField,
)
from airflow.api_fastapi.execution_api.versions.v2026_04_17 import AddStateEndpoints, AddTeamNameField
from airflow.api_fastapi.execution_api.versions.v2026_06_16 import AddRetryPolicyFields

bundle = VersionBundle(
    HeadVersion(),
    Version("2026-06-16", AddRetryPolicyFields),
    Version(
        "2026-04-17",
        AddTeamNameField,
        AddStateEndpoints,
    ),
    Version(
        "2026-04-06",
        AddPartitionKeyField,
        MovePreviousRunEndpoint,
        AddDagRunDetailEndpoint,
        MakeDagRunStartDateNullable,
        ModifyDeferredTaskKwargsToJsonValue,
        RemoveUpstreamMapIndexesField,
        AddNoteField,
        AddTaskInstanceStartDateField,
        AddRunAfterField,
        AddDagEndpoint,
    ),
    Version("2025-11-05", AddTriggeringUserNameField),
    Version("2025-10-27", MakeDagRunConfNullable),
    Version("2025-09-23", AddDagVersionIdField),
    Version(
        "2025-08-10",
        AddDagRunStateFieldAndPreviousEndpoint,
        AddIncludePriorDatesToGetXComSlice,
    ),
    Version("2025-05-20", DowngradeUpstreamMapIndexes),
    Version("2025-04-28", AddRenderedMapIndexField),
    Version("2025-04-11"),
)
