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

from cadwyn import VersionChange, endpoint


class AddTriggerEndpoints(VersionChange):
    """Add triggerer orchestration endpoints so a DB-free triggerer can run over HTTP (AIP-92)."""

    description = __doc__

    instructions_to_migrate_to_previous_version = (
        endpoint("/triggers/load", ["POST"]).didnt_exist,
        endpoint("/triggers/workloads", ["POST"]).didnt_exist,
        endpoint("/triggers/{trigger_id}/event", ["POST"]).didnt_exist,
        endpoint("/triggers/{trigger_id}/failure", ["POST"]).didnt_exist,
        endpoint("/triggers/cleanup", ["POST"]).didnt_exist,
    )


class AddJobEndpoints(VersionChange):
    """Add Job register/heartbeat endpoints so a DB-free triggerer can claim a Job identity (AIP-92)."""

    description = __doc__

    instructions_to_migrate_to_previous_version = (
        endpoint("/jobs", ["POST"]).didnt_exist,
        endpoint("/jobs/{job_id}/heartbeat", ["POST"]).didnt_exist,
        endpoint("/jobs/{job_id}/complete", ["POST"]).didnt_exist,
    )
