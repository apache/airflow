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

from cadwyn import RequestInfo, VersionChange, convert_request_to_next_version_for, endpoint


class AddDagProcessorJobEndpoints(VersionChange):
    """Add Dag processor lifecycle and parse-token endpoints."""

    description = __doc__

    instructions_to_migrate_to_previous_version = (
        endpoint("/jobs", ["POST"]).didnt_exist,
        endpoint("/jobs/{job_id}/heartbeat", ["POST"]).didnt_exist,
        endpoint("/jobs/{job_id}/complete", ["POST"]).didnt_exist,
        endpoint("/jobs/{job_id}/parse-token", ["POST"]).didnt_exist,
    )


class AddCallbackContextAndStateEndpoints(VersionChange):
    """
    Add the endpoints a routed Dag processor callback reads its context from and reports its state to.

    It can also read its Dag's previous successful run. An attempt to run a callback can also identify
    itself when it exchanges the callback's token, so it can repeat its own exchange. Requests of
    earlier versions keep the single-use exchange.
    """

    description = __doc__

    instructions_to_migrate_to_previous_version = (
        endpoint("/callbacks/{callback_id}/context", ["GET"]).didnt_exist,
        endpoint("/callbacks/{callback_id}/previous-successful-dagrun", ["GET"]).didnt_exist,
        endpoint("/callbacks/{callback_id}/state", ["PATCH"]).didnt_exist,
    )

    @convert_request_to_next_version_for("/callbacks/{callback_id}/run", ["PATCH"])  # type: ignore[arg-type]
    def drop_callback_run_payload(request: RequestInfo):  # type: ignore[misc]
        # Mypy treats Cadwyn's required request parameter as self inside this class.
        request_info: RequestInfo = request
        request_info.body = None
