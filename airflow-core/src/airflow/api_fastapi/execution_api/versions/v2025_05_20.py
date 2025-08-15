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

from cadwyn import ResponseInfo, VersionChange, convert_response_to_previous_version_for, schema

from airflow.api_fastapi.execution_api.datamodels.taskinstance import TIRunContext


class DowngradeUpstreamMapIndexes(VersionChange):
    """Downgrade the upstream map indexes type for older clients."""

    description = __doc__

    instructions_to_migrate_to_previous_version = (
        schema(TIRunContext).field("upstream_map_indexes").had(type=dict[str, int | None] | None),
    )

    @convert_response_to_previous_version_for(TIRunContext)  # type: ignore[arg-type]
    def downgrade_upstream_map_indexes(response: ResponseInfo = None) -> None:  # type: ignore
        """
        Downgrades the `upstream_map_indexes` field when converting to the previous version.

        Ensures that the field is only a dictionary of  [str, int] (old format).
        """
        resp = response.body.get("upstream_map_indexes")
        if isinstance(resp, dict):
            downgraded: dict[str, int | list | None] = {}
            for k, v in resp.items():
                if isinstance(v, int):
                    downgraded[k] = v
                elif isinstance(v, list) and v and all(isinstance(i, int) for i in v):
                    downgraded[k] = v[0]
                else:
                    # Keep values like None as is — the Task SDK expects them unchanged during mapped task expansion,
                    # and modifying them can cause unexpected failures.
                    downgraded[k] = None
            response.body["upstream_map_indexes"] = downgraded
