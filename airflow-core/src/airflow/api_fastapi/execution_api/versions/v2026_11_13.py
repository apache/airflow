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

from airflow.api_fastapi.execution_api.datamodels.dagrun import ClearDagRunPayload


class AddOnlyFailedToClearDagRunPayload(VersionChange):
    """Add the ``only_failed`` field to the clear-dag-run request payload."""

    description = __doc__

    # Request-body-only additive field: for older versions the field simply does not exist on
    # ``ClearDagRunPayload`` (which forbids extra fields), so an older client omitting it clears the
    # whole run, and one that sends it is rejected rather than silently downgraded to a whole-run clear.
    instructions_to_migrate_to_previous_version = (
        schema(ClearDagRunPayload).field("only_failed").didnt_exist,
    )
