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

from typing import Any


def bq_cast(string_field: str, bq_type: str) -> None | int | float | bool | str:
    """
    Cast a BigQuery row to the appropriate data types.

    This is useful because BigQuery returns all fields as strings.
    """
    if string_field is None:
        return None
    elif bq_type == "INTEGER":
        return int(string_field)
    elif bq_type in ("FLOAT", "TIMESTAMP"):
        return float(string_field)
    elif bq_type == "BOOLEAN":
        if string_field not in ["true", "false"]:
            raise ValueError(f"{string_field} must have value 'true' or 'false'")
        return string_field == "true"
    else:
        return string_field


def convert_job_id(job_id: str | list[str], project_id: str, location: str | None) -> Any:
    """
    Convert job_id to path: project_id:location:job_id.

    :param project_id: Required. The ID of the Google Cloud project where workspace located.
    :param location: Optional. The ID of the Google Cloud region where workspace located.
    :param job_id: Required. The ID of the job.
    :return: str or list[str] of project_id:location:job_id.
    """
    location = location or "US"
    if isinstance(job_id, list):
        return [f"{project_id}:{location}:{i}" for i in job_id]
    else:
        return f"{project_id}:{location}:{job_id}"
