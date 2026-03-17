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

from typing import TYPE_CHECKING

from airflow.providers.google.cloud.links.base import BaseGoogleLink

if TYPE_CHECKING:
    from airflow.providers.common.compat.sdk import Context

RAY_JOB_BASE_LINK = "http://{cluster_address}/#/jobs"
RAY_JOB_LINK = RAY_JOB_BASE_LINK + "/{job_id}"


class RayJobLink(BaseGoogleLink):
    """Helper class for constructing Job on Ray cluster Link."""

    name = "Ray Job"
    key = "ray_job"
    format_str = RAY_JOB_LINK

    @classmethod
    def persist(cls, context: Context, **value):
        cluster_address = value.get("cluster_address")
        job_id = value.get("job_id")
        super().persist(
            context=context,
            cluster_address=cluster_address,
            job_id=job_id,
        )
