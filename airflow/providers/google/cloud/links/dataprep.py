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
    from airflow.utils.context import Context

BASE_LINK = "https://clouddataprep.com"
DATAPREP_FLOW_LINK = BASE_LINK + "/flows/{flow_id}?projectId={project_id}"
DATAPREP_JOB_GROUP_LINK = BASE_LINK + "/jobs/{job_group_id}?projectId={project_id}"


class DataprepFlowLink(BaseGoogleLink):
    """Helper class for constructing Dataprep flow link."""

    name = "Flow details page"
    key = "dataprep_flow_page"
    format_str = DATAPREP_FLOW_LINK

    @staticmethod
    def persist(context: Context, task_instance, project_id: str, flow_id: int):
        task_instance.xcom_push(
            context=context,
            key=DataprepFlowLink.key,
            value={"project_id": project_id, "flow_id": flow_id},
        )


class DataprepJobGroupLink(BaseGoogleLink):
    """Helper class for constructing Dataprep job group link."""

    name = "Job group details page"
    key = "dataprep_job_group_page"
    format_str = DATAPREP_JOB_GROUP_LINK

    @staticmethod
    def persist(context: Context, task_instance, project_id: str, job_group_id: int):
        task_instance.xcom_push(
            context=context,
            key=DataprepJobGroupLink.key,
            value={
                "project_id": project_id,
                "job_group_id": job_group_id,
            },
        )
