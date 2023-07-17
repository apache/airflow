#
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

from typing import TYPE_CHECKING, Optional, Sequence, Union

from google.cloud.run_v2 import Job, JobsClient, CreateJobRequest

from airflow.providers.google.cloud.operators.cloud_base import \
    GoogleCloudBaseOperator

if TYPE_CHECKING:
    from airflow.utils.context import Context


class CloudRunCreateJobOperator(GoogleCloudBaseOperator):
    def __init__(self,
                 project_id: str,
                 region: str,
                 job_name: str,
                 job: Job,
                 gcp_conn_id: str = 'google_cloud_default',
                 impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
                 **kwargs,
                 ):
        super().__init__(**kwargs)
        self.project_id = project_id
        self.region = region
        self.job_name = job_name
        self.job = job
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context):
        client = JobsClient()
        job = Job()
        job.template.template.max_retries = 1187

        request = CreateJobRequest(
            parent=f"projects/{self.project_id}/locations/{self.region}",
            job=self.job,
            job_id=self.job_name
        )

        operation = client.create_job(request=request)

        response = operation.result()

        print(response)

