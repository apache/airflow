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
"""This module contains Google Cloud Run Jobs operators."""
from __future__ import annotations

from typing import TYPE_CHECKING, Sequence

from airflow.models import BaseOperator
from airflow.providers.google.cloud.hooks.cloud_run import CloudRunJobHook
from airflow.providers.google.cloud.links.cloud_run import CloudRunJobExecutionLink

if TYPE_CHECKING:
    from airflow.utils.context import Context


class CloudRunExecuteJobOperator(BaseOperator):
    """
    Executes an existing Cloud Run job.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudRunExecuteJobOperator`

    :param job_name: The name of the cloud run job to execute
    :param region: The region of the Cloud Run job (for example europe-west1)
    :param project_id: The ID of the GCP project that owns the job.
            If set to ``None`` or missing, the default project_id
            from the GCP connection is used.
    :param gcp_conn_id: The connection ID to use to connect to Google Cloud.
    :param delegate_to: The account to impersonate, if any.
            For this to work, the service account making the request
            must have domain-wide delegation enabled.
    :param wait_until_finished: If True, wait for the end of job execution
            before exiting. If False (default), only submits job.
    :param impersonation_chain: Optional service account to impersonate
            using short-term credentials, or chained list of accounts required
            to get the access_token of the last account in the list,
            which will be impersonated in the request. If set as a string, the
            account must grant the originating account the Service Account Token
            Creator IAM role. If set as a sequence, the identities from the list
            must grant Service Account Token Creator IAM role to the directly
            preceding identity, with first account from the list granting this
            role to the originating account (templated).
    """

    template_fields: Sequence[str] = ("job_name", "region", "project_id", "gcp_conn_id")
    operator_extra_links = (CloudRunJobExecutionLink(),)

    def __init__(
        self,
        job_name: str,
        region: str,
        project_id: str | None = None,
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: str | None = None,
        cancel_timeout: int | None = 10 * 60,
        wait_until_finished: bool = False,
        impersonation_chain: str | Sequence[str] | None = None,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.job_name = job_name
        self.region = region
        self.project_id = project_id
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.cancel_timeout = cancel_timeout
        self.wait_until_finished = wait_until_finished
        self.job = None
        self.hook: CloudRunJobHook | None = None
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context):

        self.hook = CloudRunJobHook(
            region=self.region,
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            wait_until_finished=self.wait_until_finished,
            impersonation_chain=self.impersonation_chain,
        )

        def set_current_execution(current_execution):
            self.execution = current_execution
            CloudRunJobExecutionLink.persist(
                operator_instance=self,
                context=context,
                project_id=self.project_id,
                region=self.region,
                execution_id=self.execution.get("metadata").get("name"),
            )

        cloud_run_job_execution = self.hook.execute_cloud_run_job(
            project_id=self.project_id,
            job_name=self.job_name,
            on_new_execution_callback=set_current_execution,
        )

        return cloud_run_job_execution
