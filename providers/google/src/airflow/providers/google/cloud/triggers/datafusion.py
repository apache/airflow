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

import asyncio
from collections.abc import AsyncIterator, Sequence
from typing import Any

from airflow.providers.google.cloud.hooks.datafusion import DataFusionAsyncHook
from airflow.providers.google.cloud.utils.datafusion import DataFusionPipelineType
from airflow.triggers.base import BaseTrigger, TriggerEvent


class DataFusionStartPipelineTrigger(BaseTrigger):
    """
    Trigger to perform checking the pipeline status until it reaches terminate state.

    :param pipeline_name: Your pipeline name.
    :param instance_url: Endpoint on which the REST APIs is accessible for the instance.
    :param pipeline_id: Unique pipeline ID associated with specific pipeline
    :param pipeline_type: Your pipeline type.
    :param namespace: if your pipeline belongs to a Basic edition instance, the namespace ID
       is always default. If your pipeline belongs to an Enterprise edition instance, you
       can create a namespace.
    :param gcp_conn_id: Reference to google cloud connection id
    :param poll_interval: polling period in seconds to check for the status
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    def __init__(
        self,
        instance_url: str,
        namespace: str,
        pipeline_name: str,
        pipeline_id: str,
        pipeline_type: str,
        poll_interval: float = 3.0,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        success_states: list[str] | None = None,
    ):
        super().__init__()
        self.instance_url = instance_url
        self.namespace = namespace
        self.pipeline_name = pipeline_name
        self.pipeline_id = pipeline_id
        self.pipeline_type = pipeline_type
        self.poll_interval = poll_interval
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain
        self.success_states = success_states

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serialize DataFusionStartPipelineTrigger arguments and classpath."""
        return (
            "airflow.providers.google.cloud.triggers.datafusion.DataFusionStartPipelineTrigger",
            {
                "gcp_conn_id": self.gcp_conn_id,
                "instance_url": self.instance_url,
                "namespace": self.namespace,
                "pipeline_name": self.pipeline_name,
                "pipeline_id": self.pipeline_id,
                "pipeline_type": self.pipeline_type,
                "success_states": self.success_states,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:  # type: ignore[override]
        """Get current pipeline status and yields a TriggerEvent."""
        hook = self._get_async_hook()
        try:
            while True:
                # Poll for job execution status
                response_from_hook = await hook.get_pipeline_status(
                    success_states=self.success_states,
                    instance_url=self.instance_url,
                    namespace=self.namespace,
                    pipeline_name=self.pipeline_name,
                    pipeline_id=self.pipeline_id,
                    pipeline_type=DataFusionPipelineType.from_str(self.pipeline_type),
                )
                if response_from_hook == "success":
                    yield TriggerEvent(
                        {
                            "pipeline_id": self.pipeline_id,
                            "status": "success",
                            "message": "Pipeline is running",
                        }
                    )
                    return
                elif response_from_hook == "pending":
                    self.log.info("Pipeline is not still in running state...")
                    self.log.info("Sleeping for %s seconds.", self.poll_interval)
                    await asyncio.sleep(self.poll_interval)
                else:
                    yield TriggerEvent({"status": "error", "message": response_from_hook})
                    return
        except Exception as e:
            self.log.exception("Exception occurred while checking for pipeline state")
            yield TriggerEvent({"status": "error", "message": str(e)})

    def _get_async_hook(self) -> DataFusionAsyncHook:
        return DataFusionAsyncHook(
            instance_url=self.instance_url,
            namespace=self.namespace,
            pipeline_name=self.pipeline_name,
            pipeline_id=self.pipeline_id,
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
