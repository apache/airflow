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

from airflow.providers.common.compat.sdk import AirflowException
from airflow.providers.google.cloud.hooks.functions import CloudFunctionsHook
from airflow.triggers.base import BaseTrigger, TriggerEvent


class CloudFunctionInvokeFunctionTrigger(BaseTrigger):
    """
    Trigger to invoke a Google Cloud Function and wait for its result.

    This trigger invokes the function via the synchronous Cloud Functions API
    using ``asyncio.to_thread`` and yields a single TriggerEvent with the
    result.

    :param function_id: ID of the function to be called.
    :param input_data: Input to be passed to the function.
    :param location: The location where the function is located.
    :param project_id: Google Cloud Project ID where the function belongs.
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
    :param api_version: API version used (for example v1).
    :param impersonation_chain: Optional service account to impersonate using
        short-term credentials, or chained list of accounts required to get
        the access_token of the last account in the list, which will be
        impersonated in the request.
    """

    def __init__(
        self,
        function_id: str,
        input_data: dict,
        location: str,
        project_id: str,
        gcp_conn_id: str = "google_cloud_default",
        api_version: str = "v1",
        impersonation_chain: str | Sequence[str] | None = None,
    ) -> None:
        super().__init__()
        self.function_id = function_id
        self.input_data = input_data
        self.location = location
        self.project_id = project_id
        self.gcp_conn_id = gcp_conn_id
        self.api_version = api_version
        self.impersonation_chain = impersonation_chain

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serialize class arguments and classpath."""
        return (
            "airflow.providers.google.cloud.triggers.cloud_functions.CloudFunctionInvokeFunctionTrigger",
            {
                "function_id": self.function_id,
                "input_data": self.input_data,
                "location": self.location,
                "project_id": self.project_id,
                "gcp_conn_id": self.gcp_conn_id,
                "api_version": self.api_version,
                "impersonation_chain": self.impersonation_chain,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """Invoke the Cloud Function and yield a single result event."""
        hook = CloudFunctionsHook(
            api_version=self.api_version,
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        try:
            result = await asyncio.to_thread(
                hook.call_function,
                function_id=self.function_id,
                input_data=self.input_data,
                location=self.location,
                project_id=self.project_id,
            )
            yield TriggerEvent(
                {
                    "status": "success",
                    "result": result,
                    "execution_id": result.get("executionId"),
                }
            )
        except AirflowException as e:
            self.log.error("Cloud Function invocation failed: %s", e)
            yield TriggerEvent({"status": "error", "message": str(e)})
        except Exception as e:
            self.log.exception("Unexpected error while invoking Cloud Function.")
            yield TriggerEvent({"status": "error", "message": str(e)})
