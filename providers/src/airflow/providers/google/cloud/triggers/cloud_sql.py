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
"""This module contains Google Cloud SQL triggers."""

from __future__ import annotations

import asyncio
from typing import Sequence

from airflow.providers.google.cloud.hooks.cloud_sql import (
    CloudSQLAsyncHook,
    CloudSqlOperationStatus,
)
from airflow.providers.google.common.hooks.base_google import PROVIDE_PROJECT_ID
from airflow.triggers.base import BaseTrigger, TriggerEvent


class CloudSQLExportTrigger(BaseTrigger):
    """
    Trigger that periodically polls information from Cloud SQL API to verify job status.

    Implementation leverages asynchronous transport.
    """

    def __init__(
        self,
        operation_name: str,
        project_id: str = PROVIDE_PROJECT_ID,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        poke_interval: int = 20,
    ):
        super().__init__()
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain
        self.operation_name = operation_name
        self.project_id = project_id
        self.poke_interval = poke_interval
        self.hook = CloudSQLAsyncHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )

    def serialize(self):
        return (
            "airflow.providers.google.cloud.triggers.cloud_sql.CloudSQLExportTrigger",
            {
                "operation_name": self.operation_name,
                "project_id": self.project_id,
                "gcp_conn_id": self.gcp_conn_id,
                "impersonation_chain": self.impersonation_chain,
                "poke_interval": self.poke_interval,
            },
        )

    async def run(self):
        try:
            while True:
                operation = await self.hook.get_operation(
                    project_id=self.project_id, operation_name=self.operation_name
                )
                if operation["status"] == CloudSqlOperationStatus.DONE:
                    if "error" in operation:
                        yield TriggerEvent(
                            {
                                "operation_name": operation["name"],
                                "status": "error",
                                "message": operation["error"]["message"],
                            }
                        )
                        return

                    yield TriggerEvent(
                        {
                            "operation_name": operation["name"],
                            "status": "success",
                        }
                    )
                    return
                else:
                    self.log.info(
                        "Operation status is %s, sleeping for %s seconds.",
                        operation["status"],
                        self.poke_interval,
                    )
                    await asyncio.sleep(self.poke_interval)
        except Exception as e:
            self.log.exception("Exception occurred while checking operation status.")
            yield TriggerEvent(
                {
                    "status": "failed",
                    "message": str(e),
                }
            )
