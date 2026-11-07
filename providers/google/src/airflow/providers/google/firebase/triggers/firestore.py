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
"""This module contains Google Cloud Firestore triggers."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Sequence
from functools import cached_property
from typing import Any

from airflow.providers.google.firebase.hooks.firestore import (
    TIME_TO_SLEEP_IN_SECONDS,
    CloudFirestoreAsyncHook,
)
from airflow.triggers.base import BaseTrigger, TriggerEvent


class CloudFirestoreExportDatabaseTrigger(BaseTrigger):
    """
    Trigger that periodically polls Cloud Firestore API to verify export operation status.

    :param operation_name: The resource name of the Firestore export operation.
    :param gcp_conn_id: The connection ID used to connect to Google Cloud.
    :param api_version: API version used (for example v1 or v1beta1).
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
    :param poll_interval: Time (seconds) to wait between calls to check the operation status.
    """

    def __init__(
        self,
        operation_name: str,
        gcp_conn_id: str = "google_cloud_default",
        api_version: str = "v1",
        impersonation_chain: str | Sequence[str] | None = None,
        poll_interval: float = TIME_TO_SLEEP_IN_SECONDS,
    ) -> None:
        super().__init__()
        self.operation_name = operation_name
        self.gcp_conn_id = gcp_conn_id
        self.api_version = api_version
        self.impersonation_chain = impersonation_chain
        self.poll_interval = poll_interval

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            "airflow.providers.google.firebase.triggers.firestore.CloudFirestoreExportDatabaseTrigger",
            {
                "operation_name": self.operation_name,
                "gcp_conn_id": self.gcp_conn_id,
                "api_version": self.api_version,
                "impersonation_chain": self.impersonation_chain,
                "poll_interval": self.poll_interval,
            },
        )

    @cached_property
    def hook(self) -> CloudFirestoreAsyncHook:
        return CloudFirestoreAsyncHook(
            gcp_conn_id=self.gcp_conn_id,
            api_version=self.api_version,
            impersonation_chain=self.impersonation_chain,
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        try:
            while True:
                operation = await self.hook.get_operation(operation_name=self.operation_name)
                if operation.get("done"):
                    error = operation.get("error")
                    if error:
                        yield TriggerEvent(
                            {
                                "operation_name": self.operation_name,
                                "status": "error",
                                "message": str(error),
                            }
                        )
                        return
                    yield TriggerEvent(
                        {
                            "operation_name": self.operation_name,
                            "status": "success",
                            "response": operation.get("response"),
                        }
                    )
                    return

                self.log.info(
                    "Operation %s is still in progress; sleeping for %s seconds.",
                    self.operation_name,
                    self.poll_interval,
                )
                await asyncio.sleep(self.poll_interval)
        except Exception as e:
            self.log.exception("Exception occurred while checking Firestore operation status.")
            yield TriggerEvent(
                {
                    "operation_name": self.operation_name,
                    "status": "error",
                    "message": str(e),
                }
            )
