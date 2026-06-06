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
"""This module contains Google Cloud SQL sensors."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import timedelta
from typing import TYPE_CHECKING, Any

from airflow.providers.common.compat.sdk import BaseSensorOperator, conf
from airflow.providers.google.cloud.hooks.cloud_sql import CloudSQLHook
from airflow.providers.google.cloud.triggers.cloud_sql import CloudSQLNoOperationInProgressTrigger
from airflow.providers.google.common.hooks.base_google import PROVIDE_PROJECT_ID

if TYPE_CHECKING:
    from airflow.providers.common.compat.sdk import Context


class CloudSQLNoOperationInProgressSensor(BaseSensorOperator):
    """
    Wait until a Cloud SQL instance has no operation in progress.

    Cloud SQL allows only one administrative operation at a time per instance.
    Use this sensor before import, export, backup, or other instance operations
    to avoid submitting a new operation while another one is still running.

    :param project_id: Google Cloud project ID. If set to None or missing,
        the default project_id from the Google Cloud connection is used.
    :param instance: Cloud SQL instance ID. This does not include the project ID.
    :param gcp_conn_id: The Airflow connection ID used to connect to Google Cloud.
    :param api_version: API version used, for example ``v1beta4``.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
    :param deferrable: Run sensor in deferrable mode.
    """

    template_fields: Sequence[str] = (
        "project_id",
        "instance",
        "gcp_conn_id",
        "api_version",
        "impersonation_chain",
    )

    ui_color = "#D4ECEA"

    def __init__(
        self,
        *,
        project_id: str = PROVIDE_PROJECT_ID,
        instance: str,
        gcp_conn_id: str = "google_cloud_default",
        api_version: str = "v1beta4",
        impersonation_chain: str | Sequence[str] | None = None,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.project_id = project_id
        self.instance = instance
        self.gcp_conn_id = gcp_conn_id
        self.api_version = api_version
        self.impersonation_chain = impersonation_chain
        self.deferrable = deferrable

    @staticmethod
    def _get_running_operations(operations: Sequence[dict], instance: str) -> list[dict]:
        return [
            operation
            for operation in operations
            if operation.get("targetId") == instance and operation.get("status") != "DONE"
        ]

    def poke(self, context: Context) -> bool:
        hook = CloudSQLHook(
            api_version=self.api_version,
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        operations = hook.get_instance_operations(project_id=self.project_id, instance=self.instance)
        running_operations = self._get_running_operations(operations=operations, instance=self.instance)

        if running_operations:
            operation = running_operations[0]
            self.log.info(
                "Cloud SQL operation %s is %s for instance %s.",
                operation.get("name"),
                operation.get("status"),
                self.instance,
            )
            return False

        self.log.info("No Cloud SQL operations are running for instance %s.", self.instance)
        return True

    def execute(self, context: Context) -> Any:
        if not self.deferrable:
            return super().execute(context)
        if self.poke(context=context):
            return None
        self.defer(
            timeout=timedelta(seconds=self.timeout),
            trigger=CloudSQLNoOperationInProgressTrigger(
                project_id=self.project_id,
                instance=self.instance,
                gcp_conn_id=self.gcp_conn_id,
                impersonation_chain=self.impersonation_chain,
                poke_interval=self.poke_interval,
                api_version=self.api_version,
            ),
            method_name="execute_complete",
        )

    def execute_complete(self, context: Context, event: dict[str, str] | None = None) -> str:
        if event is None:
            raise RuntimeError("No event received in trigger callback")
        if event["status"] == "success":
            return event["message"]
        raise RuntimeError(event["message"])
