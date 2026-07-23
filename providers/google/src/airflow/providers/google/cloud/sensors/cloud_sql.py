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
from typing import TYPE_CHECKING

from googleapiclient.errors import HttpError

from airflow.providers.common.compat.sdk import AirflowException, BaseSensorOperator, conf
from airflow.providers.google.cloud.hooks.cloud_sql import (
    CLOUD_SQL_NON_TERMINAL_STATUSES,
    CloudSQLHook,
)
from airflow.providers.google.cloud.triggers.cloud_sql import CloudSQLNoOperationInProgressTrigger
from airflow.providers.google.common.hooks.base_google import PROVIDE_PROJECT_ID

if TYPE_CHECKING:
    from airflow.providers.common.compat.sdk import Context


class CloudSQLOperationError(AirflowException):
    """Raised when the Cloud SQL operations check fails (for example, the instance is missing or access is denied)."""


class CloudSQLNoOperationInProgressSensor(BaseSensorOperator):
    """
    Wait until a Cloud SQL instance has no administrative operation in progress.

    Cloud SQL serializes administrative operations per instance: only one import, export, backup or
    similar operation can run at a time. Submitting another while one is in flight fails with HTTP
    409 ``operationInProgress``. Place this sensor upstream of
    :class:`~airflow.providers.google.cloud.operators.cloud_sql.CloudSQLImportInstanceOperator` /
    :class:`~airflow.providers.google.cloud.operators.cloud_sql.CloudSQLExportInstanceOperator`
    (or between mutually exclusive admin operators) to serialize work against the same instance.

    The sensor is operation-agnostic: it polls ``sqladmin.operations.list`` for the instance and
    succeeds once no operation is in a non-terminal (PENDING/RUNNING) state. It is best-effort -- a
    new operation could still start between the sensor passing and the next operator submitting.

    .. seealso::
        For more information on how to use this sensor, take a look at the guide:
        :ref:`howto/operator:CloudSQLNoOperationInProgressSensor`

    :param instance: Name of the Cloud SQL instance to watch.
    :param project_id: Optional, Google Cloud Project ID. If not provided the default project is used.
    :param gcp_conn_id: The connection ID used to connect to Google Cloud.
    :param api_version: API version used (e.g. v1beta4).
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token of the last
        account in the list, which will be impersonated in the request.
    :param deferrable: Run the sensor in deferrable mode.
    """

    template_fields: Sequence[str] = (
        "project_id",
        "instance",
        "impersonation_chain",
    )
    ui_color = "#D4ECEA"

    def __init__(
        self,
        *,
        instance: str,
        project_id: str = PROVIDE_PROJECT_ID,
        gcp_conn_id: str = "google_cloud_default",
        api_version: str = "v1beta4",
        impersonation_chain: str | Sequence[str] | None = None,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.instance = instance
        self.project_id = project_id
        self.gcp_conn_id = gcp_conn_id
        self.api_version = api_version
        self.impersonation_chain = impersonation_chain
        self.deferrable = deferrable

    def _get_hook(self) -> CloudSQLHook:
        return CloudSQLHook(
            api_version=self.api_version,
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )

    def poke(self, context: Context) -> bool:
        hook = self._get_hook()
        try:
            operations = hook.list_operations(project_id=self.project_id, instance=self.instance)
        except HttpError as e:
            if e.resp.status in (403, 404):
                # Instance missing or access denied - surface the misconfiguration instead of poking.
                raise CloudSQLOperationError(
                    f"Cloud SQL operations.list failed for instance {self.instance}: {e}"
                )
            raise
        in_progress = [op for op in operations if op.get("status") in CLOUD_SQL_NON_TERMINAL_STATUSES]
        if in_progress:
            self.log.info(
                "%s operation(s) still in progress on instance %s.", len(in_progress), self.instance
            )
            return False
        return True

    def execute(self, context: Context) -> None:
        """Run on the worker and defer using the trigger when in deferrable mode."""
        if self.deferrable:
            if not self.poke(context=context):
                self.defer(
                    timeout=timedelta(seconds=self.timeout),
                    trigger=CloudSQLNoOperationInProgressTrigger(
                        instance=self.instance,
                        project_id=self.project_id,
                        gcp_conn_id=self.gcp_conn_id,
                        impersonation_chain=self.impersonation_chain,
                        poke_interval=int(self.poke_interval),
                        api_version=self.api_version,
                    ),
                    method_name="execute_complete",
                )
        else:
            super().execute(context)

    def execute_complete(self, context: Context, event: dict | None = None) -> None:
        """Act as a callback for when the trigger fires."""
        if event and event.get("status") in ("failed", "error"):
            raise CloudSQLOperationError(event["message"])
        self.log.info("No administrative operation in progress on instance %s.", self.instance)
