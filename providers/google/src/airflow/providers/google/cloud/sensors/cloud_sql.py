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
from typing import TYPE_CHECKING, Any

from airflow.providers.common.compat.sdk import AirflowException, BaseSensorOperator, conf
from airflow.providers.google.cloud.hooks.cloud_sql import CloudSqlOperationStatus, CloudSQLHook
from airflow.providers.google.cloud.links.cloud_sql import CloudSQLInstanceLink
from airflow.providers.google.cloud.triggers.cloud_sql import CloudSQLInstanceOperationsTrigger
from airflow.providers.google.common.hooks.base_google import PROVIDE_PROJECT_ID

if TYPE_CHECKING:
    from airflow.providers.common.compat.sdk import Context


class CloudSQLInstanceOperationsSensor(BaseSensorOperator):
    """
    Waits until no Cloud SQL administrative operation is running on an instance.

    Cloud SQL allows only one administrative operation at a time per instance (import,
    export, clone, patch, etc.). When multiple operations are triggered concurrently,
    the API returns HTTP 409 with reason ``operationInProgress``. This sensor polls the
    Operations API and returns when no PENDING or RUNNING operations remain, allowing
    the next admin operation to proceed.

    Use this sensor upstream of import/export/clone tasks to serialize access to a
    shared instance across DAGs or dynamically mapped tasks.

    .. seealso::
        :ref:`howto/operator:CloudSQLImportInstanceOperator`
        :ref:`howto/operator:CloudSQLExportInstanceOperator`
        https://cloud.google.com/sql/docs/mysql/admin-api/rest/v1/operations/list

    :param instance: Cloud SQL instance ID. This does not include the project ID.
    :param project_id: Optional, Google Cloud Project ID. If set to None or missing,
        the default project_id from the Google Cloud connection is used.
    :param gcp_conn_id: The connection ID used to connect to Google Cloud.
    :param impersonation_chain: Optional service account to impersonate.
    :param deferrable: Run sensor in deferrable mode (uses triggerer, no worker slot).
    """

    template_fields: Sequence[str] = ("instance", "project_id", "impersonation_chain")
    operator_extra_links = (CloudSQLInstanceLink(),)

    def __init__(
        self,
        *,
        instance: str,
        project_id: str = PROVIDE_PROJECT_ID,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.instance = instance
        self.project_id = project_id
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain
        self.deferrable = deferrable

    def poke(self, context: Context) -> bool:
        hook = CloudSQLHook(
            gcp_conn_id=self.gcp_conn_id,
            api_version="v1beta4",
            impersonation_chain=self.impersonation_chain,
        )
        project_id = self.project_id or hook.project_id
        operations = hook.list_instance_operations(
            instance=self.instance,
            project_id=project_id,
        )
        active = [
            op
            for op in operations
            if op.get("status") in (CloudSqlOperationStatus.PENDING, CloudSqlOperationStatus.RUNNING)
        ]
        if not active:
            if project_id:
                CloudSQLInstanceLink.persist(context=context, project_id=project_id, instance=self.instance)
            return True
        self.log.info(
            "Instance %s has %d operation(s) in progress: %s",
            self.instance,
            len(active),
            [op.get("name") for op in active],
        )
        return False

    def execute(self, context: Context) -> None:
        if not self.deferrable:
            super().execute(context)
        elif not self.poke(context=context):
            hook = CloudSQLHook(
                gcp_conn_id=self.gcp_conn_id,
                api_version="v1beta4",
                impersonation_chain=self.impersonation_chain,
            )
            self.defer(
                timeout=self.execution_timeout,
                trigger=CloudSQLInstanceOperationsTrigger(
                    instance=self.instance,
                    project_id=self.project_id or hook.project_id,
                    gcp_conn_id=self.gcp_conn_id,
                    impersonation_chain=self.impersonation_chain,
                    poke_interval=self.poke_interval,
                ),
                method_name="execute_complete",
            )

    def execute_complete(self, context: Context, event: dict[str, Any]) -> None:
        if event.get("status") == "failed":
            raise AirflowException(event["message"])
        self.log.info("Instance %s has no operations in progress. Proceeding.", self.instance)
