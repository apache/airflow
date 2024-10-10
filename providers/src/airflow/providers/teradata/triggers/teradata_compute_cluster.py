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
from typing import Any, AsyncIterator

from airflow.exceptions import AirflowException
from airflow.providers.common.sql.hooks.sql import fetch_one_handler
from airflow.providers.teradata.hooks.teradata import TeradataHook
from airflow.providers.teradata.utils.constants import Constants
from airflow.triggers.base import BaseTrigger, TriggerEvent


class TeradataComputeClusterSyncTrigger(BaseTrigger):
    """
    Fetch the status of the suspend or resume operation for the specified compute cluster.

    :param teradata_conn_id:  The :ref:`Teradata connection id <howto/connection:teradata>`
        reference to a specific Teradata database.
    :param compute_profile_name:  Name of the Compute Profile to manage.
    :param compute_group_name: Name of compute group to which compute profile belongs.
    :param opr_type: Compute cluster operation - SUSPEND/RESUME
    :param poll_interval: polling period in minutes to check for the status
    """

    def __init__(
        self,
        teradata_conn_id: str,
        compute_profile_name: str,
        compute_group_name: str | None = None,
        operation_type: str | None = None,
        poll_interval: float | None = None,
    ):
        super().__init__()
        self.teradata_conn_id = teradata_conn_id
        self.compute_profile_name = compute_profile_name
        self.compute_group_name = compute_group_name
        self.operation_type = operation_type
        self.poll_interval = poll_interval

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serialize TeradataComputeClusterSyncTrigger arguments and classpath."""
        return (
            "airflow.providers.teradata.triggers.teradata_compute_cluster.TeradataComputeClusterSyncTrigger",
            {
                "teradata_conn_id": self.teradata_conn_id,
                "compute_profile_name": self.compute_profile_name,
                "compute_group_name": self.compute_group_name,
                "operation_type": self.operation_type,
                "poll_interval": self.poll_interval,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """Wait for Compute Cluster operation to complete."""
        try:
            while True:
                status = await self.get_status()
                if status is None or len(status) == 0:
                    self.log.info(Constants.CC_GRP_PRP_NON_EXISTS_MSG)
                    raise AirflowException(Constants.CC_GRP_PRP_NON_EXISTS_MSG)
                if (
                    self.operation_type == Constants.CC_SUSPEND_OPR
                    or self.operation_type == Constants.CC_CREATE_SUSPEND_OPR
                ):
                    if status == Constants.CC_SUSPEND_DB_STATUS:
                        break
                elif (
                    self.operation_type == Constants.CC_RESUME_OPR
                    or self.operation_type == Constants.CC_CREATE_OPR
                ):
                    if status == Constants.CC_RESUME_DB_STATUS:
                        break
                if self.poll_interval is not None:
                    self.poll_interval = float(self.poll_interval)
                else:
                    self.poll_interval = float(Constants.CC_POLL_INTERVAL)
                await asyncio.sleep(self.poll_interval)
            if (
                self.operation_type == Constants.CC_SUSPEND_OPR
                or self.operation_type == Constants.CC_CREATE_SUSPEND_OPR
            ):
                if status == Constants.CC_SUSPEND_DB_STATUS:
                    yield TriggerEvent(
                        {
                            "status": "success",
                            "message": Constants.CC_OPR_SUCCESS_STATUS_MSG
                            % (self.compute_profile_name, self.operation_type),
                        }
                    )
                else:
                    yield TriggerEvent(
                        {
                            "status": "error",
                            "message": Constants.CC_OPR_FAILURE_STATUS_MSG
                            % (self.compute_profile_name, self.operation_type),
                        }
                    )
            elif (
                self.operation_type == Constants.CC_RESUME_OPR
                or self.operation_type == Constants.CC_CREATE_OPR
            ):
                if status == Constants.CC_RESUME_DB_STATUS:
                    yield TriggerEvent(
                        {
                            "status": "success",
                            "message": Constants.CC_OPR_SUCCESS_STATUS_MSG
                            % (self.compute_profile_name, self.operation_type),
                        }
                    )
                else:
                    yield TriggerEvent(
                        {
                            "status": "error",
                            "message": Constants.CC_OPR_FAILURE_STATUS_MSG
                            % (self.compute_profile_name, self.operation_type),
                        }
                    )
            else:
                yield TriggerEvent({"status": "error", "message": "Invalid operation"})
        except Exception as e:
            yield TriggerEvent({"status": "error", "message": str(e)})
        except asyncio.CancelledError:
            self.log.error(Constants.CC_OPR_TIMEOUT_ERROR, self.operation_type)

    async def get_status(self) -> str:
        """Return compute cluster SUSPEND/RESUME operation status."""
        sql = (
            "SEL ComputeProfileState FROM DBC.ComputeProfilesVX WHERE UPPER(ComputeProfileName) = UPPER('"
            + self.compute_profile_name
            + "')"
        )
        if self.compute_group_name:
            sql += " AND UPPER(ComputeGroupName) = UPPER('" + self.compute_group_name + "')"
        hook = TeradataHook(teradata_conn_id=self.teradata_conn_id)
        result_set = hook.run(sql, handler=fetch_one_handler)
        status = ""
        if isinstance(result_set, list) and isinstance(result_set[0], str):
            status = str(result_set[0])
        return status
