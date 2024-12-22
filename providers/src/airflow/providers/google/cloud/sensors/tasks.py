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
"""This module contains a Google Cloud Task sensor."""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING

from airflow.providers.google.cloud.hooks.tasks import CloudTasksHook
from airflow.providers.google.common.hooks.base_google import PROVIDE_PROJECT_ID
from airflow.sensors.base import BaseSensorOperator

if TYPE_CHECKING:
    from airflow.utils.context import Context


class TaskQueueEmptySensor(BaseSensorOperator):
    """
    Pulls tasks count from a cloud task queue; waits for queue to return task count as 0.

    :param project_id: the Google Cloud project ID for the subscription (templated)
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
    :param queue_name: The queue name to for which task empty sensing is required.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = (
        "project_id",
        "location",
        "queue_name",
        "gcp_conn_id",
        "impersonation_chain",
    )

    def __init__(
        self,
        *,
        location: str,
        project_id: str = PROVIDE_PROJECT_ID,
        queue_name: str | None = None,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.location = location
        self.project_id = project_id
        self.queue_name = queue_name
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def poke(self, context: Context) -> bool:
        hook = CloudTasksHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )

        # TODO uncomment page_size once https://issuetracker.google.com/issues/155978649?pli=1 gets fixed
        tasks = hook.list_tasks(
            location=self.location,
            queue_name=self.queue_name,
            # page_size=1
        )

        self.log.info("tasks exhausted in cloud task queue?: %s", (len(tasks) == 0))

        return len(tasks) == 0
