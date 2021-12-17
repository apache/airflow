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

"""This module contains Google Dataplex sensors."""
from typing import List, Optional, Sequence, Union

from airflow.exceptions import AirflowException
from airflow.providers.google.cloud.hooks.dataplex import DataplexHook
from airflow.sensors.base import BaseSensorOperator


class TaskState:
    """Dataplex Task states"""

    STATE_UNSPECIFIED = "STATE_UNSPECIFIED"
    ACTIVE = "ACTIVE"
    CREATING = "CREATING"
    DELETING = "DELETING"
    ACTION_REQUIRED = "ACTION_REQUIRED"


class DataplexTaskStateSensor(BaseSensorOperator):
    """
    Check the status of the Dataplex task

    :param project_id: Required. The ID of the Google Cloud project that the task belongs to.
    :type project_id: str
    :param region: Required. The ID of the Google Cloud region that the task belongs to.
    :type region: str
    :param lake_id: Required. The ID of the Google Cloud lake that the task belongs to.
    :type lake_id: str
    :param dataplex_task_id: Required. Task identifier.
    :type dataplex_task_id: str
    :param api_version: The version of the api that will be requested for example 'v3'.
    :type api_version: str
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :type gcp_conn_id: str
    :param delegate_to: The account to impersonate, if any. For this to work, the service accountmaking the
        request must have  domain-wide delegation enabled.
    :type delegate_to: str
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :type impersonation_chain: Union[str, Sequence[str]]
    """

    template_fields = ['dataplex_task_id']

    def __init__(
        self,
        project_id: str,
        region: str,
        lake_id: str,
        dataplex_task_id: str,
        api_version: str = "v1",
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: str = None,
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.project_id = project_id
        self.region = region
        self.lake_id = lake_id
        self.dataplex_task_id = dataplex_task_id
        self.api_version = api_version
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.impersonation_chain = impersonation_chain

    def poke(self, context: dict) -> bool:
        self.log.info(f"Waiting for task {self.dataplex_task_id} to be {TaskState.ACTIVE}")
        hook = DataplexHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            api_version=self.api_version,
            impersonation_chain=self.impersonation_chain,
        )

        task = hook.get_task(
            project_id=self.project_id,
            region=self.region,
            lake_id=self.lake_id,
            dataplex_task_id=self.dataplex_task_id,
        )
        task_status = task["state"]

        if task_status == TaskState.DELETING:
            raise AirflowException(f"Task is going to be deleted {self.dataplex_task_id}")

        self.log.info(f"Current status of the Dataplex task {self.dataplex_task_id} => {task_status}")

        return task_status == TaskState.ACTIVE
