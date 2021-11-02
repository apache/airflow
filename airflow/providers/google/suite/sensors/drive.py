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
"""This module contains Google Drive sensors."""

from typing import Optional, Sequence, Union

from airflow.providers.google.suite.hooks.drive import GoogleDriveHook
from airflow.sensors.base import BaseSensorOperator


class GoogleDriveFileExistenceSensor(BaseSensorOperator):
    """
    Checks for the existence of a file in Google Cloud Storage.

    :param folder_id: The Google drive folder where the file is.
    :type folder_id: str
    :param file_name: The name of the file to check in Google Drive
    :type file_name: str
    :param drive_id: Optional. The id of the shared Google Drive in which the file resides.
    :type drive_id: str
    :param gcp_conn_id: The connection ID to use when
        connecting to Google Cloud Storage.
    :type gcp_conn_id: str
    :param delegate_to: The account to impersonate using domain-wide delegation of authority,
        if any. For this to work, the service account making the request must have
        domain-wide delegation enabled.
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

    template_fields = (
        'folder_id',
        'file_name',
        'drive_id',
        'impersonation_chain',
    )
    ui_color = '#f0eee4'

    def __init__(
        self,
        *,
        folder_id: str,
        file_name: str,
        drive_id: Optional[str] = None,
        gcp_conn_id: str = 'google_cloud_default',
        delegate_to: Optional[str] = None,
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        **kwargs,
    ) -> None:

        super().__init__(**kwargs)
        self.folder_id = folder_id
        self.file_name = file_name
        self.drive_id = drive_id
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.impersonation_chain = impersonation_chain

    def poke(self, context: dict) -> bool:
        self.log.info('Sensor is checking for the file %s in the folder %s', self.file_name, self.folder_id)
        hook = GoogleDriveHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            impersonation_chain=self.impersonation_chain,
        )
        return hook.exists(folder_id=self.folder_id, file_name=self.file_name, drive_id=self.drive_id)
