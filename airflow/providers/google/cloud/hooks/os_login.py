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

from typing import Dict, Optional, Sequence, Union

from google.cloud.oslogin_v1 import OsLoginServiceClient

from airflow.providers.google.common.hooks.base_google import GoogleBaseHook


class OSLoginHook(GoogleBaseHook):
    """
    Hook for Google OS login APIs.

    All the methods in the hook where project_id is used must be called with
    keyword arguments rather than positional.
    """

    def __init__(
        self,
        gcp_conn_id: str = 'google_cloud_default',
        delegate_to: Optional[str] = None,
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
    ) -> None:
        super().__init__(
            gcp_conn_id=gcp_conn_id,
            delegate_to=delegate_to,
            impersonation_chain=impersonation_chain,
        )
        self._conn = None  # type: Optional[OsLoginServiceClient]

    def get_conn(self) -> OsLoginServiceClient:
        """Return OS Login service client"""
        if self._conn:
            return self._conn

        self._conn = OsLoginServiceClient(credentials=self._get_credentials(), client_info=self.client_info)
        return self._conn

    @GoogleBaseHook.fallback_to_default_project_id
    def import_ssh_public_key(
        self, user: str, ssh_public_key: Dict, project_id: str, retry=None, timeout=None, metadata=None
    ):
        """
        Adds an SSH public key and returns the profile information. Default POSIX
        account information is set when no username and UID exist as part of the
        login profile.

        :param user: The unique ID for the user
        :type user: str
        :param ssh_public_key: The SSH public key and expiration time.
        :type ssh_public_key: dict
        :param project_id: The project ID of the Google Cloud project.
        :type project_id: str
        :param retry: A retry object used to retry requests. If ``None`` is specified, requests will
            be retried using a default configuration.
        :type retry: Optional[google.api_core.retry.Retry]
        :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that
            if ``retry`` is specified, the timeout applies to each individual attempt.
        :type timeout: Optional[float]
        :param metadata: Additional metadata that is provided to the method.
        :type metadata: Optional[Sequence[Tuple[str, str]]]
        :return:  A :class:`~google.cloud.oslogin_v1.types.ImportSshPublicKeyResponse` instance.
        """
        conn = self.get_conn()
        return conn.import_ssh_public_key(
            parent=OsLoginServiceClient.user_path(user=user),
            ssh_public_key=ssh_public_key,
            project_id=project_id,
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
