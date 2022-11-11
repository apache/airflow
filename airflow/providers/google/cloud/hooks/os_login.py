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
"""
.. spelling::

    ImportSshPublicKeyResponse
    oslogin
"""
from __future__ import annotations

from typing import Optional, Sequence

from google.api_core.gapic_v1.method import DEFAULT, _MethodDefault
from google.api_core.retry import Retry
from google.cloud.oslogin_v1 import ImportSshPublicKeyResponse, OsLoginServiceClient

from airflow.providers.google.common.consts import CLIENT_INFO
from airflow.providers.google.common.hooks.base_google import PROVIDE_PROJECT_ID, GoogleBaseHook


class OSLoginHook(GoogleBaseHook):
    """
    Hook for Google OS login APIs.

    All the methods in the hook where project_id is used must be called with
    keyword arguments rather than positional.
    """

    def __init__(
        self,
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: str | None = None,
        impersonation_chain: str | Sequence[str] | None = None,
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

        self._conn = OsLoginServiceClient(credentials=self.get_credentials(), client_info=CLIENT_INFO)
        return self._conn

    @GoogleBaseHook.fallback_to_default_project_id
    def import_ssh_public_key(
        self,
        user: str,
        ssh_public_key: dict,
        project_id: str = PROVIDE_PROJECT_ID,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> ImportSshPublicKeyResponse:
        """
        Adds an SSH public key and returns the profile information. Default POSIX
        account information is set when no username and UID exist as part of the
        login profile.

        :param user: The unique ID for the user
        :param ssh_public_key: The SSH public key and expiration time.
        :param project_id: The project ID of the Google Cloud project.
        :param retry: A retry object used to retry requests. If ``None`` is specified, requests will
            be retried using a default configuration.
        :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that
            if ``retry`` is specified, the timeout applies to each individual attempt.
        :param metadata: Additional metadata that is provided to the method.
        :return: A :class:`~google.cloud.oslogin_v1.ImportSshPublicKeyResponse` instance.
        """
        conn = self.get_conn()
        return conn.import_ssh_public_key(
            request=dict(
                parent=f"users/{user}",
                ssh_public_key=ssh_public_key,
                project_id=project_id,
            ),
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
