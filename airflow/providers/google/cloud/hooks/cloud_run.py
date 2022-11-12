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
"""This module contains a Google Cloud Run Hook."""
from __future__ import annotations

from typing import Sequence

import google.auth.transport.requests

from airflow.hooks.base import BaseHook
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook


class CloudRunHook(GoogleBaseHook):
    """Hook for Google Cloud Run."""

    def __init__(
        self,
        gcp_conn_id: str = "google_cloud_default",
        cloud_run_conn_id: str = "http_default",
        delegate_to: str | None = None,
        impersonation_chain: str | Sequence[str] | None = None,
    ) -> None:
        super().__init__(
            gcp_conn_id=gcp_conn_id, delegate_to=delegate_to, impersonation_chain=impersonation_chain
        )
        self.cloud_run_conn_id = cloud_run_conn_id

    def get_conn(self) -> dict:
        """
        Retrieves HTTP authentication header allowing
        authenticated Google Cloud Run call.
        :return: Authentication header
        :rtype: dict
        """
        http_connection = BaseHook.get_connection(self.cloud_run_conn_id)
        credentials = self.get_id_token_credentials(target_audience=http_connection.host)
        auth_req = google.auth.transport.requests.Request()
        credentials.refresh(auth_req)

        authentication_header = {
            "Authorization": f"Bearer {credentials.token}",
            "Content-Type": "application/json",
        }
        return authentication_header
