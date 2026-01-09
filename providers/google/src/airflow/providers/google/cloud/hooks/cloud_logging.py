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
from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING

from google.cloud.logging_v2.services.config_service_v2 import ConfigServiceV2Client
from google.cloud.logging_v2.types import (
    CreateSinkRequest,
    DeleteSinkRequest,
    GetSinkRequest,
    ListSinksRequest,
    LogSink,
    UpdateSinkRequest,
)

from airflow.providers.google.common.consts import CLIENT_INFO
from airflow.providers.google.common.hooks.base_google import PROVIDE_PROJECT_ID, GoogleBaseHook

if TYPE_CHECKING:
    from google.protobuf.field_mask_pb2 import FieldMask


class CloudLoggingHook(GoogleBaseHook):
    """
    Hook for Google Cloud Logging Log Sinks API.

    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :param impersonation_chain: Optional service account to impersonate.
    """

    def __init__(
        self,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(gcp_conn_id=gcp_conn_id, impersonation_chain=impersonation_chain, **kwargs)
        self._client: ConfigServiceV2Client | None = None

    def get_conn(self) -> ConfigServiceV2Client:
        """Return the Google Cloud Logging Config client."""
        if not self._client:
            self._client = ConfigServiceV2Client(credentials=self.get_credentials(), client_info=CLIENT_INFO)
        return self._client

    def get_parent(self, project_id):
        return f"projects/{project_id}"

    @GoogleBaseHook.fallback_to_default_project_id
    def create_sink(
        self, sink: LogSink | dict, unique_writer_identity: bool = True, project_id: str = PROVIDE_PROJECT_ID
    ) -> LogSink:
        if isinstance(sink, dict):
            sink = LogSink(**sink)
        request = CreateSinkRequest(
            parent=self.get_parent(project_id), sink=sink, unique_writer_identity=unique_writer_identity
        )
        return self.get_conn().create_sink(request=request)

    @GoogleBaseHook.fallback_to_default_project_id
    def get_sink(self, sink_name: str, project_id: str = PROVIDE_PROJECT_ID) -> LogSink:
        request = GetSinkRequest(sink_name=f"projects/{project_id}/sinks/{sink_name}")
        return self.get_conn().get_sink(request=request)

    @GoogleBaseHook.fallback_to_default_project_id
    def list_sinks(self, page_size: int | None = None, project_id: str = PROVIDE_PROJECT_ID) -> list[LogSink]:
        request = ListSinksRequest(parent=self.get_parent(project_id), page_size=page_size)
        return list(self.get_conn().list_sinks(request=request))

    @GoogleBaseHook.fallback_to_default_project_id
    def delete_sink(self, sink_name: str, project_id: str = PROVIDE_PROJECT_ID) -> None:
        request = DeleteSinkRequest(sink_name=f"projects/{project_id}/sinks/{sink_name}")
        self.get_conn().delete_sink(request=request)

    @GoogleBaseHook.fallback_to_default_project_id
    def update_sink(
        self,
        sink_name: str,
        sink: LogSink | dict,
        unique_writer_identity: bool,
        update_mask: FieldMask | dict,
        project_id: str = PROVIDE_PROJECT_ID,
    ) -> LogSink:
        if isinstance(sink, dict):
            sink = LogSink(**sink)
        request = UpdateSinkRequest(
            sink_name=f"projects/{project_id}/sinks/{sink_name}",
            sink=sink,
            unique_writer_identity=unique_writer_identity,
            update_mask=update_mask,
        )
        return self.get_conn().update_sink(request=request)
