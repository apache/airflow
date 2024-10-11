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
"""This module contains a Google Cloud Video Intelligence Hook."""

from __future__ import annotations

from typing import TYPE_CHECKING, Sequence

from google.api_core.gapic_v1.method import DEFAULT, _MethodDefault
from google.cloud.videointelligence_v1 import (
    Feature,
    VideoContext,
    VideoIntelligenceServiceClient,
)

from airflow.providers.google.common.consts import CLIENT_INFO
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook

if TYPE_CHECKING:
    from google.api_core.operation import Operation
    from google.api_core.retry import Retry


class CloudVideoIntelligenceHook(GoogleBaseHook):
    """
    Hook for Google Cloud Video Intelligence APIs.

    All the methods in the hook where project_id is used must be called with
    keyword arguments rather than positional.

    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account.
    """

    def __init__(
        self,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        if kwargs.get("delegate_to") is not None:
            raise RuntimeError(
                "The `delegate_to` parameter has been deprecated before and finally removed in this version"
                " of Google Provider. You MUST convert it to `impersonate_chain`"
            )
        super().__init__(
            gcp_conn_id=gcp_conn_id,
            impersonation_chain=impersonation_chain,
        )
        self._conn: VideoIntelligenceServiceClient | None = None

    def get_conn(self) -> VideoIntelligenceServiceClient:
        """Return Gcp Video Intelligence Service client."""
        if not self._conn:
            self._conn = VideoIntelligenceServiceClient(
                credentials=self.get_credentials(), client_info=CLIENT_INFO
            )
        return self._conn

    @GoogleBaseHook.quota_retry()
    def annotate_video(
        self,
        input_uri: str | None = None,
        input_content: bytes | None = None,
        features: Sequence[Feature] | None = None,
        video_context: dict | VideoContext | None = None,
        output_uri: str | None = None,
        location: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> Operation:
        """
        Perform video annotation.

        :param input_uri: Input video location. Currently, only Google Cloud Storage URIs are supported,
            which must be specified in the following format: ``gs://bucket-id/object-id``.
        :param input_content: The video data bytes.
            If unset, the input video(s) should be specified via ``input_uri``.
            If set, ``input_uri`` should be unset.
        :param features: Requested video annotation features.
        :param output_uri: Optional, location where the output (in JSON format) should be stored. Currently,
            only Google Cloud Storage URIs are supported, which must be specified in the following format:
            ``gs://bucket-id/object-id``.
        :param video_context: Optional, Additional video context and/or feature-specific parameters.
        :param location: Optional, cloud region where annotation should take place. Supported cloud regions:
            us-east1, us-west1, europe-west1, asia-east1.
            If no region is specified, a region will be determined based on video file location.
        :param retry: Retry object used to determine when/if to retry requests.
            If None is specified, requests will not be retried.
        :param timeout: Optional, The amount of time, in seconds, to wait for the request to complete.
            Note that if retry is specified, the timeout applies to each individual attempt.
        :param metadata: Optional, Additional metadata that is provided to the method.
        """
        client = self.get_conn()

        return client.annotate_video(
            request={
                "input_uri": input_uri,
                "features": features,
                "input_content": input_content,
                "video_context": video_context,
                "output_uri": output_uri,
                "location_id": location,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
