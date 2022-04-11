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
from typing import Dict, List, Optional, Sequence, Tuple, Union

from google.api_core.gapic_v1.method import DEFAULT, _MethodDefault
from google.api_core.operation import Operation
from google.api_core.retry import Retry
from google.cloud.videointelligence_v1 import VideoIntelligenceServiceClient
from google.cloud.videointelligence_v1.types import VideoContext

from airflow.providers.google.common.consts import CLIENT_INFO
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook


class CloudVideoIntelligenceHook(GoogleBaseHook):
    """
    Hook for Google Cloud Video Intelligence APIs.

    All the methods in the hook where project_id is used must be called with
    keyword arguments rather than positional.

    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :param delegate_to: The account to impersonate using domain-wide delegation of authority,
        if any. For this to work, the service account making the request must have
        domain-wide delegation enabled.
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
        delegate_to: Optional[str] = None,
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
    ) -> None:
        super().__init__(
            gcp_conn_id=gcp_conn_id,
            delegate_to=delegate_to,
            impersonation_chain=impersonation_chain,
        )
        self._conn = None

    def get_conn(self) -> VideoIntelligenceServiceClient:
        """
        Returns Gcp Video Intelligence Service client

        :rtype: google.cloud.videointelligence_v1.VideoIntelligenceServiceClient
        """
        if not self._conn:
            self._conn = VideoIntelligenceServiceClient(
                credentials=self._get_credentials(), client_info=CLIENT_INFO
            )
        return self._conn

    @GoogleBaseHook.quota_retry()
    def annotate_video(
        self,
        input_uri: Optional[str] = None,
        input_content: Optional[bytes] = None,
        features: Optional[List[VideoIntelligenceServiceClient.enums.Feature]] = None,
        video_context: Union[Dict, VideoContext] = None,
        output_uri: Optional[str] = None,
        location: Optional[str] = None,
        retry: Union[Retry, _MethodDefault] = DEFAULT,
        timeout: Optional[float] = None,
        metadata: Sequence[Tuple[str, str]] = (),
    ) -> Operation:
        """
        Performs video annotation.

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
            input_uri=input_uri,
            input_content=input_content,
            features=features,
            video_context=video_context,
            output_uri=output_uri,
            location_id=location,
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
