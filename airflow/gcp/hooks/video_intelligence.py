# -*- coding: utf-8 -*-
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
"""
This module contains a Google Cloud Video Intelligence Hook.
"""
from typing import Dict, List, Optional, Sequence, Tuple, Union

from google.api_core.operation import Operation
from google.api_core.retry import Retry
from google.cloud.videointelligence_v1 import VideoIntelligenceServiceClient
from google.cloud.videointelligence_v1.types import VideoContext

from airflow.gcp.hooks.base import CloudBaseHook


class CloudVideoIntelligenceHook(CloudBaseHook):
    """
    Hook for Google Cloud Video Intelligence APIs.

    All the methods in the hook where project_id is used must be called with
    keyword arguments rather than positional.

    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :type gcp_conn_id: str
    :param delegate_to: The account to impersonate, if any.
        For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :type delegate_to: str
    """

    def __init__(self, gcp_conn_id: str = "google_cloud_default", delegate_to: Optional[str] = None) -> None:
        super().__init__(gcp_conn_id, delegate_to)
        self._conn = None

    def get_conn(self) -> VideoIntelligenceServiceClient:
        """
        Returns Gcp Video Intelligence Service client

        :rtype: google.cloud.videointelligence_v1.VideoIntelligenceServiceClient
        """
        if not self._conn:
            self._conn = VideoIntelligenceServiceClient(
                credentials=self._get_credentials(),
                client_info=self.client_info
            )
        return self._conn

    @CloudBaseHook.quota_retry()
    def annotate_video(
        self,
        input_uri: Optional[str] = None,
        input_content: Optional[bytes] = None,
        features: Optional[List[VideoIntelligenceServiceClient.enums.Feature]] = None,
        video_context: Union[Dict, VideoContext] = None,
        output_uri: Optional[str] = None,
        location: Optional[str] = None,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
    ) -> Operation:
        """
        Performs video annotation.

        :param input_uri: Input video location. Currently, only Google Cloud Storage URIs are supported,
            which must be specified in the following format: ``gs://bucket-id/object-id``.
        :type input_uri: str
        :param input_content: The video data bytes.
            If unset, the input video(s) should be specified via ``input_uri``.
            If set, ``input_uri`` should be unset.
        :type input_content: bytes
        :param features: Requested video annotation features.
        :type features: list[google.cloud.videointelligence_v1.VideoIntelligenceServiceClient.enums.Feature]
        :param output_uri: Optional, location where the output (in JSON format) should be stored. Currently,
            only Google Cloud Storage URIs are supported, which must be specified in the following format:
            ``gs://bucket-id/object-id``.
        :type output_uri: str
        :param video_context: Optional, Additional video context and/or feature-specific parameters.
        :type video_context: dict or google.cloud.videointelligence_v1.types.VideoContext
        :param location: Optional, cloud region where annotation should take place. Supported cloud regions:
            us-east1, us-west1, europe-west1, asia-east1.
            If no region is specified, a region will be determined based on video file location.
        :type location: str
        :param retry: Retry object used to determine when/if to retry requests.
            If None is specified, requests will not be retried.
        :type retry: google.api_core.retry.Retry
        :param timeout: Optional, The amount of time, in seconds, to wait for the request to complete.
            Note that if retry is specified, the timeout applies to each individual attempt.
        :type timeout: float
        :param metadata: Optional, Additional metadata that is provided to the method.
        :type metadata: seq[tuple[str, str]]
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
