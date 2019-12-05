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
from google.cloud.videointelligence_v1 import VideoIntelligenceServiceClient

from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook


class CloudVideoIntelligenceHook(GoogleCloudBaseHook):
    """
    Hook for Google Cloud Video Intelligence APIs.

    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :type gcp_conn_id: str
    :param delegate_to: The account to impersonate, if any.
        For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :type delegate_to: str
    """

    _conn = None

    def __init__(self, gcp_conn_id="google_cloud_default", delegate_to=None):
        super(CloudVideoIntelligenceHook, self).__init__(gcp_conn_id, delegate_to)

    def get_conn(self):
        """
        Returns Gcp Video Intelligence Service client

        :rtype: google.cloud.videointelligence_v1.VideoIntelligenceServiceClient
        """
        if not self._conn:
            self._conn = VideoIntelligenceServiceClient(credentials=self._get_credentials())
        return self._conn

    @GoogleCloudBaseHook.quota_retry()
    def annotate_video(
        self,
        input_uri=None,
        input_content=None,
        features=None,
        video_context=None,
        output_uri=None,
        location=None,
        retry=None,
        timeout=None,
        metadata=None,
    ):
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
