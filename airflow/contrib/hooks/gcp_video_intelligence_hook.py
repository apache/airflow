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
    """

    _conn = None

    def __init__(self,
                 gcp_conn_id='google_cloud_default',
                 delegate_to=None):
        super(CloudVideoIntelligenceHook, self).__init__(gcp_conn_id, delegate_to)

    def get_conn(self):
        """

        :rtype: google.cloud.videointelligence_v1.VideoIntelligenceServiceClient
        """
        if not self._conn:
            self._conn = VideoIntelligenceServiceClient(credentials=self._get_credentials())
        return self._conn

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
