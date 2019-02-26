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
from google.protobuf.json_format import MessageToDict

from airflow.contrib.hooks.gcp_video_intelligence_hook import CloudVideoIntelligenceHook
from airflow.models import BaseOperator
from google.cloud.videointelligence_v1 import enums


class CloudVideoIntelligenceDetectVideoLabelsOperator(BaseOperator):
    """
    Performs viddeo annotation, annotating video labels

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudVideoIntelligenceDetectVideoLabelsOperator`

    :param input_uri: Input video location. Currently, only Google Cloud Storage URIs are supported,
        which must be specified in the following format: gs://bucket-id/object-id
    :type input_uri: str
    :param output_uri: Optional, location where the output (in JSON format) should be stored. Currently, only
        Google Cloud Storage URIs are supported, which must be specified in the following format:
        gs://bucket-id/object-id
    :type output_uri: str
    :param video_context: Optional, Additional video context and/or feature-specific parameters. See more:
        https://googleapis.github.io/google-cloud-python/latest/videointelligence/gapic/v1/types.html#google.cloud.videointelligence_v1.types.VideoContext
    :type video_context: dict or google.cloud.videointelligence_v1.types.VideoContext
    :param location: Optional, cloud region where annotation should take place. Supported cloud regions:
        us-east1, us-west1, europe-west1, asia-east1. If no region is specified, a region will be determined
        based on video file location.
    :type location: str
    :param retry: Retry object used to determine when/if to retry requests.
        If None is specified, requests will not be retried.
    :type retry: google.api_core.retry.Retry
    :param gcp_conn_id: Optional, The connection ID used to connect to Google Cloud
        Platform. Defaults to 'google_cloud_default'.
    :type gcp_conn_id: str
    """

    def __init__(self,
                 input_uri,
                 output_uri=None,
                 video_context=None,
                 location=None,
                 retry=None,
                 gcp_conn_id="google_cloud_default",
                 *args, **kwargs):
        super(CloudVideoIntelligenceDetectVideoLabelsOperator, self).__init__(*args, **kwargs)
        self.input_uri = input_uri
        self.output_uri = output_uri
        self.video_context = video_context
        self.location = location
        self.retry = retry
        self.gcp_conn_id = gcp_conn_id

    def execute(self, context):
        hook = CloudVideoIntelligenceHook(
            gcp_conn_id=self.gcp_conn_id
        )
        operation = hook.annotate_video(
            input_uri=self.input_uri,
            video_context=self.video_context,
            location=self.location,
            retry=self.retry,
            features=[enums.Feature.LABEL_DETECTION]
        )
        self.log.info('Processing video for label annotations')
        result = MessageToDict(operation.result(timeout=60))
        self.log.info('Finished processing.')
        return result


class CloudVideoIntelligenceDetectVideoExplicitContentOperator(BaseOperator):
    def __init__(self):
        pass

    def execute(self, context):
        pass


class CloudVideoIntelligenceDetectVideoShotsOperator(BaseOperator):
    def __init__(self, ):
        pass

    def execute(self, context):
        pass

