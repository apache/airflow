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
This module contains Google Cloud Vision operators.
"""

from google.protobuf.json_format import MessageToDict
from google.cloud.videointelligence_v1 import enums

from airflow.contrib.hooks.gcp_video_intelligence_hook import CloudVideoIntelligenceHook
from airflow.models import BaseOperator


class CloudVideoIntelligenceDetectVideoLabelsOperator(BaseOperator):
    """
    Performs video annotation, annotating video labels.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudVideoIntelligenceDetectVideoLabelsOperator`.

    :param input_uri: Input video location. Currently, only Google Cloud Storage URIs are supported,
        which must be specified in the following format: ``gs://bucket-id/object-id``.
    :type input_uri: str
    :param input_content: The video data bytes.
        If unset, the input video(s) should be specified via ``input_uri``.
        If set, ``input_uri`` should be unset.
    :type input_content: bytes
    :param output_uri: Optional, location where the output (in JSON format) should be stored. Currently, only
        Google Cloud Storage URIs are supported, which must be specified in the following format:
        ``gs://bucket-id/object-id``.
    :type output_uri: str
    :param video_context: Optional, Additional video context and/or feature-specific parameters.
    :type video_context: dict or google.cloud.videointelligence_v1.types.VideoContext
    :param location: Optional, cloud region where annotation should take place. Supported cloud regions:
        us-east1, us-west1, europe-west1, asia-east1. If no region is specified, a region will be determined
        based on video file location.
    :type location: str
    :param retry: Retry object used to determine when/if to retry requests.
        If None is specified, requests will not be retried.
    :type retry: google.api_core.retry.Retry
    :param gcp_conn_id: Optional, The connection ID used to connect to Google Cloud
        Platform. Defaults to ``google_cloud_default``.
    :type gcp_conn_id: str
    """
    # [START gcp_video_intelligence_detect_labels_template_fields]
    template_fields = ("input_uri", "output_uri", "gcp_conn_id")
    # [END gcp_video_intelligence_detect_labels_template_fields]

    def __init__(
        self,
        input_uri,
        input_content=None,
        output_uri=None,
        video_context=None,
        location=None,
        retry=None,
        gcp_conn_id="google_cloud_default",
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.input_uri = input_uri
        self.input_content = input_content
        self.output_uri = output_uri
        self.video_context = video_context
        self.location = location
        self.retry = retry
        self.gcp_conn_id = gcp_conn_id

    def execute(self, context):
        hook = CloudVideoIntelligenceHook(gcp_conn_id=self.gcp_conn_id)
        operation = hook.annotate_video(
            input_uri=self.input_uri,
            input_content=self.input_content,
            video_context=self.video_context,
            location=self.location,
            retry=self.retry,
            features=[enums.Feature.LABEL_DETECTION],
        )
        self.log.info("Processing video for label annotations")
        result = MessageToDict(operation.result())
        self.log.info("Finished processing.")
        return result


class CloudVideoIntelligenceDetectVideoExplicitContentOperator(BaseOperator):
    """
    Performs video annotation, annotating explicit content.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudVideoIntelligenceDetectVideoExplicitContentOperator`

    :param input_uri: Input video location. Currently, only Google Cloud Storage URIs are supported,
        which must be specified in the following format: ``gs://bucket-id/object-id``.
    :type input_uri: str
    :param input_content: The video data bytes.
        If unset, the input video(s) should be specified via ``input_uri``.
        If set, ``input_uri`` should be unset.
    :type input_content: bytes
    :param output_uri: Optional, location where the output (in JSON format) should be stored. Currently, only
        Google Cloud Storage URIs are supported, which must be specified in the following format:
        ``gs://bucket-id/object-id``.
    :type output_uri: str
    :param video_context: Optional, Additional video context and/or feature-specific parameters.
    :type video_context: dict or google.cloud.videointelligence_v1.types.VideoContext
    :param location: Optional, cloud region where annotation should take place. Supported cloud regions:
        us-east1, us-west1, europe-west1, asia-east1. If no region is specified, a region will be determined
        based on video file location.
    :type location: str
    :param retry: Retry object used to determine when/if to retry requests.
        If None is specified, requests will not be retried.
    :type retry: google.api_core.retry.Retry
    :param gcp_conn_id: Optional, The connection ID used to connect to Google Cloud
        Platform. Defaults to ``google_cloud_default``.
    :type gcp_conn_id: str
    """
    # [START gcp_video_intelligence_detect_explicit_content_template_fields]
    template_fields = ("input_uri", "output_uri", "gcp_conn_id")
    # [END gcp_video_intelligence_detect_explicit_content_template_fields]

    def __init__(
        self,
        input_uri,
        output_uri=None,
        input_content=None,
        video_context=None,
        location=None,
        retry=None,
        gcp_conn_id="google_cloud_default",
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.input_uri = input_uri
        self.output_uri = output_uri
        self.input_content = input_content
        self.video_context = video_context
        self.location = location
        self.retry = retry
        self.gcp_conn_id = gcp_conn_id

    def execute(self, context):
        hook = CloudVideoIntelligenceHook(gcp_conn_id=self.gcp_conn_id)
        operation = hook.annotate_video(
            input_uri=self.input_uri,
            input_content=self.input_content,
            video_context=self.video_context,
            location=self.location,
            retry=self.retry,
            features=[enums.Feature.EXPLICIT_CONTENT_DETECTION],
        )
        self.log.info("Processing video for explicit content annotations")
        result = MessageToDict(operation.result())
        self.log.info("Finished processing.")
        return result


class CloudVideoIntelligenceDetectVideoShotsOperator(BaseOperator):
    """
    Performs video annotation, annotating video shots.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudVideoIntelligenceDetectVideoShotsOperator`

    :param input_uri: Input video location. Currently, only Google Cloud Storage URIs are supported,
        which must be specified in the following format: ``gs://bucket-id/object-id``.
    :type input_uri: str
    :param input_content: The video data bytes.
        If unset, the input video(s) should be specified via ``input_uri``.
        If set, ``input_uri`` should be unset.
    :type input_content: bytes
    :param output_uri: Optional, location where the output (in JSON format) should be stored. Currently, only
        Google Cloud Storage URIs are supported, which must be specified in the following format:
        ``gs://bucket-id/object-id``.
    :type output_uri: str
    :param video_context: Optional, Additional video context and/or feature-specific parameters.
    :type video_context: dict or google.cloud.videointelligence_v1.types.VideoContext
    :param location: Optional, cloud region where annotation should take place. Supported cloud regions:
        us-east1, us-west1, europe-west1, asia-east1. If no region is specified, a region will be determined
        based on video file location.
    :type location: str
    :param retry: Retry object used to determine when/if to retry requests.
        If None is specified, requests will not be retried.
    :type retry: google.api_core.retry.Retry
    :param gcp_conn_id: Optional, The connection ID used to connect to Google Cloud
        Platform. Defaults to ``google_cloud_default``.
    :type gcp_conn_id: str
    """
    # [START gcp_video_intelligence_detect_video_shots_template_fields]
    template_fields = ("input_uri", "output_uri", "gcp_conn_id")
    # [END gcp_video_intelligence_detect_video_shots_template_fields]

    def __init__(
        self,
        input_uri,
        output_uri=None,
        input_content=None,
        video_context=None,
        location=None,
        retry=None,
        gcp_conn_id="google_cloud_default",
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.input_uri = input_uri
        self.output_uri = output_uri
        self.input_content = input_content
        self.video_context = video_context
        self.location = location
        self.retry = retry
        self.gcp_conn_id = gcp_conn_id

    def execute(self, context):
        hook = CloudVideoIntelligenceHook(gcp_conn_id=self.gcp_conn_id)
        operation = hook.annotate_video(
            input_uri=self.input_uri,
            input_content=self.input_content,
            video_context=self.video_context,
            location=self.location,
            retry=self.retry,
            features=[enums.Feature.SHOT_CHANGE_DETECTION],
        )
        self.log.info("Processing video for video shots annotations")
        result = MessageToDict(operation.result())
        self.log.info("Finished processing.")
        return result
