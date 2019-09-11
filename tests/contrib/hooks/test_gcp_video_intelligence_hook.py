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
#
import unittest

from airflow.contrib.hooks.gcp_video_intelligence_hook import CloudVideoIntelligenceHook
from google.cloud.videointelligence_v1 import enums

from tests.contrib.utils.base_gcp_mock import mock_base_gcp_hook_default_project_id
from tests.compat import mock


INPUT_URI = "gs://bucket-name/input-file"
OUTPUT_URI = "gs://bucket-name/output-file"

FEATURES = [enums.Feature.LABEL_DETECTION]

ANNOTATE_VIDEO_RESPONSE = {'test': 'test'}


class CloudVideoIntelligenceHookTestCase(unittest.TestCase):
    def setUp(self):
        with mock.patch(
            "airflow.contrib.hooks.gcp_video_intelligence_hook.CloudVideoIntelligenceHook.__init__",
            new=mock_base_gcp_hook_default_project_id,
        ):
            self.hook = CloudVideoIntelligenceHook(gcp_conn_id="test")

    @mock.patch("airflow.contrib.hooks.gcp_video_intelligence_hook.CloudVideoIntelligenceHook.get_conn")
    def test_annotate_video(self, get_conn):
        # Given
        annotate_video_method = get_conn.return_value.annotate_video
        get_conn.return_value.annotate_video.return_value = ANNOTATE_VIDEO_RESPONSE

        # When
        result = self.hook.annotate_video(input_uri=INPUT_URI, features=FEATURES)

        # Then
        self.assertIs(result, ANNOTATE_VIDEO_RESPONSE)
        annotate_video_method.assert_called_once_with(
            input_uri=INPUT_URI,
            input_content=None,
            features=FEATURES,
            video_context=None,
            output_uri=None,
            location_id=None,
            retry=None,
            timeout=None,
            metadata=None,
        )

    @mock.patch("airflow.contrib.hooks.gcp_video_intelligence_hook.CloudVideoIntelligenceHook.get_conn")
    def test_annotate_video_with_output_uri(self, get_conn):
        # Given
        annotate_video_method = get_conn.return_value.annotate_video
        get_conn.return_value.annotate_video.return_value = ANNOTATE_VIDEO_RESPONSE

        # When
        result = self.hook.annotate_video(input_uri=INPUT_URI, output_uri=OUTPUT_URI, features=FEATURES)

        # Then
        self.assertIs(result, ANNOTATE_VIDEO_RESPONSE)
        annotate_video_method.assert_called_once_with(
            input_uri=INPUT_URI,
            output_uri=OUTPUT_URI,
            input_content=None,
            features=FEATURES,
            video_context=None,
            location_id=None,
            retry=None,
            timeout=None,
            metadata=None,
        )
