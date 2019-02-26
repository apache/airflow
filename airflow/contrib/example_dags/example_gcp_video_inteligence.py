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
Example Airflow DAG that demostrated operators for the Google Cloud Video Intelligence service in the Google
Cloud Platform.

This DAG relies on the following OS environment variables

* GCP_BUCKET_NAME - Google Cloud Storage bucket where the file exists.
"""
import os

# [START howto_operator_vision_retry_import]
from google.api_core.retry import Retry
# [END howto_operator_vision_retry_import]

import airflow
from airflow import models
from airflow.contrib.operators.gcp_vision_intelligence_operator import \
    CloudVideoIntelligenceDetectVideoLabelsOperator
from airflow.operators.bash_operator import BashOperator

default_args = {'start_date': airflow.utils.dates.days_ago(1)}

# [START howto_operator_vision_args_common]
GCP_BUCKET_NAME = os.environ.get('GCP_VIDEO_INTELLIGENCE_BUCKET_NAME', 'test-bucket-name')
# [END howto_operator_vision_args_common]


with models.DAG(
    'example_gcp_video_intelligence', default_args=default_args, schedule_interval=None  # Override to match your needs
) as dag:

    # [START howto_operator_vision_annotate_image]
    detect_video_label = CloudVideoIntelligenceDetectVideoLabelsOperator(
        input_uri="gs://{}/video.mp4".format(GCP_BUCKET_NAME),
        output_uri=None,
        video_context=None,
        retry=Retry(maximum=10.0),
        timeout=5,
        task_id='detect_video_label'
    )
    # [END howto_operator_vision_annotate_image]

    # [START howto_operator_vision_annotate_image_result]
    detect_video_label_result = BashOperator(
        bash_command="echo {{ task_instance.xcom_pull('detect_video_label')['annotationResults'][0]['shotLabelAnnotations'][0]['entity']['description'] }}",
        task_id='detect_video_label_result',
    )
    # [END howto_operator_vision_annotate_image_result]

    detect_video_label >> detect_video_label_result
