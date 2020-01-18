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
Example Airflow DAG that runs speech synthesizing and stores output in Google Cloud Storage

This DAG relies on the following OS environment variables
* GCP_PROJECT_ID - Google Cloud Platform project for the Cloud SQL instance.
* GCP_SPEECH_TEST_BUCKET - Name of the bucket in which the output file should be stored.
"""

import os

from airflow import models
from airflow.gcp.operators.speech_to_text import CloudSpeechToTextRecognizeSpeechOperator
from airflow.gcp.operators.text_to_speech import CloudTextToSpeechSynthesizeOperator
from airflow.gcp.operators.translate_speech import GcpTranslateSpeechOperator
from airflow.utils import dates

# [START howto_operator_text_to_speech_env_variables]
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "example-project")
BUCKET_NAME = os.environ.get("GCP_SPEECH_TEST_BUCKET", "gcp-speech-test-bucket")
# [END howto_operator_text_to_speech_env_variables]

# [START howto_operator_text_to_speech_gcp_filename]
FILENAME = "gcp-speech-test-file"
# [END howto_operator_text_to_speech_gcp_filename]

# [START howto_operator_text_to_speech_api_arguments]
INPUT = {"text": "Sample text for demo purposes"}
VOICE = {"language_code": "en-US", "ssml_gender": "FEMALE"}
AUDIO_CONFIG = {"audio_encoding": "LINEAR16"}
# [END howto_operator_text_to_speech_api_arguments]

# [START howto_operator_speech_to_text_api_arguments]
CONFIG = {"encoding": "LINEAR16", "language_code": "en_US"}
AUDIO = {"uri": "gs://{bucket}/{object}".format(bucket=BUCKET_NAME, object=FILENAME)}
# [END howto_operator_speech_to_text_api_arguments]

# [START howto_operator_translate_speech_arguments]
TARGET_LANGUAGE = 'pl'
FORMAT = 'text'
MODEL = 'base'
SOURCE_LANGUAGE = None  # type: None
# [END howto_operator_translate_speech_arguments]

default_args = {"start_date": dates.days_ago(1)}

with models.DAG(
    "example_gcp_speech",
    default_args=default_args,
    schedule_interval=None,  # Override to match your needs
    tags=['example'],
) as dag:

    # [START howto_operator_text_to_speech_synthesize]
    text_to_speech_synthesize_task = CloudTextToSpeechSynthesizeOperator(
        project_id=GCP_PROJECT_ID,
        input_data=INPUT,
        voice=VOICE,
        audio_config=AUDIO_CONFIG,
        target_bucket_name=BUCKET_NAME,
        target_filename=FILENAME,
        task_id="text_to_speech_synthesize_task",
    )
    # [END howto_operator_text_to_speech_synthesize]

    # [START howto_operator_speech_to_text_recognize]
    speech_to_text_recognize_task = CloudSpeechToTextRecognizeSpeechOperator(
        project_id=GCP_PROJECT_ID, config=CONFIG, audio=AUDIO, task_id="speech_to_text_recognize_task"
    )
    # [END howto_operator_speech_to_text_recognize]

    text_to_speech_synthesize_task >> speech_to_text_recognize_task

    # [START howto_operator_translate_speech]
    translate_speech_task = GcpTranslateSpeechOperator(
        project_id=GCP_PROJECT_ID,
        audio=AUDIO,
        config=CONFIG,
        target_language=TARGET_LANGUAGE,
        format_=FORMAT,
        source_language=SOURCE_LANGUAGE,
        model=MODEL,
        task_id='translate_speech_task'
    )
    # [END howto_operator_translate_speech]

    text_to_speech_synthesize_task >> translate_speech_task
