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

import os

from airflow import models
from airflow.providers.google.cloud.operators.speech_to_text import CloudSpeechToTextRecognizeSpeechOperator
from airflow.providers.google.cloud.operators.text_to_speech import CloudTextToSpeechSynthesizeOperator
from airflow.utils import dates

GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "example-project")
BUCKET_NAME = os.environ.get("GCP_SPEECH_TO_TEXT_TEST_BUCKET", "gcp-speech-to-text-test-bucket")

# [START howto_operator_speech_to_text_gcp_filename]
FILENAME = "gcp-speech-test-file"
# [END howto_operator_speech_to_text_gcp_filename]

# [START howto_operator_text_to_speech_api_arguments]
INPUT = {"text": "Sample text for demo purposes"}
VOICE = {"language_code": "en-US", "ssml_gender": "FEMALE"}
AUDIO_CONFIG = {"audio_encoding": "LINEAR16"}
# [END howto_operator_text_to_speech_api_arguments]

# [START howto_operator_speech_to_text_api_arguments]
CONFIG = {"encoding": "LINEAR16", "language_code": "en_US"}
AUDIO = {"uri": "gs://{bucket}/{object}".format(bucket=BUCKET_NAME, object=FILENAME)}
# [END howto_operator_speech_to_text_api_arguments]

with models.DAG(
    "example_gcp_speech_to_text",
    start_date=dates.days_ago(1),
    schedule_interval=None,  # Override to match your needs
    tags=['example'],
) as dag:
    text_to_speech_synthesize_task = CloudTextToSpeechSynthesizeOperator(
        project_id=GCP_PROJECT_ID,
        input_data=INPUT,
        voice=VOICE,
        audio_config=AUDIO_CONFIG,
        target_bucket_name=BUCKET_NAME,
        target_filename=FILENAME,
        task_id="text_to_speech_synthesize_task",
    )
    # [START howto_operator_speech_to_text_recognize]
    speech_to_text_recognize_task2 = CloudSpeechToTextRecognizeSpeechOperator(
        config=CONFIG, audio=AUDIO, task_id="speech_to_text_recognize_task"
    )
    # [END howto_operator_speech_to_text_recognize]

    text_to_speech_synthesize_task >> speech_to_text_recognize_task2
