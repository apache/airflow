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
from datetime import datetime

from airflow import models
from airflow.providers.google.cloud.operators.text_to_speech import CloudTextToSpeechSynthesizeOperator

GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "example-project")
BUCKET_NAME = os.environ.get("GCP_TEXT_TO_SPEECH_BUCKET", "gcp-text-to-speech-test-bucket")

# [START howto_operator_text_to_speech_gcp_filename]
FILENAME = "gcp-speech-test-file"
# [END howto_operator_text_to_speech_gcp_filename]

# [START howto_operator_text_to_speech_api_arguments]
INPUT = {"text": "Sample text for demo purposes"}
VOICE = {"language_code": "en-US", "ssml_gender": "FEMALE"}
AUDIO_CONFIG = {"audio_encoding": "LINEAR16"}
# [END howto_operator_text_to_speech_api_arguments]

with models.DAG(
    "example_gcp_text_to_speech",
    schedule_interval='@once',  # Override to match your needs
    start_date=datetime(2021, 1, 1),
    catchup=False,
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
    text_to_speech_synthesize_task2 = CloudTextToSpeechSynthesizeOperator(
        input_data=INPUT,
        voice=VOICE,
        audio_config=AUDIO_CONFIG,
        target_bucket_name=BUCKET_NAME,
        target_filename=FILENAME,
        task_id="text_to_speech_synthesize_task2",
    )
    # [END howto_operator_text_to_speech_synthesize]

    text_to_speech_synthesize_task >> text_to_speech_synthesize_task2
