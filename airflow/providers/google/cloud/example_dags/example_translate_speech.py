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
from airflow.providers.google.cloud.operators.translate_speech import CloudTranslateSpeechOperator
from airflow.utils import dates

GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "example-project")
BUCKET_NAME = os.environ.get("GCP_SPEECH_TO_TEXT_TEST_BUCKET", "gcp-speech-to-text-test-bucket")

# [START howto_operator_translate_speech_gcp_filename]
FILENAME = "gcp-speech-test-file"
# [END howto_operator_translate_speech_gcp_filename]

# [START howto_operator_translate_speech_arguments]
CONFIG = {"encoding": "LINEAR16", "language_code": "en_US"}
AUDIO = {"uri": "gs://{bucket}/{object}".format(bucket=BUCKET_NAME, object=FILENAME)}
TARGET_LANGUAGE = 'pl'
FORMAT = 'text'
MODEL = 'base'
SOURCE_LANGUAGE = None  # type: None
# [END howto_operator_translate_speech_arguments]

default_args = {"start_date": dates.days_ago(1)}

with models.DAG(
    "example_gcp_translate_speech",
    default_args=default_args,
    schedule_interval=None,  # Override to match your needs
    tags=['example'],
) as dag:
    # [START howto_operator_translate_speech]
    translate_speech_task = CloudTranslateSpeechOperator(
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
