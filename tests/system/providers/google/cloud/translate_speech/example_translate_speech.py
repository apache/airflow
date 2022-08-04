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
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator, GCSDeleteBucketOperator
from airflow.providers.google.cloud.operators.text_to_speech import CloudTextToSpeechSynthesizeOperator
from airflow.providers.google.cloud.operators.translate_speech import CloudTranslateSpeechOperator
from airflow.utils.trigger_rule import TriggerRule

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT")

DAG_ID = "example_gcp_translate_speech"

BUCKET_NAME = f"bucket_{DAG_ID}_{ENV_ID}"

# [START howto_operator_translate_speech_gcp_filename]
FILE_NAME = f"test-translate-speech-file-{DAG_ID}-{ENV_ID}"
# [END howto_operator_translate_speech_gcp_filename]

# [START howto_operator_text_to_speech_api_arguments]
INPUT = {"text": "Sample text for demo purposes"}
VOICE = {"language_code": "en-US", "ssml_gender": "FEMALE"}
AUDIO_CONFIG = {"audio_encoding": "LINEAR16"}
# [END howto_operator_text_to_speech_api_arguments]

# [START howto_operator_translate_speech_arguments]
CONFIG = {"encoding": "LINEAR16", "language_code": "en_US"}
AUDIO = {"uri": f"gs://{BUCKET_NAME}/{FILE_NAME}"}
TARGET_LANGUAGE = 'pl'
FORMAT = 'text'
MODEL = 'base'
SOURCE_LANGUAGE = None  # type: None
# [END howto_operator_translate_speech_arguments]


with models.DAG(
    DAG_ID,
    schedule_interval='@once',  # Override to match your needs
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example"],
) as dag:
    create_bucket = GCSCreateBucketOperator(task_id="create_bucket", bucket_name=BUCKET_NAME)

    text_to_speech_synthesize_task = CloudTextToSpeechSynthesizeOperator(
        project_id=PROJECT_ID,
        input_data=INPUT,
        voice=VOICE,
        audio_config=AUDIO_CONFIG,
        target_bucket_name=BUCKET_NAME,
        target_filename=FILE_NAME,
        task_id="text_to_speech_synthesize_task",
    )
    # [START howto_operator_translate_speech]
    translate_speech_task = CloudTranslateSpeechOperator(
        project_id=PROJECT_ID,
        audio=AUDIO,
        config=CONFIG,
        target_language=TARGET_LANGUAGE,
        format_=FORMAT,
        source_language=SOURCE_LANGUAGE,
        model=MODEL,
        task_id='translate_speech_task',
    )
    translate_speech_task2 = CloudTranslateSpeechOperator(
        audio=AUDIO,
        config=CONFIG,
        target_language=TARGET_LANGUAGE,
        format_=FORMAT,
        source_language=SOURCE_LANGUAGE,
        model=MODEL,
        task_id='translate_speech_task2',
    )
    # [END howto_operator_translate_speech]

    delete_bucket = GCSDeleteBucketOperator(
        task_id="delete_bucket", bucket_name=BUCKET_NAME, trigger_rule=TriggerRule.ALL_DONE
    )

    (
        # TEST SETUP
        create_bucket
        # TEST BODY
        >> text_to_speech_synthesize_task
        >> translate_speech_task
        >> translate_speech_task2
        # TEST TEARDOWN
        >> delete_bucket
    )

    from tests.system.utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()


from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
