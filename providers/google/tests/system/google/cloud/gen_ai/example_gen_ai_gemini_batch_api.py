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
Example Airflow DAG for Google Gen AI Gemini Batch API and File API.
"""

from __future__ import annotations

import os
from datetime import datetime

try:
    from airflow.sdk import task
except ImportError:
    # Airflow 2 path
    from airflow.decorators import task  # type: ignore[attr-defined,no-redef]

try:
    from airflow.sdk import TriggerRule
except ImportError:
    # Compatibility for Airflow < 3.1
    from airflow.utils.trigger_rule import TriggerRule  # type: ignore[no-redef,attr-defined]

from pathlib import Path

from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.gen_ai import (
    GenAIGeminiCancelBatchJobOperator,
    GenAIGeminiCreateBatchJobOperator,
    GenAIGeminiCreateEmbeddingsBatchJobOperator,
    GenAIGeminiDeleteBatchJobOperator,
    GenAIGeminiDeleteFileOperator,
    GenAIGeminiGetBatchJobOperator,
    GenAIGeminiGetFileOperator,
    GenAIGeminiListBatchJobsOperator,
    GenAIGeminiListFilesOperator,
    GenAIGeminiUploadFileOperator,
)
from airflow.providers.google.common.utils.get_secret import get_secret
from airflow.providers.standard.operators.bash import BashOperator

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID", "default")
PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT", "default")
REGION = "us-central1"
DAG_ID = "gen_ai_gemini_batch_api"

GEMINI_API_KEY = "api_key"

INLINED_REQUESTS_FOR_BATCH_JOB = [
    {"contents": [{"parts": [{"text": "Tell me a one-sentence joke."}], "role": "user"}]},
    {"contents": [{"parts": [{"text": "Why is the sky blue?"}], "role": "user"}]},
]

INLINED_REQUESTS_FOR_EMBEDDINGS_BATCH_JOB = {
    "contents": [{"parts": [{"text": "Why is the sky blue?"}], "role": "user"}]
}


GEMINI_XCOM_API_KEY = "{{ task_instance.xcom_pull('get_gemini_api_key') }}"

LOCAL_FILE_NAME = "gemini_batch_requests.jsonl"
LOCAL_EMBEDDINGS_FILE_NAME = "gemini_batch_embeddings_requests.jsonl"
UPLOAD_FILE_PATH = str(Path(__file__).parent / "resources" / LOCAL_FILE_NAME)
PATH_TO_SAVE_RESULTS = str(Path(__file__).parent / "resources")
UPLOAD_EMBEDDINGS_FILE_PATH = str(Path(__file__).parent / "resources" / LOCAL_EMBEDDINGS_FILE_NAME)

UPLOADED_FILE_NAME = (
    "{{ task_instance.xcom_pull(task_ids='upload_file_for_batch_job_task', key='file_name') }}"
)
UPLOADED_EMBEDDINGS_FILE_NAME = (
    "{{ task_instance.xcom_pull(task_ids='upload_file_for_embeddings_batch_job_task', key='file_name') }}"
)

BATCH_JOB_WITH_INLINED_REQUESTS_NAME = (
    "{{ task_instance.xcom_pull(task_ids='create_batch_job_using_inlined_requests_task', key='job_name') }}"
)
BATCH_JOB_WITH_FILE_NAME = (
    "{{ task_instance.xcom_pull(task_ids='create_batch_job_using_file_task', key='job_name') }}"
)
EMBEDDINGS_BATCH_JOB_WITH_INLINED_REQUESTS_NAME = "{{ task_instance.xcom_pull(task_ids='create_embeddings_job_using_inlined_requests_task', key='job_name') }}"
EMBEDDINGS_BATCH_JOB_WITH_FILE_NAME = (
    "{{ task_instance.xcom_pull(task_ids='create_embeddings_job_using_file_task', key='job_name') }}"
)


with DAG(
    dag_id=DAG_ID,
    description="Sample DAG with Gemini Batch API.",
    schedule="@once",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["example", "gen_ai", "gemini_batch_api", "gemini_file_api"],
    render_template_as_native_obj=True,
) as dag:

    @task
    def get_gemini_api_key():
        return get_secret(GEMINI_API_KEY)

    get_gemini_api_key_task = get_gemini_api_key()

    # [START how_to_cloud_gen_ai_files_api_upload_file_task]
    upload_file = GenAIGeminiUploadFileOperator(
        task_id="upload_file_for_batch_job_task",
        project_id=PROJECT_ID,
        location=REGION,
        file_path=UPLOAD_FILE_PATH,
        gemini_api_key=GEMINI_XCOM_API_KEY,
    )
    # [END how_to_cloud_gen_ai_files_api_upload_file_task]

    upload_embeddings_file = GenAIGeminiUploadFileOperator(
        task_id="upload_file_for_embeddings_batch_job_task",
        project_id=PROJECT_ID,
        location=REGION,
        file_path=UPLOAD_EMBEDDINGS_FILE_PATH,
        gemini_api_key=GEMINI_XCOM_API_KEY,
    )

    # [START how_to_cloud_gen_ai_files_api_get_file_task]
    get_file = GenAIGeminiGetFileOperator(
        task_id="get_file_for_batch_job_task",
        project_id=PROJECT_ID,
        location=REGION,
        file_name=UPLOADED_FILE_NAME,
        gemini_api_key=GEMINI_XCOM_API_KEY,
    )
    # [END how_to_cloud_gen_ai_files_api_get_file_task]

    # [START how_to_cloud_gen_ai_files_api_list_files_task]
    list_files = GenAIGeminiListFilesOperator(
        task_id="list_files_files_api_task",
        project_id=PROJECT_ID,
        location=REGION,
        gemini_api_key=GEMINI_XCOM_API_KEY,
    )
    # [END how_to_cloud_gen_ai_files_api_list_files_task]

    # [START how_to_cloud_gen_ai_files_api_delete_file_task]
    delete_file = GenAIGeminiDeleteFileOperator(
        task_id="delete_file_for_batch_job_task",
        project_id=PROJECT_ID,
        location=REGION,
        file_name=UPLOADED_FILE_NAME,
        gemini_api_key=GEMINI_XCOM_API_KEY,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    # [END how_to_cloud_gen_ai_files_api_delete_file_task]

    delete_embeddings_file = GenAIGeminiDeleteFileOperator(
        task_id="delete_file_for_embeddings_batch_job_task",
        project_id=PROJECT_ID,
        location=REGION,
        file_name=UPLOADED_EMBEDDINGS_FILE_NAME,
        gemini_api_key=GEMINI_XCOM_API_KEY,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # [START how_to_cloud_gen_ai_batch_api_create_batch_job_with_inlined_requests_task]
    create_batch_job_using_inlined_requests = GenAIGeminiCreateBatchJobOperator(
        task_id="create_batch_job_using_inlined_requests_task",
        project_id=PROJECT_ID,
        location=REGION,
        model="gemini-3-pro-preview",
        gemini_api_key=GEMINI_XCOM_API_KEY,
        create_batch_job_config={
            "display_name": "inlined-requests-batch-job",
        },
        input_source=INLINED_REQUESTS_FOR_BATCH_JOB,
        wait_until_complete=True,
        retrieve_result=True,
    )
    # [END how_to_cloud_gen_ai_batch_api_create_batch_job_with_inlined_requests_task]

    # [START how_to_cloud_gen_ai_batch_api_create_batch_job_with_file_task]
    create_batch_job_using_file = GenAIGeminiCreateBatchJobOperator(
        task_id="create_batch_job_using_file_task",
        project_id=PROJECT_ID,
        location=REGION,
        model="gemini-3-pro-preview",
        gemini_api_key=GEMINI_XCOM_API_KEY,
        create_batch_job_config={
            "display_name": "file-upload-batch-job",
        },
        input_source=UPLOADED_FILE_NAME,
        wait_until_complete=True,
        retrieve_result=True,
        results_folder=PATH_TO_SAVE_RESULTS,
    )
    # [END how_to_cloud_gen_ai_batch_api_create_batch_job_with_file_task]

    # [START how_to_cloud_gen_ai_batch_api_create_embeddings_with_inlined_requests_task]
    create_embeddings_job_using_inlined_requests = GenAIGeminiCreateEmbeddingsBatchJobOperator(
        task_id="create_embeddings_job_using_inlined_requests_task",
        project_id=PROJECT_ID,
        location=REGION,
        model="gemini-embedding-001",
        wait_until_complete=False,
        gemini_api_key=GEMINI_XCOM_API_KEY,
        create_embeddings_config={
            "display_name": "inlined-requests-embeddings-job",
        },
        input_source=INLINED_REQUESTS_FOR_EMBEDDINGS_BATCH_JOB,
    )
    # [END how_to_cloud_gen_ai_batch_api_create_embeddings_with_inlined_requests_task]

    # [START how_to_cloud_gen_ai_batch_api_create_embeddings_with_file_task]
    create_embeddings_job_using_file = GenAIGeminiCreateEmbeddingsBatchJobOperator(
        task_id="create_embeddings_job_using_file_task",
        project_id=PROJECT_ID,
        location=REGION,
        model="gemini-embedding-001",
        wait_until_complete=False,
        gemini_api_key=GEMINI_XCOM_API_KEY,
        create_embeddings_config={
            "display_name": "file-upload-embeddings-job",
        },
        input_source=UPLOADED_EMBEDDINGS_FILE_NAME,
    )
    # [END how_to_cloud_gen_ai_batch_api_create_embeddings_with_file_task]

    # [START how_to_cloud_gen_ai_batch_api_get_batch_job_task]
    get_batch_job = GenAIGeminiGetBatchJobOperator(
        task_id="get_batch_job_task",
        project_id=PROJECT_ID,
        location=REGION,
        gemini_api_key=GEMINI_XCOM_API_KEY,
        job_name=BATCH_JOB_WITH_INLINED_REQUESTS_NAME,
    )
    # [END how_to_cloud_gen_ai_batch_api_get_batch_job_task]

    # [START how_to_cloud_gen_ai_batch_api_list_batch_jobs_task]
    list_batch_jobs = GenAIGeminiListBatchJobsOperator(
        task_id="list_batch_jobs_task",
        project_id=PROJECT_ID,
        location=REGION,
        gemini_api_key=GEMINI_XCOM_API_KEY,
    )
    # [END how_to_cloud_gen_ai_batch_api_list_batch_jobs_task]

    # [START how_to_cloud_gen_ai_batch_api_cancel_batch_job_task]
    cancel_batch_job = GenAIGeminiCancelBatchJobOperator(
        task_id="cancel_batch_job_task",
        project_id=PROJECT_ID,
        location=REGION,
        gemini_api_key=GEMINI_XCOM_API_KEY,
        job_name=BATCH_JOB_WITH_FILE_NAME,
    )
    # [END how_to_cloud_gen_ai_batch_api_cancel_batch_job_task]

    # [START how_to_cloud_gen_ai_batch_api_delete_batch_job_task]
    delete_batch_job_1 = GenAIGeminiDeleteBatchJobOperator(
        task_id="delete_batch_job_1_task",
        project_id=PROJECT_ID,
        location=REGION,
        gemini_api_key=GEMINI_XCOM_API_KEY,
        job_name=BATCH_JOB_WITH_INLINED_REQUESTS_NAME,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    # [END how_to_cloud_gen_ai_batch_api_delete_batch_job_task]

    delete_batch_job_2 = GenAIGeminiDeleteBatchJobOperator(
        task_id="delete_batch_job_2_task",
        project_id=PROJECT_ID,
        location=REGION,
        gemini_api_key=GEMINI_XCOM_API_KEY,
        job_name=BATCH_JOB_WITH_FILE_NAME,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    delete_embeddings_batch_job_1 = GenAIGeminiDeleteBatchJobOperator(
        task_id="delete_embeddings_batch_job_1_task",
        project_id=PROJECT_ID,
        location=REGION,
        gemini_api_key=GEMINI_XCOM_API_KEY,
        job_name=EMBEDDINGS_BATCH_JOB_WITH_INLINED_REQUESTS_NAME,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    delete_embeddings_batch_job_2 = GenAIGeminiDeleteBatchJobOperator(
        task_id="delete_embeddings_batch_job_2_task",
        project_id=PROJECT_ID,
        location=REGION,
        gemini_api_key=GEMINI_XCOM_API_KEY,
        job_name=EMBEDDINGS_BATCH_JOB_WITH_FILE_NAME,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    delete_result_file = BashOperator(
        task_id="delete_result_file_task",
        bash_command="rm "
        "{{ task_instance.xcom_pull(task_ids='create_batch_job_using_file_task', key='job_results') }}",
        trigger_rule=TriggerRule.ALL_DONE,
    )

    (
        get_gemini_api_key_task
        >> [upload_file, upload_embeddings_file]
        >> get_file
        >> list_files
        >> [
            create_batch_job_using_inlined_requests,
            create_batch_job_using_file,
            create_embeddings_job_using_file,
            create_embeddings_job_using_inlined_requests,
        ]
        >> get_batch_job
        >> list_batch_jobs
        >> cancel_batch_job
        >> delete_batch_job_1
        >> delete_batch_job_2
        >> delete_embeddings_batch_job_1
        >> delete_embeddings_batch_job_2
        >> [delete_file, delete_embeddings_file, delete_result_file]
    )

    from tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
