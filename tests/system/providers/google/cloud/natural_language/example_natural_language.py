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
Example Airflow DAG for Google Cloud Natural Language service
"""
from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path

from google.cloud.language_v1.proto.language_service_pb2 import Document

from airflow import models
from airflow.models.baseoperator import chain
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator, GCSDeleteBucketOperator
from airflow.providers.google.cloud.operators.natural_language import (
    CloudNaturalLanguageAnalyzeEntitiesOperator,
    CloudNaturalLanguageAnalyzeEntitySentimentOperator,
    CloudNaturalLanguageAnalyzeSentimentOperator,
    CloudNaturalLanguageClassifyTextOperator,
)
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.utils.trigger_rule import TriggerRule

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")

DAG_ID = "example_gcp_natural_language"

BUCKET_NAME = f"bucket-{DAG_ID}-{ENV_ID}"

# [START howto_operator_gcp_natural_language_document_text]
TEXT = """Airflow is a platform to programmatically author, schedule and monitor workflows.

Use Airflow to author workflows as Directed Acyclic Graphs (DAGs) of tasks. The Airflow scheduler executes
 your tasks on an array of workers while following the specified dependencies. Rich command line utilities
 make performing complex surgeries on DAGs a snap. The rich user interface makes it easy to visualize
 pipelines running in production, monitor progress, and troubleshoot issues when needed.
"""

document = Document(content=TEXT, type="PLAIN_TEXT")
# [END howto_operator_gcp_natural_language_document_text]

FILE_NAME = "sentiment-me.txt"
# [START howto_operator_gcp_natural_language_document_gcs]
GCS_CONTENT_URI = f"gs://{BUCKET_NAME}/{FILE_NAME}"
document_gcs = Document(gcs_content_uri=GCS_CONTENT_URI, type="PLAIN_TEXT")

# [END howto_operator_gcp_natural_language_document_gcs]
CURRENT_FOLDER = Path(__file__).parent
LOCAL_PATH = str(Path(CURRENT_FOLDER) / "resources")

FILE_LOCAL_PATH = str(Path(LOCAL_PATH))

with models.DAG(
    DAG_ID,
    schedule="@once",  # Override to match your needs
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example", "natural_language"],
) as dag:
    create_bucket = GCSCreateBucketOperator(task_id="create_bucket", bucket_name=BUCKET_NAME)
    generate_file = BashOperator(
        task_id="generate_file",
        bash_command=f"mkdir -p {LOCAL_PATH} && echo '{TEXT}' > {LOCAL_PATH}/{FILE_NAME}",
    )
    upload_file = LocalFilesystemToGCSOperator(
        task_id="upload_file",
        src=f"{FILE_LOCAL_PATH}/{FILE_NAME}",
        dst=FILE_NAME,
        bucket=BUCKET_NAME,
    )
    # [START howto_operator_gcp_natural_language_analyze_entities]
    analyze_entities = CloudNaturalLanguageAnalyzeEntitiesOperator(
        document=document, task_id="analyze_entities"
    )
    # [END howto_operator_gcp_natural_language_analyze_entities]

    # [START howto_operator_gcp_natural_language_analyze_entities_result]
    analyze_entities_result = BashOperator(
        bash_command=f"echo {analyze_entities.output}",
        task_id="analyze_entities_result",
    )
    # [END howto_operator_gcp_natural_language_analyze_entities_result]

    # [START howto_operator_gcp_natural_language_analyze_entity_sentiment]
    analyze_entity_sentiment = CloudNaturalLanguageAnalyzeEntitySentimentOperator(
        document=document, task_id="analyze_entity_sentiment"
    )
    # [END howto_operator_gcp_natural_language_analyze_entity_sentiment]

    # [START howto_operator_gcp_natural_language_analyze_entity_sentiment_result]
    analyze_entity_sentiment_result = BashOperator(
        bash_command=f"echo {analyze_entity_sentiment.output}",
        task_id="analyze_entity_sentiment_result",
    )
    # [END howto_operator_gcp_natural_language_analyze_entity_sentiment_result]

    # [START howto_operator_gcp_natural_language_analyze_sentiment]
    analyze_sentiment = CloudNaturalLanguageAnalyzeSentimentOperator(
        document=document, task_id="analyze_sentiment"
    )
    # [END howto_operator_gcp_natural_language_analyze_sentiment]

    # [START howto_operator_gcp_natural_language_analyze_sentiment_result]
    analyze_sentiment_result = BashOperator(
        bash_command=f"echo {analyze_sentiment.output}",
        task_id="analyze_sentiment_result",
    )
    # [END howto_operator_gcp_natural_language_analyze_sentiment_result]

    # [START howto_operator_gcp_natural_language_analyze_classify_text]
    analyze_classify_text = CloudNaturalLanguageClassifyTextOperator(
        document=document, task_id="analyze_classify_text"
    )
    # [END howto_operator_gcp_natural_language_analyze_classify_text]

    # [START howto_operator_gcp_natural_language_analyze_classify_text_result]
    analyze_classify_text_result = BashOperator(
        bash_command=f"echo {analyze_classify_text.output}",
        task_id="analyze_classify_text_result",
    )
    # [END howto_operator_gcp_natural_language_analyze_classify_text_result]

    analyze_entities_1 = CloudNaturalLanguageAnalyzeEntitiesOperator(
        document=document_gcs, task_id="analyze_entities_1"
    )
    analyze_entity_sentiment_1 = CloudNaturalLanguageAnalyzeEntitySentimentOperator(
        document=document_gcs, task_id="analyze_entity_sentiment_1"
    )

    delete_bucket = GCSDeleteBucketOperator(
        task_id="delete_bucket", bucket_name=BUCKET_NAME, trigger_rule=TriggerRule.ALL_DONE
    )

    chain(
        # TEST SETUP
        create_bucket,
        generate_file,
        upload_file,
        # TEST BODY
        analyze_entities,
        analyze_entities_result,
        analyze_entity_sentiment,
        analyze_entity_sentiment_result,
        analyze_sentiment,
        analyze_sentiment_result,
        analyze_classify_text,
        analyze_classify_text_result,
        analyze_entities_1,
        analyze_entity_sentiment_1,
        # TEST TEARDOWN
        delete_bucket,
    )

    from tests.system.utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()


from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
