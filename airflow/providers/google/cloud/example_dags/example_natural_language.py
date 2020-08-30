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

from google.cloud.language_v1.proto.language_service_pb2 import Document

from airflow import models
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.natural_language import (
    CloudNaturalLanguageAnalyzeEntitiesOperator,
    CloudNaturalLanguageAnalyzeEntitySentimentOperator,
    CloudNaturalLanguageAnalyzeSentimentOperator,
    CloudNaturalLanguageClassifyTextOperator,
)
from airflow.utils.dates import days_ago

# [START howto_operator_gcp_natural_language_document_text]
TEXT = """Airflow is a platform to programmatically author, schedule and monitor workflows.

Use Airflow to author workflows as Directed Acyclic Graphs (DAGs) of tasks. The Airflow scheduler executes
 your tasks on an array of workers while following the specified dependencies. Rich command line utilities
 make performing complex surgeries on DAGs a snap. The rich user interface makes it easy to visualize
 pipelines running in production, monitor progress, and troubleshoot issues when needed.
"""
document = Document(content=TEXT, type="PLAIN_TEXT")
# [END howto_operator_gcp_natural_language_document_text]

# [START howto_operator_gcp_natural_language_document_gcs]
GCS_CONTENT_URI = "gs://my-text-bucket/sentiment-me.txt"
document_gcs = Document(gcs_content_uri=GCS_CONTENT_URI, type="PLAIN_TEXT")
# [END howto_operator_gcp_natural_language_document_gcs]


with models.DAG(
    "example_gcp_natural_language",
    schedule_interval=None,  # Override to match your needs
    start_date=days_ago(1),
) as dag:

    # [START howto_operator_gcp_natural_language_analyze_entities]
    analyze_entities = CloudNaturalLanguageAnalyzeEntitiesOperator(
        document=document, task_id="analyze_entities"
    )
    # [END howto_operator_gcp_natural_language_analyze_entities]

    # [START howto_operator_gcp_natural_language_analyze_entities_result]
    analyze_entities_result = BashOperator(
        bash_command="echo \"{{ task_instance.xcom_pull('analyze_entities') }}\"",
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
        bash_command="echo \"{{ task_instance.xcom_pull('analyze_entity_sentiment') }}\"",
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
        bash_command="echo \"{{ task_instance.xcom_pull('analyze_sentiment') }}\"",
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
        bash_command="echo \"{{ task_instance.xcom_pull('analyze_classify_text') }}\"",
        task_id="analyze_classify_text_result",
    )
    # [END howto_operator_gcp_natural_language_analyze_classify_text_result]

    analyze_entities >> analyze_entities_result
    analyze_entity_sentiment >> analyze_entity_sentiment_result
    analyze_sentiment >> analyze_sentiment_result
    analyze_classify_text >> analyze_classify_text_result
