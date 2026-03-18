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
Example Airflow DAG that translates text in Google Cloud Translate using V3 API version
service in the Google Cloud.
"""

from __future__ import annotations

import os
from datetime import datetime

from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.gcs import (
    GCSCreateBucketOperator,
    GCSDeleteBucketOperator,
)
from airflow.providers.google.cloud.operators.translate import (
    TranslateDocumentBatchOperator,
    TranslateDocumentOperator,
)

try:
    from airflow.sdk import TriggerRule
except ImportError:
    # Compatibility for Airflow < 3.1
    from airflow.utils.trigger_rule import TriggerRule  # type: ignore[no-redef,attr-defined]

DAG_ID = "gcp_translate_document"
PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT", "default")
ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID", "default")
REGION = "us-central1"
RESOURCE_DATA_BUCKET = "airflow-system-tests-resources"
DATA_OUTPUT_BUCKET_NAME = f"bucket_{DAG_ID}_{ENV_ID}".replace("_", "-")

DOC_TRANSLATE_INPUT = {
    "gcs_source": {
        "input_uri": f"gs://{RESOURCE_DATA_BUCKET}/V3_translate/document_translate/translate_me_sample.xlsx"
    },
}
GCS_OUTPUT_DST = {
    "gcs_destination": {"output_uri_prefix": f"gs://{DATA_OUTPUT_BUCKET_NAME}/doc_translate_output/"}
}
BATCH_DOC_INPUT_ITEM_1 = {
    "gcs_source": {
        "input_uri": f"gs://{RESOURCE_DATA_BUCKET}/V3_translate/batch_document_translate/batch_translate_doc_sample_1.docx"
    }
}
BATCH_DOC_INPUT_ITEM_2 = {
    "gcs_source": {
        "input_uri": f"gs://{RESOURCE_DATA_BUCKET}/V3_translate/batch_document_translate/batch_translate_sample_2.pdf"
    }
}
BATCH_OUTPUT_CONFIG = {
    "gcs_destination": {"output_uri_prefix": f"gs://{DATA_OUTPUT_BUCKET_NAME}/batch_translate_docs_output/"}
}


with DAG(
    DAG_ID,
    schedule="@once",  # Override to match your needs
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["example", "document_translate", "document_translate_batch", "translate_V3"],
) as dag:
    create_bucket = GCSCreateBucketOperator(
        task_id="create_bucket",
        bucket_name=DATA_OUTPUT_BUCKET_NAME,
        storage_class="REGIONAL",
        location=REGION,
    )
    # [START howto_operator_translate_document]
    translate_document = TranslateDocumentOperator(
        task_id="translate_document_op",
        project_id=PROJECT_ID,
        location=REGION,
        source_language_code="en",
        target_language_code="uk",
        document_input_config=DOC_TRANSLATE_INPUT,
        document_output_config=GCS_OUTPUT_DST,
    )
    # [END howto_operator_translate_document]

    # [START howto_operator_translate_document_batch]
    translate_document_batch = TranslateDocumentBatchOperator(
        task_id="batch_translate_document_op",
        project_id=PROJECT_ID,
        location=REGION,
        source_language_code="en",
        target_language_codes=["uk", "fr"],
        input_configs=[BATCH_DOC_INPUT_ITEM_1, BATCH_DOC_INPUT_ITEM_2],
        output_config=BATCH_OUTPUT_CONFIG,
    )
    # [END howto_operator_translate_document_batch]

    delete_bucket = GCSDeleteBucketOperator(
        task_id="delete_bucket",
        bucket_name=DATA_OUTPUT_BUCKET_NAME,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    (
        # TEST SETUP
        create_bucket
        # TEST BODY
        >> [translate_document, translate_document_batch]
        # TEST TEARDOWN
        >> delete_bucket
    )

    from tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
