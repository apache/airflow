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
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator, GCSDeleteBucketOperator
from airflow.providers.google.cloud.operators.translate import (
    TranslateTextBatchOperator,
    TranslateTextOperator,
)

try:
    from airflow.sdk import TriggerRule
except ImportError:
    # Compatibility for Airflow < 3.1
    from airflow.utils.trigger_rule import TriggerRule  # type: ignore[no-redef,attr-defined]

DAG_ID = "gcp_translate_text"
PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT", "default")
ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID", "default")
REGION = "us-central1"
RESOURCE_DATA_BUCKET = "airflow-system-tests-resources"
BATCH_TRANSLATE_SAMPLE_URI = (
    f"gs://{RESOURCE_DATA_BUCKET}/translate/V3/text_batch/inputs/translate_sample_de_1.txt"
)
BATCH_TRANSLATE_INPUT = {
    "gcs_source": {"input_uri": BATCH_TRANSLATE_SAMPLE_URI},
    "mime_type": "text/plain",
}
DATA_SAMPLE_GCS_BUCKET_NAME = f"bucket_{DAG_ID}_{ENV_ID}".replace("_", "-")
GCS_OUTPUT_DST = {
    "gcs_destination": {"output_uri_prefix": f"gs://{DATA_SAMPLE_GCS_BUCKET_NAME}/translate_output/"}
}


with DAG(
    DAG_ID,
    schedule="@once",  # Override to match your needs
    start_date=datetime(2024, 11, 1),
    catchup=False,
    tags=["example", "translate_text", "batch_translate_text"],
) as dag:
    create_bucket = GCSCreateBucketOperator(
        task_id="create_bucket",
        bucket_name=DATA_SAMPLE_GCS_BUCKET_NAME,
        storage_class="REGIONAL",
        location=REGION,
    )
    # [START howto_operator_translate_text_advanced]
    translate_text = TranslateTextOperator(
        task_id="translate_v3_op",
        contents=["Ciao mondo!", "Mi puoi prendere una tazza di caffè, per favore?"],
        source_language_code="it",
        target_language_code="en",
    )
    # [END howto_operator_translate_text_advanced]

    # [START howto_operator_batch_translate_text]
    batch_text_translate = TranslateTextBatchOperator(
        task_id="batch_translate_v3_op",
        source_language_code="de",
        target_language_codes=["en"],  # Up to 10 language codes per run
        location="us-central1",
        input_configs=[BATCH_TRANSLATE_INPUT],
        output_config=GCS_OUTPUT_DST,
    )
    # [END howto_operator_batch_translate_text]

    delete_bucket = GCSDeleteBucketOperator(
        task_id="delete_bucket",
        bucket_name=DATA_SAMPLE_GCS_BUCKET_NAME,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    (
        # TEST SETUP
        create_bucket
        # TEST BODY
        >> [translate_text, batch_text_translate]
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
