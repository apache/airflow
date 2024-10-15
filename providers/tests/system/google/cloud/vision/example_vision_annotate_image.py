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

from __future__ import annotations

import os
from datetime import datetime

from airflow.models.baseoperator import chain
from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator, GCSDeleteBucketOperator
from airflow.providers.google.cloud.operators.vision import (
    CloudVisionDetectImageLabelsOperator,
    CloudVisionDetectImageSafeSearchOperator,
    CloudVisionDetectTextOperator,
    CloudVisionImageAnnotateOperator,
    CloudVisionTextDetectOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule

# [START howto_operator_vision_retry_import]
from google.api_core.retry import Retry  # isort:skip

# [END howto_operator_vision_retry_import]

# [START howto_operator_vision_enums_import]
from google.cloud.vision_v1 import Feature  # isort:skip
from providers.tests.system.google import DEFAULT_GCP_SYSTEM_TEST_PROJECT_ID

# [END howto_operator_vision_enums_import]

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID", "default")
PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT") or DEFAULT_GCP_SYSTEM_TEST_PROJECT_ID

DAG_ID = "gcp_vision_annotate_image"

LOCATION = "europe-west1"

BUCKET_NAME = f"bucket-{DAG_ID}-{ENV_ID}".replace("_", "-")
FILE_NAME = "image1.jpg"

GCP_VISION_ANNOTATE_IMAGE_URL = f"gs://{BUCKET_NAME}/{FILE_NAME}"

# [START howto_operator_vision_annotate_image_request]
annotate_image_request = {
    "image": {"source": {"image_uri": GCP_VISION_ANNOTATE_IMAGE_URL}},
    "features": [{"type_": Feature.Type.LOGO_DETECTION}],
}
# [END howto_operator_vision_annotate_image_request]

# [START howto_operator_vision_detect_image_param]
DETECT_IMAGE = {"source": {"image_uri": GCP_VISION_ANNOTATE_IMAGE_URL}}
# [END howto_operator_vision_detect_image_param]


# Public bucket holding the sample data
BUCKET_NAME_SRC = "cloud-samples-data"
# Path to the data inside the public bucket
PATH_SRC = "vision/logo/google_logo.jpg"


with DAG(
    DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example", "vision", "annotate_image"],
) as dag:
    create_bucket = GCSCreateBucketOperator(
        task_id="create_bucket", project_id=PROJECT_ID, bucket_name=BUCKET_NAME
    )

    copy_single_file = GCSToGCSOperator(
        task_id="copy_single_gcs_file",
        source_bucket=BUCKET_NAME_SRC,
        source_object=[PATH_SRC],
        destination_bucket=BUCKET_NAME,
        destination_object=FILE_NAME,
    )

    # [START howto_operator_vision_annotate_image]
    annotate_image = CloudVisionImageAnnotateOperator(
        request=annotate_image_request,
        retry=Retry(maximum=10.0),
        timeout=5,
        task_id="annotate_image",
    )
    # [END howto_operator_vision_annotate_image]

    # [START howto_operator_vision_annotate_image_result]
    annotate_image_result = BashOperator(
        bash_command="echo {{ task_instance.xcom_pull('annotate_image')"
        "['logoAnnotations'][0]['description'] }}",
        task_id="annotate_image_result",
    )
    # [END howto_operator_vision_annotate_image_result]

    # [START howto_operator_vision_detect_text]
    detect_text = CloudVisionDetectTextOperator(
        image=DETECT_IMAGE,
        retry=Retry(maximum=10.0),
        timeout=5,
        task_id="detect_text",
        language_hints="en",
        web_detection_params={"include_geo_results": True},
    )
    # [END howto_operator_vision_detect_text]

    # [START howto_operator_vision_detect_text_result]
    detect_text_result = BashOperator(
        bash_command="echo {{ task_instance.xcom_pull('detect_text')['textAnnotations'][0] }}",
        task_id="detect_text_result",
    )
    # [END howto_operator_vision_detect_text_result]

    # [START howto_operator_vision_document_detect_text]
    document_detect_text = CloudVisionTextDetectOperator(
        image=DETECT_IMAGE, retry=Retry(maximum=10.0), timeout=5, task_id="document_detect_text"
    )
    # [END howto_operator_vision_document_detect_text]

    # [START howto_operator_vision_document_detect_text_result]
    document_detect_text_result = BashOperator(
        bash_command="echo {{ task_instance.xcom_pull('document_detect_text')['textAnnotations'][0] }}",
        task_id="document_detect_text_result",
    )
    # [END howto_operator_vision_document_detect_text_result]

    # [START howto_operator_vision_detect_labels]
    detect_labels = CloudVisionDetectImageLabelsOperator(
        image=DETECT_IMAGE, retry=Retry(maximum=10.0), timeout=5, task_id="detect_labels"
    )
    # [END howto_operator_vision_detect_labels]

    # [START howto_operator_vision_detect_labels_result]
    detect_labels_result = BashOperator(
        bash_command="echo {{ task_instance.xcom_pull('detect_labels')['labelAnnotations'][0] }}",
        task_id="detect_labels_result",
    )
    # [END howto_operator_vision_detect_labels_result]

    # [START howto_operator_vision_detect_safe_search]
    detect_safe_search = CloudVisionDetectImageSafeSearchOperator(
        image=DETECT_IMAGE, retry=Retry(maximum=10.0), timeout=5, task_id="detect_safe_search"
    )
    # [END howto_operator_vision_detect_safe_search]

    # [START howto_operator_vision_detect_safe_search_result]
    detect_safe_search_result = BashOperator(
        bash_command=f"echo {detect_safe_search.output}",
        task_id="detect_safe_search_result",
    )
    # [END howto_operator_vision_detect_safe_search_result]

    delete_bucket = GCSDeleteBucketOperator(
        task_id="delete_bucket", bucket_name=BUCKET_NAME, trigger_rule=TriggerRule.ALL_DONE
    )

    chain(
        # TEST SETUP
        create_bucket,
        copy_single_file,
        # TEST BODY
        annotate_image,
        annotate_image_result,
        detect_text,
        detect_text_result,
        document_detect_text,
        document_detect_text_result,
        detect_labels,
        detect_labels_result,
        detect_safe_search,
        detect_safe_search_result,
        # TEST TEARDOWN
        delete_bucket,
    )

    from tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "teardown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
