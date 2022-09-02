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
Example Airflow DAG that demonstrates operators for the Google Cloud Video Intelligence service in the Google
Cloud Platform.

This DAG relies on the following OS environment variables:

* BUCKET_NAME - Google Cloud Storage bucket where the file exists.
"""
import os
from datetime import datetime

from google.api_core.retry import Retry

from airflow import models
from airflow.models.baseoperator import chain
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator, GCSDeleteBucketOperator
from airflow.providers.google.cloud.operators.video_intelligence import (
    CloudVideoIntelligenceDetectVideoExplicitContentOperator,
    CloudVideoIntelligenceDetectVideoLabelsOperator,
    CloudVideoIntelligenceDetectVideoShotsOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.utils.trigger_rule import TriggerRule

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")

DAG_ID = "example_gcp_video_intelligence"

# Public bucket holding the sample data
BUCKET_NAME_SRC = "cloud-samples-data"
# Path to the data inside the public bucket
PATH_SRC = "video/cat.mp4"

# [START howto_operator_video_intelligence_os_args]
BUCKET_NAME_DST = f"bucket-src-{DAG_ID}-{ENV_ID}"
# [END howto_operator_video_intelligence_os_args]

FILE_NAME = "video.mp4"

# [START howto_operator_video_intelligence_other_args]
INPUT_URI = f"gs://{BUCKET_NAME_DST}/{FILE_NAME}"
# [END howto_operator_video_intelligence_other_args]

with models.DAG(
    DAG_ID,
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:

    create_bucket = GCSCreateBucketOperator(task_id="create_bucket", bucket_name=BUCKET_NAME_DST)

    copy_single_file = GCSToGCSOperator(
        task_id="copy_single_gcs_file",
        source_bucket=BUCKET_NAME_SRC,
        source_object=PATH_SRC,
        destination_bucket=BUCKET_NAME_DST,
        destination_object=FILE_NAME,
    )

    # [START howto_operator_video_intelligence_detect_labels]
    detect_video_label = CloudVideoIntelligenceDetectVideoLabelsOperator(
        input_uri=INPUT_URI,
        output_uri=None,
        video_context=None,
        timeout=5,
        task_id="detect_video_label",
    )
    # [END howto_operator_video_intelligence_detect_labels]

    # [START howto_operator_video_intelligence_detect_labels_result]
    detect_video_label_result = BashOperator(
        bash_command="echo {{ task_instance.xcom_pull('detect_video_label')"
        "['annotationResults'][0]['shotLabelAnnotations'][0]['entity']}}",
        task_id="detect_video_label_result",
    )
    # [END howto_operator_video_intelligence_detect_labels_result]

    # [START howto_operator_video_intelligence_detect_explicit_content]
    detect_video_explicit_content = CloudVideoIntelligenceDetectVideoExplicitContentOperator(
        input_uri=INPUT_URI,
        output_uri=None,
        video_context=None,
        retry=Retry(maximum=10.0),
        timeout=5,
        task_id="detect_video_explicit_content",
    )
    # [END howto_operator_video_intelligence_detect_explicit_content]

    # [START howto_operator_video_intelligence_detect_explicit_content_result]
    detect_video_explicit_content_result = BashOperator(
        bash_command="echo {{ task_instance.xcom_pull('detect_video_explicit_content')"
        "['annotationResults'][0]['explicitAnnotation']['frames'][0]}}",
        task_id="detect_video_explicit_content_result",
    )
    # [END howto_operator_video_intelligence_detect_explicit_content_result]

    # [START howto_operator_video_intelligence_detect_video_shots]
    detect_video_shots = CloudVideoIntelligenceDetectVideoShotsOperator(
        input_uri=INPUT_URI,
        output_uri=None,
        video_context=None,
        retry=Retry(maximum=10.0),
        timeout=5,
        task_id="detect_video_shots",
    )
    # [END howto_operator_video_intelligence_detect_video_shots]

    # [START howto_operator_video_intelligence_detect_video_shots_result]
    detect_video_shots_result = BashOperator(
        bash_command="echo {{ task_instance.xcom_pull('detect_video_shots')"
        "['annotationResults'][0]['shotAnnotations'][0]}}",
        task_id="detect_video_shots_result",
    )
    # [END howto_operator_video_intelligence_detect_video_shots_result]

    delete_bucket = GCSDeleteBucketOperator(
        task_id="delete_bucket", bucket_name=BUCKET_NAME_DST, trigger_rule=TriggerRule.ALL_DONE
    )

    chain(
        # TEST SETUP
        create_bucket,
        copy_single_file,
        # TEST BODY
        detect_video_label,
        detect_video_label_result,
        detect_video_explicit_content,
        detect_video_explicit_content_result,
        detect_video_shots,
        detect_video_shots_result,
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
