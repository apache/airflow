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
import tempfile
from datetime import datetime
from urllib.request import urlretrieve

from airflow import DAG
from airflow.decorators import task, task_group
from airflow.models.baseoperator import chain
from airflow.providers.amazon.aws.hooks.comprehend import ComprehendHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.comprehend import (
    ComprehendCreateDocumentClassifierOperator,
)
from airflow.providers.amazon.aws.operators.s3 import (
    S3CreateBucketOperator,
    S3CreateObjectOperator,
    S3DeleteBucketOperator,
)
from airflow.providers.amazon.aws.sensors.comprehend import (
    ComprehendCreateDocumentClassifierCompletedSensor,
)
from airflow.utils.trigger_rule import TriggerRule
from tests.system.providers.amazon.aws.utils import SystemTestContextBuilder

ROLE_ARN_KEY = "ROLE_ARN"
sys_test_context_task = SystemTestContextBuilder().add_variable(ROLE_ARN_KEY).build()

DAG_ID = "example_comprehend_document_classifier"
ANNOTATION_BUCKET_KEY = "training-labels/label.csv"
TRAINING_DATA_PREFIX = "training-docs"

# To create a custom document classifier, we need a minimum of 10 documents for each label.
# for testing purpose, we will generate 10 copies of each document referenced below.
PUBLIC_DATA_SOURCES = [
    {
        "fileName": "discharge-summary.pdf",
        "url": "https://github.com/aws-samples/amazon-comprehend-examples/blob/master/building-custom-classifier/sample-docs/discharge-summary.pdf?raw=true",
    },
    {
        "fileName": "doctors-notes.pdf",
        "url": "https://github.com/aws-samples/amazon-comprehend-examples/blob/master/building-custom-classifier/sample-docs/doctors-notes.pdf?raw=true",
    },
]

# Annotations file won't allow headers
# label,document name,page number

ANNOTATIONS = """DISCHARGE_SUMMARY,discharge-summary-0.pdf,1
DISCHARGE_SUMMARY,discharge-summary-1.pdf,1
DISCHARGE_SUMMARY,discharge-summary-2.pdf,1
DISCHARGE_SUMMARY,discharge-summary-3.pdf,1
DISCHARGE_SUMMARY,discharge-summary-4.pdf,1
DISCHARGE_SUMMARY,discharge-summary-5.pdf,1
DISCHARGE_SUMMARY,discharge-summary-6.pdf,1
DISCHARGE_SUMMARY,discharge-summary-7.pdf,1
DISCHARGE_SUMMARY,discharge-summary-8.pdf,1
DISCHARGE_SUMMARY,discharge-summary-9.pdf,1
DOCTOR_NOTES,doctors-notes-0.pdf,1
DOCTOR_NOTES,doctors-notes-1.pdf,1
DOCTOR_NOTES,doctors-notes-2.pdf,1
DOCTOR_NOTES,doctors-notes-3.pdf,1
DOCTOR_NOTES,doctors-notes-4.pdf,1
DOCTOR_NOTES,doctors-notes-5.pdf,1
DOCTOR_NOTES,doctors-notes-6.pdf,1
DOCTOR_NOTES,doctors-notes-7.pdf,1
DOCTOR_NOTES,doctors-notes-8.pdf,1
DOCTOR_NOTES,doctors-notes-9.pdf,1"""


@task_group
def document_classifier_workflow():
    # [START howto_operator_create_document_classifier]
    create_document_classifier = ComprehendCreateDocumentClassifierOperator(
        task_id="create_document_classifier",
        document_classifier_name=classifier_name,
        input_data_config=input_data_configurations,
        output_data_config=output_data_configurations,
        mode="MULTI_CLASS",
        data_access_role_arn=test_context[ROLE_ARN_KEY],
        language_code="en",
        document_classifier_kwargs=document_classifier_kwargs,
    )
    # [END howto_operator_create_document_classifier]
    create_document_classifier.wait_for_completion = False

    # [START howto_sensor_create_document_classifier]
    await_create_document_classifier = ComprehendCreateDocumentClassifierCompletedSensor(
        task_id="await_create_document_classifier", document_classifier_arn=create_document_classifier.output
    )
    # [END howto_sensor_create_document_classifier]

    @task
    def delete_classifier(document_classifier_arn: str):
        ComprehendHook().conn.delete_document_classifier(DocumentClassifierArn=document_classifier_arn)

    chain(
        create_document_classifier,
        await_create_document_classifier,
        delete_classifier(create_document_classifier.output),
    )


@task
def copy_data_to_s3(bucket: str, sources: list[dict], prefix: str, number_of_copies=1):
    """

    Download some sample data and upload it to S3.

    :param bucket: Name of the Amazon S3 bucket to send the data.
    :param prefix: Folder to store the files
    :param number_of_copies: Number of files to create for a document from the sources
    :param sources: Public available data locations
    """
    file_names = [source["fileName"] for source in sources]

    # Monkey patch the list of names available for NamedTempFile so we can pick the names of the downloaded files.
    backup_get_candidate_names = tempfile._get_candidate_names  # type: ignore[attr-defined]
    destinations = iter(file_names)
    tempfile._get_candidate_names = lambda: destinations  # type: ignore[attr-defined]

    """
    Download the sample data files, save them as named temp files using the names above.
    EX: If number_of_copies is 2, named temp file is 'file.pdf', and prefix is 'training-docs'.
    Will generate two copies and upload to s3:
        - training-docs/file-0.pdf
        - training-docs/file-1.pdf
    """

    for source in sources:
        with tempfile.NamedTemporaryFile(mode="w", prefix="") as data_file:
            urlretrieve(source["url"], data_file.name)
            file_name, file_type = os.path.splitext(os.path.basename(data_file.name))

            [
                S3Hook().conn.upload_file(
                    Filename=data_file.name, Bucket=bucket, Key=f"{prefix}/{file_name}-{counter}{file_type}"
                )
                for counter in range(number_of_copies)
            ]

    # Revert the monkey patch.
    tempfile._get_candidate_names = backup_get_candidate_names  # type: ignore[attr-defined]
    # Verify the path reversion worked.
    with tempfile.NamedTemporaryFile(mode="w", prefix=""):
        # If the reversion above did not apply correctly, this will fail with
        # a StopIteration error because the iterator will run out of names.
        ...


with DAG(
    dag_id=DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    tags=["example"],
    catchup=False,
) as dag:
    test_context = sys_test_context_task()
    env_id = test_context["ENV_ID"]
    classifier_name = f"{env_id}-custom-insurance-doc-classifier"
    bucket_name = f"{env_id}-comprehend-document-classifier"

    input_data_configurations = {
        "S3Uri": f"s3://{bucket_name}/{ANNOTATION_BUCKET_KEY}",
        "DataFormat": "COMPREHEND_CSV",
        "DocumentType": "SEMI_STRUCTURED_DOCUMENT",
        "Documents": {"S3Uri": f"s3://{bucket_name}/{TRAINING_DATA_PREFIX}/"},
        "DocumentReaderConfig": {
            "DocumentReadAction": "TEXTRACT_DETECT_DOCUMENT_TEXT",
            "DocumentReadMode": "SERVICE_DEFAULT",
        },
    }
    output_data_configurations = {"S3Uri": f"s3://{bucket_name}/output/"}
    document_classifier_kwargs = {"VersionName": "v1"}

    create_bucket = S3CreateBucketOperator(
        task_id="create_bucket",
        bucket_name=bucket_name,
    )

    upload_annotation_file = S3CreateObjectOperator(
        task_id="upload_annotation_file",
        s3_bucket=bucket_name,
        s3_key=ANNOTATION_BUCKET_KEY,
        data=ANNOTATIONS.encode("utf-8"),
    )

    delete_bucket = S3DeleteBucketOperator(
        task_id="delete_bucket",
        trigger_rule=TriggerRule.ALL_DONE,
        bucket_name=bucket_name,
        force_delete=True,
    )

    chain(
        test_context,
        create_bucket,
        upload_annotation_file,
        copy_data_to_s3(
            bucket=bucket_name, sources=PUBLIC_DATA_SOURCES, prefix=TRAINING_DATA_PREFIX, number_of_copies=10
        ),
        # TEST BODY
        document_classifier_workflow(),
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
