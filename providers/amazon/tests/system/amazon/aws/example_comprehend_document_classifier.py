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

from datetime import datetime

from airflow.providers.amazon.aws.hooks.comprehend import ComprehendHook
from airflow.providers.amazon.aws.operators.comprehend import (
    ComprehendCreateDocumentClassifierOperator,
)
from airflow.providers.amazon.aws.operators.s3 import (
    S3CopyObjectOperator,
    S3CreateBucketOperator,
    S3CreateObjectOperator,
    S3DeleteBucketOperator,
)
from airflow.providers.amazon.aws.sensors.comprehend import (
    ComprehendCreateDocumentClassifierCompletedSensor,
)

from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS

if AIRFLOW_V_3_0_PLUS:
    from airflow.sdk import DAG, chain, task, task_group
else:
    # Airflow 2 path
    from airflow.decorators import task, task_group  # type: ignore[attr-defined,no-redef]
    from airflow.models.baseoperator import chain  # type: ignore[attr-defined,no-redef]
    from airflow.models.dag import DAG  # type: ignore[attr-defined,no-redef,assignment]

try:
    from airflow.sdk import TriggerRule
except ImportError:
    # Compatibility for Airflow < 3.1
    from airflow.utils.trigger_rule import TriggerRule  # type: ignore[no-redef,attr-defined]

from system.amazon.aws.utils import SystemTestContextBuilder

ROLE_ARN_KEY = "ROLE_ARN"
BUCKET_NAME_KEY = "BUCKET_NAME"
BUCKET_KEY_DISCHARGE_KEY = "BUCKET_KEY_DISCHARGE"
BUCKET_KEY_DOCTORS_NOTES = "BUCKET_KEY_DOCTORS_NOTES"
sys_test_context_task = (
    SystemTestContextBuilder()
    .add_variable(ROLE_ARN_KEY)
    .add_variable(BUCKET_NAME_KEY)
    .add_variable(BUCKET_KEY_DISCHARGE_KEY)
    .add_variable(BUCKET_KEY_DOCTORS_NOTES)
    .build()
)

DAG_ID = "example_comprehend_document_classifier"
ANNOTATION_BUCKET_KEY = "training-labels/label.csv"
TRAINING_DATA_PREFIX = "training-docs"

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

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def delete_classifier(document_classifier_arn: str):
        ComprehendHook().conn.delete_document_classifier(DocumentClassifierArn=document_classifier_arn)

    chain(
        create_document_classifier,
        await_create_document_classifier,
        delete_classifier(create_document_classifier.output),
    )


@task
def create_kwargs_discharge():
    return [
        {
            "source_bucket_key": str(test_context[BUCKET_KEY_DISCHARGE_KEY]),
            "dest_bucket_key": f"{TRAINING_DATA_PREFIX}/discharge-summary-{counter}.pdf",
        }
        for counter in range(10)
    ]


@task
def create_kwargs_doctors_notes():
    return [
        {
            "source_bucket_key": str(test_context[BUCKET_KEY_DOCTORS_NOTES]),
            "dest_bucket_key": f"{TRAINING_DATA_PREFIX}/doctors-notes-{counter}.pdf",
        }
        for counter in range(10)
    ]


with DAG(
    dag_id=DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
) as dag:
    test_context = sys_test_context_task()
    env_id = test_context["ENV_ID"]
    classifier_name = f"{env_id}-custom-document-classifier"
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

    discharge_kwargs = create_kwargs_discharge()
    s3_copy_discharge_task = S3CopyObjectOperator.partial(
        task_id="s3_copy_discharge_task",
        source_bucket_name=test_context[BUCKET_NAME_KEY],
        dest_bucket_name=bucket_name,
        meta_data_directive="REPLACE",
    ).expand_kwargs(discharge_kwargs)

    doctors_notes_kwargs = create_kwargs_doctors_notes()
    s3_copy_doctors_notes_task = S3CopyObjectOperator.partial(
        task_id="s3_copy_doctors_notes_task",
        source_bucket_name=test_context[BUCKET_NAME_KEY],
        dest_bucket_name=bucket_name,
        meta_data_directive="REPLACE",
    ).expand_kwargs(doctors_notes_kwargs)

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
        s3_copy_discharge_task,
        s3_copy_doctors_notes_task,
        upload_annotation_file,
        # TEST BODY
        document_classifier_workflow(),
        # TEST TEARDOWN
        delete_bucket,
    )

    from tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
