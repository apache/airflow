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
This is an example dag for using `ImapAttachmentToS3Operator` to transfer an email attachment via IMAP
protocol from a mail server to S3 Bucket.
"""
from __future__ import annotations

from datetime import datetime

from airflow.models.baseoperator import chain
from airflow.models.dag import DAG
from airflow.providers.amazon.aws.operators.s3 import S3CreateBucketOperator, S3DeleteBucketOperator
from airflow.providers.amazon.aws.transfers.imap_attachment_to_s3 import ImapAttachmentToS3Operator
from airflow.utils.trigger_rule import TriggerRule
from tests.system.providers.amazon.aws.utils import SystemTestContextBuilder

DAG_ID = "example_imap_attachment_to_s3"

# Externally fetched variables:
IMAP_ATTACHMENT_NAME_KEY = "IMAP_ATTACHMENT_NAME"
IMAP_MAIL_FOLDER_KEY = "IMAP_MAIL_FOLDER"

sys_test_context_task = (
    SystemTestContextBuilder()
    .add_variable(IMAP_ATTACHMENT_NAME_KEY)
    .add_variable(IMAP_MAIL_FOLDER_KEY)
    .build()
)


with DAG(
    dag_id=DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example"],
) as dag:
    test_context = sys_test_context_task()
    env_id = test_context["ENV_ID"]
    imap_attachment_name = test_context[IMAP_ATTACHMENT_NAME_KEY]
    imap_mail_folder = test_context[IMAP_MAIL_FOLDER_KEY]

    s3_bucket = f"{env_id}-imap-attachment-to-s3-bucket"
    s3_key = f"{env_id}-imap-attachment-to-s3-key"

    create_s3_bucket = S3CreateBucketOperator(task_id="create_s3_bucket", bucket_name=s3_bucket)

    # [START howto_transfer_imap_attachment_to_s3]
    task_transfer_imap_attachment_to_s3 = ImapAttachmentToS3Operator(
        task_id="transfer_imap_attachment_to_s3",
        imap_attachment_name=imap_attachment_name,
        s3_bucket=s3_bucket,
        s3_key=s3_key,
        imap_mail_folder=imap_mail_folder,
        imap_mail_filter="All",
    )
    # [END howto_transfer_imap_attachment_to_s3]

    delete_s3_bucket = S3DeleteBucketOperator(
        task_id="delete_s3_bucket",
        bucket_name=s3_bucket,
        force_delete=True,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    chain(
        # TEST SETUP
        test_context,
        create_s3_bucket,
        # TEST BODY
        task_transfer_imap_attachment_to_s3,
        # TEST TEARDOWN
        delete_s3_bucket,
    )

    from tests.system.utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
