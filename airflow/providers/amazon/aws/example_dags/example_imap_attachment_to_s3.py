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

from os import getenv

from airflow import DAG
from airflow.providers.amazon.aws.transfers.imap_attachment_to_s3 import ImapAttachmentToS3Operator
from airflow.utils.dates import days_ago

# [START howto_operator_imap_attachment_to_s3_env_variables]
IMAP_ATTACHMENT_NAME = getenv("IMAP_ATTACHMENT_NAME", "test.txt")
IMAP_MAIL_FOLDER = getenv("IMAP_MAIL_FOLDER", "INBOX")
IMAP_MAIL_FILTER = getenv("IMAP_MAIL_FILTER", "All")
S3_DESTINATION_KEY = getenv("S3_DESTINATION_KEY", "s3://bucket/key.json")
# [END howto_operator_imap_attachment_to_s3_env_variables]

with DAG(
    dag_id="example_imap_attachment_to_s3", start_date=days_ago(1), schedule_interval=None, tags=['example']
) as dag:
    # [START howto_operator_imap_attachment_to_s3_task_1]
    task_transfer_imap_attachment_to_s3 = ImapAttachmentToS3Operator(
        imap_attachment_name=IMAP_ATTACHMENT_NAME,
        s3_key=S3_DESTINATION_KEY,
        imap_mail_folder=IMAP_MAIL_FOLDER,
        imap_mail_filter=IMAP_MAIL_FILTER,
        task_id='transfer_imap_attachment_to_s3',
        dag=dag,
    )
    # [END howto_operator_imap_attachment_to_s3_task_1]
