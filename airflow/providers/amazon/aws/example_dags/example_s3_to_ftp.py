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


import os
from datetime import datetime

from airflow import models
from airflow.providers.amazon.aws.transfers.s3_to_ftp import S3ToFTPOperator

S3_BUCKET = os.environ.get("S3_BUCKET", "test-bucket")
S3_KEY = os.environ.get("S3_KEY", "key")

with models.DAG(
    "example_s3_to_ftp",
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
) as dag:
    # [START howto_transfer_s3_to_ftp]
    s3_to_ftp_task = S3ToFTPOperator(
        task_id="ftp_to_s3_task",
        ftp_path="/tmp/ftp_path",
        s3_bucket=S3_BUCKET,
        s3_key=S3_KEY,
    )
    # [END howto_transfer_s3_to_ftp]
