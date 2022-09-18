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

from airflow import DAG
from airflow.providers.amazon.aws.transfers.gcs_to_s3 import GCSToS3Operator

BUCKET = os.getenv("BUCKET", "bucket")
S3_KEY = os.getenv("S3_KEY", "s3://<bucket>/<prefix>")

with DAG(
    dag_id="example_gcs_to_s3",
    start_date=datetime(2021, 1, 1),
    tags=["example"],
    catchup=False,
) as dag:
    # [START howto_transfer_gcs_to_s3]
    gcs_to_s3 = GCSToS3Operator(
        task_id="gcs_to_s3",
        bucket=BUCKET,
        dest_s3_key=S3_KEY,
        replace=True,
    )
    # [END howto_transfer_gcs_to_s3]
