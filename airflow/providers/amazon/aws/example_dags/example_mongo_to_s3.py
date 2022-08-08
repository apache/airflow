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

from airflow import models
from airflow.providers.amazon.aws.transfers.mongo_to_s3 import MongoToS3Operator
from airflow.utils.dates import datetime

S3_BUCKET = os.environ.get("S3_BUCKET", "test-bucket")
S3_KEY = os.environ.get("S3_KEY", "key")
MONGO_DATABASE = os.environ.get("MONGO_DATABASE", "Test")
MONGO_COLLECTION = os.environ.get("MONGO_COLLECTION", "Test")

with models.DAG(
    "example_mongo_to_s3",
    start_date=datetime(2021, 1, 1),
    catchup=False,
) as dag:
    # [START howto_transfer_mongo_to_s3]
    create_local_to_s3_job = MongoToS3Operator(
        task_id="create_mongo_to_s3_job",
        mongo_collection=MONGO_COLLECTION,
        # Mongo query by matching values
        # Here returns all documents which have "OK" as value for the key "status"
        mongo_query={"status": "OK"},
        s3_bucket=S3_BUCKET,
        s3_key=S3_KEY,
        mongo_db=MONGO_DATABASE,
        replace=True,
    )
    # [END howto_transfer_mongo_to_s3]
