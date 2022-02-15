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

# Ignore missing args provided by default_args
# type: ignore[call-arg]

import os
from datetime import datetime

from airflow.models import DAG
from airflow.providers.microsoft.azure.operators.wasb_delete_blob import WasbDeleteBlobOperator
from airflow.providers.microsoft.azure.transfers.local_to_wasb import LocalFilesystemToWasbOperator

PATH_TO_UPLOAD_FILE = os.environ.get('AZURE_PATH_TO_UPLOAD_FILE', 'example-text.txt')

with DAG(
    "example_local_to_wasb",
    schedule_interval="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    default_args={"container_name": "mycontainer", "blob_name": "myblob"},
) as dag:
    upload = LocalFilesystemToWasbOperator(task_id="upload_file", file_path=PATH_TO_UPLOAD_FILE)
    delete = WasbDeleteBlobOperator(task_id="delete_file")

    upload >> delete
