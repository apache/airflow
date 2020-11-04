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
from airflow.providers.microsoft.azure.transfers.local_to_adls import LocalToAzureDataLakeStorageOperator
from airflow.utils.dates import days_ago

LOCAL_FILE_PATH = os.environ.get("LOCAL_FILE_PATH", 'localfile.txt')
REMOTE_FILE_PATH = os.environ.get("REMOTE_LOCAL_PATH", 'remote')


with models.DAG(
    "example_local_to_adls",
    start_date=days_ago(1),
    schedule_interval=None,
    tags=['example'],
) as dag:
    # [START howto_operator_local_to_adls]
    upload_file = LocalToAzureDataLakeStorageOperator(
        task_id='upload_task',
        local_path=LOCAL_FILE_PATH,
        remote_path=REMOTE_FILE_PATH,
    )
    # [END howto_operator_local_to_adls]
