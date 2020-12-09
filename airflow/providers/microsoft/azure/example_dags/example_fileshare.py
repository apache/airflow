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

from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.azure.hooks.azure_fileshare import AzureFileShareHook
from airflow.utils.dates import days_ago

NAME = 'myfileshare'
DIRECTORY = "mydirectory"


def create_fileshare():
    """Create a fileshare with directory"""
    hook = AzureFileShareHook()
    hook.create_share(NAME)
    hook.create_directory(share_name=NAME, directory_name=DIRECTORY)
    exists = hook.check_for_directory(share_name=NAME, directory_name=DIRECTORY)
    if not exists:
        raise Exception


def delete_fileshare():
    """Delete a fileshare"""
    hook = AzureFileShareHook()
    hook.delete_share(NAME)


with DAG("example_fileshare", schedule_interval="@once", start_date=days_ago(2)) as dag:
    create = PythonOperator(task_id="create-share", python_callable=create_fileshare)
    delete = PythonOperator(task_id="delete-share", python_callable=delete_fileshare)

    create >> delete
